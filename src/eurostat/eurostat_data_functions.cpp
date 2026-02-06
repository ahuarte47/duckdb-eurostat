#include "eurostat_info_functions.hpp"
#include "eurostat_data_functions.hpp"
#include "function_builder.hpp"
#include <iterator>
#include <sstream>
#include <unordered_map>

// DuckDB
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

// EUROSTAT
#include "eurostat.hpp"
#include "filter_encoder.hpp"
#include "http_request.hpp"

// Debug logging controlled by EUROSTAT_DEBUG environment variable
static int GetDebugLevel() {
	static int level = -1;
	if (level == -1) {
		const char *env = std::getenv("EUROSTAT_DEBUG");
		level = env ? std::atoi(env) : 0;
	}
	return level;
}

#define EUROSTAT_SCAN_DEBUG_LOG(level, fmt, ...)                                                                       \
	do {                                                                                                               \
		if (GetDebugLevel() >= level) {                                                                                \
			fprintf(stderr, "EUROSTAT: " fmt "\n", ##__VA_ARGS__);                                                     \
		}                                                                                                              \
	} while (0)

namespace duckdb {

namespace {

//======================================================================================================================
// ES_Read
//======================================================================================================================

struct ES_Read {

	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	struct BindData final : TableFunctionData {
		string provider_id;
		string dataflow_id;
		std::vector<eurostat::Dimension> data_structure;
		std::vector<string> complex_filters;

		explicit BindData(const string &provider_id, const string &dataflow_id,
		                  const std::vector<eurostat::Dimension> &data_structure)
		    : provider_id(provider_id), dataflow_id(dataflow_id), data_structure(std::move(data_structure)) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		D_ASSERT(input.inputs.size() == 2);

		const string &provider_id = StringValue::Get(input.inputs[0]);
		const string &dataflow_id = StringValue::Get(input.inputs[1]);

		// Validate input parameters.

		if (provider_id.empty()) {
			throw InvalidInputException("EUROSTAT: First parameter, the 'provider' identifier, cannot be empty.");
		}

		if (dataflow_id.empty()) {
			throw InvalidInputException("EUROSTAT: Second parameter, the 'dataflow' code, cannot be empty.");
		}

		if (eurostat::ENDPOINTS.find(provider_id) == eurostat::ENDPOINTS.end()) {
			throw InvalidInputException("EUROSTAT: Unknown Endpoint '%s'.", provider_id.c_str());
		}

		// Get dataflow metadata.

		auto data_structure = EurostatUtils::DataStructureOf(context, provider_id, dataflow_id);

		for (const auto &dim : data_structure) {
			names.emplace_back(dim.name);
			return_types.push_back(LogicalType::VARCHAR);
		}
		names.emplace_back("observation_value");
		return_types.push_back(LogicalType::DOUBLE);

		return unique_ptr<FunctionData>(new BindData(provider_id, dataflow_id, data_structure));
	}

	//------------------------------------------------------------------------------------------------------------------
	// Init
	//------------------------------------------------------------------------------------------------------------------

	//! Dimension values structure.
	struct DimensionValues {
		std::vector<string> values;
	};

	//! Data row structure.
	struct Datarow {
		size_t dimension_index;
		string time_period;
		double observation_value = NAN;
	};

	struct State final : GlobalTableFunctionState {
		std::vector<column_t> column_ids;
		std::vector<DimensionValues> dimensions;
		std::vector<Datarow> rows;
		idx_t current_row;

		explicit State() : current_row(0) {
		}
	};

	//! Parse a data row from a TSV line.
	static bool ParseDatarow(State &data_table, const std::vector<string> &time_periods, int32_t geo_column_index,
	                         const string &line, std::unordered_map<string, bool> &row_keys, const bool &check_keys) {

		std::vector<bool> state_keys;

		// Split line by tabs.

		std::istringstream line_stream(line);
		std::string token;
		std::vector<std::string> tokens;

		while (std::getline(line_stream, token, '\t')) {
			tokens.emplace_back(token);
		}
		if (tokens.empty()) {
			return false;
		}

		// Check if the row keys are valid (if enabled).

		if (check_keys) {
			bool all_are_duplicated = true;

			for (const auto &time_period : time_periods) {
				std::string row_key = tokens[0] + "|" + time_period;

				if (row_keys.find(row_key) != row_keys.end()) {
					state_keys.push_back(true);
				} else {
					state_keys.push_back(false);
					row_keys.emplace(row_key, true);
					all_are_duplicated = false;
				}
			}
			if (all_are_duplicated) {
				return true;
			}
		}

		// Parse dimensions from first token (comma-separated).

		DimensionValues dim_values;
		std::istringstream dim_stream(tokens[0]);

		while (std::getline(dim_stream, token, ',')) {
			dim_values.values.emplace_back(token);
		}
		if (geo_column_index != -1) {
			auto geo_level = eurostat::Dimension::GetGeoLevelFromGeoCode(dim_values.values[geo_column_index]);
			dim_values.values.emplace_back(std::move(geo_level));
		}

		data_table.dimensions.emplace_back(std::move(dim_values));

		// Parse observation values for each time period.

		for (size_t i = 0; i < time_periods.size(); i++) {

			// Duplicate row, skip.

			if (check_keys && state_keys[i]) {
				continue;
			}

			string value_str = tokens[i + 1];
			StringUtil::Trim(value_str);

			// Store the row.

			if (!value_str.empty() && value_str != ":") {
				double value = 0.0;

				if (TryCast::Operation(string_t(value_str), value, false)) {
					Datarow datarow;
					datarow.dimension_index = data_table.dimensions.size() - 1;
					datarow.time_period = time_periods[i];
					datarow.observation_value = value;

					data_table.rows.emplace_back(datarow);
				}
			}
		}
		return true;
	}

	//! Generate data URLs based on input filters and data structure.
	static std::vector<string> GetDataUrls(ClientContext &context, TableFunctionInitInput &input,
	                                       const std::vector<eurostat::Dimension> &data_structure,
	                                       const string &base_url, const BindData &bind_data) {

		std::unordered_map<string, bool> urls;

		// Use complex filters previously parsed in 'PushdownComplexFilter' function.

		for (const auto &filter_clause : bind_data.complex_filters) {
			if (!filter_clause.empty()) {
				string url = base_url + filter_clause + "format=TSV&compressed=true";

				if (urls.find(url) == urls.end()) {
					urls.emplace(url, true);
				}
			}
		}
		if (urls.empty()) {
			string url = base_url + "?format=TSV&compressed=true";
			urls.emplace(url, true);
		}

		// Return the list of unique URLs.

		std::vector<std::string> keys;
		keys.reserve(urls.size());

		for (const auto &pair : urls) {
			keys.push_back(pair.first);
		}

		return keys;
	}

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {

		auto &bind_data = input.bind_data->Cast<BindData>();
		auto global_state = make_uniq_base<GlobalTableFunctionState, State>();
		auto &data_table = global_state->Cast<State>();

		std::copy(input.column_ids.begin(), input.column_ids.end(), std::back_inserter(data_table.column_ids));
		const string &provider_id = bind_data.provider_id;
		const string &dataflow_id = bind_data.dataflow_id;

		const auto it = eurostat::ENDPOINTS.find(provider_id);
		string base_url = it->second.api_url + "data/" + dataflow_id;
		int32_t url_count = 0;

		// Fetch data from all generated URLs

		HttpSettings settings;

		const auto &data_urls = GetDataUrls(context, input, bind_data.data_structure, base_url, bind_data);
		std::unordered_map<string, bool> row_keys;
		bool check_keys = data_urls.size() > 1;

		for (const auto &data_url : data_urls) {

			EUROSTAT_SCAN_DEBUG_LOG(1, "Fetching data from URL: %s", data_url.c_str());

			// Execute HTTP GET request.

			if (url_count == 0) {
				settings = HttpRequest::ExtractHttpSettings(context, data_url);
				settings.timeout = 90;
			}
			url_count++;

			auto response =
			    HttpRequest::ExecuteHttpRequest(settings, data_url, "GET", duckdb_httplib_openssl::Headers(), "", "");

			if (response.content_type == "application/xml") {
				std::string error_msg = EurostatUtils::GetXmlErrorMessage(response.body);

				EUROSTAT_SCAN_DEBUG_LOG(1, "Failed to fetch a dataset from provider='%s', dataflow='%s': %s",
				                        provider_id.c_str(), dataflow_id.c_str(), error_msg.c_str());

				data_table.rows.clear();
				return global_state;
			}
			if (response.status_code != 200) {
				throw IOException("EUROSTAT: Failed to fetch a dataset from provider='%s', dataflow='%s': (%d) %s",
				                  provider_id.c_str(), dataflow_id.c_str(), response.status_code,
				                  response.error.c_str());
			}
			if (!response.error.empty()) {
				throw IOException("EUROSTAT: " + response.error);
			}

			// Parse TSV response (Header + Rows).

			std::istringstream line_stream(response.body);
			std::string line, token;
			int64_t line_index = 0;

			std::vector<string> time_periods;
			int32_t geo_column_index = -1;

			while (std::getline(line_stream, line)) {
				if (!line.empty()) {

					// Parse header line to...
					if (line_index == 0) {
						size_t pos = line.find("\\TIME_PERIOD");

						if (pos == string::npos) {
							throw IOException("EUROSTAT: TIME_PERIOD not found in TSV header.");
						}

						// Extract dimension column names (before TIME_PERIOD).

						std::istringstream stream_1(line.substr(0, pos));
						int32_t token_index = 0;

						while (std::getline(stream_1, token, ',')) {
							token = StringUtil::Lower(token);

							// Add GEO_LEVEL virtual dimension.
							if (token == "geo") {
								geo_column_index = token_index;
								break;
							}
							token_index++;
						}

						// Extract time periods (after TIME_PERIOD).

						std::istringstream stream_2(line.substr(pos + strlen("\\TIME_PERIOD") + 1));

						while (std::getline(stream_2, token, '\t')) {
							StringUtil::Trim(token);

							if (!token.empty()) {
								time_periods.push_back(token);
							}
						}

					} else {
						// Add data row.
						ParseDatarow(data_table, time_periods, geo_column_index, line, row_keys, check_keys);
					}
					line_index++;
				}
			}
		}

		EUROSTAT_SCAN_DEBUG_LOG(1, "Finished fetching data. Total URLs: %d", url_count);
		EUROSTAT_SCAN_DEBUG_LOG(1, "Total rows: %zu", data_table.rows.size());

		return global_state;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {

		auto &gstate = input.global_state->Cast<State>();

		// Calculate how many record we can fit in the output
		const auto output_size = std::min<idx_t>(STANDARD_VECTOR_SIZE, gstate.rows.size() - gstate.current_row);
		const auto current_row = gstate.current_row;

		if (output_size == 0) {
			output.SetCardinality(0);
			return;
		}

		// Load current subset of rows.
		for (idx_t row_idx = 0, record_idx = current_row; row_idx < output_size; row_idx++, record_idx++) {

			const auto &datarow = gstate.rows[record_idx];
			const DimensionValues &dim_values = gstate.dimensions[datarow.dimension_index];
			const auto &dim_count = dim_values.values.size();

			for (idx_t col_idx = 0; col_idx < gstate.column_ids.size(); col_idx++) {
				const auto &dim_index = gstate.column_ids[col_idx];

				if (dim_index == dim_count) {
					// Set time period.
					output.data[col_idx].SetValue(row_idx, Value(datarow.time_period));
				} else if (dim_index == dim_count + 1) {
					// Set observation value.
					output.data[col_idx].SetValue(row_idx, Value(datarow.observation_value));
				} else {
					// Set dimension value.
					output.data[col_idx].SetValue(row_idx, Value(dim_values.values[dim_index]));
				}
			}
		}

		// Update the row index.
		gstate.current_row += output_size;

		// Set the cardinality of the output.
		output.SetCardinality(output_size);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Reads the dataset of an EUROSTAT Dataflow.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT * FROM EUROSTAT_Read('ESTAT', 'DEMO_R_D2JAN') LIMIT 5;

		┌─────────┬─────────┬─────────┬─────────┬─────────┬───────────┬─────────────┬───────────────────┐
		│  freq   │  unit   │   sex   │   age   │   geo   │ geo_level │ TIME_PERIOD │ observation_value │
		│ varchar │ varchar │ varchar │ varchar │ varchar │  varchar  │   varchar   │      double       │
		├─────────┼─────────┼─────────┼─────────┼─────────┼───────────┼─────────────┼───────────────────┤
		│ A       │ NR      │ F       │ TOTAL   │ AL      │ country   │ 2000        │         1526762.0 │
		│ A       │ NR      │ F       │ TOTAL   │ AL      │ country   │ 2001        │         1535822.0 │
		│ A       │ NR      │ F       │ TOTAL   │ AL      │ country   │ 2002        │         1532563.0 │
		│ A       │ NR      │ F       │ TOTAL   │ AL      │ country   │ 2003        │         1526180.0 │
		│ A       │ NR      │ F       │ TOTAL   │ AL      │ country   │ 2004        │         1520481.0 │
		└─────────┴─────────┴─────────┴─────────┴─────────┴───────────┴─────────────┴───────────────────┘
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Complex Filter Pushdown
	//------------------------------------------------------------------------------------------------------------------

	static void PushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
	                                  vector<unique_ptr<Expression>> &expressions) {

		auto &bind_data = bind_data_p->Cast<BindData>();

		// Get column_ids from LogicalGet to map expression column indices to table columns.

		const auto &get_column_ids = get.GetColumnIds();

		std::vector<column_t> column_ids;
		for (const auto &col_idx : get_column_ids) {
			column_ids.push_back(col_idx.IsVirtualColumn() ? COLUMN_IDENTIFIER_ROW_ID : col_idx.GetPrimaryIndex());
		}

		// Encoded filters to be generated from the input expressions.

		auto result = FilterEncoder::EncodeExpression(expressions, bind_data.data_structure, column_ids);
		std::vector<std::string> filters;

		if (result.supported) {
			for (const auto &filter_clause : result.filters) {
				if (!filter_clause.empty()) {
					filters.emplace_back(filter_clause);
				}
			}
		}

		// Store encoded filters in bind data.
		bind_data.complex_filters = std::move(filters);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "eurostat");
		tags.insert("category", "table");

		TableFunction func("EUROSTAT_Read", {LogicalType::VARCHAR, LogicalType::VARCHAR}, Execute, Bind, Init);

		// Enable projection pushdown - allows DuckDB to tell us which columns are needed
		// The column_ids will be passed to InitGlobal via TableFunctionInitInput
		func.projection_pushdown = true;

		// Enable complex filter pushdown - handles expressions like (A AND B) OR (C AND D)
		// that cannot be represented as simple TableFilter objects
		func.pushdown_complex_filter = PushdownComplexFilter;

		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

} // namespace

// #####################################################################################################################
// Register Data Functions
// #####################################################################################################################

void EurostatDataFunctions::Register(ExtensionLoader &loader) {

	ES_Read::Register(loader);
}

} // namespace duckdb
