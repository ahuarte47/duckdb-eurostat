#include "eurostat_data_functions.hpp"
#include "function_builder.hpp"
#include <iterator>
#include <sstream>

// DuckDB
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

// EUROSTAT
#include "eurostat.hpp"
#include "http_request.hpp"

namespace duckdb {

namespace {

//======================================================================================================================
// ES_Read
//======================================================================================================================

struct ES_Read {

	//! Dimension values structure
	struct DimensionValues {
		std::vector<string> values;
	};

	//! Data row structure
	struct Datarow {
		size_t dimension_index;
		string time_period;
		double observation_value = NAN;
	};

	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	struct BindData final : TableFunctionData {
		std::vector<DimensionValues> dimensions;
		std::vector<Datarow> rows;
		std::vector<string> time_periods;

		explicit BindData() {
		}
	};

	//! Parse a data row from a TSV line
	static bool ParseDatarow(const string &line, BindData &bind_data) {

		// Split line by tabs
		std::istringstream line_stream(line);
		std::string token;
		std::vector<string> tokens;

		while (std::getline(line_stream, token, '\t')) {
			tokens.push_back(token);
		}
		if (tokens.empty()) {
			return false;
		}

		// Parse dimensions from first token (comma-separated)

		DimensionValues dim_values;
		std::istringstream dim_stream(tokens[0]);

		while (std::getline(dim_stream, token, ',')) {
			dim_values.values.push_back(token);
		}

		bind_data.dimensions.emplace_back(dim_values);

		// Parse observation values for each time period

		for (size_t i = 0; i < bind_data.time_periods.size(); i++) {
			string value_str = tokens[i + 1];
			StringUtil::Trim(value_str);

			if (!value_str.empty() && value_str != ":") {
				double value = 0.0;

				if (TryCast::Operation(string_t(value_str), value, false)) {
					Datarow datarow;
					datarow.dimension_index = bind_data.dimensions.size() - 1;
					datarow.time_period = bind_data.time_periods[i];
					datarow.observation_value = value;

					bind_data.rows.push_back(datarow);
				}
			}
		}
		return true;
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		D_ASSERT(input.inputs.size() == 2);

		const string provider_id = StringValue::Get(input.inputs[0]);
		const string dataflow_id = StringValue::Get(input.inputs[1]);

		// Validate input parameters

		if (provider_id.empty()) {
			throw InvalidInputException("EUROSTAT_Read: first parameter, the 'provider' identifier, cannot be empty.");
		}

		if (dataflow_id.empty()) {
			throw InvalidInputException("EUROSTAT_Read: second parameter, the 'dataflow' code, cannot be empty.");
		}

		if (eurostat::ENDPOINTS.find(provider_id) == eurostat::ENDPOINTS.end()) {
			throw InvalidInputException("Unknown EUROSTAT Endpoint '%s'.", provider_id.c_str());
		}

		// Execute HTTP GET request

		const auto it = eurostat::ENDPOINTS.find(provider_id);
		string url = it->second.api_url + "data/" + dataflow_id + "?format=TSV&compressed=true";

		HttpSettings settings = HttpRequest::ExtractHttpSettings(context, url);
		auto response =
		    HttpRequest::ExecuteHttpRequest(settings, url, "GET", duckdb_httplib_openssl::Headers(), "", "");

		if (response.status_code != 200) {
			throw IOException("Failed to fetch EUROSTAT dataflow dataset from provider='%s', dataflow='%s': (%d) %s",
			                  provider_id.c_str(), dataflow_id.c_str(), response.status_code, response.error.c_str());
		}
		if (!response.error.empty()) {
			throw IOException(response.error);
		}

		// Parse TSV response (Header + Rows)

		auto bind_data = make_uniq_base<FunctionData, BindData>();
		auto &the_data = bind_data->Cast<BindData>();

		std::istringstream line_stream(response.body);
		std::string line, token;
		int64_t line_index = 0;

		while (std::getline(line_stream, line)) {
			if (!line.empty()) {
				if (line_index == 0) {
					size_t pos = line.find("\\TIME_PERIOD");

					if (pos == string::npos) {
						throw IOException("TIME_PERIOD not found in TSV header");
					}

					// Extract dimension column names (before TIME_PERIOD)

					std::istringstream stream_1(line.substr(0, pos));

					while (std::getline(stream_1, token, ',')) {
						token = StringUtil::Lower(token);
						names.emplace_back(token);
						return_types.push_back(LogicalType::VARCHAR);
					}

					names.emplace_back("time_period");
					return_types.push_back(LogicalType::VARCHAR);

					// Extract time periods (after TIME_PERIOD)

					std::istringstream stream_2(line.substr(pos + strlen("\\TIME_PERIOD") + 1));

					while (std::getline(stream_2, token, '\t')) {
						StringUtil::Trim(token);

						if (!token.empty()) {
							the_data.time_periods.push_back(token);
						}
					}

					names.emplace_back("observation_value");
					return_types.push_back(LogicalType::DOUBLE);

				} else {
					// Add data row
					ParseDatarow(line, the_data);
				}
				line_index++;
			}
		}

		return bind_data;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Init
	//------------------------------------------------------------------------------------------------------------------

	struct State final : GlobalTableFunctionState {
		idx_t current_row;
		explicit State() : current_row(0) {
		}
	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq_base<GlobalTableFunctionState, State>();
	}

	//------------------------------------------------------------------------------------------------------------------
	// Cardinality
	//------------------------------------------------------------------------------------------------------------------

	static unique_ptr<NodeStatistics> Cardinality(ClientContext &context, const FunctionData *data) {

		auto &bind_data = data->Cast<BindData>();
		auto result = make_uniq<NodeStatistics>();

		// This is the maximum number of points in a single file
		result->has_max_cardinality = true;
		result->max_cardinality = bind_data.rows.size();

		return result;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {

		auto &bind_data = input.bind_data->Cast<BindData>();
		auto &gstate = input.global_state->Cast<State>();

		// Calculate how many record we can fit in the output
		const auto output_size = std::min<idx_t>(STANDARD_VECTOR_SIZE, bind_data.rows.size() - gstate.current_row);
		const auto current_row = gstate.current_row;

		if (output_size == 0) {
			output.SetCardinality(0);
			return;
		}

		// Load current subset of rows.
		for (idx_t row_idx = 0, record_idx = current_row; row_idx < output_size; row_idx++, record_idx++) {
			const auto &datarow = bind_data.rows[record_idx];
			const DimensionValues &dim_values = bind_data.dimensions[datarow.dimension_index];
			idx_t col_idx = 0;

			// Set dimension values
			for (const auto &dim_value : dim_values.values) {
				output.data[col_idx].SetValue(row_idx, Value(dim_value));
				col_idx++;
			}

			// Set time period
			output.data[col_idx].SetValue(row_idx, Value(datarow.time_period));
			col_idx++;

			// Set observation value
			output.data[col_idx].SetValue(row_idx, Value(datarow.observation_value));
		}

		// Update the point index
		gstate.current_row += output_size;

		// Set the cardinality of the output
		output.SetCardinality(output_size);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Reads the dataset of an EUROSTAT Dataflow.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT * FROM EUROSTAT_Read('ESTAT', 'demo_r_d2jan') LIMIT 5;

		┌─────────┬─────────┬─────────┬─────────┬─────────┬─────────────┬───────────────────┐
		│  freq   │  unit   │   sex   │   age   │   geo   │ time_period │ observation_value │
		│ varchar │ varchar │ varchar │ varchar │ varchar │   varchar   │      double       │
		├─────────┼─────────┼─────────┼─────────┼─────────┼─────────────┼───────────────────┤
		│ A       │ NR      │ F       │ TOTAL   │ AL      │ 2000        │         1526762.0 │
		│ A       │ NR      │ F       │ TOTAL   │ AL      │ 2001        │         1535822.0 │
		│ A       │ NR      │ F       │ TOTAL   │ AL      │ 2002        │         1532563.0 │
		│ A       │ NR      │ F       │ TOTAL   │ AL      │ 2003        │         1526180.0 │
		│ A       │ NR      │ F       │ TOTAL   │ AL      │ 2004        │         1520481.0 │
		└─────────┴─────────┴─────────┴─────────┴─────────┴─────────────┴───────────────────┘
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "eurostat");
		tags.insert("category", "table");

		TableFunction func("EUROSTAT_Read", {LogicalType::VARCHAR, LogicalType::VARCHAR}, Execute, Bind, Init);
		func.cardinality = Cardinality;

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
