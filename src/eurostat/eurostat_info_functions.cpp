#include "eurostat_info_functions.hpp"
#include "function_builder.hpp"
#include <iterator>

// DuckDB
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table_function.hpp"
#include "yyjson.hpp"
using namespace duckdb_yyjson; // NOLINT

// EUROSTAT
#include "eurostat.hpp"
#include "http_request.hpp"

namespace duckdb {

namespace {

//======================================================================================================================
// ES_Endpoints
//======================================================================================================================

struct ES_Endpoints {

	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	struct BindData final : TableFunctionData {
		idx_t endpoint_count;
		explicit BindData(const idx_t count_p) : endpoint_count(count_p) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		names.emplace_back("provider_id");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("organization");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("description");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("api_url");
		return_types.push_back(LogicalType::VARCHAR);

		return make_uniq_base<FunctionData, BindData>(eurostat::ENDPOINTS.size());
	}

	//------------------------------------------------------------------------------------------------------------------
	// Init
	//------------------------------------------------------------------------------------------------------------------

	struct State final : GlobalTableFunctionState {
		idx_t current_idx;
		explicit State() : current_idx(0) {
		}
	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq_base<GlobalTableFunctionState, State>();
	}

	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		auto &state = input.global_state->Cast<State>();

		idx_t count = 0;
		auto next_idx = MinValue<idx_t>(state.current_idx + STANDARD_VECTOR_SIZE, bind_data.endpoint_count);

		for (; state.current_idx < next_idx; state.current_idx++) {
			auto it = std::next(eurostat::ENDPOINTS.begin(), state.current_idx);
			auto &provider_id = it->first;
			auto &endpoint = it->second;

			output.data[0].SetValue(count, provider_id);
			output.data[1].SetValue(count, endpoint.organization);
			output.data[2].SetValue(count, endpoint.description);
			output.data[3].SetValue(count, endpoint.api_url);
			count++;
		}
		output.SetCardinality(count);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Returns the list of supported EUROSTAT API Endpoints.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT provider_id, organization, description FROM EUROSTAT_Endpoints();

		┌─────────────┬──────────────┬──────────────────────────────────────────────────────┐
		│ provider_id │ organization │                     description                      │
		│   varchar   │   varchar    │                       varchar                        │
		├─────────────┼──────────────┼──────────────────────────────────────────────────────┤
		│ ECFIN       │ DG ECFIN     │ Economic and Financial Affairs                       │
		│ EMPL        │ DG EMPL      │ Employment, Social Affairs and Inclusion             │
		│ ESTAT       │ EUROSTAT     │ EUROSTAT database                                    │
		│ GROW        │ DG GROW      │ Internal Market, Industry, Entrepreneurship and SMEs │
		│ TAXUD       │ DG TAXUD     │ Taxation and Customs Union                           │
		└─────────────┴──────────────┴──────────────────────────────────────────────────────┘
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "eurostat");
		tags.insert("category", "table");

		const TableFunction func("EUROSTAT_Endpoints", {}, Execute, Bind, Init);
		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

} // namespace

// #####################################################################################################################
// Register Metadata/Info Functions
// #####################################################################################################################

void EurostatInfoFunctions::Register(ExtensionLoader &loader) {

	ES_Endpoints::Register(loader);
}

} // namespace duckdb
