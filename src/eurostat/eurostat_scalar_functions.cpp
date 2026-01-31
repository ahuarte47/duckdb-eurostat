#include "eurostat_scalar_functions.hpp"
#include "function_builder.hpp"

// DuckDB
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/function_entry.hpp"
#include "duckdb/function/scalar_function.hpp"

// EUROSTAT
#include "eurostat.hpp"

namespace duckdb {

namespace {

//======================================================================================================================
// ES_GeoLevel
//======================================================================================================================

struct ES_GeoLevel {

	//! Returns the level for a GEO code in the NUTS classification or if it is considered aggregates.
	inline static void GetGeoLevelFromGeoCode(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 1);

		UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t geo_code) {
			std::string geo_code_str = geo_code.GetString();
			std::string geo_level = eurostat::Dimension::GetGeoLevelFromGeoCode(geo_code_str);
			return string_t(geo_level);
		});
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Returns the level for a GEO code in the NUTS classification or if it is considered aggregates.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT EUROSTAT_GetGeoLevelFromGeoCode('DE');        -- returns 'country'
		SELECT EUROSTAT_GetGeoLevelFromGeoCode('DE1');       -- returns 'nuts1'
		SELECT EUROSTAT_GetGeoLevelFromGeoCode('DE12');      -- returns 'nuts2'
		SELECT EUROSTAT_GetGeoLevelFromGeoCode('DE123');     -- returns 'nuts3'
		SELECT EUROSTAT_GetGeoLevelFromGeoCode('DE_DEL1');   -- returns 'city'
		SELECT EUROSTAT_GetGeoLevelFromGeoCode('EU27_2020'); -- returns 'aggregate'
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "eurostat");
		tags.insert("category", "scalar");

		ScalarFunction func("EUROSTAT_GetGeoLevelFromGeoCode", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
		                    ES_GeoLevel::GetGeoLevelFromGeoCode);

		RegisterFunction<ScalarFunction>(loader, func, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	};
};

} // namespace

// #####################################################################################################################
// Register Metadata/Info Functions
// #####################################################################################################################

void EurostatScalarFunctions::Register(ExtensionLoader &loader) {

	ES_GeoLevel::Register(loader);
}

} // namespace duckdb
