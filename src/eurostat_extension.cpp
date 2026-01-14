#define DUCKDB_EXTENSION_MAIN

#include "eurostat_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

inline void EurostatScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Eurostat " + name.GetString() + " 🐥");
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto eurostat_scalar_function =
	    ScalarFunction("eurostat", {LogicalType::VARCHAR}, LogicalType::VARCHAR, EurostatScalarFun);
	loader.RegisterFunction(eurostat_scalar_function);
}

void EurostatExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string EurostatExtension::Name() {
	return "eurostat";
}

std::string EurostatExtension::Version() const {
#ifdef EXT_VERSION_EUROSTAT
	return EXT_VERSION_EUROSTAT;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(eurostat, loader) {
	duckdb::LoadInternal(loader);
}
}
