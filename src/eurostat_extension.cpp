#define DUCKDB_EXTENSION_MAIN

#include "eurostat_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// EUROSTAT
#include "eurostat/eurostat_info_functions.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register functions
	EurostatInfoFunctions::Register(loader);
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
