#pragma once

#include <string>
#include <vector>
#include "eurostat.hpp"

namespace duckdb {

class ClientContext;
class ExtensionLoader;

struct EurostatUtils {
public:
	//! Returns the data structure (dimensions) of a given dataflow
	static std::vector<eurostat::Dimension> DataStructureOf(ClientContext &context, const std::string &provider_id,
	                                                        const std::string &dataflow_id);
};

struct EurostatInfoFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
