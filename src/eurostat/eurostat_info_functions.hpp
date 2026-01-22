#pragma once

namespace duckdb {

class ExtensionLoader;

struct EurostatInfoFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
