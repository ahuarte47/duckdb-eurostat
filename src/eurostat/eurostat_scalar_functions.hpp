#pragma once

namespace duckdb {

class ExtensionLoader;

struct EurostatScalarFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
