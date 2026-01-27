#pragma once

namespace duckdb {

class ExtensionLoader;

struct EurostatDataFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
