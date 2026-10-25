#include "eurostat.hpp"
#include <string.h>

namespace eurostat {

//! Get the level for a GEO code in the NUTS classification or if it is considered aggregates.
std::string Dimension::GetGeoLevelFromGeoCode(const std::string &geo_code) {
#define STARTS_WITH(str, prefix) ((strlen(prefix) <= str.size()) && strncmp(str.c_str(), prefix, strlen(prefix)) == 0)

	//
	// https://ec.europa.eu/eurostat/statistics-explained/index.php?title=Glossary:Country_codes
	//
	if (STARTS_WITH(geo_code, "EU") || STARTS_WITH(geo_code, "EA") || STARTS_WITH(geo_code, "EFTA")) {
		return "aggregate";
	}

	const auto &country_codes = CountryCode::GetCountryCodes();

	switch (geo_code.length()) {
	case 2:
		return (country_codes.find(geo_code) != country_codes.end()) ? "country" : "unknown";
	case 3:
		return (country_codes.find(geo_code.substr(0, 2)) != country_codes.end()) ? "nuts1" : "unknown";
	case 4:
		return (country_codes.find(geo_code.substr(0, 2)) != country_codes.end()) ? "nuts2" : "unknown";
	case 5:
		return (country_codes.find(geo_code.substr(0, 2)) != country_codes.end()) ? "nuts3" : "unknown";
	case 7:
		return (country_codes.find(geo_code.substr(0, 2)) != country_codes.end() && geo_code[2] == '_') ? "city"
		                                                                                                : "unknown";
	default:
		return "unknown";
	}
};

} // namespace eurostat
