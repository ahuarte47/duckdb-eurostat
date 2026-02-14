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

	switch (geo_code.length()) {
	case 2:
		return (COUNTRY_CODES.find(geo_code) != COUNTRY_CODES.end()) ? "country" : "unknown";
	case 3:
		return (COUNTRY_CODES.find(geo_code.substr(0, 2)) != COUNTRY_CODES.end()) ? "nuts1" : "unknown";
	case 4:
		return (COUNTRY_CODES.find(geo_code.substr(0, 2)) != COUNTRY_CODES.end()) ? "nuts2" : "unknown";
	case 5:
		return (COUNTRY_CODES.find(geo_code.substr(0, 2)) != COUNTRY_CODES.end()) ? "nuts3" : "unknown";
	case 7:
		return (COUNTRY_CODES.find(geo_code.substr(0, 2)) != COUNTRY_CODES.end() && geo_code[2] == '_') ? "city"
		                                                                                                : "unknown";
	default:
		return "unknown";
	}
};

} // namespace eurostat
