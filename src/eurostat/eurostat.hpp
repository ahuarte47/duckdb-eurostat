#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>

namespace eurostat {

//! API Endpoint of an EUROSTAT data provider
struct Endpoint {
	std::string organization; // (aka agency)
	std::string description;
	std::string api_url;
};

//! API Endpoints
static const std::unordered_map<std::string, Endpoint> ENDPOINTS = {
    {"ESTAT", {"EUROSTAT", "EUROSTAT database", "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/"}},
    {"ECFIN",
     {"DG ECFIN", "Economic and Financial Affairs",
      "https://webgate.ec.europa.eu/ecfin/redisstat/api/dissemination/sdmx/2.1/"}},
    {"EMPL",
     {"DG EMPL", "Employment, Social Affairs and Inclusion",
      "https://webgate.ec.europa.eu/empl/redisstat/api/dissemination/sdmx/2.1/"}},
    {"GROW",
     {"DG GROW", "Internal Market, Industry, Entrepreneurship and SMEs",
      "https://webgate.ec.europa.eu/grow/redisstat/api/dissemination/sdmx/2.1/"}},
    {"TAXUD",
     {"DG TAXUD", "Taxation and Customs Union",
      "https://webgate.ec.europa.eu/taxation_customs/redisstat/api/dissemination/sdmx/2.1/"}},
};

/**
 * Country code mappings for EUROSTAT datasets
 * References:
 * https://ec.europa.eu/eurostat/statistics-explained/index.php?title=Glossary:Country_codes
 */
static const std::unordered_map<std::string, std::string> COUNTRY_CODES = {
    // European Union (EU)
    {"BE", "Belgium"},
    {"BG", "Bulgaria"},
    {"CZ", "Czechia"},
    {"DK", "Denmark"},
    {"DE", "Germany"},
    {"EE", "Estonia"},
    {"IE", "Ireland"},
    {"EL", "Greece"},
    {"ES", "Spain"},
    {"FR", "France"},
    {"HR", "Croatia"},
    {"IT", "Italy"},
    {"CY", "Cyprus"},
    {"LV", "Latvia"},
    {"LT", "Lithuania"},
    {"LU", "Luxembourg"},
    {"HU", "Hungary"},
    {"MT", "Malta"},
    {"NL", "Netherlands"},
    {"AT", "Austria"},
    {"PL", "Poland"},
    {"PT", "Portugal"},
    {"RO", "Romania"},
    {"SI", "Slovenia"},
    {"SK", "Slovakia"},
    {"FI", "Finland"},
    {"SE", "Sweden"},
    // European Free Trade Association (EFTA)
    {"IS", "Iceland"},
    {"LI", "Liechtenstein"},
    {"NO", "Norway"},
    {"CH", "Switzerland"},
    // EU candidate countries
    {"BA", "Bosnia and Herzegovina"},
    {"ME", "Montenegro"},
    {"MD", "Moldova"},
    {"MK", "North Macedonia"},
    {"GE", "Georgia"},
    {"AL", "Albania"},
    {"RS", "Serbia"},
    {"TR", "Türkiye"},
    {"UA", "Ukraine"},
    // Potential candidates
    {"XK", "Kosovo"},
    // European Neighbourhood Policy (ENP)-East countries
    {"AM", "Armenia"},
    {"BY", "Belarus"},
    {"AZ", "Azerbaijan"},
    // European Neighbourhood Policy (ENP)-South countries
    {"DZ", "Algeria"},
    {"EG", "Egypt"},
    {"IL", "Israel"},
    {"JO", "Jordan"},
    {"LB", "Lebanon"},
    {"LY", "Libya"},
    {"MA", "Morocco"},
    {"PS", "Palestine"},
    {"SY", "Syria"},
    {"TN", "Tunisia"},
    // Other countries
    {"AR", "Argentina"},
    {"AU", "Australia"},
    {"BR", "Brazil"},
    {"CA", "Canada"},
    {"CN_X_HK", "China (except Hong Kong)"},
    {"HK", "Hong Kong"},
    {"IN", "India"},
    {"JP", "Japan"},
    {"MX", "Mexico"},
    {"NG", "Nigeria"},
    {"NZ", "New Zealand"},
    {"RU", "Russia"},
    {"SG", "Singapore"},
    {"ZA", "South Africa"},
    {"KR", "South Korea"},
    {"TW", "Taiwan"},
    {"UK", "United Kingdom"},
    {"US", "United States"},
};

//! Dimension of an EUROSTAT Dataflow
struct Dimension {
	int32_t position = -1;
	std::string name;
	std::string concept_label;

	Dimension() = default;
	Dimension(int32_t position, const std::string &name, const std::string &concept_label)
	    : position(position), name(name), concept_label(concept_label) {
	}

	//! Get the level for a GEO code in the NUTS classification or if it is considered aggregates.
	static std::string GetGeoLevelFromGeoCode(const std::string &geo_code);
};

} // namespace eurostat
