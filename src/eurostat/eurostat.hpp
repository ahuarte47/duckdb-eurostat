#pragma once

#include <string>
#include <unordered_map>

namespace eurostat {

//! API Endpoint of an EUROSTAT data provider
struct Endpoint {
	std::string organization; // (aka agency)
	std::string description;
	std::string api_url;
};

//! API Endpoints
static const std::map<std::string, Endpoint> ENDPOINTS = {
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

} // namespace eurostat
