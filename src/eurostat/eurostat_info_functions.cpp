#include "eurostat_info_functions.hpp"
#include "function_builder.hpp"
#include <iterator>

// DuckDB
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table_function.hpp"
#include "yyjson.hpp"
using namespace duckdb_yyjson; // NOLINT

// LibXml2
#include "libxml/parser.h"
#include "libxml/xpath.h"

// EUROSTAT
#include "eurostat.hpp"
#include "http_request.hpp"
#include "xml_element.hpp"

namespace duckdb {

namespace {

//======================================================================================================================
// ES_Endpoints
//======================================================================================================================

struct ES_Endpoints {

	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	struct BindData final : TableFunctionData {
		idx_t endpoint_count;
		explicit BindData(const idx_t count_p) : endpoint_count(count_p) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		names.emplace_back("provider_id");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("organization");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("description");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("api_url");
		return_types.push_back(LogicalType::VARCHAR);

		return make_uniq_base<FunctionData, BindData>(eurostat::ENDPOINTS.size());
	}

	//------------------------------------------------------------------------------------------------------------------
	// Init
	//------------------------------------------------------------------------------------------------------------------

	struct State final : GlobalTableFunctionState {
		idx_t current_idx;
		explicit State() : current_idx(0) {
		}
	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq_base<GlobalTableFunctionState, State>();
	}

	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		auto &state = input.global_state->Cast<State>();

		idx_t count = 0;
		auto next_idx = MinValue<idx_t>(state.current_idx + STANDARD_VECTOR_SIZE, bind_data.endpoint_count);

		for (; state.current_idx < next_idx; state.current_idx++) {
			auto it = std::next(eurostat::ENDPOINTS.begin(), state.current_idx);
			auto &provider_id = it->first;
			auto &endpoint = it->second;

			output.data[0].SetValue(count, provider_id);
			output.data[1].SetValue(count, endpoint.organization);
			output.data[2].SetValue(count, endpoint.description);
			output.data[3].SetValue(count, endpoint.api_url);
			count++;
		}
		output.SetCardinality(count);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Returns the list of supported EUROSTAT API Endpoints.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT provider_id, organization, description FROM EUROSTAT_Endpoints();

		┌─────────────┬──────────────┬──────────────────────────────────────────────────────┐
		│ provider_id │ organization │                     description                      │
		│   varchar   │   varchar    │                       varchar                        │
		├─────────────┼──────────────┼──────────────────────────────────────────────────────┤
		│ ECFIN       │ DG ECFIN     │ Economic and Financial Affairs                       │
		│ EMPL        │ DG EMPL      │ Employment, Social Affairs and Inclusion             │
		│ ESTAT       │ EUROSTAT     │ EUROSTAT database                                    │
		│ GROW        │ DG GROW      │ Internal Market, Industry, Entrepreneurship and SMEs │
		│ TAXUD       │ DG TAXUD     │ Taxation and Customs Union                           │
		└─────────────┴──────────────┴──────────────────────────────────────────────────────┘
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "eurostat");
		tags.insert("category", "table");

		const TableFunction func("EUROSTAT_Endpoints", {}, Execute, Bind, Init);
		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

//======================================================================================================================
// ES_Dataflows
//======================================================================================================================

struct ES_Dataflows {

	//! Metadata of an EUROSTAT Dataflow
	struct DataflowInfo {
		string provider_id;
		string dataflow_id;
		string type;
		string version;
		string label;
		string language;

		int64_t number_of_values = -1;
		string data_start;
		string data_end;
		string update_data;
		string update_structure;

		string data_structure;
		string annotations;
	};

	//! Parse DataflowInfo from JSON object
	inline static DataflowInfo ParseDataflowInfo(const string &provider_id, yyjson_val *object_val) {

		yyjson_val *extension_val = nullptr;
		yyjson_val *annotation_val = nullptr;
		yyjson_val *attrib_val = nullptr;

		if (!yyjson_is_obj(extension_val = yyjson_obj_get(object_val, "extension"))) {
			throw InvalidInputException("EUROSTAT: Missing or incorrect 'extension' attribute in dataflow metadata.");
		}
		if (!yyjson_is_arr(annotation_val = yyjson_obj_get(extension_val, "annotation"))) {
			throw InvalidInputException(
			    "EUROSTAT: Missing or incorrect 'extension/annotation' attribute in dataflow metadata.");
		}

		// Extract main attributes

		DataflowInfo info;
		info.provider_id = provider_id;

		if (yyjson_is_str(attrib_val = yyjson_obj_get(extension_val, "id"))) {
			info.dataflow_id = string(yyjson_get_str(attrib_val));
		}
		if (yyjson_is_str(attrib_val = yyjson_obj_get(object_val, "class"))) {
			info.type = string(yyjson_get_str(attrib_val));
		}
		if (yyjson_is_str(attrib_val = yyjson_obj_get(extension_val, "version"))) {
			info.version = string(yyjson_get_str(attrib_val));
		}
		if (yyjson_is_str(attrib_val = yyjson_obj_get(object_val, "label"))) {
			info.label = string(yyjson_get_str(attrib_val));
		}
		if (yyjson_is_str(attrib_val = yyjson_obj_get(extension_val, "lang"))) {
			info.language = string(yyjson_get_str(attrib_val));
		}

		if (yyjson_is_obj(attrib_val = yyjson_obj_get(extension_val, "datastructure"))) {
			const char *json = yyjson_val_write(attrib_val, YYJSON_WRITE_NOFLAG, nullptr);
			if (json) {
				info.data_structure = string(json);
				free((void *)json);
			}
		}
		if (yyjson_is_arr(attrib_val = yyjson_obj_get(extension_val, "annotation"))) {
			const char *json = yyjson_val_write(attrib_val, YYJSON_WRITE_NOFLAG, nullptr);
			if (json) {
				info.annotations = string(json);
				free((void *)json);
			}
		}

		// Extract attributes from annotations

		auto annotation_len = yyjson_arr_size(annotation_val);

		for (size_t i = 0; i < annotation_len; i++) {
			auto elem_val = yyjson_arr_get(annotation_val, i);

			if (!yyjson_is_obj(elem_val)) {
				continue;
			}

			auto type_val = yyjson_obj_get(elem_val, "type");

			if (!yyjson_is_str(type_val)) {
				continue;
			}

			auto key = yyjson_get_str(type_val);

			if (StringUtil::Equals(key, "OBS_COUNT")) {
				auto key_val = yyjson_obj_get(elem_val, "title");

				if (yyjson_is_str(key_val)) {
					auto val = yyjson_get_str(key_val);
					info.number_of_values = val ? std::stoll(val) : -1;
				}
			} else if (StringUtil::Equals(key, "OBS_PERIOD_OVERALL_OLDEST")) {
				auto key_val = yyjson_obj_get(elem_val, "title");

				if (yyjson_is_str(key_val)) {
					auto val = yyjson_get_str(key_val);
					info.data_start = val ? string(val) : "";
				}
			} else if (StringUtil::Equals(key, "OBS_PERIOD_OVERALL_LATEST")) {
				auto key_val = yyjson_obj_get(elem_val, "title");

				if (yyjson_is_str(key_val)) {
					auto val = yyjson_get_str(key_val);
					info.data_end = val ? string(val) : "";
				}
			} else if (StringUtil::Equals(key, "UPDATE_DATA")) {
				auto key_val = yyjson_obj_get(elem_val, "date");

				if (yyjson_is_str(key_val)) {
					auto val = yyjson_get_str(key_val);
					info.update_data = val ? string(val) : "";
				}
			} else if (StringUtil::Equals(key, "UPDATE_STRUCTURE")) {
				auto key_val = yyjson_obj_get(elem_val, "date");

				if (yyjson_is_str(key_val)) {
					auto val = yyjson_get_str(key_val);
					info.update_structure = val ? string(val) : "";
				}
			}
		}

		return info;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	struct BindData final : TableFunctionData {
		std::vector<DataflowInfo> rows;
		explicit BindData(const std::vector<DataflowInfo> &rows) : rows(std::move(rows)) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		std::vector<string> providers;
		std::vector<string> dataflows;
		string language = "en";

		names.emplace_back("provider_id");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("dataflow_id");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("class");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("version");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("label");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("language");
		return_types.push_back(LogicalType::VARCHAR);

		names.emplace_back("number_of_values");
		return_types.push_back(LogicalType::BIGINT);
		names.emplace_back("data_start");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("data_end");
		return_types.push_back(LogicalType::VARCHAR);

		names.emplace_back("update_data");
		return_types.push_back(LogicalType::TIMESTAMP_TZ);
		names.emplace_back("update_structure");
		return_types.push_back(LogicalType::TIMESTAMP_TZ);

		names.emplace_back("data_structure");
		return_types.push_back(LogicalType::JSON());
		names.emplace_back("annotations");
		return_types.push_back(LogicalType::JSON());

		// Extract desired API Endpoints from named parameters

		auto options_param = input.named_parameters.find("providers");

		if (options_param != input.named_parameters.end()) {
			auto &items = options_param->second;

			if (!items.IsNull() && items.type() == LogicalType::LIST(LogicalType::VARCHAR)) {
				for (const auto &item : ListValue::GetChildren(items)) {
					auto value = item.GetValue<string>();

					// Validate Endpoint name
					if (eurostat::ENDPOINTS.find(value) == eurostat::ENDPOINTS.end()) {
						throw InvalidInputException("EUROSTAT: Unknown Endpoint '%s'.", value.c_str());
					}
					providers.push_back(value);
				}
			}
		}
		if (providers.empty()) {
			for (const auto &it : eurostat::ENDPOINTS) {
				providers.push_back(it.first);
			}
		}

		// Extract desired Dataflows from named parameters

		options_param = input.named_parameters.find("dataflows");

		if (options_param != input.named_parameters.end()) {
			auto &items = options_param->second;

			if (!items.IsNull() && items.type() == LogicalType::LIST(LogicalType::VARCHAR)) {
				for (const auto &item : ListValue::GetChildren(items)) {
					auto value = item.GetValue<string>();

					// When "all" is provided, ignore other dataflow values
					if (value == "all") {
						dataflows.clear();
						break;
					}
					dataflows.push_back(value);
				}
			}
		}
		if (dataflows.empty()) {
			dataflows.push_back("all");
		}

		// Extract desired Language from named parameters

		options_param = input.named_parameters.find("language");

		if (options_param != input.named_parameters.end()) {
			auto &item = options_param->second;

			if (!item.IsNull() && item.type() == LogicalType::VARCHAR) {
				language = item.GetValue<string>();
			}
		}
		if (language.empty()) {
			language = "en";
		}

		// Get the dataflow metadata collection (synchronously for now)

		std::vector<DataflowInfo> rows;
		HttpSettings settings;
		idx_t req_count = 0;

		for (const auto &provider_id : providers) {
			const auto it = eurostat::ENDPOINTS.find(provider_id);

			for (const auto &dataflow_id : dataflows) {
				string url = it->second.api_url + "dataflow/" + provider_id + "/" + dataflow_id +
				             "?format=JSON&compressed=true&lang=" + language;

				if (req_count == 0) {
					settings = HttpRequest::ExtractHttpSettings(context, url);
				}
				req_count++;

				// Execute HTTP GET request

				auto response =
				    HttpRequest::ExecuteHttpRequest(settings, url, "GET", duckdb_httplib_openssl::Headers(), "", "");

				if (response.status_code != 200) {
					throw IOException(
					    "EUROSTAT: Failed to fetch dataflow metadata from provider='%s', dataflow='%s': (%d) %s",
					    provider_id.c_str(), dataflow_id.c_str(), response.status_code, response.error.c_str());
				}
				if (!response.error.empty()) {
					throw IOException("EUROSTAT: " + response.error);
				}

				// Parse JSON response

				const auto json_data = yyjson_read(response.body.c_str(), response.body.size(), YYJSON_READ_NOFLAG);
				if (!json_data) {
					throw IOException("EUROSTAT: Failed to parse dataflow metadata from provider='%s', dataflow='%s'.",
					                  provider_id.c_str(), dataflow_id.c_str());
				}

				try {
					auto root_val = yyjson_doc_get_root(json_data);

					// Parse input metadata of dataflows

					if (dataflow_id == "all") {
						yyjson_val *link_val = nullptr;
						yyjson_val *item_val = nullptr;

						if (!yyjson_is_obj(link_val = yyjson_obj_get(root_val, "link"))) {
							throw InvalidInputException("EUROSTAT: Missing 'link' attribute in dataflow metadata.");
						}
						if (!yyjson_is_arr(item_val = yyjson_obj_get(link_val, "item"))) {
							throw InvalidInputException(
							    "EUROSTAT: Missing 'link/item' attribute in dataflow metadata.");
						}

						auto array_len = yyjson_arr_size(item_val);

						for (size_t i = 0; i < array_len; i++) {
							auto elem_val = yyjson_arr_get(item_val, i);
							auto dataflow_info = ParseDataflowInfo(provider_id, elem_val);
							rows.push_back(dataflow_info);
						}
					} else if (yyjson_is_obj(root_val)) {
						auto dataflow_info = ParseDataflowInfo(provider_id, root_val);
						rows.push_back(dataflow_info);
					}

					// Make sure to free the JSON document
					yyjson_doc_free(json_data);

				} catch (...) {
					// Make sure to free the JSON document in case of an exception
					yyjson_doc_free(json_data);
					throw;
				}
			}
		}

		return make_uniq_base<FunctionData, BindData>(rows);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Init
	//------------------------------------------------------------------------------------------------------------------

	struct State final : GlobalTableFunctionState {
		idx_t current_row;
		explicit State() : current_row(0) {
		}
	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq_base<GlobalTableFunctionState, State>();
	}

	//------------------------------------------------------------------------------------------------------------------
	// Cardinality
	//------------------------------------------------------------------------------------------------------------------

	static unique_ptr<NodeStatistics> Cardinality(ClientContext &context, const FunctionData *data) {

		auto &bind_data = data->Cast<BindData>();
		auto result = make_uniq<NodeStatistics>();

		// This is the maximum number of points in a single file
		result->has_max_cardinality = true;
		result->max_cardinality = bind_data.rows.size();

		return result;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {

		auto &bind_data = input.bind_data->Cast<BindData>();
		auto &gstate = input.global_state->Cast<State>();

		// Calculate how many record we can fit in the output
		const auto output_size = std::min<idx_t>(STANDARD_VECTOR_SIZE, bind_data.rows.size() - gstate.current_row);
		const auto current_row = gstate.current_row;

		if (output_size == 0) {
			output.SetCardinality(0);
			return;
		}

		// Load current subset of rows.
		for (idx_t row_idx = 0, record_idx = current_row; row_idx < output_size; row_idx++, record_idx++) {
			const auto &dataflow_info = bind_data.rows[record_idx];

			output.data[0].SetValue(row_idx, dataflow_info.provider_id);
			output.data[1].SetValue(row_idx, dataflow_info.dataflow_id);
			output.data[2].SetValue(row_idx, dataflow_info.type);
			output.data[3].SetValue(row_idx, dataflow_info.version);
			output.data[4].SetValue(row_idx, dataflow_info.label);
			output.data[5].SetValue(row_idx, dataflow_info.language);

			if (dataflow_info.number_of_values == -1) {
				output.data[6].SetValue(row_idx, Value());
			} else {
				output.data[6].SetValue(row_idx, dataflow_info.number_of_values);
			}

			if (dataflow_info.data_start.empty()) {
				output.data[7].SetValue(row_idx, Value());
			} else {
				output.data[7].SetValue(row_idx, dataflow_info.data_start);
			}

			if (dataflow_info.data_end.empty()) {
				output.data[8].SetValue(row_idx, Value());
			} else {
				output.data[8].SetValue(row_idx, dataflow_info.data_end);
			}

			if (dataflow_info.update_data.empty()) {
				output.data[9].SetValue(row_idx, Value());
			} else {
				timestamp_t value = Timestamp::FromString(dataflow_info.update_data, true);
				output.data[9].SetValue(row_idx, Value::TIMESTAMPTZ(timestamp_tz_t(value)));
			}

			if (dataflow_info.update_structure.empty()) {
				output.data[10].SetValue(row_idx, Value());
			} else {
				timestamp_t value = Timestamp::FromString(dataflow_info.update_structure, true);
				output.data[10].SetValue(row_idx, Value::TIMESTAMPTZ(timestamp_tz_t(value)));
			}

			if (dataflow_info.data_structure.empty()) {
				output.data[11].SetValue(row_idx, Value());
			} else {
				output.data[11].SetValue(row_idx, dataflow_info.data_structure);
			}

			if (dataflow_info.annotations.empty()) {
				output.data[12].SetValue(row_idx, Value());
			} else {
				output.data[12].SetValue(row_idx, dataflow_info.annotations);
			}
		}

		// Update the point index
		gstate.current_row += output_size;

		// Set the cardinality of the output
		output.SetCardinality(output_size);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Returns info of the dataflows provided by EUROSTAT Providers.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT * FROM EUROSTAT_Dataflows();
		SELECT * FROM EUROSTAT_Dataflows(providers = ['ESTAT','ECFIN'], language := 'en');

		--- You can also filter by specific dataflows:

		SELECT
			provider_id,
			dataflow_id,
			class,
			version,
			label
		FROM
			EUROSTAT_Dataflows(providers = ['ESTAT'], dataflows = ['DEMO_R_D2JAN'], language := 'de')
		;

		┌─────────────┬──────────────┬─────────┬─────────┬───────────────────────────────────────────────────────────────────┐
		│ provider_id │  dataflow_id │  class  │ version │                               label                               │
		│   varchar   │   varchar    │ varchar │ varchar │                              varchar                              │
		├─────────────┼──────────────┼─────────┼─────────┼───────────────────────────────────────────────────────────────────┤
		│ ESTAT       │ DEMO_R_D2JAN │ dataset │ 1.0     │ Bevölkerung am 1. Januar nach Alter, Geschlecht und NUTS-2-Region │
		└─────────────┴──────────────┴─────────┴─────────┴───────────────────────────────────────────────────────────────────┘
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "eurostat");
		tags.insert("category", "table");

		TableFunction func("EUROSTAT_Dataflows", {}, Execute, Bind, Init);

		func.cardinality = Cardinality;
		func.named_parameters["providers"] = LogicalType::LIST(LogicalType::VARCHAR);
		func.named_parameters["dataflows"] = LogicalType::LIST(LogicalType::VARCHAR);
		func.named_parameters["language"] = LogicalType::VARCHAR;

		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

//======================================================================================================================
// ES_DataStructure
//======================================================================================================================

static constexpr const char *ES_XMLSNS_M = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}";
static constexpr const char *ES_XMLSNS_S = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}";

static constexpr const char *ES_DIMENSION_PATH =
    "/m:Structure/m:Structures/s:DataStructures/s:DataStructure/s:DataStructureComponents/s:DimensionList/s:Dimension";
static constexpr const char *ES_TIME_DIMENSION_PATH = "/m:Structure/m:Structures/s:DataStructures/s:DataStructure/"
                                                      "s:DataStructureComponents/s:DimensionList/s:TimeDimension";
static constexpr const char *ES_CONCEPT_PATH = "/m:Structure/m:Structures/s:Concepts/s:ConceptScheme/s:Concept";
static constexpr const char *ES_VALUES_PATH =
    "/m:Structure/m:Structures/s:Constraints/s:ContentConstraint/s:CubeRegion/c:KeyValue";

static constexpr const char *ES_ERROR_PATH = "/S:Fault/faultstring";

struct ES_DataStructure {

	//! Information of a Dimension of an EUROSTAT Dataflow
	struct Dimension {
		int32_t position = -1;
		string id;
		string concept_id;
		string concept_label;
		std::vector<string> values;
	};

	//! Returns the basic data structure of an EUROSTAT Dataflow.
	static std::vector<Dimension> GetBasicDataSchema(ClientContext &context, const string &provider_id,
	                                                 const string &dataflow_id, const string &language) {

		std::vector<Dimension> dimensions;

		// Execute HTTP GET request

		const auto it = eurostat::ENDPOINTS.find(provider_id);
		string url = it->second.api_url + "dataflow/" + provider_id + "/" + dataflow_id +
		             "/latest?detail=referencepartial&references=descendants";

		HttpSettings settings = HttpRequest::ExtractHttpSettings(context, url);
		auto response =
		    HttpRequest::ExecuteHttpRequest(settings, url, "GET", duckdb_httplib_openssl::Headers(), "", "");

		if (response.status_code != 200) {
			throw IOException("EUROSTAT: Failed to fetch dataflow metadata from provider='%s', dataflow='%s': (%d) %s",
			                  provider_id.c_str(), dataflow_id.c_str(), response.status_code, response.error.c_str());
		}
		if (!response.error.empty()) {
			throw IOException("EUROSTAT: " + response.error);
		}

		// Get the dimensions from XML response

		XmlDocument document = XmlDocument(response.body);
		xmlDocPtr doc_obj = document.GetDoc();
		xmlXPathContextPtr xpath_ctx = document.GetXPathContext();
		xmlXPathObjectPtr xpath_obj = nullptr;

		for (const auto &xpath : {ES_DIMENSION_PATH, ES_TIME_DIMENSION_PATH}) {

			if ((xpath_obj = xmlXPathEvalExpression(BAD_CAST xpath, xpath_ctx)) && xpath_obj->nodesetval) {

				for (int i = 0; i < xpath_obj->nodesetval->nodeNr; i++) {
					xmlNodePtr node = xpath_obj->nodesetval->nodeTab[i];

					auto dim_id = XmlUtils::GetNodeAttributeValue(node, "id");
					if (!dim_id.empty()) {
						Dimension dim;
						dim.id = StringUtil::Lower(dim_id);
						dim.position = std::atoi(XmlUtils::GetNodeAttributeValue(node, "position").c_str());

						// Get the Concept ID for the Dimension
						xmlXPathContextPtr local_ctx = xmlXPathNewContext(doc_obj);
						if (local_ctx) {
							document.RegisterNamespaces(local_ctx);
							local_ctx->node = node;

							xmlXPathObjectPtr temp_obj = nullptr;
							if ((temp_obj = xmlXPathEvalExpression(BAD_CAST "./s:ConceptIdentity/Ref", local_ctx)) &&
							    temp_obj->nodesetval && temp_obj->nodesetval->nodeNr > 0) {
								xmlNodePtr ref_node = temp_obj->nodesetval->nodeTab[0];
								dim.concept_id = XmlUtils::GetNodeAttributeValue(ref_node, "id");
							}
							if (temp_obj) {
								xmlXPathFreeObject(temp_obj);
							}
							xmlXPathFreeContext(local_ctx);
						}
						dimensions.emplace_back(dim);

						// Do we can add the virtual GEO_LEVEL dimension ?
						if (dim.id == "geo") {
							Dimension d;
							d.position = -1;
							d.id = "geo_level";
							d.concept_label = "NUTS classification level";
							d.values.assign({"aggregate", "country", "nuts1", "nuts2", "nuts3", "city"});
							dimensions.emplace_back(d);
						}
					}
				}
			}
			if (xpath_obj) {
				xmlXPathFreeObject(xpath_obj);
				xpath_obj = nullptr;
			}
		}

		// Get the Concept names for Dimensions

		if ((xpath_obj = xmlXPathEvalExpression(BAD_CAST ES_CONCEPT_PATH, xpath_ctx)) && xpath_obj->nodesetval) {

			for (int i = 0; i < xpath_obj->nodesetval->nodeNr; i++) {
				xmlNodePtr node = xpath_obj->nodesetval->nodeTab[i];

				auto concept_id = XmlUtils::GetNodeAttributeValue(node, "id");
				if (!concept_id.empty()) {
					for (auto &dim : dimensions) {
						if (dim.concept_id == concept_id) {

							for (xmlNodePtr child = node->children; child; child = child->next) {

								if (strcmp((const char *)child->name, "Name") == 0) {
									string lang = XmlUtils::GetNodeAttributeValue(child, "lang", language);

									if (lang == language || dim.concept_label.empty()) {
										dim.concept_label = XmlUtils::GetNodeTextContent(child);
									}
								}
							}
							break;
						}
					}
				}
			}
		}
		if (xpath_obj) {
			xmlXPathFreeObject(xpath_obj);
			xpath_obj = nullptr;
		}

		return dimensions;
	}

	//! Returns the data structure of an EUROSTAT Dataflow.
	static std::vector<Dimension> GetDataSchema(ClientContext &context, const string &provider_id,
	                                            const string &dataflow_id, const string &language) {

		auto dimensions = ES_DataStructure::GetBasicDataSchema(context, provider_id, dataflow_id, language);

		// Execute HTTP GET request

		const auto it = eurostat::ENDPOINTS.find(provider_id);
		string url = it->second.api_url + "contentconstraint/" + provider_id + "/" + dataflow_id;

		HttpSettings settings = HttpRequest::ExtractHttpSettings(context, url);
		auto response =
		    HttpRequest::ExecuteHttpRequest(settings, url, "GET", duckdb_httplib_openssl::Headers(), "", "");

		if (response.status_code != 200) {
			throw IOException("EUROSTAT: Failed to fetch dataflow metadata from provider='%s', dataflow='%s': (%d) %s",
			                  provider_id.c_str(), dataflow_id.c_str(), response.status_code, response.error.c_str());
		}
		if (!response.error.empty()) {
			throw IOException("EUROSTAT: " + response.error);
		}

		// Get the different values of dimensions from XML response

		XmlDocument document = XmlDocument(response.body);
		xmlXPathContextPtr xpath_ctx = document.GetXPathContext();
		xmlXPathObjectPtr xpath_obj = nullptr;

		if ((xpath_obj = xmlXPathEvalExpression(BAD_CAST ES_VALUES_PATH, xpath_ctx)) && xpath_obj->nodesetval) {

			for (int i = 0; i < xpath_obj->nodesetval->nodeNr; i++) {
				xmlNodePtr node = xpath_obj->nodesetval->nodeTab[i];

				auto dim_id = XmlUtils::GetNodeAttributeValue(node, "id");
				if (!dim_id.empty()) {
					dim_id = StringUtil::Lower(dim_id);

					for (auto &dim : dimensions) {
						if (dim.id == dim_id) {

							for (xmlNodePtr child = node->children; child; child = child->next) {
								if (strcmp((const char *)child->name, "Value") == 0) {
									string code_value = XmlUtils::GetNodeTextContent(child);
									dim.values.emplace_back(code_value);
								}
							}
							break;
						}
					}
				}
			}
		}
		if (xpath_obj) {
			xmlXPathFreeObject(xpath_obj);
			xpath_obj = nullptr;
		}

		return dimensions;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	struct BindData final : TableFunctionData {
		string provider_id;
		string dataflow_id;
		std::vector<Dimension> rows;

		explicit BindData(const string &provider_id, const string &dataflow_id, const std::vector<Dimension> &rows)
		    : provider_id(provider_id), dataflow_id(dataflow_id), rows(std::move(rows)) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		D_ASSERT(input.inputs.size() == 2);

		const string provider_id = StringValue::Get(input.inputs[0]);
		const string dataflow_id = StringValue::Get(input.inputs[1]);
		string language = "en";

		// Validate input parameters

		if (provider_id.empty()) {
			throw InvalidInputException("EUROSTAT: First parameter, the 'provider' identifier, cannot be empty.");
		}

		if (dataflow_id.empty()) {
			throw InvalidInputException("EUROSTAT: Second parameter, the 'dataflow' code, cannot be empty.");
		}

		if (eurostat::ENDPOINTS.find(provider_id) == eurostat::ENDPOINTS.end()) {
			throw InvalidInputException("EUROSTAT: Unknown Endpoint '%s'.", provider_id.c_str());
		}

		// Extract desired Language from named parameters

		auto options_param = input.named_parameters.find("language");

		if (options_param != input.named_parameters.end()) {
			auto &item = options_param->second;

			if (!item.IsNull() && item.type() == LogicalType::VARCHAR) {
				language = item.GetValue<string>();
			}
		}
		if (language.empty()) {
			language = "en";
		}

		// Get list of Dimensions of a Dataflow

		std::vector<Dimension> rows = ES_DataStructure::GetDataSchema(context, provider_id, dataflow_id, language);

		names.emplace_back("provider_id");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("dataflow_id");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("position");
		return_types.push_back(LogicalType::INTEGER);
		names.emplace_back("dimension");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("concept");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("values");
		return_types.push_back(LogicalType::JSON());

		return make_uniq_base<FunctionData, BindData>(provider_id, dataflow_id, rows);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Init
	//------------------------------------------------------------------------------------------------------------------

	struct State final : GlobalTableFunctionState {
		idx_t current_row;
		explicit State() : current_row(0) {
		}
	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq_base<GlobalTableFunctionState, State>();
	}

	//------------------------------------------------------------------------------------------------------------------
	// Cardinality
	//------------------------------------------------------------------------------------------------------------------

	static unique_ptr<NodeStatistics> Cardinality(ClientContext &context, const FunctionData *data) {

		auto &bind_data = data->Cast<BindData>();
		auto result = make_uniq<NodeStatistics>();

		// This is the maximum number of points in a single file
		result->has_max_cardinality = true;
		result->max_cardinality = bind_data.rows.size();

		return result;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {

		auto &bind_data = input.bind_data->Cast<BindData>();
		auto &gstate = input.global_state->Cast<State>();

		// Calculate how many record we can fit in the output
		const auto output_size = std::min<idx_t>(STANDARD_VECTOR_SIZE, bind_data.rows.size() - gstate.current_row);
		const auto current_row = gstate.current_row;

		if (output_size == 0) {
			output.SetCardinality(0);
			return;
		}

		// Load current subset of rows.
		for (idx_t row_idx = 0, record_idx = current_row; row_idx < output_size; row_idx++, record_idx++) {
			const auto &dimension = bind_data.rows[record_idx];

			output.data[0].SetValue(row_idx, bind_data.provider_id);
			output.data[1].SetValue(row_idx, bind_data.dataflow_id);
			output.data[2].SetValue(row_idx, dimension.position);
			output.data[3].SetValue(row_idx, dimension.id);

			if (dimension.concept_label.empty()) {
				output.data[4].SetValue(row_idx, Value());
			} else {
				output.data[4].SetValue(row_idx, dimension.concept_label);
			}

			if (dimension.values.empty()) {
				output.data[5].SetValue(row_idx, Value());
			} else {
				std::ostringstream values_stream;
				values_stream << "[";
				string values_str;

				for (const auto &val : dimension.values) {
					values_stream << '"' << val << "\",";
				}
				if (values_stream.tellp() > 1) {
					values_stream.seekp(-1, std::ios_base::end); // Remove last comma
					values_stream << "]";
					values_str = values_stream.str();
				} else {
					values_str = "[]";
				}

				output.data[5].SetValue(row_idx, values_str);
			}
		}

		// Update the point index
		gstate.current_row += output_size;

		// Set the cardinality of the output
		output.SetCardinality(output_size);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Returns information of the data structure of an EUROSTAT Dataflow.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT
			provider_id,
			dataflow_id,
			position,
			dimension,
			concept
		FROM
			EUROSTAT_DataStructure('ESTAT', 'DEMO_R_D2JAN', language := 'en')
		;

		┌─────────────┬──────────────┬──────────┬─────────────┬─────────────────────────────────┐
		│ provider_id │ dataflow_id  │ position │  dimension  │             concept             │
		│   varchar   │   varchar    │  int32   │   varchar   │             varchar             │
		├─────────────┼──────────────┼──────────┼─────────────┼─────────────────────────────────┤
		│ ESTAT       │ DEMO_R_D2JAN │        1 │ freq        │ Time frequency                  │
		│ ESTAT       │ DEMO_R_D2JAN │        2 │ unit        │ Unit of measure                 │
		│ ESTAT       │ DEMO_R_D2JAN │        3 │ sex         │ Sex                             │
		│ ESTAT       │ DEMO_R_D2JAN │        4 │ age         │ Age class                       │
		│ ESTAT       │ DEMO_R_D2JAN │        5 │ geo         │ Geopolitical entity (reporting) │
		│ ESTAT       │ DEMO_R_D2JAN │       -1 │ geo_level   │ NUTS classification level       │
		│ ESTAT       │ DEMO_R_D2JAN │        6 │ time_period │ Time                            │
		└─────────────┴──────────────┴──────────┴─────────────┴─────────────────────────────────┘
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "eurostat");
		tags.insert("category", "table");

		TableFunction func("EUROSTAT_DataStructure", {LogicalType::VARCHAR, LogicalType::VARCHAR}, Execute, Bind, Init);

		func.cardinality = Cardinality;
		func.named_parameters["language"] = LogicalType::VARCHAR;

		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	};
};

} // namespace

// #####################################################################################################################
// Register Metadata/Info Functions
// #####################################################################################################################

//! Returns the data structure (dimensions) of a given dataflow
std::vector<eurostat::Dimension> EurostatUtils::DataStructureOf(ClientContext &context, const std::string &provider_id,
                                                                const std::string &dataflow_id) {

	auto dimensions = ES_DataStructure::GetBasicDataSchema(context, provider_id, dataflow_id, "en");
	std::vector<eurostat::Dimension> data_structure;

	for (const auto &dim : dimensions) {
		auto d = eurostat::Dimension {dim.position, dim.id, dim.concept_label};
		data_structure.emplace_back(d);
	}
	return data_structure;
}

//! Extracts the error message of a given Eurostat API response body
std::string EurostatUtils::GetXmlErrorMessage(const std::string &response_body) {

	XmlDocument document = XmlDocument(response_body);
	xmlXPathContextPtr xpath_ctx = document.GetXPathContext();
	xmlXPathObjectPtr xpath_obj = nullptr;

	string error_msg;

	if ((xpath_obj = xmlXPathEvalExpression(BAD_CAST ES_ERROR_PATH, xpath_ctx)) && xpath_obj->nodesetval &&
	    xpath_obj->nodesetval->nodeNr > 0) {

		xmlNodePtr node = xpath_obj->nodesetval->nodeTab[0];
		error_msg = XmlUtils::GetNodeTextContent(node);
	}
	if (xpath_obj) {
		xmlXPathFreeObject(xpath_obj);
		xpath_obj = nullptr;
	}

	return error_msg;
}

void EurostatInfoFunctions::Register(ExtensionLoader &loader) {

	ES_Endpoints::Register(loader);
	ES_Dataflows::Register(loader);
	ES_DataStructure::Register(loader);
}

} // namespace duckdb
