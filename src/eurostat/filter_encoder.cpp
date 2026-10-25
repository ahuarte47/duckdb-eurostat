#include "filter_encoder.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/legacy_bound_comparison_expression.hpp"

// Debug logging controlled by EUROSTAT_DEBUG environment variable
static int GetDebugLevel() {
	static int level = -1;
	if (level == -1) {
		const char *env = std::getenv("EUROSTAT_DEBUG");
		level = env ? std::stoi(env) : 0;
	}
	return level;
}

#define EUROSTAT_SCAN_DEBUG_LOG(level, fmt, ...)                                                                       \
	do {                                                                                                               \
		if (GetDebugLevel() >= level) {                                                                                \
			fprintf(stderr, "EUROSTAT: " fmt "\n", ##__VA_ARGS__);                                                     \
		}                                                                                                              \
	} while (0)

namespace duckdb {

//======================================================================================================================
// EncodedExpression Functions
//======================================================================================================================

//! Special flag for virtual/special dimensions (e.g., time_period).
static constexpr char VIRTUAL_DIMENSION_FLAG[] = "---";

//! Name for time period dimension in Eurostat data structures.
static constexpr char TIME_PERIOD_DIMENSION_NAME[] = "time_period";

EurostatFilter::EurostatFilter(const std::vector<eurostat::Dimension> &ds) : data_structure(ds) {
	for (const auto &dim : data_structure) {
		if (dim.position == -1 || dim.name == TIME_PERIOD_DIMENSION_NAME) {
			dim_mask.emplace_back(VIRTUAL_DIMENSION_FLAG);
		} else {
			dim_mask.emplace_back("");
		}
	}
}

bool EurostatFilter::IsEmpty() const {
	if (!start_period.empty() || !end_period.empty()) {
		return false;
	}
	for (const auto &m : dim_mask) {
		if (!m.empty() && m != VIRTUAL_DIMENSION_FLAG) {
			return false;
		}
	}
	return true;
}

std::string EurostatFilter::GetFilterString() const {
	std::string filter_clause;

	// Dimension filters part (e.g., "A.B+X.C.D+Y").

	for (size_t i = 0; i < dim_mask.size(); i++) {
		const auto &m = dim_mask[i];

		if (m == VIRTUAL_DIMENSION_FLAG) {
			continue;
		}
		if (filter_clause.empty()) {
			filter_clause += '/';
		}
		if (!m.empty()) {
			filter_clause += m;
		}
		filter_clause += '.';
	}
	if (!filter_clause.empty()) {
		filter_clause.pop_back();
	}

	filter_clause += '?';

	// Time period filters part (e.g., "startPeriod=2020&endPeriod=2021&").

	if (!start_period.empty()) {
		filter_clause += start_period;
		filter_clause += '&';
	}
	if (!end_period.empty()) {
		filter_clause += end_period;
		filter_clause += '&';
	}

	return filter_clause;
}

void EurostatFilterSet::PushEmptyFilter() {
	EurostatFilter new_filter(data_structure);
	filters.emplace_back(std::move(new_filter));
}

//======================================================================================================================
// TableFilter Encoding
//======================================================================================================================

bool FilterEncoder::GetComparisonOperator(ExpressionType type, std::string &out_operator) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		out_operator = "=";
		return true;
	default:
		return false;
	}
}

bool FilterEncoder::EncodeFilter(const TableFilter &filter, const eurostat::Dimension &dimension,
                                 const idx_t &dim_index, EurostatFilterSet &out_result) {
	switch (filter.filter_type) {
	case TableFilterType::LEGACY_CONSTANT_COMPARISON:
		return EncodeConstantComparison(filter.Cast<LegacyConstantFilter>(), dimension, dim_index, out_result);

	case TableFilterType::LEGACY_IN_FILTER:
		return EncodeInFilter(filter.Cast<LegacyInFilter>(), dimension, dim_index, out_result);

	case TableFilterType::LEGACY_CONJUNCTION_AND:
		return EncodeConjunctionAnd(filter.Cast<LegacyConjunctionAndFilter>(), dimension, dim_index, out_result);

	case TableFilterType::LEGACY_CONJUNCTION_OR:
		return EncodeConjunctionOr(filter.Cast<LegacyConjunctionOrFilter>(), dimension, dim_index, out_result);

	case TableFilterType::LEGACY_OPTIONAL_FILTER: {
		auto &opt_filter = filter.Cast<LegacyOptionalFilter>();
		if (!opt_filter.child_filter) {
			return true; // No child filters, always OK.
		}
		return EncodeFilter(*opt_filter.child_filter, dimension, dim_index, out_result);
	}
	default:
		// Other filter types cannot be pushed down to Eurostat API.
		out_result.supported = false;
		return false;
	}
}

bool FilterEncoder::EncodeConstantComparison(const LegacyConstantFilter &filter, const eurostat::Dimension &dimension,
                                             const idx_t &dim_index, EurostatFilterSet &out_result) {
	if (filter.constant.IsNull()) {
		out_result.supported = false;
		return false;
	}

	auto &out_filter = out_result.GetCurrentFilter();

	if (dimension.name == TIME_PERIOD_DIMENSION_NAME) {
		if (filter.comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
			out_filter.start_period = "startPeriod=" + filter.constant.ToString();
			return true;
		}
		if (filter.comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
			out_filter.end_period = "endPeriod=" + filter.constant.ToString();
			return true;
		}
		if (filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
			out_filter.start_period = "startPeriod=" + filter.constant.ToString();
			out_filter.end_period = "endPeriod=" + filter.constant.ToString();
			return true;
		}
		out_result.supported = false;
		return false;
	}

	std::string op;
	if (!GetComparisonOperator(filter.comparison_type, op)) {
		out_result.supported = false;
		return false;
	}

	// Append the constant value to the current dimension mask.
	std::string mask_v = out_filter.dim_mask[dim_index];
	mask_v = mask_v.empty() ? filter.constant.ToString() : mask_v + "+" + filter.constant.ToString();
	out_filter.dim_mask[dim_index] = mask_v;

	return true;
}

bool FilterEncoder::EncodeInFilter(const LegacyInFilter &filter, const eurostat::Dimension &dimension,
                                   const idx_t &dim_index, EurostatFilterSet &out_result) {
	if (dimension.name == TIME_PERIOD_DIMENSION_NAME && filter.values.size() > 1) {
		// Eurostat API does not support multiple time period values.
		out_result.supported = false;
		return false;
	}
	for (idx_t i = 0; i < filter.values.size(); i++) {
		LegacyConstantFilter const_f(ExpressionType::COMPARE_EQUAL, filter.values[i]);

		if (!FilterEncoder::EncodeConstantComparison(const_f, dimension, dim_index, out_result)) {
			out_result.supported = false;
			return false;
		}
	}
	return true;
}

bool FilterEncoder::EncodeConjunctionAnd(const LegacyConjunctionAndFilter &filter, const eurostat::Dimension &dimension,
                                         const idx_t &dim_index, EurostatFilterSet &out_result) {
	// Something's wrong if there are no child filters.
	if (filter.child_filters.empty()) {
		out_result.supported = false;
		return false;
	}

	for (const auto &child : filter.child_filters) {
		if (!FilterEncoder::EncodeFilter(*child, dimension, dim_index, out_result)) {
			out_result.supported = false;
			return false;
		}
	}
	return true;
}

bool FilterEncoder::EncodeConjunctionOr(const LegacyConjunctionOrFilter &filter, const eurostat::Dimension &dimension,
                                        const idx_t &dim_index, EurostatFilterSet &out_result) {
	// Something's wrong if there are no child filters.
	if (filter.child_filters.empty()) {
		out_result.supported = false;
		return false;
	}

	for (const auto &child : filter.child_filters) {
		if (!FilterEncoder::EncodeFilter(*child, dimension, dim_index, out_result)) {
			out_result.supported = false;
			return false;
		}
		out_result.PushEmptyFilter();
	}
	return true;
}

FilterEncoderResult FilterEncoder::Encode(const TableFilterSet *filters,
                                          const std::vector<eurostat::Dimension> &data_structure,
                                          const std::vector<column_t> &column_ids) {
	FilterEncoderResult result;
	result.supported = true;

	// No filters to encode.
	if (!filters || !filters->HasFilters()) {
		result.supported = false;
		return result;
	}

	// Virtual/special column identifiers start at 2^63.
	constexpr column_t VIRTUAL_COL_START = UINT64_C(9223372036854775808);

	// Encode each filter.

	EurostatFilterSet filter_set(data_structure);

	for (const auto &filter_entry : *filters) {
		idx_t projected_col_idx = filter_entry.GetIndex().GetIndex();
		idx_t table_col_idx;

		// Map from projected column index to actual table column index.
		if (column_ids.empty()) {
			// No projection - use filter index directly as table column index.
			table_col_idx = projected_col_idx;
		} else if (projected_col_idx >= column_ids.size()) {
			// Something's wrong, filter column index out of projected range.
			result.supported = false;
			return result;
		} else {
			// Map through column_ids to get actual table column index.
			table_col_idx = column_ids[projected_col_idx];
		}

		// Skip virtual/special columns.
		if (table_col_idx >= VIRTUAL_COL_START) {
			result.supported = false;
			return result;
		}
		// Filter column index out of projected range.
		if (table_col_idx >= data_structure.size()) {
			result.supported = false;
			return result;
		}
		// Dimension is not defined in data source (e.g., "geo_level").
		if (data_structure[table_col_idx].position == -1) {
			result.supported = false;
			return result;
		}

		// Encode the filter for current dimension.

		const eurostat::Dimension &dimension = data_structure[table_col_idx];

		if (!FilterEncoder::EncodeFilter(filter_entry.Filter(), dimension, table_col_idx, filter_set)) {
			result.supported = false;
			return result;
		}
	}

	// Everything was encoded? build the final API Eurostat filter clauses.

	if (result.supported) {
		for (const auto &out_filter : filter_set.filters) {
			if (!out_filter.IsEmpty()) {
				std::string filter_clause = out_filter.GetFilterString();
				result.filters.emplace_back(filter_clause);
			}
		}
		result.supported = !result.filters.empty();
	}
	return result;
}

//======================================================================================================================
// Expression Encoding
//======================================================================================================================

FilterEncoderResult FilterEncoder::EncodeExpression(vector<unique_ptr<Expression>> &expressions,
                                                    const std::vector<eurostat::Dimension> &data_structure,
                                                    const std::vector<column_t> &column_ids) {
	FilterEncoderResult result;
	result.supported = true;

	// No expressions to encode.
	if (expressions.empty()) {
		result.supported = false;
		return result;
	}

	// Encode each expression.

	EurostatFilterSet filter_set(data_structure);

	for (const auto &expr : expressions) {
		if (!EncodeExpressionNode(*expr, data_structure, column_ids, filter_set)) {
			result.supported = false;
			return result;
		}
	}

	// Everything was encoded? build the final API Eurostat filter clauses.

	if (result.supported) {
		for (const auto &out_filter : filter_set.filters) {
			if (!out_filter.IsEmpty()) {
				std::string filter_clause = out_filter.GetFilterString();
				result.filters.emplace_back(filter_clause);
			}
		}
		result.supported = !result.filters.empty();

		// Remove the expressions we handled.
		if (result.supported) {
			expressions.clear();
		}
	}

	return result;
}

int FilterEncoder::GetDimensionIndexFromColumnRef(const Expression &expr,
                                                  const std::vector<eurostat::Dimension> &data_structure,
                                                  const std::vector<column_t> &column_ids) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return -1;
	}

	const auto &column_ref = expr.Cast<BoundColumnRefExpression>();
	column_t binding_index = column_ref.Binding().column_index;

	// Map the binding index to actual column.
	if (binding_index >= column_ids.size()) {
		return -1;
	}

	column_t dim_index = column_ids[binding_index];
	if (dim_index >= data_structure.size()) {
		return -1;
	}
	return static_cast<int>(dim_index);
}

bool FilterEncoder::EncodeExpressionNode(const Expression &expr, const std::vector<eurostat::Dimension> &data_structure,
                                         const std::vector<column_t> &column_ids, EurostatFilterSet &out_result) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::LEGACY_BOUND_COMPARISON: {
		const auto &comparison = expr.Cast<LegacyBoundComparisonExpression>();
		if (comparison.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		    comparison.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			int dim_idx = GetDimensionIndexFromColumnRef(*comparison.left, data_structure, column_ids);
			if (dim_idx != -1) {
				const auto &dimension = data_structure[dim_idx];
				const auto &const_expr = comparison.right->Cast<BoundConstantExpression>();
				LegacyConstantFilter const_f(expr.GetExpressionType(), const_expr.GetValue());

				return EncodeConstantComparison(const_f, dimension, dim_idx, out_result);
			}
		}

		out_result.supported = false;
		return false;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		const auto &function = expr.Cast<BoundFunctionExpression>();
		if (BoundComparisonExpression::IsComparison(expr)) {
			const auto &left = BoundComparisonExpression::Left(function);
			const auto &right = BoundComparisonExpression::Right(function);
			if (left.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				int dim_idx = GetDimensionIndexFromColumnRef(left, data_structure, column_ids);
				if (dim_idx != -1) {
					const auto &dimension = data_structure[dim_idx];
					const auto &constant = right.Cast<BoundConstantExpression>().GetValue();
					LegacyConstantFilter const_f(expr.GetExpressionType(), constant);
					return EncodeConstantComparison(const_f, dimension, dim_idx, out_result);
				}
			}

			out_result.supported = false;
			return false;
		}

		if (expr.GetExpressionType() == ExpressionType::COMPARE_BETWEEN &&
		    BoundBetweenExpression::HasValidBindData(function)) {
			// Handle column BETWEEN constant AND constant comparisons.

			const auto &input = BoundBetweenExpression::Input(function);
			const auto &lower = BoundBetweenExpression::LowerBound(function);
			const auto &upper = BoundBetweenExpression::UpperBound(function);
			if (input.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
			    lower.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT ||
			    upper.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
				out_result.supported = false;
				return false;
			}

			int dim_idx = GetDimensionIndexFromColumnRef(input, data_structure, column_ids);
			if (dim_idx != -1) {
				const auto &dimension = data_structure[dim_idx];

				// Only supported for time periods, encoded as startPeriod and endPeriod.
				if (dimension.name == TIME_PERIOD_DIMENSION_NAME) {
					const auto &lower_const = lower.Cast<BoundConstantExpression>();
					const auto &upper_const = upper.Cast<BoundConstantExpression>();

					auto &out_filter = out_result.GetCurrentFilter();
					out_filter.start_period = "startPeriod=" + lower_const.GetValue().ToString();
					out_filter.end_period = "endPeriod=" + upper_const.GetValue().ToString();
					return true;
				}
			}
		}

		out_result.supported = false;
		return false;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		const auto &op = expr.Cast<BoundOperatorExpression>();
		const auto &children = op.GetChildren();

		// Handle column IN (values) comparisons.

		if (expr.GetExpressionType() != ExpressionType::COMPARE_IN) {
			out_result.supported = false;
			return false;
		}
		if (children.size() < 2) {
			throw InvalidInputException("Operator needs at least two children");
		}
		if (children[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			// we need col IN (...) to be able to push this down.
			out_result.supported = false;
			return false;
		}

		// Build the IN filter values.

		int dim_idx = GetDimensionIndexFromColumnRef(*children[0], data_structure, column_ids);
		if (dim_idx != -1) {
			vector<Value> values;

			for (idx_t child = 1; child < children.size(); child++) {
				if (children[child]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					out_result.supported = false;
					return false;
				} else {
					const auto &const_expr = children[child]->Cast<BoundConstantExpression>();
					values.emplace_back(const_expr.GetValue());
				}
			}

			LegacyInFilter in_f(values);
			const auto &dimension = data_structure[dim_idx];

			return EncodeInFilter(in_f, dimension, dim_idx, out_result);
		}

		out_result.supported = false;
		return false;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		const auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		const auto &children = conjunction.GetChildren();

		if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
			// For AND, all children must be supported.
			for (const auto &child : children) {
				if (!EncodeExpressionNode(*child, data_structure, column_ids, out_result)) {
					out_result.supported = false;
					return false;
				}
			}
			return true;
		}
		if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
			// For OR, create separate dimension filter for each child.
			for (const auto &child : children) {
				if (!EncodeExpressionNode(*child, data_structure, column_ids, out_result)) {
					out_result.supported = false;
					return false;
				}
				out_result.PushEmptyFilter();
			}
			return true;
		}

		out_result.supported = false;
		return false;
	}
	default:
		EUROSTAT_SCAN_DEBUG_LOG(1, "EncodeExpressionNode: Class %d not supported", (int)expr.GetExpressionClass());

		out_result.supported = false;
		return false;
	}
}

} // namespace duckdb
