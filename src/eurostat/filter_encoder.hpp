#pragma once

#include "eurostat.hpp"
#include "duckdb.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/filter/in_filter.hpp"

namespace duckdb {

/**
 * Result of encoding a single T-SQL expression or filter to an Eurostat filter.
 */
struct EurostatFilter {

	//! Reference to a data structure of dimensions.
	const std::vector<eurostat::Dimension> &data_structure;

	//! Dimension filter mask (e.g., "A.B+X.C.D+Y").
	std::vector<std::string> dim_mask;
	//! Start period filter.
	std::string start_period;
	//! End period filter.
	std::string end_period;

	//! Constructor.
	EurostatFilter(const std::vector<eurostat::Dimension> &ds);

	//! Check if the expression is empty.
	bool IsEmpty() const;

	//! Get the Eurostat filter string from the expression.
	std::string GetFilterString() const;
};

/**
 * Result of encoding an entire T-SQL filter to a set of Eurostat filter clauses.
 */
struct EurostatFilterSet {

	//! Reference to a data structure of dimensions.
	const std::vector<eurostat::Dimension> &data_structure;

	//! Set of encoded Eurostat filters.
	std::vector<EurostatFilter> filters;
	//! True if filter was encoded.
	bool supported = false;

	//! Constructor.
	EurostatFilterSet(const std::vector<eurostat::Dimension> &ds) : data_structure(ds) {
		PushEmptyFilter();
	}

	//! Add a new empty Eurostat filter.
	void PushEmptyFilter();

	//! Get the current Eurostat filter, the last one in the stack.
	EurostatFilter &GetCurrentFilter() {
		return filters.back();
	}
};

/**
 * Result of encoding an entire T-SQL filter to Eurostat filter clauses.
 */
struct FilterEncoderResult {
	//! Set of encoded Eurostat filters.
	std::vector<std::string> filters;
	//! True if entire filter was fully encoded
	bool supported = false;
};

/**
 * Main filter encoder class.
 * Converts DuckDB filter expressions to Eurostat API filter clauses.
 *
 * See Eurostat API filtering documentation:
 * https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx2-1/data-query#estat-inpage-nav-heading-2
 */
class FilterEncoder {
public:
	/**
	 * Encode a TableFilterSet to Eurostat filter clauses.
	 */
	static FilterEncoderResult Encode(const TableFilterSet *filters,
	                                  const std::vector<eurostat::Dimension> &data_structure,
	                                  const std::vector<column_t> &column_ids);

	/**
	 * Encode a complex T-SQL Expression to a set of Eurostat API filter clauses.
	 * Handles BoundComparisonExpression, BoundConjunctionExpression, etc.
	 */
	static FilterEncoderResult EncodeExpression(vector<unique_ptr<Expression>> &expressions,
	                                            const std::vector<eurostat::Dimension> &data_structure,
	                                            const std::vector<column_t> &column_ids);

private:
	/**
	 * Get comparison operator given a DuckDB ExpressionType.
	 */
	static bool GetComparisonOperator(ExpressionType type, std::string &out_operator);

	//--------------------------------------------------------------------------
	// TableFilter Encoding
	//--------------------------------------------------------------------------

	/**
	 * Encode a single TableFilter to a set of Eurostat API filter clauses.
	 */
	static bool EncodeFilter(const TableFilter &filter, const eurostat::Dimension &dimension, const idx_t &dim_index,
	                         EurostatFilterSet &out_result);

	/**
	 * Encode CONSTANT_COMPARISON filter (col OP value).
	 */
	static bool EncodeConstantComparison(const ConstantFilter &filter, const eurostat::Dimension &dimension,
	                                     const idx_t &dim_index, EurostatFilterSet &out_result);

	/**
	 * Encode IN_FILTER (col IN (values)).
	 */
	static bool EncodeInFilter(const InFilter &filter, const eurostat::Dimension &dimension, const idx_t &dim_index,
	                           EurostatFilterSet &out_result);

	/**
	 * Encode CONJUNCTION_AND filter.
	 * All-or-nothing: if any child unsupported, entire AND is skipped.
	 */
	static bool EncodeConjunctionAnd(const ConjunctionAndFilter &filter, const eurostat::Dimension &dimension,
	                                 const idx_t &dim_index, EurostatFilterSet &out_result);

	/**
	 * Encode CONJUNCTION_OR filter.
	 * All-or-nothing: if any child unsupported, entire OR is skipped.
	 */
	static bool EncodeConjunctionOr(const ConjunctionOrFilter &filter, const eurostat::Dimension &dimension,
	                                const idx_t &dim_index, EurostatFilterSet &out_result);

	//--------------------------------------------------------------------------
	// Expression Encoding
	//--------------------------------------------------------------------------

	/**
	 * Get the dimension index from a BoundColumnRefExpression.
	 */
	static int GetDimensionIndexFromColumnRef(const Expression &expr,
	                                          const std::vector<eurostat::Dimension> &data_structure,
	                                          const std::vector<column_t> &column_ids);

	/**
	 * Encode a complex Expression node.
	 */
	static bool EncodeExpressionNode(const Expression &expr, const std::vector<eurostat::Dimension> &data_structure,
	                                 const std::vector<column_t> &column_ids, EurostatFilterSet &out_result);
};

} // namespace duckdb
