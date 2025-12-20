package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/aryamaansaha/golap/operators"
	"github.com/aryamaansaha/golap/types"
	"github.com/xwb1989/sqlparser"
)

// ParseAndPlan parses a SQL query and builds an operator tree
// Query Format: SELECT ... FROM "file.csv" WHERE ... ORDER BY ... LIMIT ...
// sortChunkSize controls memory usage for ORDER BY (number of rows per chunk)
func ParseAndPlan(sql string, sortChunkSize int) (types.Operator, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL parse error: %w", err)
	}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("only SELECT statements are supported")
	}

	// Extract table name (file path)
	if len(selectStmt.From) != 1 {
		return nil, fmt.Errorf("exactly one table (CSV file) required in FROM clause")
	}

	tableName, err := extractTableName(selectStmt.From[0])
	if err != nil {
		return nil, err
	}

	// Build operator chain from inside out:
	// Scan -> Filter -> Aggregate -> Sort -> Limit -> Project

	// 1. Start with CSV Scan
	scan, err := operators.NewCSVScan(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSV scan: %w", err)
	}

	var op types.Operator = scan
	schema := scan.Schema()

	// 2. Apply WHERE filters
	if selectStmt.Where != nil {
		predicates, err := buildPredicates(selectStmt.Where.Expr, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to build WHERE predicates: %w", err)
		}
		for _, pred := range predicates {
			op = operators.NewFilterOp(op, pred)
		}
	}

	// 3. Check for aggregates and GROUP BY
	aggregates, selectColumns, hasAggregates := parseSelectExprs(selectStmt.SelectExprs, schema)

	if hasAggregates {
		// Build aggregate operator
		if len(selectStmt.GroupBy) > 0 {
			// Hash aggregate with GROUP BY
			groupByIndices := make([]int, len(selectStmt.GroupBy))
			for i, expr := range selectStmt.GroupBy {
				colName := sqlparser.String(expr)
				colName = strings.Trim(colName, "`\"")
				groupByIndices[i] = schema.ColumnIndex(colName)
			}
			op = operators.NewHashAggregateOp(op, groupByIndices, aggregates)
		} else {
			// Scalar aggregate (no GROUP BY)
			op = operators.NewScalarAggregateOp(op, aggregates)
		}
		// Update schema after aggregation
		schema = op.Schema()
	}

	// 4. Apply ORDER BY
	if len(selectStmt.OrderBy) > 0 {
		// MVP: single column ORDER BY only
		orderExpr := selectStmt.OrderBy[0]
		colName := sqlparser.String(orderExpr.Expr)
		colName = strings.Trim(colName, "`\"")

		// Find column index in current schema
		colIdx := schema.ColumnIndex(colName)
		if colIdx < 0 {
			return nil, fmt.Errorf("ORDER BY column not found: %s", colName)
		}

		desc := orderExpr.Direction == sqlparser.DescScr
		op = operators.NewSortOpWithChunkSize(op, colIdx, desc, sortChunkSize)
	}

	// 5. Apply LIMIT
	if selectStmt.Limit != nil {
		limitVal, err := parseLimit(selectStmt.Limit)
		if err != nil {
			return nil, err
		}
		op = operators.NewLimitOp(op, limitVal)
	}

	// 6. Apply projection (SELECT columns) - last step
	if !hasAggregates && len(selectColumns) > 0 {
		// Only project if we have specific columns (not SELECT *)
		// After aggregation, the schema is already correct
		op = operators.NewProjectOp(op, selectColumns)
	}

	return op, nil
}

// extractTableName gets the file path from the FROM clause
func extractTableName(tableExpr sqlparser.TableExpr) (string, error) {
	switch t := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch expr := t.Expr.(type) {
		case sqlparser.TableName:
			name := expr.Name.String()
			// Remove backticks or quotes if present
			name = strings.Trim(name, "`\"")
			return name, nil
		default:
			return "", fmt.Errorf("unsupported table expression type")
		}
	default:
		return "", fmt.Errorf("unsupported FROM clause type")
	}
}

// buildPredicates converts WHERE expression to filter predicates
// Returns multiple predicates for implicit AND chaining
func buildPredicates(expr sqlparser.Expr, schema types.Schema) ([]operators.Predicate, error) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		// Recursively handle AND
		left, err := buildPredicates(e.Left, schema)
		if err != nil {
			return nil, err
		}
		right, err := buildPredicates(e.Right, schema)
		if err != nil {
			return nil, err
		}
		return append(left, right...), nil

	case *sqlparser.ComparisonExpr:
		return buildComparisonPredicate(e, schema)

	case *sqlparser.ParenExpr:
		return buildPredicates(e.Expr, schema)

	default:
		return nil, fmt.Errorf("unsupported WHERE expression type: %T", expr)
	}
}

// buildComparisonPredicate builds a single comparison predicate
func buildComparisonPredicate(expr *sqlparser.ComparisonExpr, schema types.Schema) ([]operators.Predicate, error) {
	// Get column name from left side
	colName, err := extractColumnName(expr.Left)
	if err != nil {
		return nil, err
	}

	colIdx := schema.ColumnIndex(colName)
	if colIdx < 0 {
		return nil, fmt.Errorf("column not found in schema: %s", colName)
	}

	// Get comparison value from right side
	value, err := extractValue(expr.Right)
	if err != nil {
		return nil, err
	}

	// Map operator
	var comp types.Comparator
	switch expr.Operator {
	case "=":
		comp = types.Eq
	case "<":
		comp = types.Lt
	case ">":
		comp = types.Gt
	case "<=":
		comp = types.Lte
	case ">=":
		comp = types.Gte
	case "!=", "<>":
		comp = types.Neq
	default:
		return nil, fmt.Errorf("unsupported comparison operator: %s", expr.Operator)
	}

	comparison := operators.Comparison{
		ColumnIndex: colIdx,
		Comparator:  comp,
		Value:       value,
	}

	pred := operators.BuildComparisonPredicate(comparison)
	return []operators.Predicate{pred}, nil
}

// extractColumnName gets column name from an expression
func extractColumnName(expr sqlparser.Expr) (string, error) {
	switch e := expr.(type) {
	case *sqlparser.ColName:
		name := e.Name.String()
		return strings.Trim(name, "`\""), nil
	default:
		return "", fmt.Errorf("expected column name, got: %T", expr)
	}
}

// extractValue gets a literal value from an expression
func extractValue(expr sqlparser.Expr) (interface{}, error) {
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		switch e.Type {
		case sqlparser.IntVal:
			val, err := strconv.ParseInt(string(e.Val), 10, 64)
			if err != nil {
				return nil, err
			}
			return val, nil
		case sqlparser.FloatVal:
			val, err := strconv.ParseFloat(string(e.Val), 64)
			if err != nil {
				return nil, err
			}
			return val, nil
		case sqlparser.StrVal:
			return string(e.Val), nil
		default:
			return string(e.Val), nil
		}
	default:
		return nil, fmt.Errorf("unsupported value type: %T", expr)
	}
}

// parseSelectExprs analyzes SELECT expressions for aggregates and columns
// Returns: aggregate expressions, column indices for projection, whether aggregates exist
func parseSelectExprs(exprs sqlparser.SelectExprs, schema types.Schema) ([]operators.AggregateExpr, []int, bool) {
	var aggregates []operators.AggregateExpr
	var columns []int
	hasAggregates := false
	isSelectStar := false

	for _, expr := range exprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			isSelectStar = true

		case *sqlparser.AliasedExpr:
			alias := e.As.String()
			alias = strings.Trim(alias, "`\"")

			switch inner := e.Expr.(type) {
			case *sqlparser.FuncExpr:
				// Aggregate function
				hasAggregates = true
				agg, err := parseAggregateFunc(inner, schema, alias)
				if err == nil {
					aggregates = append(aggregates, agg)
				}

			case *sqlparser.ColName:
				// Regular column
				colName := inner.Name.String()
				colName = strings.Trim(colName, "`\"")
				colIdx := schema.ColumnIndex(colName)
				if colIdx >= 0 {
					columns = append(columns, colIdx)
				}
			}
		}
	}

	// SELECT * means no projection needed
	if isSelectStar {
		columns = nil
	}

	return aggregates, columns, hasAggregates
}

// parseAggregateFunc parses an aggregate function call
func parseAggregateFunc(fn *sqlparser.FuncExpr, schema types.Schema, alias string) (operators.AggregateExpr, error) {
	funcName := strings.ToUpper(fn.Name.String())

	var aggType types.AggregateType
	switch funcName {
	case "COUNT":
		aggType = types.Count
	case "SUM":
		aggType = types.Sum
	case "MIN":
		aggType = types.Min
	case "MAX":
		aggType = types.Max
	case "AVG":
		aggType = types.Avg
	default:
		return operators.AggregateExpr{}, fmt.Errorf("unsupported aggregate function: %s", funcName)
	}

	// Get column index (or -1 for COUNT(*))
	colIdx := -1
	if len(fn.Exprs) > 0 {
		switch arg := fn.Exprs[0].(type) {
		case *sqlparser.StarExpr:
			colIdx = -1 // COUNT(*)
		case *sqlparser.AliasedExpr:
			if colName, ok := arg.Expr.(*sqlparser.ColName); ok {
				name := strings.Trim(colName.Name.String(), "`\"")
				colIdx = schema.ColumnIndex(name)
			}
		}
	}

	// Default alias if not provided
	if alias == "" {
		if colIdx >= 0 && colIdx < len(schema.Columns) {
			alias = fmt.Sprintf("%s(%s)", funcName, schema.Columns[colIdx])
		} else {
			alias = fmt.Sprintf("%s(*)", funcName)
		}
	}

	return operators.AggregateExpr{
		Type:        aggType,
		ColumnIndex: colIdx,
		Alias:       alias,
	}, nil
}

// parseLimit extracts the limit value
func parseLimit(limit *sqlparser.Limit) (int, error) {
	if limit.Rowcount == nil {
		return 0, fmt.Errorf("LIMIT requires a value")
	}

	switch v := limit.Rowcount.(type) {
	case *sqlparser.SQLVal:
		if v.Type == sqlparser.IntVal {
			val, err := strconv.Atoi(string(v.Val))
			if err != nil {
				return 0, err
			}
			return val, nil
		}
	}

	return 0, fmt.Errorf("LIMIT must be an integer")
}
