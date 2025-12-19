package operators

import (
	"fmt"

	"github.com/aryamaansaha/golap/types"
)

// Predicate is a function that evaluates whether a row passes the filter
type Predicate func(*types.Row) bool

// FilterOp filters rows based on a predicate (WHERE clause)
type FilterOp struct {
	input     types.Operator
	predicate Predicate
}

// NewFilterOp creates a new filter operator
func NewFilterOp(input types.Operator, predicate Predicate) *FilterOp {
	return &FilterOp{
		input:     input,
		predicate: predicate,
	}
}

// Next returns the next row that passes the predicate
// Rows that fail the predicate are skipped
func (f *FilterOp) Next() (*types.Row, error) {
	for {
		row, err := f.input.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil // End of input
		}

		if f.predicate(row) {
			return row, nil
		}
		// Row failed predicate, continue to next
	}
}

// Close releases resources
func (f *FilterOp) Close() error {
	return f.input.Close()
}

// Schema returns the schema (unchanged from input)
func (f *FilterOp) Schema() types.Schema {
	return f.input.Schema()
}

// Comparison represents a single comparison condition in a WHERE clause
type Comparison struct {
	ColumnIndex int
	Comparator  types.Comparator
	Value       interface{} // int64, float64, or string
}

// BuildComparisonPredicate creates a predicate from a comparison
func BuildComparisonPredicate(comp Comparison) Predicate {
	return func(row *types.Row) bool {
		if comp.ColumnIndex < 0 || comp.ColumnIndex >= len(row.Values) {
			return false
		}

		rowVal := row.Values[comp.ColumnIndex]
		return compare(rowVal, comp.Comparator, comp.Value)
	}
}

// compare performs the comparison based on the comparator type
func compare(left interface{}, comp types.Comparator, right interface{}) bool {
	// Handle integer comparisons
	if leftInt, ok := left.(int64); ok {
		rightInt, ok := toInt64(right)
		if !ok {
			return false
		}
		return compareInt64(leftInt, comp, rightInt)
	}

	// Handle float comparisons
	if leftFloat, ok := left.(float64); ok {
		rightFloat, ok := toFloat64(right)
		if !ok {
			return false
		}
		return compareFloat64(leftFloat, comp, rightFloat)
	}

	// Handle string comparisons
	if leftStr, ok := left.(string); ok {
		rightStr, ok := right.(string)
		if !ok {
			rightStr = fmt.Sprintf("%v", right)
		}
		return compareString(leftStr, comp, rightStr)
	}

	return false
}

func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	case float64:
		return int64(val), true
	default:
		return 0, false
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int64:
		return float64(val), true
	case int:
		return float64(val), true
	default:
		return 0, false
	}
}

func compareInt64(left int64, comp types.Comparator, right int64) bool {
	switch comp {
	case types.Eq:
		return left == right
	case types.Lt:
		return left < right
	case types.Gt:
		return left > right
	case types.Lte:
		return left <= right
	case types.Gte:
		return left >= right
	case types.Neq:
		return left != right
	default:
		return false
	}
}

func compareFloat64(left float64, comp types.Comparator, right float64) bool {
	switch comp {
	case types.Eq:
		return left == right
	case types.Lt:
		return left < right
	case types.Gt:
		return left > right
	case types.Lte:
		return left <= right
	case types.Gte:
		return left >= right
	case types.Neq:
		return left != right
	default:
		return false
	}
}

func compareString(left string, comp types.Comparator, right string) bool {
	switch comp {
	case types.Eq:
		return left == right
	case types.Lt:
		return left < right
	case types.Gt:
		return left > right
	case types.Lte:
		return left <= right
	case types.Gte:
		return left >= right
	case types.Neq:
		return left != right
	default:
		return false
	}
}

// AndPredicate combines multiple predicates with AND logic
func AndPredicate(predicates ...Predicate) Predicate {
	return func(row *types.Row) bool {
		for _, p := range predicates {
			if !p(row) {
				return false
			}
		}
		return true
	}
}
