package types

import "fmt"

// DataType represents the type of a column value
type DataType int

const (
	Int DataType = iota
	Float
	String
)

func (dt DataType) String() string {
	switch dt {
	case Int:
		return "Int"
	case Float:
		return "Float"
	case String:
		return "String"
	default:
		return "Unknown"
	}
}

// Schema describes the structure of a row
type Schema struct {
	Columns []string   // Column names
	Types   []DataType // Column types
}

// ColumnIndex returns the index of a column by name, or -1 if not found
func (s Schema) ColumnIndex(name string) int {
	for i, col := range s.Columns {
		if col == name {
			return i
		}
	}
	return -1
}

// Row represents a single row of data
type Row struct {
	Values []interface{}
}

// GetInt returns the integer value at the given index
func (r *Row) GetInt(idx int) (int64, bool) {
	if idx < 0 || idx >= len(r.Values) {
		return 0, false
	}
	v, ok := r.Values[idx].(int64)
	return v, ok
}

// GetFloat returns the float value at the given index
func (r *Row) GetFloat(idx int) (float64, bool) {
	if idx < 0 || idx >= len(r.Values) {
		return 0, false
	}
	switch v := r.Values[idx].(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}

// GetString returns the string value at the given index
func (r *Row) GetString(idx int) (string, bool) {
	if idx < 0 || idx >= len(r.Values) {
		return "", false
	}
	v, ok := r.Values[idx].(string)
	return v, ok
}

// String returns a string representation of the row
func (r *Row) String() string {
	return fmt.Sprintf("%v", r.Values)
}

// Operator is the Volcano Iterator interface
// Every operator (Scan, Filter, Sort, Limit, etc.) implements this interface
// allowing them to be chained: Limit(Sort(Filter(Scan())))
type Operator interface {
	// Next returns the next row, or (nil, nil) when exhausted
	Next() (*Row, error)

	// Close releases resources held by this operator
	Close() error

	// Schema returns the schema of rows produced by this operator
	Schema() Schema
}

// Comparator defines comparison operations for WHERE clauses
type Comparator int

const (
	Eq  Comparator = iota // =
	Lt                    // <
	Gt                    // >
	Lte                   // <=
	Gte                   // >=
	Neq                   // != or <>
)

func (c Comparator) String() string {
	switch c {
	case Eq:
		return "="
	case Lt:
		return "<"
	case Gt:
		return ">"
	case Lte:
		return "<="
	case Gte:
		return ">="
	case Neq:
		return "!="
	default:
		return "?"
	}
}

// AggregateType defines aggregation functions
type AggregateType int

const (
	Count AggregateType = iota
	Sum
	Min
	Max
	Avg
)

func (a AggregateType) String() string {
	switch a {
	case Count:
		return "COUNT"
	case Sum:
		return "SUM"
	case Min:
		return "MIN"
	case Max:
		return "MAX"
	case Avg:
		return "AVG"
	default:
		return "?"
	}
}

