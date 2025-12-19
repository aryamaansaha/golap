package operators

import (
	"github.com/aryamaan/golap/types"
)

// LimitOp limits the number of rows returned
type LimitOp struct {
	input   types.Operator
	limit   int
	offset  int // Optional: skip first N rows (for OFFSET clause)
	count   int // Current count of returned rows
	skipped int // Current count of skipped rows (for OFFSET)
}

// NewLimitOp creates a new limit operator
func NewLimitOp(input types.Operator, limit int) *LimitOp {
	return &LimitOp{
		input:  input,
		limit:  limit,
		offset: 0,
		count:  0,
	}
}

// NewLimitOffsetOp creates a limit operator with offset (LIMIT N OFFSET M)
func NewLimitOffsetOp(input types.Operator, limit, offset int) *LimitOp {
	return &LimitOp{
		input:  input,
		limit:  limit,
		offset: offset,
		count:  0,
	}
}

// Next returns the next row, stopping after limit rows
func (l *LimitOp) Next() (*types.Row, error) {
	// Skip rows for OFFSET
	for l.skipped < l.offset {
		row, err := l.input.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil // Exhausted before reaching offset
		}
		l.skipped++
	}

	// Check if we've hit the limit
	if l.count >= l.limit {
		return nil, nil
	}

	row, err := l.input.Next()
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}

	l.count++
	return row, nil
}

// Close releases resources
func (l *LimitOp) Close() error {
	return l.input.Close()
}

// Schema returns the schema (unchanged from input)
func (l *LimitOp) Schema() types.Schema {
	return l.input.Schema()
}

