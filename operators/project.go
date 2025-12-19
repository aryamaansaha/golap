package operators

import (
	"github.com/aryamaansaha/golap/types"
)

// ProjectOp projects (selects) specific columns from the input
type ProjectOp struct {
	input         types.Operator
	columnIndices []int        // Indices of columns to project
	outputSchema  types.Schema // Schema of projected output
	passthrough   bool         // If true, return input rows unchanged (SELECT *)
}

// NewProjectOp creates a new projection operator
// If columnIndices is nil or empty, operates in passthrough mode (SELECT *)
func NewProjectOp(input types.Operator, columnIndices []int) *ProjectOp {
	inputSchema := input.Schema()

	// Check for passthrough mode (SELECT *)
	passthrough := len(columnIndices) == 0

	var outputSchema types.Schema
	if passthrough {
		outputSchema = inputSchema
	} else {
		// Build output schema from selected columns
		columns := make([]string, len(columnIndices))
		colTypes := make([]types.DataType, len(columnIndices))
		for i, idx := range columnIndices {
			if idx >= 0 && idx < len(inputSchema.Columns) {
				columns[i] = inputSchema.Columns[idx]
				colTypes[i] = inputSchema.Types[idx]
			}
		}
		outputSchema = types.Schema{
			Columns: columns,
			Types:   colTypes,
		}
	}

	return &ProjectOp{
		input:         input,
		columnIndices: columnIndices,
		outputSchema:  outputSchema,
		passthrough:   passthrough,
	}
}

// NewProjectOpByNames creates a projection operator using column names
// If columnNames is nil or empty, operates in passthrough mode (SELECT *)
func NewProjectOpByNames(input types.Operator, columnNames []string) *ProjectOp {
	if len(columnNames) == 0 {
		return NewProjectOp(input, nil) // Passthrough
	}

	inputSchema := input.Schema()
	indices := make([]int, len(columnNames))
	for i, name := range columnNames {
		indices[i] = inputSchema.ColumnIndex(name)
	}

	return NewProjectOp(input, indices)
}

// Next returns the next projected row
func (p *ProjectOp) Next() (*types.Row, error) {
	row, err := p.input.Next()
	if err != nil || row == nil {
		return row, err
	}

	// Passthrough mode: return input row unchanged
	if p.passthrough {
		return row, nil
	}

	// Build projected row with only selected columns
	values := make([]interface{}, len(p.columnIndices))
	for i, idx := range p.columnIndices {
		if idx >= 0 && idx < len(row.Values) {
			values[i] = row.Values[idx]
		} else {
			values[i] = nil
		}
	}

	return &types.Row{Values: values}, nil
}

// Close releases resources
func (p *ProjectOp) Close() error {
	return p.input.Close()
}

// Schema returns the projected schema
func (p *ProjectOp) Schema() types.Schema {
	return p.outputSchema
}
