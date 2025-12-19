package operators

import (
	"fmt"
	"math"

	"github.com/aryamaan/golap/types"
)

// AggregateExpr represents a single aggregation expression
type AggregateExpr struct {
	Type        types.AggregateType
	ColumnIndex int    // Column to aggregate (-1 for COUNT(*))
	Alias       string // Output column name
}

// aggregateState holds the running state for one aggregate computation
type aggregateState struct {
	count   int64
	sum     float64
	min     float64
	max     float64
	hasData bool
}

// ScalarAggregateOp performs scalar aggregation (no GROUP BY)
// Returns a single row with aggregated values
type ScalarAggregateOp struct {
	input        types.Operator
	aggregates   []AggregateExpr
	outputSchema types.Schema
	computed     bool
	resultRow    *types.Row
}

// NewScalarAggregateOp creates a scalar aggregate operator
func NewScalarAggregateOp(input types.Operator, aggregates []AggregateExpr) *ScalarAggregateOp {
	// Build output schema
	columns := make([]string, len(aggregates))
	colTypes := make([]types.DataType, len(aggregates))
	for i, agg := range aggregates {
		if agg.Alias != "" {
			columns[i] = agg.Alias
		} else {
			columns[i] = fmt.Sprintf("%s_%d", agg.Type.String(), i)
		}
		// COUNT returns Int, others return Float for precision
		if agg.Type == types.Count {
			colTypes[i] = types.Int
		} else {
			colTypes[i] = types.Float
		}
	}

	return &ScalarAggregateOp{
		input:      input,
		aggregates: aggregates,
		outputSchema: types.Schema{
			Columns: columns,
			Types:   colTypes,
		},
		computed: false,
	}
}

// Next computes and returns the aggregate result (single row)
func (s *ScalarAggregateOp) Next() (*types.Row, error) {
	if s.computed {
		return nil, nil // Already returned the single result
	}

	// Initialize state for each aggregate
	states := make([]aggregateState, len(s.aggregates))
	for i := range states {
		states[i].min = math.MaxFloat64
		states[i].max = -math.MaxFloat64
	}

	// Stream through all input and update running state
	for {
		row, err := s.input.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			break
		}

		// Update each aggregate's state
		for i, agg := range s.aggregates {
			s.updateState(&states[i], agg, row)
		}
	}

	// Compute final results
	values := make([]interface{}, len(s.aggregates))
	for i, agg := range s.aggregates {
		values[i] = s.finalizeState(&states[i], agg)
	}

	s.computed = true
	s.resultRow = &types.Row{Values: values}
	return s.resultRow, nil
}

func (s *ScalarAggregateOp) updateState(state *aggregateState, agg AggregateExpr, row *types.Row) {
	state.count++

	// For COUNT(*), we don't need the column value
	if agg.Type == types.Count && agg.ColumnIndex < 0 {
		state.hasData = true
		return
	}

	// Get column value
	if agg.ColumnIndex < 0 || agg.ColumnIndex >= len(row.Values) {
		return
	}

	val := row.Values[agg.ColumnIndex]
	numVal, ok := toNumericValue(val)
	if !ok {
		return
	}

	state.hasData = true
	state.sum += numVal

	if numVal < state.min {
		state.min = numVal
	}
	if numVal > state.max {
		state.max = numVal
	}
}

func (s *ScalarAggregateOp) finalizeState(state *aggregateState, agg AggregateExpr) interface{} {
	switch agg.Type {
	case types.Count:
		return state.count
	case types.Sum:
		if !state.hasData {
			return float64(0)
		}
		return state.sum
	case types.Min:
		if !state.hasData {
			return nil
		}
		return state.min
	case types.Max:
		if !state.hasData {
			return nil
		}
		return state.max
	case types.Avg:
		if state.count == 0 {
			return nil
		}
		return state.sum / float64(state.count)
	default:
		return nil
	}
}

// Close releases resources
func (s *ScalarAggregateOp) Close() error {
	return s.input.Close()
}

// Schema returns the output schema
func (s *ScalarAggregateOp) Schema() types.Schema {
	return s.outputSchema
}

// HashAggregateOp performs aggregation with GROUP BY
type HashAggregateOp struct {
	input          types.Operator
	groupByIndices []int // Columns to group by
	aggregates     []AggregateExpr
	outputSchema   types.Schema

	// State
	computed bool
	groups   map[string]*groupState
	keys     []string // Preserve insertion order
	keyIndex int
}

type groupState struct {
	keyValues []interface{}
	states    []aggregateState
}

// NewHashAggregateOp creates a hash aggregate operator with GROUP BY
func NewHashAggregateOp(input types.Operator, groupByIndices []int, aggregates []AggregateExpr) *HashAggregateOp {
	inputSchema := input.Schema()

	// Build output schema: GROUP BY columns + aggregate columns
	numCols := len(groupByIndices) + len(aggregates)
	columns := make([]string, numCols)
	colTypes := make([]types.DataType, numCols)

	// Group by columns first
	for i, idx := range groupByIndices {
		if idx >= 0 && idx < len(inputSchema.Columns) {
			columns[i] = inputSchema.Columns[idx]
			colTypes[i] = inputSchema.Types[idx]
		}
	}

	// Then aggregate columns
	offset := len(groupByIndices)
	for i, agg := range aggregates {
		if agg.Alias != "" {
			columns[offset+i] = agg.Alias
		} else {
			columns[offset+i] = fmt.Sprintf("%s_%d", agg.Type.String(), i)
		}
		if agg.Type == types.Count {
			colTypes[offset+i] = types.Int
		} else {
			colTypes[offset+i] = types.Float
		}
	}

	return &HashAggregateOp{
		input:          input,
		groupByIndices: groupByIndices,
		aggregates:     aggregates,
		outputSchema: types.Schema{
			Columns: columns,
			Types:   colTypes,
		},
		computed: false,
		groups:   make(map[string]*groupState),
		keys:     []string{},
	}
}

// NewHashAggregateOpByNames creates a hash aggregate using column names
func NewHashAggregateOpByNames(input types.Operator, groupByNames []string, aggregates []AggregateExpr) *HashAggregateOp {
	inputSchema := input.Schema()
	indices := make([]int, len(groupByNames))
	for i, name := range groupByNames {
		indices[i] = inputSchema.ColumnIndex(name)
	}
	return NewHashAggregateOp(input, indices, aggregates)
}

// computeGroups processes all input and builds group states
func (h *HashAggregateOp) computeGroups() error {
	for {
		row, err := h.input.Next()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}

		// Build group key
		key := h.buildGroupKey(row)
		group, exists := h.groups[key]

		if !exists {
			// Create new group
			keyValues := make([]interface{}, len(h.groupByIndices))
			for i, idx := range h.groupByIndices {
				if idx >= 0 && idx < len(row.Values) {
					keyValues[i] = row.Values[idx]
				}
			}
			states := make([]aggregateState, len(h.aggregates))
			for i := range states {
				states[i].min = math.MaxFloat64
				states[i].max = -math.MaxFloat64
			}
			group = &groupState{
				keyValues: keyValues,
				states:    states,
			}
			h.groups[key] = group
			h.keys = append(h.keys, key)
		}

		// Update aggregate states for this group
		for i, agg := range h.aggregates {
			h.updateState(&group.states[i], agg, row)
		}
	}

	return nil
}

func (h *HashAggregateOp) buildGroupKey(row *types.Row) string {
	key := ""
	for i, idx := range h.groupByIndices {
		if i > 0 {
			key += "\x00" // Null separator
		}
		if idx >= 0 && idx < len(row.Values) {
			key += fmt.Sprintf("%v", row.Values[idx])
		}
	}
	return key
}

func (h *HashAggregateOp) updateState(state *aggregateState, agg AggregateExpr, row *types.Row) {
	state.count++

	if agg.Type == types.Count && agg.ColumnIndex < 0 {
		state.hasData = true
		return
	}

	if agg.ColumnIndex < 0 || agg.ColumnIndex >= len(row.Values) {
		return
	}

	val := row.Values[agg.ColumnIndex]
	numVal, ok := toNumericValue(val)
	if !ok {
		return
	}

	state.hasData = true
	state.sum += numVal

	if numVal < state.min {
		state.min = numVal
	}
	if numVal > state.max {
		state.max = numVal
	}
}

func (h *HashAggregateOp) finalizeState(state *aggregateState, agg AggregateExpr) interface{} {
	switch agg.Type {
	case types.Count:
		return state.count
	case types.Sum:
		if !state.hasData {
			return float64(0)
		}
		return state.sum
	case types.Min:
		if !state.hasData {
			return nil
		}
		return state.min
	case types.Max:
		if !state.hasData {
			return nil
		}
		return state.max
	case types.Avg:
		if state.count == 0 {
			return nil
		}
		return state.sum / float64(state.count)
	default:
		return nil
	}
}

// Next returns the next group's result
func (h *HashAggregateOp) Next() (*types.Row, error) {
	if !h.computed {
		if err := h.computeGroups(); err != nil {
			return nil, err
		}
		h.computed = true
	}

	if h.keyIndex >= len(h.keys) {
		return nil, nil
	}

	key := h.keys[h.keyIndex]
	h.keyIndex++

	group := h.groups[key]

	// Build output row: group key values + aggregated values
	values := make([]interface{}, len(h.groupByIndices)+len(h.aggregates))

	// Copy group key values
	for i, v := range group.keyValues {
		values[i] = v
	}

	// Compute aggregate results
	offset := len(h.groupByIndices)
	for i, agg := range h.aggregates {
		values[offset+i] = h.finalizeState(&group.states[i], agg)
	}

	return &types.Row{Values: values}, nil
}

// Close releases resources
func (h *HashAggregateOp) Close() error {
	return h.input.Close()
}

// Schema returns the output schema
func (h *HashAggregateOp) Schema() types.Schema {
	return h.outputSchema
}

// toNumericValue converts a value to float64 for aggregation
func toNumericValue(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case int:
		return float64(v), true
	default:
		return 0, false
	}
}

