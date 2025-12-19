package operators

import (
	"container/heap"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"

	"github.com/aryamaansaha/golap/types"
)

const DefaultChunkSize = 1000

// SortOp performs external merge sort for ORDER BY
type SortOp struct {
	input       types.Operator
	columnIndex int    // Column to sort by
	desc        bool   // Descending order
	chunkSize   int    // Number of rows per chunk
	schema      types.Schema

	// State for merge phase
	prepared   bool
	tempFiles  []string
	readers    []*csv.Reader
	files      []*os.File
	mergeHeap  *mergeHeap
	exhausted  bool
}

// NewSortOp creates a new sort operator
func NewSortOp(input types.Operator, columnIndex int, desc bool) *SortOp {
	return NewSortOpWithChunkSize(input, columnIndex, desc, DefaultChunkSize)
}

// NewSortOpWithChunkSize creates a sort operator with custom chunk size
func NewSortOpWithChunkSize(input types.Operator, columnIndex int, desc bool, chunkSize int) *SortOp {
	return &SortOp{
		input:       input,
		columnIndex: columnIndex,
		desc:        desc,
		chunkSize:   chunkSize,
		schema:      input.Schema(),
		prepared:    false,
		tempFiles:   []string{},
	}
}

// NewSortOpByName creates a sort operator using column name
func NewSortOpByName(input types.Operator, columnName string, desc bool) *SortOp {
	schema := input.Schema()
	columnIndex := schema.ColumnIndex(columnName)
	return NewSortOp(input, columnIndex, desc)
}

// prepare consumes all input, creates sorted chunks on disk, and prepares for merge
func (s *SortOp) prepare() error {
	if s.prepared {
		return nil
	}

	// Phase 1: Chunk and flush sorted runs to temp files
	chunk := make([]*types.Row, 0, s.chunkSize)

	for {
		row, err := s.input.Next()
		if err != nil {
			return fmt.Errorf("error reading input for sort: %w", err)
		}
		if row == nil {
			break // Input exhausted
		}

		chunk = append(chunk, row)

		if len(chunk) >= s.chunkSize {
			if err := s.flushChunk(chunk); err != nil {
				return err
			}
			chunk = make([]*types.Row, 0, s.chunkSize)
		}
	}

	// Flush remaining rows
	if len(chunk) > 0 {
		if err := s.flushChunk(chunk); err != nil {
			return err
		}
	}

	// Phase 2: Set up K-way merge
	if err := s.setupMerge(); err != nil {
		return err
	}

	s.prepared = true
	return nil
}

// flushChunk sorts a chunk in memory and writes it to a temp file
func (s *SortOp) flushChunk(chunk []*types.Row) error {
	// Sort chunk in memory
	sort.Slice(chunk, func(i, j int) bool {
		cmp := s.compareRows(chunk[i], chunk[j])
		if s.desc {
			return cmp > 0
		}
		return cmp < 0
	})

	// Create temp file
	tempFile, err := os.CreateTemp("", "golap_sort_*.csv")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempFile.Close()

	// Write sorted chunk to temp file
	writer := csv.NewWriter(tempFile)
	for _, row := range chunk {
		record := s.rowToRecord(row)
		if err := writer.Write(record); err != nil {
			os.Remove(tempFile.Name())
			return fmt.Errorf("failed to write to temp file: %w", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("failed to flush temp file: %w", err)
	}

	s.tempFiles = append(s.tempFiles, tempFile.Name())
	return nil
}

// rowToRecord converts a Row to a CSV record (string slice)
func (s *SortOp) rowToRecord(row *types.Row) []string {
	record := make([]string, len(row.Values))
	for i, val := range row.Values {
		switch v := val.(type) {
		case int64:
			record[i] = strconv.FormatInt(v, 10)
		case float64:
			record[i] = strconv.FormatFloat(v, 'f', -1, 64)
		case string:
			record[i] = v
		default:
			record[i] = fmt.Sprintf("%v", val)
		}
	}
	return record
}

// recordToRow converts a CSV record back to a Row
func (s *SortOp) recordToRow(record []string) *types.Row {
	values := make([]interface{}, len(record))
	for i, val := range record {
		if i < len(s.schema.Types) {
			values[i] = parseValue(val, s.schema.Types[i])
		} else {
			values[i] = val
		}
	}
	return &types.Row{Values: values}
}

// setupMerge opens all temp files and initializes the merge heap
func (s *SortOp) setupMerge() error {
	if len(s.tempFiles) == 0 {
		s.exhausted = true
		return nil
	}

	s.readers = make([]*csv.Reader, len(s.tempFiles))
	s.files = make([]*os.File, len(s.tempFiles))
	s.mergeHeap = &mergeHeap{
		items:       make([]*heapItem, 0, len(s.tempFiles)),
		columnIndex: s.columnIndex,
		desc:        s.desc,
	}
	heap.Init(s.mergeHeap)

	// Open each temp file and push first row to heap
	for i, path := range s.tempFiles {
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open temp file for merge: %w", err)
		}
		s.files[i] = file
		s.readers[i] = csv.NewReader(file)

		// Read first row from this file
		record, err := s.readers[i].Read()
		if err == io.EOF {
			continue // Empty file
		}
		if err != nil {
			return fmt.Errorf("failed to read from temp file: %w", err)
		}

		row := s.recordToRow(record)
		heap.Push(s.mergeHeap, &heapItem{row: row, fileIndex: i})
	}

	return nil
}

// compareRows compares two rows by the sort column
func (s *SortOp) compareRows(a, b *types.Row) int {
	if s.columnIndex < 0 || s.columnIndex >= len(a.Values) || s.columnIndex >= len(b.Values) {
		return 0
	}

	aVal := a.Values[s.columnIndex]
	bVal := b.Values[s.columnIndex]

	// Compare based on type
	switch av := aVal.(type) {
	case int64:
		bv, ok := bVal.(int64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case float64:
		bv, ok := bVal.(float64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case string:
		bv, ok := bVal.(string)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// Next returns the next sorted row using K-way merge
func (s *SortOp) Next() (*types.Row, error) {
	if !s.prepared {
		if err := s.prepare(); err != nil {
			return nil, err
		}
	}

	if s.exhausted || s.mergeHeap == nil || s.mergeHeap.Len() == 0 {
		return nil, nil
	}

	// Pop the smallest (or largest if desc) item from heap
	item := heap.Pop(s.mergeHeap).(*heapItem)
	result := item.row

	// Read next row from the same file and push to heap
	record, err := s.readers[item.fileIndex].Read()
	if err != io.EOF {
		if err != nil {
			return nil, fmt.Errorf("error reading during merge: %w", err)
		}
		newRow := s.recordToRow(record)
		heap.Push(s.mergeHeap, &heapItem{row: newRow, fileIndex: item.fileIndex})
	}

	return result, nil
}

// Close releases resources and deletes temp files
func (s *SortOp) Close() error {
	// Close input
	if err := s.input.Close(); err != nil {
		return err
	}

	// Close temp file readers
	for _, f := range s.files {
		if f != nil {
			f.Close()
		}
	}

	// Delete temp files
	for _, path := range s.tempFiles {
		os.Remove(path)
	}

	return nil
}

// Schema returns the schema (unchanged from input)
func (s *SortOp) Schema() types.Schema {
	return s.schema
}

// heapItem represents an item in the merge heap
type heapItem struct {
	row       *types.Row
	fileIndex int
}

// mergeHeap implements container/heap.Interface for K-way merge
type mergeHeap struct {
	items       []*heapItem
	columnIndex int
	desc        bool
}

func (h *mergeHeap) Len() int { return len(h.items) }

func (h *mergeHeap) Less(i, j int) bool {
	cmp := h.compareRows(h.items[i].row, h.items[j].row)
	if h.desc {
		return cmp > 0
	}
	return cmp < 0
}

func (h *mergeHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *mergeHeap) Push(x interface{}) {
	h.items = append(h.items, x.(*heapItem))
}

func (h *mergeHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

func (h *mergeHeap) compareRows(a, b *types.Row) int {
	if h.columnIndex < 0 || h.columnIndex >= len(a.Values) || h.columnIndex >= len(b.Values) {
		return 0
	}

	aVal := a.Values[h.columnIndex]
	bVal := b.Values[h.columnIndex]

	switch av := aVal.(type) {
	case int64:
		bv, ok := bVal.(int64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case float64:
		bv, ok := bVal.(float64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case string:
		bv, ok := bVal.(string)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	default:
		return 0
	}
}

