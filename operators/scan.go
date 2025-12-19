package operators

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/aryamaan/golap/types"
)

// CSVScan is the storage layer operator that streams rows from a CSV file
type CSVScan struct {
	reader     *csv.Reader
	file       *os.File
	schema     types.Schema
	firstRow   []string // buffered first data row (used for type inference, then returned)
	firstRowReturned bool
}

// NewCSVScan creates a new CSV scanner with automatic schema inference
// It reads the header row and peeks at the first data row to infer column types
func NewCSVScan(filePath string) (*CSVScan, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}

	reader := csv.NewReader(file)

	// Read header row
	header, err := reader.Read()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	// Read first data row to infer types
	firstRow, err := reader.Read()
	if err != nil && err != io.EOF {
		file.Close()
		return nil, fmt.Errorf("failed to read first data row: %w", err)
	}

	// Infer types from first data row
	colTypes := make([]types.DataType, len(header))
	if firstRow != nil {
		for i, val := range firstRow {
			colTypes[i] = inferType(val)
		}
	} else {
		// Empty CSV (no data rows), default all to String
		for i := range colTypes {
			colTypes[i] = types.String
		}
	}

	schema := types.Schema{
		Columns: header,
		Types:   colTypes,
	}

	return &CSVScan{
		reader:   reader,
		file:     file,
		schema:   schema,
		firstRow: firstRow,
		firstRowReturned: false,
	}, nil
}

// inferType attempts to determine the data type of a string value
// Priority: Int -> Float -> String
func inferType(val string) types.DataType {
	if val == "" {
		return types.String // Empty values default to String
	}

	// Try Int first
	if _, err := strconv.ParseInt(val, 10, 64); err == nil {
		return types.Int
	}

	// Try Float
	if _, err := strconv.ParseFloat(val, 64); err == nil {
		return types.Float
	}

	// Default to String
	return types.String
}

// parseValue converts a string value to the appropriate Go type based on DataType
func parseValue(val string, dt types.DataType) interface{} {
	switch dt {
	case types.Int:
		if v, err := strconv.ParseInt(val, 10, 64); err == nil {
			return v
		}
		return int64(0) // Parse failure, return zero value
	case types.Float:
		if v, err := strconv.ParseFloat(val, 64); err == nil {
			return v
		}
		return float64(0) // Parse failure, return zero value
	default:
		return val
	}
}

// Next returns the next row from the CSV file
// Returns (nil, nil) when the file is exhausted
func (s *CSVScan) Next() (*types.Row, error) {
	var record []string

	// Return the buffered first row if not yet returned
	if !s.firstRowReturned && s.firstRow != nil {
		record = s.firstRow
		s.firstRowReturned = true
	} else {
		var err error
		record, err = s.reader.Read()
		if err == io.EOF {
			return nil, nil // End of file
		}
		if err != nil {
			return nil, fmt.Errorf("error reading CSV row: %w", err)
		}
	}

	// Parse values according to schema types
	values := make([]interface{}, len(record))
	for i, val := range record {
		if i < len(s.schema.Types) {
			values[i] = parseValue(val, s.schema.Types[i])
		} else {
			values[i] = val // Extra columns beyond schema treated as strings
		}
	}

	return &types.Row{Values: values}, nil
}

// Close releases resources held by this operator
func (s *CSVScan) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// Schema returns the schema of rows produced by this operator
func (s *CSVScan) Schema() types.Schema {
	return s.schema
}

