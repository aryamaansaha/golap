package metadata

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/aryamaansaha/golap/types"
)

// ZoneMap stores min/max statistics for integer columns in a CSV file
// This enables partition pruning: skipping files that can't contain matching rows
type ZoneMap struct {
	Filename  string         `json:"filename"`
	RowCount  int64          `json:"row_count"`
	MinValues map[string]int64 `json:"min_values"` // Column name -> min value
	MaxValues map[string]int64 `json:"max_values"` // Column name -> max value
}

// ZoneMapPath returns the path to the zone map JSON file for a CSV
func ZoneMapPath(csvPath string) string {
	dir := filepath.Dir(csvPath)
	base := filepath.Base(csvPath)
	ext := filepath.Ext(base)
	name := base[:len(base)-len(ext)]
	return filepath.Join(dir, name+".zonemap.json")
}

// GenerateZoneMap scans a CSV file and generates zone map statistics
func GenerateZoneMap(csvPath string) (*ZoneMap, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read header
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	// Initialize min/max tracking
	minValues := make(map[string]int64)
	maxValues := make(map[string]int64)
	initialized := make(map[string]bool)
	isIntColumn := make(map[string]bool)

	// First pass: determine which columns are integers
	firstRow, err := reader.Read()
	if err == io.EOF {
		// Empty file
		return &ZoneMap{
			Filename:  csvPath,
			RowCount:  0,
			MinValues: minValues,
			MaxValues: maxValues,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read first data row: %w", err)
	}

	// Check which columns are integers based on first row
	for i, val := range firstRow {
		if i < len(header) {
			if v, err := strconv.ParseInt(val, 10, 64); err == nil {
				isIntColumn[header[i]] = true
				minValues[header[i]] = v
				maxValues[header[i]] = v
				initialized[header[i]] = true
			}
		}
	}

	rowCount := int64(1)

	// Continue scanning remaining rows
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading CSV row: %w", err)
		}

		rowCount++

		for i, val := range record {
			if i >= len(header) {
				continue
			}
			colName := header[i]

			// Only track columns that were initially identified as integers
			if !isIntColumn[colName] {
				continue
			}

			v, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				// This value isn't an integer; mark column as non-integer
				delete(isIntColumn, colName)
				delete(minValues, colName)
				delete(maxValues, colName)
				continue
			}

			if v < minValues[colName] {
				minValues[colName] = v
			}
			if v > maxValues[colName] {
				maxValues[colName] = v
			}
		}
	}

	return &ZoneMap{
		Filename:  csvPath,
		RowCount:  rowCount,
		MinValues: minValues,
		MaxValues: maxValues,
	}, nil
}

// SaveZoneMap writes the zone map to a JSON sidecar file
func SaveZoneMap(zm *ZoneMap) error {
	path := ZoneMapPath(zm.Filename)

	data, err := json.MarshalIndent(zm, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal zone map: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write zone map file: %w", err)
	}

	return nil
}

// LoadZoneMap loads a zone map from a JSON sidecar file
func LoadZoneMap(csvPath string) (*ZoneMap, error) {
	path := ZoneMapPath(csvPath)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err // File doesn't exist or can't be read
	}

	var zm ZoneMap
	if err := json.Unmarshal(data, &zm); err != nil {
		return nil, fmt.Errorf("failed to parse zone map: %w", err)
	}

	return &zm, nil
}

// CanPrune checks if a zone map allows pruning based on a predicate
// Returns true if the file can be skipped (no rows will match)
func (zm *ZoneMap) CanPrune(columnName string, comp types.Comparator, value int64) bool {
	min, hasMin := zm.MinValues[columnName]
	max, hasMax := zm.MaxValues[columnName]

	if !hasMin || !hasMax {
		// Column not tracked in zone map, can't prune
		return false
	}

	switch comp {
	case types.Eq:
		// WHERE col = X: prune if X is outside [min, max]
		return value < min || value > max

	case types.Lt:
		// WHERE col < X: prune if min >= X (all values >= X)
		return min >= value

	case types.Lte:
		// WHERE col <= X: prune if min > X
		return min > value

	case types.Gt:
		// WHERE col > X: prune if max <= X (all values <= X)
		return max <= value

	case types.Gte:
		// WHERE col >= X: prune if max < X
		return max < value

	case types.Neq:
		// WHERE col != X: prune if min == max == X (only one distinct value)
		return min == max && min == value

	default:
		return false
	}
}

// PrintSummary prints a human-readable summary of the zone map
func (zm *ZoneMap) PrintSummary() {
	fmt.Printf("Zone Map for: %s\n", zm.Filename)
	fmt.Printf("Row Count: %d\n", zm.RowCount)
	fmt.Println("Integer Column Statistics:")
	for col := range zm.MinValues {
		fmt.Printf("  %s: [%d, %d]\n", col, zm.MinValues[col], zm.MaxValues[col])
	}
}

