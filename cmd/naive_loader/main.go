package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"
)

// Naive approach: load entire CSV into memory, then compute aggregates
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: naive_loader <csv_file>")
		fmt.Println("Example: naive_loader testdata/small_test.csv")
		os.Exit(1)
	}

	csvPath := os.Args[1]

	// Force GC before starting to get clean baseline
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	startTime := time.Now()

	// NAIVE APPROACH: Load everything into memory
	rows, err := loadCSVNaive(csvPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	loadTime := time.Since(startTime)

	// Measure memory after loading
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Compute aggregates (to simulate actual query work)
	count := len(rows)
	var sum int64
	for _, row := range rows {
		if len(row) > 1 {
			if val, err := strconv.ParseInt(row[1], 10, 64); err == nil {
				sum += val
			}
		}
	}

	totalTime := time.Since(startTime)

	// Calculate memory used
	memUsedBytes := memAfter.Alloc - memBefore.Alloc
	memUsedMB := float64(memUsedBytes) / (1024 * 1024)

	// Also check HeapAlloc for more accurate picture
	heapUsedMB := float64(memAfter.HeapAlloc-memBefore.HeapAlloc) / (1024 * 1024)

	fmt.Println("=== Go Naive Loader Results ===")
	fmt.Printf("File: %s\n", csvPath)
	fmt.Printf("Rows loaded: %d\n", count)
	fmt.Printf("SUM(value): %d\n", sum)
	fmt.Printf("Load time: %v\n", loadTime)
	fmt.Printf("Total time: %v\n", totalTime)
	fmt.Printf("Memory used (Alloc): %.2f MB\n", memUsedMB)
	fmt.Printf("Memory used (HeapAlloc): %.2f MB\n", heapUsedMB)

	// Output in JSON-like format for easy parsing
	fmt.Println("\n--- Metrics ---")
	fmt.Printf("MEMORY_MB=%.2f\n", memUsedMB)
	fmt.Printf("ROWS=%d\n", count)
	fmt.Printf("TIME_MS=%d\n", totalTime.Milliseconds())
}

// loadCSVNaive loads entire CSV into memory as [][]string
func loadCSVNaive(path string) ([][]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Skip header
	_, err = reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Read all rows into memory
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	return rows, nil
}

