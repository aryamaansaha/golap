package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/aryamaansaha/golap/engine"
	"github.com/aryamaansaha/golap/metadata"
)

func main() {
	// Parse flags
	sortChunkSize := flag.Int("sort-chunk-size", 1000, "Number of rows per chunk for external sort (default: 1000)")
	flag.Parse()

	args := flag.Args()

	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	command := args[0]

	switch command {
	case "query", "q":
		if len(args) < 2 {
			fmt.Println("Error: SQL query required")
			fmt.Println("Usage: golap query \"SELECT * FROM data.csv\"")
			os.Exit(1)
		}
		query := args[1]
		runQuery(query, *sortChunkSize)

	case "zonemap", "zm":
		if len(args) < 2 {
			fmt.Println("Error: CSV file path required")
			fmt.Println("Usage: golap zonemap data.csv")
			os.Exit(1)
		}
		csvPath := args[1]
		generateZoneMap(csvPath)

	case "help", "-h", "--help":
		printUsage()

	default:
		// Assume it's a direct SQL query
		query := strings.Join(args, " ")
		runQuery(query, *sortChunkSize)
	}
}

func printUsage() {
	fmt.Println(`GOLAP - Go Serverless OLAP Engine

Usage:
  golap query "SQL_QUERY"     Execute a SQL query
  golap zonemap FILE.csv      Generate zone map metadata for a CSV file
  golap "SQL_QUERY"           Execute a SQL query (shorthand)

Examples:
  golap query "SELECT * FROM data.csv LIMIT 10"
  golap "SELECT id, name FROM users.csv WHERE age > 25 ORDER BY age LIMIT 10"
  golap "SELECT COUNT(*), SUM(amount) FROM sales.csv"
  golap "SELECT category, SUM(amount) FROM sales.csv GROUP BY category"
  golap zonemap large_dataset.csv

Supported SQL Features:
  - SELECT columns or * (all columns)
  - FROM "file.csv" (relative or absolute path)
  - WHERE with =, <, >, <=, >=, != and AND (implicit)
  - ORDER BY column [ASC|DESC]
  - LIMIT n
  - GROUP BY column
  - Aggregates: COUNT, SUM, MIN, MAX, AVG

Flags:
  -sort-chunk-size=N    Number of rows per chunk for ORDER BY (default: 1000)
                        Larger values use more memory but sort faster

Notes:
  - CSV files must have a header row
  - Column types are auto-inferred (Int, Float, String)
  - Large datasets are sorted using external merge sort (disk-based)`)
}

func runQuery(query string, sortChunkSize int) {
	op, err := engine.ParseAndPlan(query, sortChunkSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer op.Close()

	// Print header
	schema := op.Schema()
	fmt.Println(strings.Join(schema.Columns, "\t"))
	fmt.Println(strings.Repeat("-", len(strings.Join(schema.Columns, "\t"))+8))

	// Print rows
	rowCount := 0
	for {
		row, err := op.Next()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading row: %v\n", err)
			os.Exit(1)
		}
		if row == nil {
			break
		}

		// Format row values
		values := make([]string, len(row.Values))
		for i, v := range row.Values {
			if v == nil {
				values[i] = "NULL"
			} else {
				values[i] = fmt.Sprintf("%v", v)
			}
		}
		fmt.Println(strings.Join(values, "\t"))
		rowCount++
	}

	fmt.Printf("\n(%d rows)\n", rowCount)
}

func generateZoneMap(csvPath string) {
	fmt.Printf("Generating zone map for: %s\n", csvPath)

	zm, err := metadata.GenerateZoneMap(csvPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if err := metadata.SaveZoneMap(zm); err != nil {
		fmt.Fprintf(os.Stderr, "Error saving zone map: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Zone map generated successfully!")
	zm.PrintSummary()
	fmt.Printf("Saved to: %s\n", metadata.ZoneMapPath(csvPath))
}
