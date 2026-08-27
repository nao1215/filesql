// Package parser provides file parsing functionality for various tabular data formats.
// It supports CSV, TSV, LTSV, XLSX, Parquet, JSON, and JSONL files, with optional compression
// (gzip, bzip2, xz, zstd, zlib, snappy, s2, lz4).
//
// This package can be used by filesql, prep, or any application
// that needs to parse tabular data files.
//
// # Memory Considerations
//
// All parsing functions in this package load the entire dataset into memory.
// This design is intentional for simplicity and compatibility with formats that
// require random access (Parquet, XLSX), but has implications for large files:
//
//   - CSV/TSV/LTSV: Entire file content is read into memory
//   - XLSX: Entire workbook is loaded (Excel files can be large even with few rows)
//   - Parquet: Entire file is read into memory for random access
//
// For files larger than available memory, consider:
//   - Using streaming APIs for CSV/TSV
//   - Pre-filtering or splitting large files before processing
//   - Increasing available memory for the process
//
// # Example usage
//
//	f, _ := os.Open("data.csv")
//	defer f.Close()
//	result, err := parser.Parse(f, parser.CSV)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println("Columns:", result.Headers)
//	fmt.Println("Rows:", len(result.Records))
package parser
