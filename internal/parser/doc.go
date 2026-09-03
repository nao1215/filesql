// Package parser provides file parsing functionality for various tabular data formats.
// It supports CSV, TSV, LTSV, XLSX, Parquet, JSON, and JSONL files.
//
// Compression is not this package's concern. Parse reads the bytes it is given
// as the format it is told, so a compressed file is unwrapped first, by
// filesql.OpenReader for a path or by filesql.CompressionType.NewReader for a
// stream whose codec the caller knows. DetectFileType looks through a compression extension for the
// same reason: the codec says how to read the bytes, not what they spell.
//
// It is internal to this module and is used by filesql and by prep.
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
// A load does not come through here for that reason: filesql reads CSV, TSV,
// JSONL, JSON arrays and Parquet in chunks through internal/reader, and this
// package is the whole-table door, for a caller that wants the table itself.
//
// # One table per parse
//
// A TableData is one table. A workbook of several sheets therefore contributes
// one of them -- the first the sheet policy admits -- and the rest are not read.
// Use filesql to load a workbook as one table per sheet.
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
