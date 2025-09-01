// Package filesql provides a file-based SQL driver implementation that enables
// querying CSV, TSV, LTSV, Parquet, and Excel (XLSX) files using SQLite3 SQL syntax.
//
// filesql allows you to treat structured text files as SQL databases without
// any data import or transformation steps. It uses SQLite3 as an in-memory
// database engine, providing full SQL capabilities including JOINs, aggregations,
// window functions, and CTEs.
//
// # Features
//
//   - Query CSV, TSV, LTSV, Parquet, and Excel (XLSX) files using standard SQL
//   - Automatic handling of compressed files (gzip, bzip2, xz, zstandard)
//   - Support for multiple input sources (files, directories, io.Reader, embed.FS)
//   - Efficient streaming for large files with configurable chunk sizes
//   - Cross-platform compatibility (Linux, macOS, Windows)
//   - Optional auto-save functionality to persist changes
//
// # Basic Usage
//
// The simplest way to use filesql is with the Open or OpenContext functions:
//
//	db, err := filesql.Open("data.csv")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	rows, err := db.Query("SELECT * FROM data WHERE age > 25")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer rows.Close()
//
// # Advanced Usage
//
// For more complex scenarios, use the Builder pattern:
//
//	builder := filesql.NewBuilder().
//	    AddPath("users.csv").
//	    AddPath("orders.tsv").
//	    EnableAutoSave("./output")
//
//	validatedBuilder, err := builder.Build(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	db, err := validatedBuilder.Open(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
// # Table Naming
//
// Table names are automatically derived from file paths:
//   - "users.csv" becomes table "users"
//   - "data.tsv.gz" becomes table "data"
//   - "/path/to/logs.ltsv" becomes table "logs"
//   - "sales.xlsx" with multiple sheets becomes tables "sales_Sheet1", "sales_Sheet2", etc.
//
// # Data Modifications
//
// INSERT, UPDATE, and DELETE operations affect only the in-memory database.
// Original files remain unchanged unless auto-save is enabled. To persist
// changes manually, use the DumpDatabase function.
//
// # SQL Syntax
//
// Since filesql uses SQLite3 as its underlying engine, all SQL syntax follows
// SQLite3's SQL dialect. This includes support for:
//   - Common Table Expressions (CTEs)
//   - Window functions
//   - JSON functions
//   - Date and time functions
//   - And all other SQLite3 features
//
// # Column Name Handling
//
// Column names are handled with case-sensitive comparison for duplicate detection,
// maintaining backward compatibility. Headers with identical names after trimming
// whitespace (regardless of case differences) are considered duplicates and will
// result in an error.
//
// For complete SQL syntax documentation, see: https://www.sqlite.org/lang.html
package filesql
