// Package filesql provides a file-based SQL driver implementation that enables
// querying CSV, TSV, LTSV, JSON, JSONL, Parquet, and Excel (XLSX) files using
// SQLite3 SQL syntax.
//
// filesql allows you to treat structured text files as SQL databases without
// any data import or transformation steps. It uses SQLite3 as an in-memory
// database engine, providing full SQL capabilities including JOINs, aggregations,
// window functions, and CTEs.
//
// ACH (NACHA) and Fedwire files are also loaded, and both are experimental: what
// they turn into and how they behave on a malformed file may still change.
//
// # Features
//
//   - Query CSV, TSV, LTSV, JSON, JSONL, Parquet, and Excel (XLSX) files using
//     standard SQL
//   - Automatic handling of compressed files: gzip (.gz), bzip2 (.bz2), xz
//     (.xz), zstandard (.zst), zlib (.z), snappy (.snappy), s2 (.s2) and
//     LZ4 (.lz4)
//   - Support for multiple input sources (files, directories, io.Reader, embed.FS)
//   - Efficient streaming for large files with configurable chunk sizes
//   - Cross-platform compatibility (Linux, macOS, Windows)
//   - Optional auto-save functionality to persist changes
//   - Optional MySQL, PostgreSQL, and GoogleSQL query dialects (queries are
//     translated to SQLite; see WithDialect and the dialect subpackage)
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
// # Excel Sheet Visibility
//
// Every sheet of a workbook is loaded by default, whether or not the workbook
// shows it. Use DBBuilder.WithExcelSheetPolicy with ExcelSheetPolicyVisibleOnly
// to load only the shown ones, and ExcelSheetsInFile to report what a workbook
// holds without loading it.
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
// A header that names one column twice is refused with ErrDuplicateColumn. Two
// names are the same column if either of two separate rules says so, and the
// rules are applied one at a time rather than combined:
//
//   - Two names that differ only in ASCII letter case are one column, which is
//     SQLite's rule, since SQLite is what ends up holding them: "ID" and "id"
//     are a duplicate. The folding stops at ASCII, as SQLite's does, so "ä" and
//     "Ä" remain two columns.
//   - Two names that are identical after leading and trailing whitespace is
//     trimmed are one column, which is filesql's own rule: " name " and "name"
//     are one name typed twice.
//
// Because the two are separate, neither is applied on top of the other: " A"
// beside "a" is accepted, since trimming alone does not make them equal and
// folding alone does not either, and SQLite likewise keeps them as two columns.
//
// LTSV carries its labels on every record rather than in a header, so the check
// runs per record; a record holding "A:1\ta:2" is refused for the same reason.
//
// For complete SQL syntax documentation, see: https://www.sqlite.org/lang.html
//
// # Query Dialects
//
// By default queries use SQLite3 syntax. To accept a different dialect, configure
// the builder with WithDialect:
//
//	db, err := filesql.NewBuilder().
//	    AddPath("users.csv").
//	    WithDialect(dialect.PostgreSQL).
//	    Build(ctx)
//	// db.Query("SELECT name::text FROM users WHERE name ILIKE 'a%'")
//
// The supported dialects are MySQL, PostgreSQL, and GoogleSQL (BigQuery / Cloud
// Spanner). Queries are translated to SQLite before execution on a best-effort
// basis: common incompatibilities are rewritten, constructs with no SQLite
// equivalent are rejected with a clear error, and everything else is passed
// through. Loading files always uses SQLite regardless of the dialect. See the
// dialect subpackage for the full list of translations and their limitations.
package filesql
