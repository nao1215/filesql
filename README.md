# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

[日本語](./doc/ja/README.md) | [Русский](./doc/ru/README.md) | [中文](./doc/zh-cn/README.md) | [한국어](./doc/ko/README.md) | [Español](./doc/es/README.md) | [Français](./doc/fr/README.md)

![logo](./doc/image/filesql-logo.png)

filesql is a Go SQL driver that queries CSV, TSV, LTSV, JSON, JSONL, Parquet, and Excel (XLSX) files using SQLite3 SQL syntax. It queries data files directly without imports or transformations.

[sqly](https://github.com/nao1215/sqly) is a command-line tool built on filesql that runs SQL queries against CSV, TSV, LTSV, and Excel files from the shell.

## Why filesql?

filesql was extracted from the shared file-to-SQL logic in [sqly](https://github.com/nao1215/sqly) and [sqluv](https://github.com/nao1215/sqluv). Both tools executed SQL queries against CSV, TSV, and other file formats. The common functionality was moved into this SQL driver so any Go developer can reuse it.

## Features

- SQLite3 SQL Interface - Use SQLite3's SQL dialect to query files
- Multiple File Formats - CSV, TSV, LTSV, Parquet, and Excel (XLSX) files
- Compression Support - Handles .gz, .bz2, .xz, .zst, .z, .snappy, .s2, and .lz4 compressed files
- Stream Processing - Handles large files through streaming with configurable chunk sizes
- Flexible Input Sources - File paths, directories, io.Reader, and embed.FS
- Zero Setup - No database server required, runs in-memory
- Auto-Save - Persist changes back to files
- Cross-Platform - Linux, macOS, and Windows
- SQLite3 Powered - Built on the SQLite3 engine for SQL processing

## Supported File Formats

| Extension | Format | Description |
|-----------|--------|-------------|
| `.csv` | CSV | Comma-separated values |
| `.tsv` | TSV | Tab-separated values |
| `.ltsv` | LTSV | Labeled Tab-separated Values |
| `.parquet` | Parquet | Apache Parquet columnar format |
| `.xlsx` | Excel XLSX | Microsoft Excel workbook format |
| `.json` | JSON | JSON format (use `json_extract()` for field access) |
| `.jsonl` | JSONL | JSON Lines format (one JSON object per line) |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz`, `.parquet.gz`, `.xlsx.gz`, `.json.gz`, `.jsonl.gz` | Gzip compressed | Gzip compressed files |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2`, `.parquet.bz2`, `.xlsx.bz2`, `.json.bz2`, `.jsonl.bz2` | Bzip2 compressed | Bzip2 compressed files |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz`, `.parquet.xz`, `.xlsx.xz`, `.json.xz`, `.jsonl.xz` | XZ compressed | XZ compressed files |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst`, `.parquet.zst`, `.xlsx.zst`, `.json.zst`, `.jsonl.zst` | Zstandard compressed | Zstandard compressed files |
| `.csv.z`, `.tsv.z`, `.ltsv.z`, `.parquet.z`, `.xlsx.z`, `.json.z`, `.jsonl.z` | Zlib compressed | Zlib compressed files |
| `.csv.snappy`, `.tsv.snappy`, `.ltsv.snappy`, `.parquet.snappy`, `.xlsx.snappy`, `.json.snappy`, `.jsonl.snappy` | Snappy compressed | Snappy compressed files |
| `.csv.s2`, `.tsv.s2`, `.ltsv.s2`, `.parquet.s2`, `.xlsx.s2`, `.json.s2`, `.jsonl.s2` | S2 compressed | S2 compressed files (Snappy compatible) |
| `.csv.lz4`, `.tsv.lz4`, `.ltsv.lz4`, `.parquet.lz4`, `.xlsx.lz4`, `.json.lz4`, `.jsonl.lz4` | LZ4 compressed | LZ4 compressed files |
| `.ach` | ACH (NACHA) | Automated Clearing House files (Experimental) |
| `.fed` | Fedwire | Legacy Fedwire message files (Experimental) |

## Installation

```bash
go get github.com/nao1215/filesql
```

## Requirements

- Go Version: 1.25 or later
- Operating Systems:
  - Linux
  - macOS  
  - Windows

## Quick Start

### Simple Usage

The recommended way to get started is with `OpenContext` for proper timeout handling:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    
    "github.com/nao1215/filesql"
)

func main() {
    // Create context with timeout for large file operations
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    // Open a CSV file as a database
    db, err := filesql.OpenContext(ctx, "data.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Query the data (table name = filename without extension)
    rows, err := db.QueryContext(ctx, "SELECT * FROM data WHERE age > 25")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    // Process results
    for rows.Next() {
        var name string
        var age int
        if err := rows.Scan(&name, &age); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("Name: %s, Age: %d\n", name, age)
    }
}
```

### Multiple Files and Formats

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Open multiple files at once (including Parquet)
db, err := filesql.OpenContext(ctx, "users.csv", "orders.tsv", "logs.ltsv.gz", "analytics.parquet")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Join data across different file formats
rows, err := db.QueryContext(ctx, `
    SELECT u.name, o.order_date, l.event, a.metrics
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN logs l ON u.id = l.user_id
    JOIN analytics a ON u.id = a.user_id
    WHERE o.order_date > '2024-01-01'
`)
```

### Working with Directories

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Load all supported files from a directory (recursive)
db, err := filesql.OpenContext(ctx, "/path/to/data/directory")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// See what tables are available
rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
```

### JSON / JSONL Support

JSON and JSONL files are stored as raw JSON in a single `data` TEXT column. Use SQLite's `json_extract()` function to query fields:

```go
// Open a JSON file
db, err := filesql.OpenContext(ctx, "users.json")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Query using json_extract()
rows, err := db.QueryContext(ctx, `
    SELECT json_extract(data, '$.name') AS name,
           json_extract(data, '$.age') AS age
    FROM users
    WHERE json_extract(data, '$.age') > 25
`)

// Nested fields work too
rows, err = db.QueryContext(ctx, `
    SELECT json_extract(data, '$.address.city') AS city
    FROM users
    WHERE json_extract(data, '$.address.country') = 'Japan'
`)
```

## Advanced Usage

### Builder Pattern

For advanced scenarios, use the builder pattern:

```go
package main

import (
    "context"
    "embed"
    "log"
    
    "github.com/nao1215/filesql"
)

//go:embed data/*.csv
var embeddedFiles embed.FS

func main() {
    ctx := context.Background()
    
    // Configure data sources with builder
    validatedBuilder, err := filesql.NewBuilder().
        AddPath("local_file.csv").      // Local file
        AddFS(embeddedFiles).           // Embedded files
        SetDefaultChunkSize(5000). // 5000 rows per chunk
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }
    
    db, err := validatedBuilder.Open(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Query across all data sources
    rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table'")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
}
```

### Loading into an Existing Database

`Open`/`OpenContext` create a new in-memory database. When you already manage a database, use `LoadInto` to load files into it instead of copying through a second database. Use it for a long-lived session that imports files over time, or to join file data with tables you created yourself.

```go
package main

import (
    "context"
    "database/sql"
    "log"

    "github.com/nao1215/filesql"
    _ "modernc.org/sqlite"
)

func main() {
    db, err := sql.Open("sqlite", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    // ":memory:" is private per connection; pin the pool so the loaded tables
    // are visible to later queries on the same database.
    db.SetMaxOpenConns(1)

    // Load files into the database you own. LoadInto does not close db.
    if err := filesql.LoadInto(context.Background(), db, "users.csv", "orders.parquet"); err != nil {
        log.Fatal(err)
    }

    // Load more files later into the same database (last-wins on same names).
    if err := filesql.LoadInto(context.Background(), db, "more_users.csv"); err != nil {
        log.Fatal(err)
    }
}
```

A table whose name matches a loaded file is replaced, so reloading a file is idempotent; tables you created yourself are left untouched. For readers or filesystems, configure a builder and call its `LoadInto` method. Auto-save is not supported here because the caller owns the database lifecycle.

### Auto-Save Features

#### Auto-Save on Database Close

```go
// Auto-save changes when database is closed
validatedBuilder, err := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSave("./backup"). // Save to backup directory
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close() // Changes are automatically saved here

// Make changes
db.Exec("UPDATE data SET status = 'processed' WHERE id = 1")
db.Exec("INSERT INTO data (name, age) VALUES ('John', 30)")
```

#### Auto-Save on Transaction Commit

```go
// Auto-save after each transaction
validatedBuilder, err := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSaveOnCommit(""). // Empty = overwrite original files
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Changes are saved after each commit
tx, _ := db.Begin()
tx.Exec("UPDATE data SET status = 'processed' WHERE id = 1")
tx.Commit() // Auto-save happens here
```

### Working with io.Reader and Network Data

```go
import (
    "net/http"
    "github.com/nao1215/filesql"
)

// Load data from HTTP response
resp, err := http.Get("https://example.com/data.csv")
if err != nil {
    log.Fatal(err)
}
defer resp.Body.Close()

validatedBuilder, err := filesql.NewBuilder().
    AddReader(resp.Body, "remote_data", filesql.FileTypeCSV).
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Query remote data
rows, err := db.QueryContext(ctx, "SELECT * FROM remote_data LIMIT 10")
```

### Manual Data Export

If you prefer manual control over saving:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "data.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Make modifications
db.Exec("UPDATE data SET status = 'processed'")

// Manually export changes
err = filesql.DumpDatabase(db, "./output")
if err != nil {
    log.Fatal(err)
}

// Or with custom format and compression
options := filesql.NewDumpOptions().
    WithFormat(filesql.OutputFormatTSV).
    WithCompression(filesql.CompressionGZ)
err = filesql.DumpDatabase(db, "./output", options)

// Export to Parquet format
parquetOptions := filesql.NewDumpOptions().
    WithFormat(filesql.OutputFormatParquet)
// Note: Parquet export is implemented, but external compression is not supported (use Parquet's built-in compression)
```

### Custom Logger

filesql supports pluggable logging via the `Logger` interface. By default, a no-op logger is used with zero performance overhead. You can inject your own logger (e.g., `slog`) for debugging and monitoring.

```go
import (
    "log/slog"
    "os"
    "github.com/nao1215/filesql"
)

// Create a slog logger
slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug,
}))

// Wrap it with SlogAdapter and pass to the builder
logger := filesql.NewSlogAdapter(slogLogger)

validatedBuilder, err := filesql.NewBuilder().
    WithLogger(logger).
    AddPath("data.csv").
    Build(ctx)
```

#### Logger Interface

```go
type Logger interface {
    Debug(msg string, args ...any)
    Info(msg string, args ...any)
    Warn(msg string, args ...any)
    Error(msg string, args ...any)
    With(args ...any) Logger
}
```

#### Context-Aware Logger

For context-aware logging, use `ContextLogger`:

```go
type ContextLogger interface {
    Logger
    DebugContext(ctx context.Context, msg string, args ...any)
    InfoContext(ctx context.Context, msg string, args ...any)
    WarnContext(ctx context.Context, msg string, args ...any)
    ErrorContext(ctx context.Context, msg string, args ...any)
}

// Use SlogContextAdapter for context-aware logging
logger := filesql.NewSlogContextAdapter(slogLogger)
```

#### Performance

| Logger Type | Performance | Memory |
|-------------|-------------|--------|
| nopLogger (default) | ~0.2 ns/op | 0 B/op |
| SlogAdapter | ~1000 ns/op | ~630 B/op |

The default no-op logger has virtually zero overhead, making it safe to leave logging calls in production code.

## Table Naming Rules

filesql automatically derives table names from file paths:

- `users.csv` → table `users`
- `data.tsv.gz` → table `data`
- `/path/to/sales.csv` → table `sales`
- `products.ltsv.bz2` → table `products`
- `analytics.parquet` → table `analytics`

## Important Notes

### SQL Syntax
Since filesql uses SQLite3 as its underlying engine, all SQL syntax follows [SQLite3's SQL dialect](https://www.sqlite.org/lang.html). This includes:
- Functions (e.g., `date()`, `substr()`, `json_extract()`)
- Window functions
- Common Table Expressions (CTEs)
- Triggers and views

### Data Modifications
- `INSERT`, `UPDATE`, and `DELETE` operations affect the in-memory database
- Original files remain unchanged by default
- Use auto-save features or `DumpDatabase()` to persist changes
- This makes it safe to experiment with data transformations

### Performance Tips
- Use `OpenContext()` with timeouts for large files
- Configure chunk sizes (rows per chunk) with `SetDefaultChunkSize()` for memory optimization
- Single SQLite connection works best for most scenarios
- Use streaming for files larger than available memory

## Benchmark

Performance with a 100,000-row CSV file:

| Metric | Value |
|--------|-------|
| Execution Time | ~430 ms |
| Memory Usage | ~141 MB |

Run benchmarks yourself:
```bash
make benchmark
```

### Concurrency
The `*sql.DB` returned by `Open`/`OpenContext` is safe to share across
goroutines. It is backed by a single in-memory SQLite connection, so filesql
pins the pool to one connection (`SetMaxOpenConns(1)`) and `database/sql`
serialises access for you:

```go
// Safe: share one *sql.DB across goroutines.
db, err := filesql.Open("data.csv")
if err != nil {
    return err
}
defer db.Close()

var wg sync.WaitGroup
for range 8 {
    wg.Go(func() {
        rows, err := db.Query("SELECT * FROM data")
        // ... use rows ...
    })
}
wg.Wait()
```

Note that because access is serialised through a single connection, queries do
not run in parallel; concurrency is safe but not faster. If you need true
parallelism, open a separate `*sql.DB` per goroutine.

> When you use `LoadInto` with your own `*sql.DB`, you are responsible for the
> pool configuration. For an in-memory database, call `db.SetMaxOpenConns(1)`
> yourself, because SQLite `:memory:` data is private to each connection.

### Parquet Support
- Reading: Full support for Apache Parquet files with complex data types
- Writing: Export functionality is implemented (external compression not supported, use Parquet's built-in compression)
- Type Mapping: Parquet types are mapped to SQLite types
- Compression: Parquet's built-in compression is used instead of external compression
- Large Data: Parquet files are processed with Arrow's columnar format

### Excel (XLSX) Support
- 1-Sheet-1-Table Structure: Each sheet in an Excel workbook becomes a separate SQL table
- Table Naming: SQL table names follow the format `{filename}_{sheetname}` (e.g., "sales_Q1", "sales_Q2")
- Header Row Processing: First row of each sheet becomes the column headers for that table
- Standard SQL Operations: Query each sheet independently or use JOINs to combine data across sheets
- Memory Requirements: XLSX files require full loading into memory due to the ZIP-based format structure, even during streaming operations
- Implementation Note: XLSX files are fully loaded into memory due to ZIP structure and all sheets are processed (CSV/TSV streaming parsers are not applicable)
- Export Functionality: When exporting to XLSX format, table names become sheet names automatically
- Compression Support: Full support for compressed XLSX files (.xlsx.gz, .xlsx.bz2, .xlsx.xz, .xlsx.zst, .xlsx.z, .xlsx.snappy, .xlsx.s2, .xlsx.lz4)

### ACH (NACHA) Support - Experimental

> Warning: ACH file support is experimental. The API may change in future versions.

ACH (Automated Clearing House) files following the NACHA format can be queried using SQL. Each ACH file is converted to multiple tables:

| Table Name | Description |
|------------|-------------|
| `{filename}_file_header` | File header information |
| `{filename}_batches` | Batch header and control information |
| `{filename}_entries` | Entry detail records (transactions) |
| `{filename}_addenda` | Standard addenda records |
| `{filename}_iat_entries` | IAT entry details |
| `{filename}_iat_addenda` | IAT addenda records |

#### Limitations

Read-only fields: The following fields are exported for viewing but changes are not written back:
- IAT Addenda sequence numbers (`entry_detail_sequence_number`, `sequence_number`)

Addenda05 index behavior: When an entry has multiple addenda types (e.g., Addenda02 + Addenda05), the `addenda_index` represents the position within all addenda for that entry, not the index within Addenda05 array. For updates targeting specific Addenda05 records, use `addenda_type = '05'` to filter correctly.

Validation: Modifying ACH data via SQL may create invalid ACH files. Users should ensure data consistency (e.g., `AddendaRecordIndicator` matches actual addenda presence).

Compression: ACH files do not support compression wrappers (`.ach.gz`, etc.).

#### Example

```go
ctx := context.Background()
db, err := filesql.OpenContext(ctx, "payments.ach")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Query entry details
rows, err := db.QueryContext(ctx, `
    SELECT individual_name, amount, trace_number
    FROM payments_entries
    WHERE transaction_code IN (22, 32)
`)

// Query with batch information
rows, err := db.QueryContext(ctx, `
    SELECT e.individual_name, e.amount, b.company_name
    FROM payments_entries e
    JOIN payments_batches b ON e.batch_index = b.batch_index
`)
```

### Fedwire Support - Experimental

> Warning: Fedwire file support is experimental. The API may change in future versions.

Legacy Fedwire message files (`.fed`) can be loaded, queried, modified, and exported back to Fedwire format. Each Fedwire file contains a single FEDWireMessage and is converted to a single flat table with approximately 326 columns.

| Table Name | Description |
|------------|-------------|
| `{filename}_message` | Flat table with all FEDWireMessage fields (~326 columns, 1 row) |

All columns are TEXT type since the wire format stores all values as fixed-width strings.

#### Limitations

UPDATE only: Only UPDATE operations on existing rows are supported for round-trip editing. INSERT/DELETE operations in SQL are not reflected in the output wire file.

No new sections: Optional message sections that were not present in the original file cannot be added via SQL modifications.

Compression: Fedwire files do not support compression wrappers (`.fed.gz`, etc.).

Security: Fedwire data contains sensitive banking information including routing numbers, account numbers, names, and transaction amounts. Do not log or export wire table data verbatim in production environments.

#### Example

```go
ctx := context.Background()
db, err := filesql.OpenContext(ctx, "payment.fed")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Query sender and receiver information
rows, err := db.QueryContext(ctx, `
    SELECT sender_di_routing_number, receiver_di_routing_number, amount
    FROM payment_message
`)

// Modify and export back to Fedwire format
db.ExecContext(ctx, "UPDATE payment_message SET amount = '000005000000'")
filesql.DumpFedWire(ctx, db, "payment", "modified.fed")
```

#### Excel File Structure Example
```
Excel File with Multiple Sheets:

┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Sheet1      │    │ Sheet2      │    │ Sheet3      │
│ Name   Age  │    │ Product     │    │ Region      │
│ Alice   25  │    │ Laptop      │    │ North       │
│ Bob     30  │    │ Mouse       │    │ South       │
└─────────────┘    └─────────────┘    └─────────────┘

Results in 3 separate SQL tables:

sales_Sheet1:           sales_Sheet2:           sales_Sheet3:
┌──────┬─────┐          ┌─────────┐             ┌────────┐
│ Name │ Age │          │ Product │             │ Region │
├──────┼─────┤          ├─────────┤             ├────────┤
│ Alice│  25 │          │ Laptop  │             │ North  │
│ Bob  │  30 │          │ Mouse   │             │ South  │
└──────┴─────┘          └─────────┘             └────────┘

SQL Examples:
SELECT * FROM sales_Sheet1 WHERE Age > 27;
SELECT s1.Name, s2.Product FROM sales_Sheet1 s1 
  JOIN sales_Sheet2 s2 ON s1.rowid = s2.rowid;
```

## Advanced Examples

### Complex SQL Queries

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "employees.csv", "departments.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Use advanced SQLite features
query := `
    WITH dept_stats AS (
        SELECT 
            department_id,
            AVG(salary) as avg_salary,
            COUNT(*) as emp_count
        FROM employees
        GROUP BY department_id
    )
    SELECT 
        e.name,
        e.salary,
        d.name as department,
        ds.avg_salary as dept_avg,
        RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) as salary_rank
    FROM employees e
    JOIN departments d ON e.department_id = d.id
    JOIN dept_stats ds ON e.department_id = ds.department_id
    WHERE e.salary > ds.avg_salary * 0.8
    ORDER BY d.name, salary_rank
`

rows, err := db.QueryContext(ctx, query)
```

### Context and Cancellation

```go
import (
    "context"
    "time"
)

// Set timeout for large file operations
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
defer cancel()

db, err := filesql.OpenContext(ctx, "huge_dataset.csv.gz")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Query with context for cancellation support
rows, err := db.QueryContext(ctx, "SELECT * FROM huge_dataset WHERE status = 'active'")
```

## Examples

The [examples](./examples) directory contains sample code demonstrating various filesql features:

| Example | Description |
|---------|-------------|
| [basic](./examples/basic) | Basic CSV query operations |
| [multi-format](./examples/multi-format) | Working with multiple file formats (CSV, TSV, LTSV, Parquet) |
| [sqlc](./examples/sqlc) | Integration with [sqlc](https://sqlc.dev/) - type-safe SQL code generator |
| [gorm](./examples/gorm) | Integration with [GORM](https://gorm.io/) - full-featured ORM |
| [sqlx](./examples/sqlx) | Integration with [sqlx](https://github.com/jmoiron/sqlx) - extensions to database/sql |
| [bun](./examples/bun) | Integration with [Bun](https://bun.uptrace.dev/) - SQL-first ORM |
| [squirrel](./examples/squirrel) | Integration with [Squirrel](https://github.com/Masterminds/squirrel) - fluent SQL query builder |
| [ent](./examples/ent) | Integration with [Ent](https://entgo.io/) - entity framework by Facebook |


## Data Preprocessing with fileprep

For data validation and preprocessing before querying with filesql, use [nao1215/fileprep](https://github.com/nao1215/fileprep).

fileprep is a companion library that provides:
- Struct tag-based preprocessing (`prep` tag): trim, lowercase, uppercase, default values, and more
- Struct tag-based validation (`validate` tag): required fields, format validation, cross-field validation
- filesql integration: Returns `io.Reader` for direct use with filesql's Builder pattern

```go
// Define struct with preprocessing and validation tags
type User struct {
    // Name: trim whitespace, require non-empty
    Name  string `prep:"trim" validate:"required"`
    // Email: trim, convert to lowercase, validate email format
    Email string `prep:"trim,lowercase" validate:"required,email"`
    // Age: set default if empty, validate range 0-150
    Age   string `prep:"default=0" validate:"numeric,gte=0,lte=150"`
    // Role: trim, uppercase, must be one of the allowed values
    Role  string `prep:"trim,uppercase" validate:"oneof=ADMIN USER GUEST"`
}

func main() {
    // CSV data with messy input
    csvData := `name,email,age,role
  John Doe  ,JOHN@EXAMPLE.COM,25,admin
Alice,alice@example.com,,user`

    // Create processor and process the CSV
    processor := fileprep.NewProcessor(fileprep.FileTypeCSV)
    var users []User

    reader, result, err := processor.Process(strings.NewReader(csvData), &users)
    if err != nil {
        log.Fatal(err)
    }

    // Check validation results
    fmt.Printf("Processed: %d rows, Valid: %d rows\n", result.RowCount, result.ValidRowCount)
    if result.HasErrors() {
        for _, e := range result.ValidationErrors() {
            log.Printf("Row %d, Column %s: %s", e.Row, e.Column, e.Message)
        }
    }

    // Pass preprocessed data to filesql
    // The data is now cleaned: trimmed, lowercased emails, defaults applied
    ctx := context.Background()
    db, err := filesql.NewBuilder().
        AddReader(reader, "users", filesql.FileTypeCSV).
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Query the clean data
    rows, _ := db.QueryContext(ctx, "SELECT * FROM users WHERE role = 'ADMIN'")
    // ...
}
```

For the complete list of preprocessing and validation options, see the [fileprep documentation](https://github.com/nao1215/fileprep).

## Related Projects

If you use filesql in your project, [open an issue](https://github.com/nao1215/filesql/issues) and it will be added to the list below.

### Related Libraries

| Project | Description |
|---------|-------------|
| [nao1215/fileprep](https://github.com/nao1215/fileprep) | Data preprocessing library with struct tag validation. Clean and validate CSV/TSV data using Go struct tags before querying. |
| [nao1215/fileframe](https://github.com/nao1215/fileframe)        | DataFrame API for CSV/TSV/LTSV, Parquet, Excel.  |


### CLI Tools Using filesql

| Project | Description |
|---------|-------------|
| [nao1215/sqly](https://github.com/nao1215/sqly) | Interactive shell for executing SQL queries against CSV, TSV, LTSV, JSON, and Excel files. Perfect for ad-hoc data analysis from the command line. |
| [kanmu/gocon2025-ctf](https://github.com/kanmu/gocon2025-ctf) | Go Conference 2025 CTF repository (in japanese) |

## Contributing

Contributions are welcome. See the [Contributing Guide](./CONTRIBUTING.md) for more details.

## Support

If you find this project useful, consider:

- Giving it a star on GitHub - it helps others discover the project
- [Becoming a sponsor](https://github.com/sponsors/nao1215)

### Star History

[![Star History Chart](https://api.star-history.com/svg?repos=nao1215/filesql&type=date&legend=top-left)](https://www.star-history.com/#nao1215/filesql&Date)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
