# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

[日本語](./doc/ja/README.md) | [Русский](./doc/ru/README.md) | [中文](./doc/zh-cn/README.md) | [한국어](./doc/ko/README.md) | [Español](./doc/es/README.md) | [Français](./doc/fr/README.md)

**filesql** is a Go SQL driver that enables you to query CSV, TSV, and LTSV files using SQLite3 SQL syntax. Query your data files directly without any imports or transformations!

## 🎯 Why filesql?

This library was born from the experience of maintaining two separate CLI tools - [sqly](https://github.com/nao1215/sqly) and [sqluv](https://github.com/nao1215/sqluv). Both tools shared a common feature: executing SQL queries against CSV, TSV, and other file formats. 

Rather than maintaining duplicate code across both projects, we extracted the core functionality into this reusable SQL driver. Now, any Go developer can leverage this capability in their own applications!

## ✨ Features

- 🔍 **SQLite3 SQL Interface** - Use SQLite3's powerful SQL dialect to query your files
- 📁 **Multiple File Formats** - Support for CSV, TSV, and LTSV files
- 🗜️ **Compression Support** - Automatically handles .gz, .bz2, .xz, and .zst compressed files
- 🌊 **Stream Processing** - Efficiently handles large files through streaming with configurable chunk sizes
- 📖 **Flexible Input Sources** - Support for file paths, directories, io.Reader, and embed.FS
- 🚀 **Zero Setup** - No database server required, everything runs in-memory
- 💾 **Auto-Save** - Automatically persist changes back to files
- 🌍 **Cross-Platform** - Works seamlessly on Linux, macOS, and Windows
- ⚡ **SQLite3 Powered** - Built on the robust SQLite3 engine for reliable SQL processing

## 📋 Supported File Formats

| Extension | Format | Description |
|-----------|--------|-------------|
| `.csv` | CSV | Comma-separated values |
| `.tsv` | TSV | Tab-separated values |
| `.ltsv` | LTSV | Labeled Tab-separated Values |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz` | Gzip compressed | Gzip compressed files |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2` | Bzip2 compressed | Bzip2 compressed files |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz` | XZ compressed | XZ compressed files |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst` | Zstandard compressed | Zstandard compressed files |

## 📦 Installation

```bash
go get github.com/nao1215/filesql
```

## 🚀 Quick Start

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

// Open multiple files at once
db, err := filesql.OpenContext(ctx, "users.csv", "orders.tsv", "logs.ltsv.gz")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Join data across different file formats
rows, err := db.QueryContext(ctx, `
    SELECT u.name, o.order_date, l.event
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN logs l ON u.id = l.user_id
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

## 🔧 Advanced Usage

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
        SetDefaultChunkSize(50*1024*1024). // 50MB chunks
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
```

## 📝 Table Naming Rules

filesql automatically derives table names from file paths:

- `users.csv` → table `users`
- `data.tsv.gz` → table `data`
- `/path/to/sales.csv` → table `sales`
- `products.ltsv.bz2` → table `products`

## ⚠️ Important Notes

### SQL Syntax
Since filesql uses SQLite3 as its underlying engine, all SQL syntax follows [SQLite3's SQL dialect](https://www.sqlite.org/lang.html). This includes:
- Functions (e.g., `date()`, `substr()`, `json_extract()`)
- Window functions
- Common Table Expressions (CTEs)
- Triggers and views

### Data Modifications
- `INSERT`, `UPDATE`, and `DELETE` operations affect the in-memory database
- **Original files remain unchanged by default**
- Use auto-save features or `DumpDatabase()` to persist changes
- This makes it safe to experiment with data transformations

### Performance Tips
- Use `OpenContext()` with timeouts for large files
- Configure chunk sizes with `SetDefaultChunkSize()` for memory optimization  
- Single SQLite connection works best for most scenarios
- Use streaming for files larger than available memory

## 🎨 Advanced Examples

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

## 🤝 Contributing

Contributions are welcome! Please see the [Contributing Guide](./CONTRIBUTING.md) for more details.

## 💖 Support

If you find this project useful, please consider:

- ⭐ Giving it a star on GitHub - it helps others discover the project
- 💝 [Becoming a sponsor](https://github.com/sponsors/nao1215) - your support keeps the project alive and motivates continued development

Your support, whether through stars, sponsorships, or contributions, is what drives this project forward. Thank you!

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.