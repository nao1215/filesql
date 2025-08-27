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
- 🚀 **Zero Setup** - No database server required, everything runs in-memory
- 🌍 **Cross-Platform** - Works seamlessly on Linux, macOS, and Windows
- 💾 **SQLite3 Powered** - Built on the robust SQLite3 engine for reliable SQL processing

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

[Example codes is here](./example_test.go).

### Simple Usage (Files)

For simple file access, use the convenient `Open` or `OpenContext` functions:

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
    // Open a CSV file as a database with context
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    db, err := filesql.OpenContext(ctx, "data.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Execute SQL query (table name is derived from filename without extension)
    rows, err := db.QueryContext(ctx, "SELECT * FROM data WHERE age > 25 ORDER BY name")
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

### Builder Pattern (Required for fs.FS)

For advanced use cases like embedded files (`go:embed`) or custom filesystems, use the **Builder pattern**:

```go
package main

import (
    "context"
    "embed"
    "io/fs"
    "log"
    
    "github.com/nao1215/filesql"
)

//go:embed data/*.csv data/*.tsv
var dataFS embed.FS

func main() {
    ctx := context.Background()
    
    // Use Builder pattern for embedded filesystem
    subFS, _ := fs.Sub(dataFS, "data")
    
    db, err := filesql.NewBuilder().
        AddPath("local_file.csv").  // Regular file
        AddFS(subFS).               // Embedded filesystem
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }
    
    connection, err := db.Open(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer connection.Close()
    
    // Query across files from different sources
    rows, err := connection.Query("SELECT name FROM sqlite_master WHERE type='table'")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    // Process results...
}
```

### Opening with Context Support

```go
// Open files with timeout control
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "large_dataset.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Query with context for cancellation support
rows, err := db.QueryContext(ctx, "SELECT * FROM large_dataset WHERE status = 'active'")
```

### Opening Multiple Files

```go
// Open multiple files in a single database
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "users.csv", "orders.tsv", "products.ltsv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Join data across different file formats!
rows, err := db.QueryContext(ctx, `
    SELECT u.name, o.order_date, p.product_name
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN products p ON o.product_id = p.id
    WHERE o.order_date > '2024-01-01'
`)
```

### Working with Directories

```go
// Open all supported files in a directory (recursive)
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "/path/to/data/directory")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Query all loaded tables
rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
```

### Compressed Files Support

```go
// Automatically handles compressed files
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "large_dataset.csv.gz", "archive.tsv.bz2")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Query compressed data seamlessly
rows, err := db.QueryContext(ctx, "SELECT COUNT(*) FROM large_dataset")
```

### Table Naming Rules

filesql automatically derives table names from file paths:

```go
// Table naming examples:
// "users.csv"           -> table name: "users"
// "data.tsv"            -> table name: "data"
// "logs.ltsv"           -> table name: "logs"
// "archive.csv.gz"      -> table name: "archive"
// "backup.tsv.bz2"      -> table name: "backup"
// "/path/to/sales.csv"  -> table name: "sales"

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "employees.csv", "departments.tsv.gz")
if err != nil {
    log.Fatal(err)
}

// Use the derived table names in queries
rows, err := db.QueryContext(ctx, `
    SELECT * FROM employees 
    JOIN departments ON employees.dept_id = departments.id
`)
```

## ⚠️ Important Notes

### SQL Syntax
Since filesql uses SQLite3 as its underlying engine, all SQL syntax follows [SQLite3's SQL dialect](https://www.sqlite.org/lang.html). This includes:
- Functions (e.g., `date()`, `substr()`, `json_extract()`)
- Window functions
- Common Table Expressions (CTEs)
- And much more!

### Data Modifications
- `INSERT`, `UPDATE`, and `DELETE` operations affect the in-memory database
- **Original files remain unchanged by default** - filesql doesn't modify your source files unless you use auto-save
- You can use **auto-save** to automatically persist changes to files on close or commit
- This makes it safe to experiment with data transformations while providing optional persistence

### Advanced SQL Features

Since filesql uses SQLite3, you can leverage its full power:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "employees.csv", "departments.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Use window functions, CTEs, and complex queries
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
        RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) as rank
    FROM employees e
    JOIN departments d ON e.department_id = d.id
    JOIN dept_stats ds ON e.department_id = ds.department_id
    WHERE e.salary > ds.avg_salary * 0.8
`

rows, err := db.QueryContext(ctx, query)
```

### Auto-Save Feature

filesql provides auto-save functionality to automatically persist database changes to files. You can choose between two timing options:

#### Auto-Save on Database Close

Automatically save changes when the database connection is closed (recommended for most use cases):

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Enable auto-save on close
builder := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSave("./backup") // Save to backup directory

validatedBuilder, err := builder.Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close() // Auto-save triggered here

// Make modifications - they will be automatically saved on close
_, err = db.ExecContext(ctx, "UPDATE data SET status = 'processed' WHERE status = 'pending'")
_, err = db.ExecContext(ctx, "INSERT INTO data (name, status) VALUES ('New Record', 'active')")
```

#### Auto-Save on Transaction Commit

Automatically save changes after each transaction commit (for frequent persistence):

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Enable auto-save on commit - empty string means overwrite original files
builder := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSaveOnCommit("") // Overwrite original files

validatedBuilder, err := builder.Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Each commit will automatically save to files
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    log.Fatal(err)
}

_, err = tx.ExecContext(ctx, "UPDATE data SET status = 'processed' WHERE id = 1")
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

err = tx.Commit() // Auto-save triggered here
if err != nil {
    log.Fatal(err)
}
```

#### Auto-Save Input Type Restrictions

**Important**: Auto-save behavior depends on your input data source:

- **File Paths** (`AddPath`, `AddPaths`): Supports both overwrite mode (empty string) and output directory
  ```go
  // ✅ Overwrite original files
  builder.AddPath("data.csv").EnableAutoSave("")
  
  // ✅ Save to output directory  
  builder.AddPath("data.csv").EnableAutoSave("./backup")
  ```

- **io.Reader** (`AddReader`): **Only supports output directory mode**
  ```go
  // ❌ Build error - overwrite mode not supported
  builder.AddReader(reader, "table", model.FileTypeCSV).EnableAutoSave("")
  
  // ✅ Must specify output directory
  builder.AddReader(reader, "table", model.FileTypeCSV).EnableAutoSave("./output")
  ```

- **Filesystems** (`AddFS`): **Only supports output directory mode**
  ```go
  // ❌ Build error - overwrite mode not supported  
  builder.AddFS(filesystem).EnableAutoSave("")
  
  // ✅ Must specify output directory
  builder.AddFS(filesystem).EnableAutoSave("./output")
  ```

This restriction exists because io.Reader and filesystem inputs don't have original file paths that can be overwritten. The builder will return an error at build time if you try to use overwrite mode with these input types.

### Manual Data Export (Alternative to Auto-Save)

If you prefer manual control over when to save changes to files instead of using auto-save:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "data.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Make modifications
_, err = db.ExecContext(ctx, "UPDATE data SET status = 'processed' WHERE status = 'pending'")
if err != nil {
    log.Fatal(err)
}

// Export the modified data to a new directory
err = filesql.DumpDatabase(db, "/path/to/output/directory")
if err != nil {
    log.Fatal(err)
}
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
