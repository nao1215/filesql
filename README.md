# filesql

[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

![logo](./doc/image/filesql-logo.png)

filesql loads files into an in-memory SQLite database. Open CSV, TSV, LTSV, JSON, JSONL, Parquet, XLSX, ACH, or Fedwire inputs, then query them with normal SQLite syntax.

The same module also includes two companion packages for work that usually happens before or after SQL:

- [`prep`](https://pkg.go.dev/github.com/nao1215/filesql/prep) cleans and validates rows before they become tables.
- [`frame`](https://pkg.go.dev/github.com/nao1215/filesql/frame) handles small in-memory transforms in plain Go.

[sqly](https://github.com/nao1215/sqly) is the shell built on the same core.

## Why filesql?

filesql is for cases where the data is already in a file and the fastest useful tool is SQL.

- Open files as tables without setting up a server.
- Join across CSV, TSV, LTSV, JSON, JSONL, Parquet, XLSX, ACH, and Fedwire.
- Keep edits in memory until you decide to save them.
- Clean inputs with `prep` before loading them.
- Reshape small datasets with `frame` when SQL is not the right fit.

## Features

- Query file data with standard SQLite syntax, including joins, CTEs, and `json_extract()`.
- Optionally query with MySQL, PostgreSQL, or GoogleSQL syntax via `WithDialect` (translated to SQLite).
- Read from file paths, directories, `io.Reader`, and `embed.FS`.
- Handle compressed CSV, TSV, LTSV, JSON, JSONL, Parquet, and XLSX files transparently.
- Write csv, tsv, and ltsv output in Shift-JIS, EUC-JP, ISO-2022-JP, or UTF-16 as well as UTF-8.
- Load into a new in-memory database or into a `*sql.DB` you already manage.
- Save changes with `DumpDatabase`, `EnableAutoSave`, or `EnableAutoSaveOnCommit`.
- Stay in one module for loading (`filesql`), cleanup (`prep`), and lightweight transforms (`frame`).

## Supported File Formats

| Extension | Format | Notes |
|-----------|--------|-------|
| `.csv` | CSV | Header row becomes column names |
| `.tsv` | TSV | Tab-separated text |
| `.ltsv` | LTSV | Labeled tab-separated text |
| `.json` | JSON | Query nested data with `json_extract()` |
| `.jsonl` | JSONL | One JSON value per line |
| `.parquet` | Parquet | Columnar format |
| `.xlsx` | Excel XLSX | One sheet becomes one table, named `file_sheet` (just `file` when the sheet repeats it). Every sheet is loaded by default; see [Excel sheet visibility](#excel-sheet-visibility) |
| `.ach` | ACH (NACHA) | Experimental |
| `.fed` | Fedwire | Experimental |

Two inputs are the same source only when they are in the same place. `dir/users.csv` and `dir/users.csv.gz` are one dataset offered twice, and the plain one is read; `a/users.csv` and `b/users.csv` are two files, and both are loaded. What happens when both then want the table `users` is the loading API's business: `Open` and `OpenContext` build a fresh database and refuse it with `ErrDuplicateTable`, while `LoadInto` and `LoadIntoTx` load into a database you own and keep their last-wins rule, so the later input replaces the table. Neither one silently drops a file.

Compressed wrappers are supported for CSV, TSV, LTSV, JSON, JSONL, Parquet, and XLSX:
`.gz`, `.bz2`, `.xz`, `.zst`, `.z`, `.snappy`, `.s2`, `.lz4`.

ACH and Fedwire do not use external compression wrappers.

Format and compression are separate. `FileType` names the format only —
`FileTypeCSV` is a CSV whether or not a codec wraps it — and a path says which
codec that is. A reader has no path, so `AddReader` takes the codec as an
option:

```go
gz, err := os.Open("users.csv.gz")
if err != nil {
	return err
}
defer gz.Close()

builder.AddReader(gz, "users", filesql.FileTypeCSV, filesql.WithCompression(filesql.CompressionGZ))
```

Without `WithCompression` the reader's bytes are read as the format directly,
so the ordinary three-argument call is unchanged.

## Installation

```bash
go get github.com/nao1215/filesql
```

Requirements:

- Go 1.25 or later
- Linux, macOS, or Windows

## Quick Start

For a one-off load, `filesql.Open` is fine. `OpenContext` is the better default when you already have a context or want a timeout.

### Query files with SQLite

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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := filesql.OpenContext(ctx, "users.csv", "orders.jsonl")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, `
		SELECT
			u.name,
			COUNT(*) AS order_count
		FROM users u
		JOIN orders o
			ON u.id = json_extract(o.data, '$.user_id')
		GROUP BY u.name
		ORDER BY order_count DESC, u.name
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var orderCount int
		if err := rows.Scan(&name, &orderCount); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s: %d\n", name, orderCount)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
```

### Query with another SQL dialect

By default queries use SQLite syntax. `WithDialect` lets you write queries in
MySQL, PostgreSQL, or GoogleSQL (BigQuery / Cloud Spanner) instead; filesql
translates them to SQLite before running. Loading files always uses SQLite, so
only the queries you write are affected.

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/nao1215/filesql"
	"github.com/nao1215/filesql/dialect"
)

func main() {
	ctx := context.Background()

	builder, err := filesql.NewBuilder().
		AddPath("users.csv").
		WithDialect(dialect.PostgreSQL).
		Build(ctx)
	if err != nil {
		log.Fatal(err)
	}
	db, err := builder.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// PostgreSQL syntax: "::" cast and ILIKE.
	rows, err := db.QueryContext(ctx,
		"SELECT name, age::text FROM users WHERE name ILIKE 'a%'")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	// ...
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok")
}
```

Translation is best-effort compatibility, not a full emulator: common
incompatibilities (identifier quoting, `DATE_ADD`, `SPLIT_PART`, `SAFE_DIVIDE`,
`EXTRACT`, casts, …) are rewritten or backed by helper functions, constructs
with no SQLite equivalent (for example `QUALIFY`, `DISTINCT ON`, MySQL's `XOR`,
or MySQL's `0x` literal, which is a string in one place and a number in another)
return a clear error, and anything else is passed through to SQLite.
A non-SQLite dialect
cannot be combined with auto-save. See the [`dialect`](./dialect) package for the
full list of supported translations.

### Load into a database you already own

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

	// A plain ":memory:" database is private per connection.
	db.SetMaxOpenConns(1)

	if err := filesql.LoadInto(context.Background(), db, "users.csv", "payments.parquet"); err != nil {
		log.Fatal(err)
	}
}
```

### Clean rows before loading with prep

Use `prep` when the file needs normalization before it becomes a table: trimming, case normalization, defaults, and validation errors with row numbers.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/nao1215/filesql"
	"github.com/nao1215/filesql/prep"
)

type User struct {
	Name  string `prep:"trim" validate:"required"`
	Email string `prep:"trim,lowercase" validate:"required,email"`
	Role  string `prep:"trim,uppercase" validate:"required,oneof=ADMIN USER"`
}

func main() {
	csvData := `name,email,role
  Alice  ,ALICE@EXAMPLE.COM, admin
Bob,bob@example.com,user
`

	processor := prep.NewProcessor(prep.FileTypeCSV)
	var users []User

	reader, result, err := processor.Process(strings.NewReader(csvData), &users)
	if err != nil {
		log.Fatal(err)
	}
	if result.HasErrors() {
		log.Fatal(result.ValidationErrors())
	}

	fmt.Println(users[0].Name, users[0].Email, users[0].Role)

	cleaned, err := io.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(string(cleaned))

	ctx := context.Background()
	validatedBuilder, err := filesql.NewBuilder().
		AddReader(strings.NewReader(string(cleaned)), "users", filesql.FileTypeCSV).
		Build(ctx)
	if err != nil {
		log.Fatal(err)
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}
```

### Transform small datasets with frame

Use `frame` when the data should stay in Go values instead of SQL: filter rows, add calculated columns, and aggregate small or medium datasets in memory.

```go
package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/nao1215/filesql/frame"
)

func main() {
	csvData := `region,product,qty,price
north,apple,2,100
south,apple,1,100
north,orange,3,80
north,apple,1,100
`

	df, err := frame.NewDataFrame(strings.NewReader(csvData), frame.CSV)
	if err != nil {
		log.Fatal(err)
	}

	sales := df.Mutate("revenue", func(row map[string]any) any {
		qty, _ := frame.Row(row).Int("qty")
		price, _ := frame.Row(row).Int("price")
		return qty * price
	})

	northOnly := sales.Filter(func(row map[string]any) bool {
		region, _ := frame.Row(row).String("region")
		return region == "north"
	})

	grouped, err := northOnly.GroupBy("product")
	if err != nil {
		log.Fatal(err)
	}

	summary, err := grouped.Sum("revenue")
	if err != nil {
		log.Fatal(err)
	}

	for _, row := range summary.ToRecords() {
		fmt.Printf("%s: %.0f\n", row["product"], row["sum_revenue"])
	}
}
```

## Important Notes

### Column types

CSV, TSV, LTSV, and XLSX carry no types, so filesql reads the values and picks INTEGER, REAL, or TEXT per column. A column whose values are all datetimes is recognized as one and stored as TEXT in ISO 8601, which is the form SQLite's date functions read; the [`parser`](./parser) and [`frame`](./frame) packages name that column DATETIME, since they report what was recognized rather than what SQLite stores. Parquet, ACH, and Fedwire bring their own schema and are not inferred.

Which of the three a column gets follows from every value in the column, wherever the value sits and however large the file is. Four kinds of value are damaged by a numeric column, and one of them anywhere in the file makes the column TEXT:

| Value | Column | Why |
|:--|:--|:--|
| `007`, `02134` | TEXT | A leading zero is part of a code, and INTEGER drops it |
| `11040320260000000000` | TEXT | Past int64, and float64 would render it `1.104032026e+19` |
| `1_000`, `0x1p4` | TEXT | Go parses these, SQLite's affinity does not convert them |
| `  42`, ` 5 ` | TEXT | Numeric affinity drops the padding a fixed-width code carries |

One decimal is enough to make a numeric column REAL. An INTEGER column either rewrites the decimal or stores it against its own declared type, and it turns the column's arithmetic into integer division, so `5 / 2` answers 2 rather than 2.5.

What is not on that list is decimal formatting. `2.50` loads as the REAL `2.5`, `1.00` as `1`, and `1e3` as `1000`: the quantity is preserved and the way it was written is not. Storing those as TEXT would keep the spelling and break the arithmetic — SQLite compares a TEXT column against a number as text, so `WHERE amount > 9.5` over `9.00` and `10.00` returns nothing at all. A column of money is worth more as numbers than as the string it was typed as, so the trailing zeros go. Keep the source file if you need the original spelling, or read the column into a TEXT column of your own before loading.

`frame` applies the same rules to its own values. `1`, `1.0` and `1.00` are one value there too, so `Distinct` collapses them to a single row and `Join` matches them to each other, while `007` and `7` stay two codes. A `frame` row therefore holds an `int64` or a `float64` where the file held text, which is what `frame.Row` is for: `frame.Row(row).String("code")` gives the value as the file spelled it, and `Int` and `Float` give it as a number, so a predicate does not have to guess which type the inference picked.

### Memory and streaming

filesql loads data into an in-memory SQLite database. CSV, TSV, and JSON arrays are read in chunks while loading. LTSV, non-array JSON/JSONL values, Parquet, XLSX, ACH, and Fedwire are read in full before they are turned into rows.

Because the rows end up in that database rather than on the Go heap, the heap is not where the cost is. Loading CSVs of 16 MB through 131 MB, the Go heap stayed flat at about 24 MB — chunked loading holds roughly a chunk, not the file — while resident memory grew by about **2x the file's size**. Budget from the file size, and expect the database, not the parser, to be what occupies it.

Measured on Linux with `go test -tags benchmark -run TestLoadMemoryFootprint -v .`, which prints the table it is drawn from so the figure can be re-derived rather than taken on trust. Note that `B/op` from `go test -benchmem` answers a different question: it counts every byte a load ever allocated, garbage included, so it runs several times higher than the memory actually held.

Use `SetDefaultChunkSize` on the builder when you need to tune chunked loading:

```go
validatedBuilder, err := filesql.NewBuilder().
	AddPath("large.csv").
	SetDefaultChunkSize(5000).
	Build(ctx)
```

The final memory cost is still dominated by the size of the in-memory SQLite database. Chunking reduces loader overhead; it does not make a large dataset free.

A column's type is the same at any chunk size, but the text of a numeric-looking cell is not always. A column that reads as a number for a while and turns out to be text — `1`, then `2.50`, then `abc` — is created numeric and rebuilt as TEXT when the text arrives, and the rows already stored then carry SQLite's spelling of the number rather than the file's: `2.50` comes back as `2.5`, and `1` as `1.0`. The default chunk size decides the type before any row is stored for a file of 1000 rows or fewer, so this shows up when the chunk size is lowered below the row where such a column turns text. Leave the chunk size alone, or raise it, when a column mixes numbers and text and the exact text matters.

### Concurrency

The `*sql.DB` returned by `Open` and `OpenContext` is safe to share across goroutines. filesql uses a shared-cache in-memory SQLite database so pooled connections can see the same tables. Auto-save does not change that: with `EnableAutoSave` the save runs once, when `Close` returns, and with `EnableAutoSaveOnCommit` the saves run one at a time, so committing from several goroutines is safe.

Loading is the exception. `LoadInto`, `DBBuilder.LoadInto` and `LoadIntoTx` create tables, and creating one takes a schema lock: two loads into the same database at the same time leave one of them reporting `database schema is locked`, with its table not created. Load from one goroutine, or hold your own lock around the load, and share the database for queries once it is loaded.

`LoadInto` is different: you own the database and pool settings there. If you use `sql.Open("sqlite", ":memory:")`, keep `SetMaxOpenConns(1)` so every query hits the same in-memory database.

### Saving changes

Changes live in memory until you save them.

- `DumpDatabase` writes the current database out to files when you want an explicit export step.
- `EnableAutoSave` saves when `db.Close()` runs.
- `EnableAutoSaveOnCommit` saves after each committed transaction, and again when `db.Close()` runs, so a statement executed outside a transaction is not lost.

`DumpOptions` decides the format, the compression, the text encoding, and the line terminator of csv, tsv, and ltsv output:

```go
options := filesql.NewDumpOptions().
	WithFormat(filesql.OutputFormatCSV).
	WithEncoding(filesql.EncodingShiftJIS).
	WithLineEnding(filesql.LineEndingCRLF)

err := filesql.DumpDatabase(db, "./output", options)
```

`WithEncoding` chooses the encoding of an export. filesql reads UTF-8 only, so a file written in another encoding is for other tools; transcode it before loading it back. Output is UTF-8 unless `WithEncoding` says otherwise, which is what a save wrote before the option existed. `EncodingShiftJIS`, `EncodingEUCJP`, `EncodingISO2022JP`, `EncodingUTF16LE`, and `EncodingUTF16BE` are the others; the UTF-16 pair write a byte-order mark, so the read side recognizes them without being told. A value the encoding has no way to write fails the save with `ErrEncoding` and leaves the destination as it was, rather than being replaced with a substitute character — the same answer the read side gives to bytes it cannot decode. Parquet and XLSX carry their own encoding and ignore the option.

Records end with `\n` unless `WithLineEnding` says otherwise. `EnableAutoSave("")` does not need to be told: writing a table back to the file it was loaded from reads the terminator that file already uses and writes the same one, so a CRLF file edited in place stays CRLF and the rows nobody touched keep the ending they had. A file with mixed terminators keeps whichever one the majority of its lines use. Every other save is an export, and an export writes the same bytes whatever happens to sit in the destination: `DumpDatabase` and `EnableAutoSave("./dir")` write `\n` even when the directory they are pointed at is the one a source was loaded from, so pass `WithLineEnding(LineEndingCRLF)` when the output has to be CRLF. Parquet and XLSX are not line-based and ignore the option.

### Excel sheet visibility

A workbook can hide a sheet, and a hidden sheet often holds the spreadsheet's own working-out rather than data anyone meant to publish. filesql loads every sheet by default, hidden or not, so existing programs keep the tables they have.

Ask for only the sheets the workbook shows with `WithExcelSheetPolicy`:

```go
validatedBuilder, err := filesql.NewBuilder().
	AddPath("book.xlsx").
	WithExcelSheetPolicy(filesql.ExcelSheetPolicyVisibleOnly).
	Build(ctx)
```

The policy applies to every source — a path, a directory, an embedded filesystem, a reader, and a compressed workbook alike. Table names are worked out after it has run, so a hidden sheet that would sanitize to the same table as a visible one is not a collision when it is not loaded.

Excel separates "hidden", which a reader can undo from the sheet tabs, from "very hidden", which only the VBA editor can. The library filesql reads workbooks with reports one boolean covering both, so filesql does not tell them apart: `ExcelSheetPolicyVisibleOnly` leaves out either kind.

`ExcelSheetsInFile` reports what a workbook holds without loading it, which is how a caller explains which sheets a policy left behind:

```go
sheets, err := filesql.ExcelSheetsInFile("book.xlsx")
for _, sheet := range sheets {
	fmt.Println(sheet.Name, sheet.Visible)
}
```

### ACH and Fedwire

ACH (`.ach`) and Fedwire (`.fed`) support are experimental. They are useful for inspection, joins, and controlled updates, but the exported files still need domain knowledge from the caller.

Control records are derived, not stored: writing an ACH file rebuilds each batch control and the file control from the entries, so an edited amount is balanced by the write rather than by the caller. An edit to a control column (`total_debit`, `total_credit`, `entry_hash`, `entry_addenda_count`) is therefore overwritten by the recalculation.

A write-back rewrites the whole file. Both formats are written from the parsed structure rather than patched, so records the caller did not edit can come back formatted differently: an ACH record is written at its full width with its padding normalized, and Fedwire tags are written in the order the format defines rather than the order the file had them. The values are unchanged; the bytes are not, so a file diff after a write-back shows lines nobody edited.

Writing needs the source file. Neither format can be rebuilt from its SQL tables alone: fields no table exposes exist only in the original. `DumpACH` and `DumpFedWire` therefore read the file the tables were loaded from and apply the edits to it, and fail with `ErrSourceUnavailable`, naming the file, when it is gone or unreadable. A database loaded from an `io.Reader` has no such file: parse the reader with `parser/ach` or `parser/wire` and pass the result to `DumpACHWithTableSet` or `DumpFedWireWithTableSet`.

Each database records its own source, so two databases loaded from files that share a name in different directories each export their own data. The record lives in a reserved table named `_filesql_sources`. Table names beginning with `_filesql_` belong to this package: they are hidden from `DumpDatabase` and from the table listings filesql returns, and an input that would load into one is refused with `ErrReservedTableName`, the way SQLite refuses its own `sqlite_` prefix.

## Examples

### API example index

The GoDoc examples are fully tested with `go test`. The tables below show the fastest path from a feature name in the README to the exact example function in the repo.

#### filesql

| Feature | Example function | Source |
|---------|------------------|--------|
| Open files and query them | `ExampleOpen`, `ExampleOpenContext` | [example_api_test.go](./example_api_test.go), [example_test.go](./example_test.go) |
| Load files into an existing `*sql.DB` | `ExampleLoadInto`, `ExampleDBBuilder_LoadInto` | [example_api_test.go](./example_api_test.go) |
| Load into your own database, edit, and save it back | `ExampleLoadInto_dumpDatabase` | [example_api_test.go](./example_api_test.go) |
| Build from readers, paths, or embedded FS | `ExampleNewBuilder`, `ExampleDBBuilder_AddReader`, `ExampleDBBuilder_AddPath`, `ExampleDBBuilder_AddFS` | [example_test.go](./example_test.go) |
| Read a compressed reader | `ExampleDBBuilder_AddReader_compressed` | [example_test.go](./example_test.go) |
| Tune chunked loading | `ExampleDBBuilder_SetDefaultChunkSize` | [example_api_test.go](./example_api_test.go) |
| Handle malformed CSV/TSV rows | `ExampleDBBuilder_WithMalformedRowPolicy` | [example_api_test.go](./example_api_test.go) |
| Load only the sheets a workbook shows | `ExampleDBBuilder_WithExcelSheetPolicy` | [example_api_test.go](./example_api_test.go) |
| Report a workbook's sheets and their visibility | `ExampleExcelSheetsInFile` | [example_api_test.go](./example_api_test.go) |
| Attach your own logger | `ExampleDBBuilder_WithLogger`, `ExampleNewSlogAdapter` | [example_api_test.go](./example_api_test.go) |
| Open a read-only wrapper | `ExampleDBBuilder_OpenReadOnly` | [example_api_test.go](./example_api_test.go) |
| Save on close or commit | `ExampleDBBuilder_EnableAutoSave`, `ExampleDBBuilder_EnableAutoSaveOnCommit`, `ExampleDBBuilder_DisableAutoSave` | [example_api_test.go](./example_api_test.go), [example_test.go](./example_test.go) |
| Export tables with format/compression/encoding/line-ending options | `ExampleDumpDatabase`, `ExampleNewDumpOptions`, `ExampleDumpOptions_WithFormat`, `ExampleDumpOptions_WithCompression`, `ExampleDumpOptions_WithEncoding`, `ExampleDumpOptions_WithLineEnding` | [example_api_test.go](./example_api_test.go), [example_test.go](./example_test.go) |
| Work with compression helpers directly | `ExampleNewCompressionHandler`, `ExampleNewCompressionFactory`, `ExampleCompressionFactory_DetectCompressionType` | [example_api_test.go](./example_api_test.go) |
| Strip compression suffixes and inspect file types | `ExampleCompressionFactory_RemoveCompressionExtension`, `ExampleCompressionFactory_GetBaseFileType` | [example_api_test.go](./example_api_test.go) |
| Inspect the malformed-row policy | `ExampleMalformedRowPolicy_String` | [example_api_test.go](./example_api_test.go) |

#### prep

| Feature | Example function | Source |
|---------|------------------|--------|
| Strict tag parsing for invalid prep/validate tags | `ExampleWithStrictTagParsing` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Keep only valid rows in the output stream | `ExampleWithValidRowsOnly` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Clean CSV data into structs and a reusable reader | `ExampleProcessor_Process` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Convert JSON arrays into JSONL output | `ExampleProcessor_Process_json` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Stream cleaned output into any writer | `ExampleProcessor_ProcessToWriter` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Inspect validation counts | `ExampleProcessResult_InvalidRowCount`, `ExampleProcessResult_HasErrors` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Read validation error details | `ExampleProcessResult_ValidationErrors` | [prep/example_api_test.go](./prep/example_api_test.go) |
| See a struct field that names no column refused | `ExampleProcessor_Process_unknownColumn` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Give a field a default when the column is absent | `ExampleProcessor_Process_defaultForAbsentColumn` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Read preprocessing error details | `ExampleProcessResult_PrepErrors` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Detect compressed inputs | `ExampleIsCompressed`, `Example_detectFileType` | [prep/example_api_test.go](./prep/example_api_test.go), [prep/example_test.go](./prep/example_test.go) |
| Check output and original formats | `ExampleStream_Format`, `ExampleStream_OriginalFormat` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Rewind and reread the processed stream | `Example_streamLen`, `Example_streamSeek` | [prep/example_api_test.go](./prep/example_api_test.go) |

#### frame

| Feature | Example function | Source |
|---------|------------------|--------|
| Create a DataFrame from a reader or path | `ExampleNewDataFrame`, `ExampleNewDataFrameFromPath` | [frame/example_api_test.go](./frame/example_api_test.go) |
| Build a DataFrame directly from Go records | `ExampleNewDataFrameFromRecords` | [frame/example_api_test.go](./frame/example_api_test.go) |
| Write transformed data back to CSV or TSV | `ExampleDataFrame_ToCSV`, `ExampleDataFrame_ToTSV` | [frame/example_api_test.go](./frame/example_api_test.go) |
| Select, filter, and mutate rows | `ExampleDataFrame_Select`, `ExampleDataFrame_Filter`, `ExampleDataFrame_Mutate` | [frame/example_api_test.go](./frame/example_api_test.go) |
| Read a callback's row without guessing its types | `ExampleRow` | [frame/example_api_test.go](./frame/example_api_test.go) |
| Join in-memory tables | `ExampleDataFrame_Join` | [frame/example_api_test.go](./frame/example_api_test.go) |
| Append frames with matching or mixed schemas | `ExampleDataFrame_Concat`, `ExampleConcatAll` | [frame/example_api_test.go](./frame/example_api_test.go) |
| Group rows for aggregation | `ExampleDataFrame_GroupBy`, `ExampleGroupedDataFrame_Count` | [frame/example_api_test.go](./frame/example_api_test.go) |
| Run built-in or custom aggregations | `ExampleGroupedDataFrame_Sum`, `ExampleGroupedDataFrame_Agg` | [frame/example_api_test.go](./frame/example_api_test.go) |
| Sort by one or multiple columns | `ExampleDataFrame_Sort`, `ExampleDataFrame_SortBy` | [frame/example_api_test.go](./frame/example_api_test.go) |
| Remove duplicates by key columns | `ExampleDataFrame_DistinctBy` | [frame/example_api_test.go](./frame/example_api_test.go) |
| Keep only the first rows | `ExampleDataFrame_Head` | [frame/example_api_test.go](./frame/example_api_test.go) |
| Rename columns in bulk | `ExampleDataFrame_RenameColumns` | [frame/example_api_test.go](./frame/example_api_test.go) |
| Fill or drop missing values | `ExampleDataFrame_FillNAByColumn`, `ExampleDataFrame_DropNASubset` | [frame/example_api_test.go](./frame/example_api_test.go) |

### Integration examples

The [examples](./examples) directory shows how to use filesql with regular Go database tooling:

| Example | Description |
|---------|-------------|
| [basic](./examples/basic) | Basic CSV queries |
| [multi-format](./examples/multi-format) | Join across CSV, TSV, LTSV, and Parquet |
| [sqlc](./examples/sqlc) | Use filesql with `sqlc` |
| [gorm](./examples/gorm) | Use filesql with GORM |
| [sqlx](./examples/sqlx) | Use filesql with `sqlx` |
| [bun](./examples/bun) | Use filesql with Bun |
| [squirrel](./examples/squirrel) | Use filesql with Squirrel |
| [ent](./examples/ent) | Use filesql with Ent |

## Related Projects

| Project | Description |
|---------|-------------|
| [sqly](https://github.com/nao1215/sqly) | Interactive shell for ad-hoc SQL against files |
| [filesql/prep](https://pkg.go.dev/github.com/nao1215/filesql/prep) | Row cleanup and validation before SQL |
| [filesql/frame](https://pkg.go.dev/github.com/nao1215/filesql/frame) | Lightweight in-memory transforms in Go |

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](./CONTRIBUTING.md) before sending a PR.

## Support

If filesql is useful in your work:

- give it a star
- use it in your tools
- support development at https://github.com/sponsors/nao1215

## License

filesql is released under the [MIT License](./LICENSE).
