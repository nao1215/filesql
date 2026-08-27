# filesql

[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

![logo](./doc/image/filesql-logo.png)

filesql loads files into an in-memory SQLite database. Open CSV, TSV, LTSV, JSON, JSONL, Parquet, XLSX, ACH, or Fedwire inputs, then query them with normal SQLite syntax.

The same module also includes [`prep`](https://pkg.go.dev/github.com/nao1215/filesql/prep), which cleans and validates rows before they become tables.

[sqly](https://github.com/nao1215/sqly) is the shell built on the same core.

## Why filesql?

filesql is for cases where the data is already in a file and the fastest useful tool is SQL.

- Open files as tables without setting up a server.
- Join across CSV, TSV, LTSV, JSON, JSONL, Parquet, XLSX, ACH, and Fedwire.
- Keep edits in memory until you decide to save them.
- Clean inputs with `prep` before loading them.

## Features

- Query file data with standard SQLite syntax, including joins, CTEs, and `json_extract()`.
- Optionally query with MySQL, PostgreSQL, or GoogleSQL syntax via `WithDialect` (translated to SQLite).
- Read from file paths, directories, `io.Reader`, and `embed.FS`.
- Handle compressed CSV, TSV, LTSV, JSON, JSONL, Parquet, and XLSX files transparently.
- Write csv, tsv, and ltsv output in Shift-JIS, EUC-JP, ISO-2022-JP, or UTF-16 as well as UTF-8.
- Load into a new in-memory database or into a `*sql.DB` you already manage.
- Save changes with `DumpDatabase`, `EnableAutoSave`, or `EnableAutoSaveOnCommit`.
- Stay in one module for loading (`filesql`) and cleanup (`prep`).

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
| `.ach` | ACH (NACHA) | One table per record kind; see [ACH and Fedwire](#ach-and-fedwire) |
| `.fed` | Fedwire | One message becomes one row 326 columns wide; see [ACH and Fedwire](#ach-and-fedwire) |

Two inputs are the same source only when they are in the same place. `dir/users.csv` and `dir/users.csv.gz` are one dataset offered twice, and the plain one is read; `a/users.csv` and `b/users.csv` are two files, and both are loaded. What happens when both then want the table `users` is the loading API's business: `Open` and `OpenContext` build a fresh database and refuse it with `ErrDuplicateTable`, while `LoadInto` and `LoadIntoTx` load into a database you own and keep their last-wins rule, so the later input replaces the table. Neither one silently drops a file. Table names are compared the way SQLite compares identifiers, with ASCII case folded, so `Users.csv` and `users.csv` want the same table too.

Column names inside a file follow two separate rules, and a header that breaks either is refused with `ErrDuplicateColumn` before it reaches SQLite. Two names differing only in ASCII letter case are one column, because SQLite is what holds them — `ID` and `id` are a duplicate — and the folding stops at ASCII as SQLite's does, so `ä` and `Ä` stay two columns. Two names identical after their surrounding whitespace is trimmed are one column too: `name` and `" name "` are one name typed twice. The rules are applied one at a time and never combined, so `" A"` beside `a` is accepted, which is what SQLite does with it as well. LTSV carries its labels on every record rather than in a header, so the same check runs per record.

Compressed wrappers are supported for CSV, TSV, LTSV, JSON, JSONL, Parquet, and XLSX:
`.gz`, `.bz2`, `.xz`, `.zst`, `.z`, `.snappy`, `.s2`, `.lz4`.

ACH and Fedwire do not use external compression wrappers.

An xz or zstd stream states in its header how much working memory its decoder
must hold, and a decoder allocates it before reading any data. filesql caps that
at a 256 MiB xz dictionary, four times what `xz -9` declares, and a 128 MiB zstd
window, which is the largest the `zstd` CLI reaches on its own. A damaged file
therefore costs a fixed ceiling rather than whatever its header names. The zstd
cap holds for every frame; the xz one is read from the first block of the first
stream, so a later block or a concatenated second stream is not covered.

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

- Go 1.25.13 or later, or 1.26.6 or later on the 1.26 line
- Linux, macOS, or Windows

The patch releases are the point: 1.25.13 and 1.26.6 are the ones carrying the
standard library fixes for [GO-2026-6088](https://pkg.go.dev/vuln/GO-2026-6088)
(`encoding/xml`) and [GO-2026-5972](https://pkg.go.dev/vuln/GO-2026-5972)
(`encoding/asn1`), and filesql reaches `encoding/xml` on every XLSX it reads. An
earlier 1.26 is not supported, even though it is a later release than 1.25.13.

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
What the translation cannot reach is SQLite's type system: there is no boolean,
no interval and no array, so a comparison answers `1` or `0`, an `INTERVAL`
literal works only in date arithmetic, and a construct whose result would be one
of those types is refused rather than answered.
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

## Important Notes

### Column types

CSV, TSV, LTSV, and XLSX carry no types, so filesql reads the values and picks INTEGER, REAL, or TEXT per column. A column whose values are all datetimes is recognized as one and stored as TEXT; the [`parser`](./parser) package names that column DATETIME, since it reports what was recognized rather than what SQLite stores. A text cell is stored as the file wrote it, so a column written in a layout SQLite's date functions do not read — `1/2/2024`, `2024/01/02`, `02.01.2024` — is recognized as a datetime and still needs converting before `date()` or `strftime()` can answer about it; the layouts those functions read are ISO 8601 and `YYYY-MM-DD HH:MM:SS`. An XLSX date cell is the one that is rewritten, into ISO 8601: it holds a serial number and a number format rather than text, so there is no spelling to keep. Parquet, ACH, and Fedwire bring their own schema and are not inferred; a Parquet UINT64 column loads as TEXT, since the upper half of its range is past what SQLite's INTEGER holds exactly. A Parquet export writes each column as the type its values call for — STRING when no numeric type holds every value exactly — and a blank cell in a numeric column is written as a null, since that format has no other way to say a number is missing.

Which of the three a column gets follows from every value in the column, wherever the value sits and however large the file is. Four kinds of value are damaged by a numeric column, and one of them anywhere in the file makes the column TEXT:

| Value | Column | Why |
|:--|:--|:--|
| `007`, `02134` | TEXT | A leading zero is part of a code, and INTEGER drops it |
| `11040320260000000000` | TEXT | Past int64, and float64 would render it `1.104032026e+19` |
| `1_000`, `0x1p4` | TEXT | Go parses these, SQLite's affinity does not convert them |
| `  42`, ` 5 ` | TEXT | Numeric affinity drops the padding a fixed-width code carries |

One decimal is enough to make a numeric column REAL. An INTEGER column either rewrites the decimal or stores it against its own declared type, and it turns the column's arithmetic into integer division, so `5 / 2` answers 2 rather than 2.5. The exception is a column where a decimal meets an integer past 2^53 that REAL would round to a neighboring double: that column is TEXT, while the same integers with no decimal beside them stay INTEGER and exact.

A table is dumped to a file named after it, so a table whose name cannot be a file name is refused rather than written: the two path separators, `< > : " | ? *`, a control character, a name ending in a dot or a space, and the names Windows reserves for devices — `CON`, `PRN`, `AUX`, `NUL`, `COM1` to `COM9` and `LPT1` to `LPT9`, with or without an extension. The same set is refused on every platform, so a database dumped on Linux and on Windows agrees about which tables it can write. Rename the table before dumping it. A name derived from a file cannot reach that set except as a device name, since only letters, digits, marks and underscore survive; a name given to `AddReader` or to `CREATE TABLE` can.

A blank cell in an INTEGER or REAL column is a missing number, and the database holds it as NULL. That is what makes `MAX` answer the largest value rather than the blank, `AVG` divide by the values that are there, `COUNT(column)` count them, and `WHERE column IS NULL` find the rows that have none. A blank cell in a TEXT column is the empty string, which is a value the file holds and is worth telling apart from a missing one; a column recognized as DATETIME is stored as TEXT and follows that rule.

What is not on that list is decimal formatting. `2.50` loads as the REAL `2.5`, `1.00` as `1`, and `1e3` as `1000`: the quantity is preserved and the way it was written is not. Storing those as TEXT would keep the spelling and break the arithmetic — SQLite compares a TEXT column against a number as text, so `WHERE amount > 9.5` over `9.00` and `10.00` returns nothing at all. A column of money is worth more as numbers than as the string it was typed as, so the trailing zeros go. Keep the source file if you need the original spelling, or read the column into a TEXT column of your own before loading. What a save does keep is the type: a REAL column is written with a decimal point, so `1.00` comes back from a dump as `1.0` and the column loads as REAL again rather than as INTEGER. An infinity is written as the literal `9e999`, which loads back as the infinity in a REAL column.

### Memory and streaming

filesql loads data into an in-memory SQLite database. CSV, TSV, JSON arrays, and Parquet arrive in chunks while loading. LTSV, non-array JSON/JSONL values, XLSX, ACH, and Fedwire are read in full before they are turned into rows. A Parquet file named by path is read where it lies, since the format needs to read at an offset and a file already serves that; a Parquet reader passed to `AddReader` is buffered whole instead, because a stream cannot go back and the format is read back to front.

A blank line is not a record in CSV, in LTSV or in a sheet. In TSV it is one in a one-column file, where it is that column's empty value: TSV has no quote to write an empty field with, so an empty line is the only spelling left and a reader that skipped it would drop the row. A one-column TSV that ends with an extra newline therefore loads with a trailing row holding nothing, the file having no way to say whether that line was a record or a stray terminator. An XLSX row holding no cell at all is skipped, so a workbook whose used range reaches far down the sheet — a header in row 1 and one stray cell near the bottom — costs what it holds rather than what its range spans.

Because the rows end up in that database rather than on the Go heap, the heap is not where the cost is. Loading CSVs of 16 MB through 131 MB, the Go heap stayed flat at about 24 MB — chunked loading holds roughly a chunk, not the file — while resident memory grew by about **2x the file's size**. Budget from the file size, and expect the database, not the parser, to be what occupies it.

Measured on Linux with `go test -tags benchmark -run TestLoadMemoryFootprint -v .`, which prints the table it is drawn from so the figure can be re-derived rather than taken on trust. Note that `B/op` from `go test -benchmem` answers a different question: it counts every byte a load ever allocated, garbage included, so it runs several times higher than the memory actually held.

The multiplier belongs to the format rather than to the library. The same 200,000 rows, each format loaded in a process of its own so the peak resident memory is that load's, with about 24 MB of it being the process before the load:

| format | file | peak RSS | per extra file byte |
|--------|------|----------|---------------------|
| CSV | 32.8 MB | 101 MB | 2.1x |
| Parquet | 32.2 MB | 111 MB | 2.1x |
| XLSX | 17.6 MB | 422 MB | about 24x |

A Parquet file can cost more than its size whatever the table above says. A page header states how large the page's statistics are and the reader allocates that before reading them, so a damaged 473-byte file costs 98 MB before it is refused; the number sits inside a column chunk, where filesql cannot check it first. Do not point a memory-constrained process at Parquet files you did not write.

A workbook is the one to plan around, and it costs the same whether or not it holds dates. Its rows are read as a stream, and the date cells are found by reading the sheet's own XML, so nothing in a load asks the library about one cell at a time: doing that once makes it build the whole sheet as objects, which was another 1470 MB. XLSX is a compressed container, so the multiplier is against a smaller file than the same table as CSV. `go test -tags benchmark -run TestLoadMemoryFootprintByFormat -v .` prints this table.

Use `SetDefaultChunkSize` on the builder when you need to tune chunked loading:

```go
validatedBuilder, err := filesql.NewBuilder().
	AddPath("large.csv").
	SetDefaultChunkSize(5000).
	Build(ctx)
```

The final memory cost is still dominated by the size of the in-memory SQLite database. Chunking reduces loader overhead; it does not make a large dataset free.

Chunk size changes when rows reach the database, not what reaches it. A column's type and the text of every cell are the same at any chunk size, including a column that reads as a number for a while and turns out to be text: a file is loaded under the types its first chunk calls for and, when a later chunk needs a wider type, read again under the types the whole file calls for, so `2.50` is stored as `2.50` in a column that ends up TEXT however the file was chunked. A reader passed to `AddReader` cannot be read twice, so it is staged as text and typed once it has all been read, at the cost of one copy of the table inside SQLite.

### Concurrency

The `*sql.DB` returned by `Open` and `OpenContext` is safe to share across goroutines. filesql uses a shared-cache in-memory SQLite database so pooled connections can see the same tables. Auto-save does not change that: with `EnableAutoSave` the save runs once, when `Close` returns, and with `EnableAutoSaveOnCommit` the saves run one at a time, so committing from several goroutines is safe.

Loading into one database from several goroutines works. Creating a table takes a write lock, and SQLite refuses a second holder rather than queueing it, so a load that meets another load's lock waits it out and starts over, for up to five seconds before it gives the database's own error back. Paths and directories are covered whatever database they load into. A reader passed to `AddReader` is not fully covered, because starting over would have nothing left to read: only the steps before the reading are tried again, which is enough for a database this package opened and not always enough for a file database. `LoadIntoTx` is not retried at all — the transaction is yours, and one transaction belongs to one goroutine.

A `DBBuilder` is not safe to share across goroutines; the database is. Build one per goroutine.

`LoadInto` is different: you own the database and pool settings there. If you use `sql.Open("sqlite", ":memory:")`, keep `SetMaxOpenConns(1)` so every query hits the same in-memory database.

### Saving changes

Changes live in memory until you save them.

- `DumpDatabase` writes the current database out to files when you want an explicit export step.
- `EnableAutoSave` saves when `db.Close()` runs.
- `EnableAutoSaveOnCommit` saves after each committed transaction, and again when `db.Close()` runs, so a statement executed outside a transaction is not lost.

A transaction still open when `db.Close()` runs is one you have neither committed nor rolled back, so the save is skipped and `Close` reports that it was.

`EnableAutoSave("")` writes each source back to itself, so `Build` refuses a source this package reads but cannot write: the JSON and JSONL formats, and the bzip2 codec. Pass an output directory to load those alongside auto-save. A source reached through a symbolic link is followed: the file the link names receives the rows and the link stays a link. `DumpDatabase` does the same with a destination that already exists as a link.

`DumpOptions` decides the format, the compression, the text encoding, and the line terminator of csv, tsv, and ltsv output:

```go
options := filesql.NewDumpOptions().
	WithFormat(filesql.OutputFormatCSV).
	WithEncoding(filesql.EncodingShiftJIS).
	WithLineEnding(filesql.LineEndingCRLF)

err := filesql.DumpDatabase(db, "./output", options)
```

`WithEncoding` chooses the encoding of an export. filesql reads UTF-8 only, so a file written in another encoding is for other tools; transcode it before loading it back. Output is UTF-8 unless `WithEncoding` says otherwise, which is what a save wrote before the option existed. `EncodingShiftJIS`, `EncodingEUCJP`, `EncodingISO2022JP`, `EncodingUTF16LE`, and `EncodingUTF16BE` are the others; the UTF-16 pair write a byte-order mark, so the read side recognizes them without being told. A value the encoding has no way to write fails the save with `ErrEncoding` and leaves the destination as it was, rather than being replaced with a substitute character — the same answer the read side gives to bytes it cannot decode. Parquet and XLSX carry their own encoding and ignore the option.

Records end with `\n` unless `WithLineEnding` says otherwise. `EnableAutoSave("")` does not need to be told: writing a table back to the file it was loaded from reads the terminator that file already uses and writes the same one, so a CRLF file edited in place stays CRLF and the rows nobody touched keep the ending they had. A file with mixed terminators keeps whichever one the majority of its lines use. Every other save is an export, and an export writes the same bytes whatever happens to sit in the destination: `DumpDatabase` and `EnableAutoSave("./dir")` write `\n` even when the directory they are pointed at is the one a source was loaded from, so pass `WithLineEnding(LineEndingCRLF)` when the output has to be CRLF. Parquet and XLSX are not line-based and ignore the option. A workbook saved in place is written onto the file it replaces, so a sheet no table was loaded from is untouched and the widths, styles, merges and comments of the sheets that were loaded survive. Within a loaded sheet, only the cells whose value the session changed are written, so a formula stays a formula and a date stays a date wherever nothing was edited. A cell that was edited holds the text the table has, which means a formula whose value changed becomes that value.

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

ACH (`.ach`) and Fedwire (`.fed`) are loaded, queried and written back like any other format. They are useful for inspection, joins, and controlled updates, but the exported files still need domain knowledge from the caller: both formats carry rules about what a valid file is, and this package checks only the ones the format itself defines.

Control records are derived, not stored: writing an ACH file rebuilds each batch control and the file control from the entries, so an edited amount is balanced by the write rather than by the caller. An edit to a control column (`total_debit`, `total_credit`, `entry_hash`, `entry_addenda_count`) is therefore overwritten by the recalculation.

The `batch_index`, `entry_index` and `addenda_index` columns say which record a row updates. They are not values the row stores, and a write refuses a row whose coordinates name a record that is not there or one another row has already named. Editing them used to retarget the update: a row pointed at another row's coordinates overwrote that record's account number, amount and trace number, the edited row came back unchanged, and nothing reported it.

A Fedwire write is verified before it reaches your file. The message is written to a buffer, read back, and compared column by column with the table it was written from; if any field did not survive, the write is refused and the error names the field. Nothing is written in that case, so the file you started with is still there. One field reaches this today: `remittance_originator_address_line_four`, which the underlying library writes from line one, so a message whose fourth address line differs from its first cannot be exported until that is fixed upstream.

Fedwire files must be the delimiter-separated form. filesql parses a message's variable-length fields by looking for the `*` that ends each one, so a file written in the fixed-width form is refused, naming every record it could not read. Files written by filesql are always the delimiter-separated form.

A write-back rewrites the whole file. Both formats are written from the parsed structure rather than patched, so records the caller did not edit can come back formatted differently: an ACH record is written at its full width with its padding normalized, and Fedwire tags are written in the order the format defines rather than the order the file had them. The values are unchanged; the bytes are not, so a file diff after a write-back shows lines nobody edited.

Writing needs the source file. Neither format can be rebuilt from its SQL tables alone: fields no table exposes exist only in the original. `DumpACH` and `DumpFedWire` therefore read the file the tables were loaded from and apply the edits to it, and fail with `ErrSourceUnavailable`, naming the file, when it is gone or unreadable. A database loaded from an `io.Reader` has no such file: parse the reader with `parser/ach` or `parser/wire` and pass the result to `DumpACHWithTableSet` or `DumpFedWireWithTableSet`.

Each database records its own source, so two databases loaded from files that share a name in different directories each export their own data. The record lives in a reserved table named `_filesql_sources`. Table names beginning with `_filesql_` belong to this package: they are hidden from `DumpDatabase` and from the table listings filesql returns, and an input that would load into one is refused with `ErrReservedTableName`. A name beginning with `sqlite_` is refused the same way, because SQLite keeps that prefix for its own tables. Both comparisons ignore ASCII case, as SQLite does.

## Examples

### API example index

The GoDoc examples are fully tested with `go test`. The tables below show the fastest path from a feature name in the README to the exact example function in the repo.

#### filesql

| Feature | Example function | Source |
|---------|------------------|--------|
| Open files and query them | `ExampleOpen`, `ExampleOpenContext` | [example_api_test.go](./example_api_test.go), [example_test.go](./example_test.go) |
| Load files into an existing `*sql.DB` | `ExampleLoadInto`, `ExampleDBBuilder_LoadInto` | [example_api_test.go](./example_api_test.go) |
| Load into a transaction you own | `ExampleDBBuilder_LoadIntoTx` | [example_api_test.go](./example_api_test.go) |
| Load into your own database, edit, and save it back | `ExampleLoadInto_dumpDatabase` | [example_api_test.go](./example_api_test.go) |
| Build from readers, paths, or embedded FS | `ExampleNewBuilder`, `ExampleDBBuilder_AddReader`, `ExampleDBBuilder_AddPath`, `ExampleDBBuilder_AddFS` | [example_test.go](./example_test.go) |
| Read a compressed reader | `ExampleDBBuilder_AddReader_compressed` | [example_test.go](./example_test.go) |
| Tune chunked loading | `ExampleDBBuilder_SetDefaultChunkSize` | [example_api_test.go](./example_api_test.go) |
| Handle malformed CSV/TSV rows | `ExampleDBBuilder_WithMalformedRowPolicy` | [example_api_test.go](./example_api_test.go) |
| Count the rows a skip policy discarded | `ExampleDBBuilder_SkippedRows` | [example_api_test.go](./example_api_test.go) |
| Query with MySQL, PostgreSQL, or GoogleSQL syntax | `ExampleDBBuilder_WithDialect` | [example_api_test.go](./example_api_test.go) |
| Load only the sheets a workbook shows | `ExampleDBBuilder_WithExcelSheetPolicy` | [example_api_test.go](./example_api_test.go) |
| Report a workbook's sheets and their visibility | `ExampleExcelSheetsInFile`, `ExampleExcelSheetsInReader` | [example_api_test.go](./example_api_test.go) |
| Check a workbook's sheets for table names that collide | `ExampleExcelSheetTableNames` | [example_api_test.go](./example_api_test.go) |
| Attach a slog logger | `ExampleDBBuilder_WithLogger` | [example_api_test.go](./example_api_test.go) |
| Open a database that refuses writes | `ExampleDBBuilder_OpenReadOnly` | [example_api_test.go](./example_api_test.go) |
| Save on close or commit | `ExampleDBBuilder_EnableAutoSave`, `ExampleDBBuilder_EnableAutoSaveOnCommit`, `ExampleDBBuilder_DisableAutoSave` | [example_api_test.go](./example_api_test.go), [example_test.go](./example_test.go) |
| Export tables with format/compression/encoding/line-ending options | `ExampleDumpDatabase`, `ExampleNewDumpOptions`, `ExampleDumpOptions_WithFormat`, `ExampleDumpOptions_WithCompression`, `ExampleDumpOptions_WithEncoding`, `ExampleDumpOptions_WithLineEnding` | [example_api_test.go](./example_api_test.go), [example_test.go](./example_test.go) |
| Work with compression helpers directly | `ExampleNewCompressionHandler`, `ExampleNewCompressionFactory`, `ExampleCompressionFactory_DetectCompressionType` | [example_api_test.go](./example_api_test.go) |
| Strip compression suffixes and inspect file types | `ExampleCompressionFactory_RemoveCompressionExtension`, `ExampleCompressionFactory_GetBaseFileType` | [example_api_test.go](./example_api_test.go) |
| Write an ACH or Fedwire file back after editing it | `ExampleDumpACH`, `ExampleDumpFedWire` | [example_api_test.go](./example_api_test.go) |
| Write one back when the database came from an `io.Reader` | `ExampleDumpACHWithTableSet`, `ExampleDumpFedWireWithTableSet` | [example_api_test.go](./example_api_test.go) |
| Inspect enum names | `ExampleMalformedRowPolicy_String`, `ExampleFileType_String`, `ExampleCompressionType_String`, `ExampleEncoding_String`, `ExampleLineEnding_String`, `ExampleOutputFormat_String` | [example_api_test.go](./example_api_test.go) |

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
| Compare one column against another | `ExampleProcessor_Process_crossField` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Require a column only when other columns say so | `ExampleProcessor_Process_conditionalRequired` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Forbid a column when other columns say so | `ExampleProcessor_Process_conditionalExcluded` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Validate IP address and port columns | `ExampleProcessor_Process_networkColumns` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Validate JSON, time zone, version and digest columns | `ExampleProcessor_Process_encodedColumns` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Verify a check digit on an ISBN or a card number | `ExampleProcessor_Process_checksummedIdentifiers` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Validate country and currency code columns | `ExampleProcessor_Process_codeColumns` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Refuse a duplicate in a key column | `ExampleProcessor_Process_uniqueColumn` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Validate DNS label, color and numeric currency columns | `ExampleProcessor_Process_labelColorAndNumericCode` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Read preprocessing error details | `ExampleProcessResult_PrepErrors` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Check output and original formats | `ExampleStream_Format`, `ExampleStream_OriginalFormat` | [prep/example_api_test.go](./prep/example_api_test.go) |
| Rewind and reread the processed stream | `Example_streamLen`, `Example_streamSeek` | [prep/example_api_test.go](./prep/example_api_test.go) |

#### parser

| Feature | Example function | Source |
|---------|------------------|--------|
| Parse a delimited file into headers, records, and column types | `ExampleParse_csv`, `ExampleParse_tsv`, `ExampleParse_ltsv` | [parser/example_test.go](./parser/example_test.go) |
| Detect a file type and whether it is compressed | `ExampleDetectFileType`, `ExampleIsCompressed`, `ExampleBaseFileType` | [parser/example_test.go](./parser/example_test.go) |
| Read the type each column was given | `ExampleTableData_columnTypes` | [parser/example_test.go](./parser/example_test.go) |
| Turn an ACH file into its tables | `ExampleParseReader` | [parser/ach/example_test.go](./parser/ach/example_test.go) |
| Turn a Fedwire file into its table | `ExampleParseReader` | [parser/wire/example_test.go](./parser/wire/example_test.go) |

#### dialect

| Feature | Example function | Source |
|---------|------------------|--------|
| Translate a query into SQLite SQL and recognize what has no equivalent | `ExampleTranslate` | [dialect/example_test.go](./dialect/example_test.go) |
| Run PostgreSQL constructs SQLite has no form for | `ExampleTranslate_postgreSQL` | [dialect/example_test.go](./dialect/example_test.go) |
| Run BigQuery constructs SQLite has no form for | `ExampleTranslate_googleSQL` | [dialect/example_test.go](./dialect/example_test.go) |
| Turn a user-supplied dialect name into a `Dialect` | `ExampleParse` | [dialect/example_test.go](./dialect/example_test.go) |
| List the built-in dialects and spell one for a person | `ExampleDialects`, `ExampleDialect_DisplayName` | [dialect/example_test.go](./dialect/example_test.go) |

### Integration examples

The [examples](./examples) directory shows how to use filesql with regular Go database tooling:

| Example | Description |
|---------|-------------|
| [basic](./examples/basic) | Basic CSV queries |
| [multi-format](./examples/multi-format) | Join across CSV, TSV, and LTSV |
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

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](./CONTRIBUTING.md) before sending a PR.

## Support

If filesql is useful in your work:

- give it a star
- use it in your tools
- support development at https://github.com/sponsors/nao1215

## License

filesql is released under the [MIT License](./LICENSE).

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://debimate.jp/"><img src="https://avatars.githubusercontent.com/u/22737008?v=4?s=75" width="75px;" alt="CHIKAMATSU Naohiro"/><br /><sub><b>CHIKAMATSU Naohiro</b></sub></a><br /><a href="https://github.com/nao1215/filesql/commits?author=nao1215" title="Code">💻</a> <a href="https://github.com/nao1215/filesql/commits?author=nao1215" title="Documentation">📖</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://sayportfolio.vercel.app/"><img src="https://avatars.githubusercontent.com/u/240962040?v=4?s=75" width="75px;" alt="Sai Asish Y"/><br /><sub><b>Sai Asish Y</b></sub></a><br /><a href="https://github.com/nao1215/filesql/pulls?q=is%3Apr+author%3ASAY-5" title="Documentation">📖</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/krishna3554"><img src="https://avatars.githubusercontent.com/u/87197325?v=4?s=75" width="75px;" alt="Krishna lokhande"/><br /><sub><b>Krishna lokhande</b></sub></a><br /><a href="https://github.com/nao1215/filesql/issues?q=author%3Akrishna3554" title="Bug reports">🐛</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->
