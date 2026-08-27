# filesql

[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

![logo](./doc/image/filesql-logo.png)

filesql loads files into an in-memory SQLite database. Open CSV, TSV, LTSV, JSON, JSONL, Parquet, XLSX, ACH, or Fedwire inputs, then query them with normal SQLite syntax.

The same module also includes [`prep`](https://pkg.go.dev/github.com/nao1215/filesql/prep), which cleans and validates rows before they become tables.

[sqly](https://github.com/nao1215/sqly) is the shell built on the same core.

Documentation: https://pkg.go.dev/github.com/nao1215/filesql

## Try it in 30 seconds

```go
package main

import (
	"fmt"
	"log"

	"github.com/nao1215/filesql"
)

func main() {
	db, err := filesql.Open("users.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM users WHERE age > 25").Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Println(n)
}
```

The table is named after the file, so `users.csv` is `users`. Join across formats by opening more of them:

```go
db, err := filesql.Open("users.csv", "orders.jsonl", "returns.parquet")
```

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

## Recipes

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

## Supported File Formats

| Extension | Format | Notes |
|-----------|--------|-------|
| `.csv` | CSV | Header row becomes column names |
| `.tsv` | TSV | Tab-separated text |
| `.ltsv` | LTSV | Labeled tab-separated text |
| `.json` | JSON | Query nested data with `json_extract()` |
| `.jsonl` | JSONL | One JSON value per line |
| `.parquet` | Parquet | Columnar format |
| `.xlsx` | Excel XLSX | One sheet becomes one table, named `file_sheet` (just `file` when the sheet repeats it). A workbook handed to `AddReader` hangs its sheets off the table name given there instead of off a file name, so a workbook added as `book` loads as `book_Sheet1` and as plain `book` when the sheet is itself named `book`. `ExcelSheetTableNames` works out the same names when it is given that table name in place of a path, and `sqlite_master` has them after a load. Every sheet that names a column is loaded by default, a blank scratch sheet being passed over; see [Excel sheet visibility](#excel-sheet-visibility) |
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

## Behavior and limits

The rules the loader and the writers follow are documented beside the API they belong to, so they are in reach while the call is being written: column typing and what a blank cell means, memory and chunked loading, sharing a database across goroutines, what each way of saving writes, Excel sheet visibility, and the ACH and Fedwire write-back. Read them at [pkg.go.dev/github.com/nao1215/filesql](https://pkg.go.dev/github.com/nao1215/filesql), or with `go doc github.com/nao1215/filesql`.

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
| Handle malformed rows | `ExampleDBBuilder_WithMalformedRowPolicy` | [example_api_test.go](./example_api_test.go) |
| Count the rows a skip policy discarded | `ExampleDBBuilder_SkippedRows` | [example_api_test.go](./example_api_test.go) |
| Query with MySQL, PostgreSQL, or GoogleSQL syntax | `ExampleDBBuilder_WithDialect` | [example_api_test.go](./example_api_test.go) |
| Load only the sheets a workbook shows | `ExampleDBBuilder_WithExcelSheetPolicy` | [example_api_test.go](./example_api_test.go) |
| Report a workbook's sheets and their visibility | `ExampleExcelSheetsInFile`, `ExampleExcelSheetsInReader` | [example_api_test.go](./example_api_test.go) |
| Check a workbook's sheets for table names that collide | `ExampleExcelSheetTableNames` | [example_api_test.go](./example_api_test.go) |
| Attach a slog logger | `ExampleDBBuilder_WithLogger` | [example_api_test.go](./example_api_test.go) |
| Open a database that refuses writes | `ExampleDBBuilder_OpenReadOnly` | [example_api_test.go](./example_api_test.go) |
| Save on close or commit | `ExampleDBBuilder_EnableAutoSave`, `ExampleDBBuilder_EnableAutoSaveOnCommit`, `ExampleDBBuilder_DisableAutoSave` | [example_api_test.go](./example_api_test.go), [example_test.go](./example_test.go) |
| Export under a deadline | `ExampleDumpDatabaseContext` | [example_api_test.go](./example_api_test.go) |
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
