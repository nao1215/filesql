# filesql

<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-4-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)
[![Gitleaks](https://github.com/nao1215/filesql/actions/workflows/gitleaks.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/gitleaks.yml)
[![Trivy](https://github.com/nao1215/filesql/actions/workflows/trivy.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/trivy.yml)
[![govulncheck](https://github.com/nao1215/filesql/actions/workflows/govulncheck.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/govulncheck.yml)
[![Gosec](https://github.com/nao1215/filesql/actions/workflows/gosec.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/gosec.yml)
[![reviewdog](https://github.com/nao1215/filesql/actions/workflows/reviewdog.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/reviewdog.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![License](https://img.shields.io/github/license/nao1215/filesql)](./LICENSE)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/nao1215/filesql/badge)](https://scorecard.dev/viewer/?uri=github.com/nao1215/filesql)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)

![logo](./doc/image/filesql-logo.png)

filesql loads CSV, TSV, LTSV, JSON, JSONL, Parquet, XLSX, ACH, and Fedwire files into an in-memory SQLite database behind `database/sql`, with no cgo. The [`prep`](https://pkg.go.dev/github.com/nao1215/filesql/prep) package in the same module cleans and validates rows before they become tables, and [sqly](https://github.com/nao1215/sqly) is an interactive shell built on filesql.

The rules the loader follows are in the godoc: https://pkg.go.dev/github.com/nao1215/filesql

## Quick start

Given this `users.csv`:

```csv
id,name,age
1,Alice,31
2,Bob,24
3,Carol,28
```

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/nao1215/filesql"
)

func main() {
	db, err := filesql.Open(context.Background(), "users.csv")
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

```text
2
```

The table is named after the file, so `users.csv` is `users`. Join across formats by opening more of them:

```go
db, err := filesql.Open(ctx, "users.csv", "orders.jsonl", "returns.parquet")
```

## Features

- Query with SQLite syntax, including joins, CTEs, and `json_extract()`, or with MySQL, PostgreSQL, or GoogleSQL syntax through `WithDialect`.
- Read from paths, directories, `io.Reader`, and `embed.FS`, compressed or not.
- Load into a new in-memory database or into a `*sql.DB` you already manage.
- Keep edits in memory and write them back with `DumpDatabase`, `EnableAutoSave`, or `EnableAutoSaveOnCommit`, as UTF-8, Shift-JIS, EUC-JP, ISO-2022-JP, or UTF-16 for csv, tsv, and ltsv.

For analytics on data larger than memory, or on files in S3, use [DuckDB](https://duckdb.org/): its columnar engine is faster and its SQL is richer.

## Installation

```bash
go get github.com/nao1215/filesql
```

Requirements:

- Go 1.26.11 or later
- Linux, macOS, or Windows

Earlier 1.26 releases are not supported because they lack standard library fixes that `govulncheck` finds reachable from filesql: [GO-2026-6088](https://pkg.go.dev/vuln/GO-2026-6088) (`encoding/xml`, read on every XLSX), [GO-2026-5972](https://pkg.go.dev/vuln/GO-2026-5972) (`encoding/asn1`), and GO-2026-6603, GO-2026-6611, GO-2026-6612, GO-2026-6613, GO-2026-6617, and GO-2026-6859 (`net/http`).

## Recipes

`filesql.Open` takes a context, so a load can time out or be canceled with its request.

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

	db, err := filesql.Open(ctx, "users.csv", "orders.jsonl")
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

`WithDialect` lets you write queries in MySQL, PostgreSQL, or GoogleSQL
(BigQuery, Cloud Spanner). filesql translates each query to SQLite before it
runs; loading is unaffected.

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

	db, err := filesql.NewBuilder().
		AddPath("users.csv").
		WithDialect(dialect.PostgreSQL).
		Open(ctx)
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

Translation covers a stated subset of SQL. A query outside it, or a construct
with no SQLite equivalent such as `QUALIFY` or `DISTINCT ON`, is refused with
the line and column of the construct. A non-SQLite dialect cannot be combined
with auto-save. The [`dialect`](./dialect) package lists what is translated.

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

`prep` trims, normalizes case, fills defaults, and reports validation errors with row numbers before the data becomes a table.

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
	db, err := filesql.NewBuilder().
		AddReader(strings.NewReader(string(cleaned)), "users", filesql.FileTypeCSV).
		Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}
```

## Supported formats

| Extension | Format | Notes |
|-----------|--------|-------|
| `.csv` | CSV | Header row becomes column names |
| `.tsv` | TSV | Tab-separated text |
| `.ltsv` | LTSV | Labeled tab-separated text |
| `.json` | JSON | Query nested data with `json_extract()` |
| `.jsonl` | JSONL | One JSON value per line |
| `.parquet` | Parquet | Columnar format |
| `.xlsx` | Excel XLSX | One sheet becomes one table, named `file_sheet`, or `file` when the sheet repeats the file name; see [Table Naming](https://pkg.go.dev/github.com/nao1215/filesql#hdr-Table_Naming) and [Excel Sheet Visibility](https://pkg.go.dev/github.com/nao1215/filesql#hdr-Excel_Sheet_Visibility) |
| `.ach` | ACH (NACHA) | One table per record kind; see [ACH and Fedwire](https://pkg.go.dev/github.com/nao1215/filesql#hdr-ACH_and_Fedwire) |
| `.fed` | Fedwire | One message becomes one row 326 columns wide; see [ACH and Fedwire](https://pkg.go.dev/github.com/nao1215/filesql#hdr-ACH_and_Fedwire) |

Compressed wrappers are supported for CSV, TSV, LTSV, JSON, JSONL, Parquet, and XLSX:
`.gz`, `.bz2`, `.xz`, `.zst`, `.z`, `.snappy`, `.s2`, `.lz4`.

ACH and Fedwire take no compression wrapper.

`FileType` names the format only, and a path's suffix names the codec. A reader
has no path, so `AddReader` takes the codec as an option:

```go
gz, err := os.Open("users.csv.gz")
if err != nil {
	return err
}
defer gz.Close()

builder.AddReader(gz, "users", filesql.FileTypeCSV, filesql.WithCompression(filesql.CompressionGZ))
```

## Behavior and limits

Column typing and blank cells, memory use, duplicate tables and columns, decompression limits, concurrency, saving, Excel sheet visibility, and ACH and Fedwire write-back are documented in the godoc at [pkg.go.dev/github.com/nao1215/filesql](https://pkg.go.dev/github.com/nao1215/filesql), or with `go doc github.com/nao1215/filesql`.

## Examples

The examples of `filesql`, [`prep`](https://pkg.go.dev/github.com/nao1215/filesql/prep#pkg-examples), and [`dialect`](https://pkg.go.dev/github.com/nao1215/filesql/dialect#pkg-examples) are run by `go test`; read them on [pkg.go.dev](https://pkg.go.dev/github.com/nao1215/filesql#pkg-examples) or in [example_api_test.go](./example_api_test.go).

### Integration examples

The [examples](./examples) directory uses filesql with common Go database tools:

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

## Verifying releases

Each GitHub Release from v0.60.2 on carries a source archive (`filesql-<tag>.tar.gz`), `checksums.txt`, its keyless cosign signature (`checksums.txt.sigstore.json`), and the archive's build provenance (`filesql-<tag>.intoto.jsonl`). Download them, replace `<tag>` with the release tag, and verify:

```bash
gh attestation verify filesql-<tag>.tar.gz --repo nao1215/filesql \
  --bundle filesql-<tag>.intoto.jsonl

cosign verify-blob --bundle checksums.txt.sigstore.json \
  --certificate-identity-regexp '^https://github\.com/nao1215/filesql/\.github/workflows/release\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  checksums.txt

sha256sum --check checksums.txt
```

## Testing

| Technique | Bug class caught here |
|---|---|
| Examples and unit tests ([example_api_test.go](example_api_test.go)) | A public function that no longer does what its godoc shows |
| Property-based tests ([compression_test.go](compression_test.go)) | A codec that does not give back the bytes it compressed, or an extension that is not undone |
| Metamorphic tests ([filesql_integration_test.go](filesql_integration_test.go)) | One table loading with other types or values from another format, codec, line ending, chunk size, or from its own dump |
| Differential tests ([filesql_integration_test.go](filesql_integration_test.go)) | A cell stored as another value than SQLite's own column affinity makes of the same text |
| Fuzzing ([internal/parser/parser_test.go](internal/parser/parser_test.go), [internal/codec/codec_fuzz_test.go](internal/codec/codec_fuzz_test.go), [dialect/fuzz_test.go](dialect/fuzz_test.go)) | A crash, hang or unbounded allocation on a malformed file or query |
| Mutation testing ([.github/workflows/mutation.yml](.github/workflows/mutation.yml)) | A test that runs a changed comparison or bound without noticing the change |

How to run each one is in [doc/testing.md](doc/testing.md).

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) before sending a PR. You can support development at https://github.com/sponsors/nao1215.

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
      <td align="center" valign="top" width="14.28%"><a href="https://ashannon.us/"><img src="https://avatars.githubusercontent.com/u/120951?v=4?s=75" width="75px;" alt="Adam Shannon"/><br /><sub><b>Adam Shannon</b></sub></a><br /><a href="#ideas-adamdecaf" title="Ideas, Planning, & Feedback">🤔</a> <a href="#financial-adamdecaf" title="Financial">💵</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->
