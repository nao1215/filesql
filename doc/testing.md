# Testing

`make test` runs everything below except the long fuzz and property searches, which run on a schedule in CI.

## Examples and unit tests

Public functions have runnable examples in `example_api_test.go` (and `prep/example_api_test.go`, `dialect/example_test.go`), checked by `go test` against their `Output:` comments. Unit tests sit beside the source they test, one `_test.go` per source file.

## Property-based tests

The property tests draw their inputs with [rapid](https://pkg.go.dev/pgregory.net/rapid) and shrink a failure to a small counterexample. Their names start with `TestProperty_`.

- `compression_test.go`: what each codec writes it reads back byte for byte, two streams written one after another read back as their concatenation, and `RemoveCompressionExtension` takes off exactly the extension a codec adds.

## Metamorphic tests

A table has no single right answer to compare against, but the same table written in another way has to load the same. These tests generate a table and change only how it is written. Their names start with `TestMetamorphic_` and they live in `filesql_integration_test.go`.

- The same table as CSV, CSV with CRLF, TSV, LTSV and XLSX, as CSV through every codec that has a writer, and as CSV read one row per chunk, loads with the same column types and the same typed values.
- A table dumped as CSV, TSV, LTSV, Parquet or XLSX loads back as the table it was dumped from.

The generator mixes in empty cells, padded numbers, numbers past int64, exponent spellings, dates and text that needs quoting, because type inference draws its lines there.

## Differential tests

`TestDifferential_StoredValuesMatchSQLiteColumnAffinity` in `filesql_integration_test.go` loads a generated CSV with filesql, then builds the same table in a plain SQLite database with the column types filesql chose and every cell inserted as text. SQLite's column affinity decides what each cell becomes there, so the two have to hold the same values, apart from the documented rule that a blank cell in an INTEGER or REAL column is NULL.

## Running the generated tests longer

`go test` draws a hundred inputs per test. Draw more, or replay a failure, with rapid's flags:

```sh
go test . -run '^Test(Property|Metamorphic|Differential)_' -rapid.checks=20000
go test . -run '^TestMetamorphic_' -rapid.seed=<seed printed by the failure>
```

The `Property` workflow (`.github/workflows/property.yml`) draws 20,000 inputs per test every night with a new seed. A failure writes the shrunk input under `testdata/rapid/`, which is ignored by git and uploaded by the workflow.

## Fuzzing

The fuzz targets parse files and queries this package did not write: `FuzzCSVReader` (`internal/reader`), `FuzzParseFormats` and `FuzzParseBinary` (`internal/parser`), `FuzzCodecReader` (`internal/codec`), `FuzzTranslate` and `FuzzTranslationPrepares` (`dialect`), and `FuzzParse` and `FuzzLex` (`dialect/internal/parser`). `go test` runs their seed corpus; the `Fuzz` workflow searches each for ten minutes every night.

```sh
go test ./internal/parser -run '^$' -fuzz '^FuzzParseFormats$' -fuzztime 1m
```

## Mutation testing

[gremlins](https://github.com/go-gremlins/gremlins) v0.6.0 changes one comparison, bound or arithmetic operator at a time and runs the package's tests; a mutant no test notices is a line the tests execute without checking. The `Mutation` workflow (`.github/workflows/mutation.yml`) mutates the lines a pull request changes, and every package once a week in one job per package.

```sh
gremlins unleash --workers 4 --timeout-coefficient 10 --exclude-files / ./internal/infer
gremlins unleash --diff origin/main --workers 4 .
```

`--exclude-files /` keeps a run to the one directory it is given. Run `--diff` from the module root, where the paths git prints match the files gremlins mutates.

Efficacy (killed / (killed + lived)) measured on 2026-11-19, the first full run. A timed-out mutant is one whose tests no longer finish, counted apart from both:

| Package | Killed | Lived | Timed out | Efficacy |
|---|---|---|---|---|
| `.` (root package) | 598 | 8 | 354 | 98.68% |
| `prep` | 583 | 47 | 12 | 92.54% |
| `internal/infer` | 68 | 1 | 5 | 98.55% |
| `internal/reader` | 360 | 68 | 375 | 84.11% |
| `internal/codec` | 64 | 12 | 9 | 84.21% |
| `internal/textin` | 38 | 10 | 3 | 79.17% |
| `internal/writer` | 44 | 7 | 0 | 86.27% |
| `internal/atomicwrite` | 35 | 1 | 0 | 97.22% |
| `internal/parser/ach` | 189 | 250 | 0 | 43.05% |
| `internal/parser/wire` | 367 | 16 | 0 | 95.82% |
| `dialect/internal/parser` | 353 | 111 | 17 | 76.08% |
| `dialect/internal/lower` | 306 | 92 | 2 | 76.88% |
| `dialect/internal/token` | 170 | 72 | 4 | 70.25% |
| `dialect/internal/render` | 147 | 46 | 0 | 76.17% |

`internal/infer` was 79.73% before the edge cases in `TestNumericLiteralPredicatesOnTheirEdges` were added; the one mutant left changes `>=` to `>` at 2^63, where the conversion back to int64 fails either way. Most of the ACH survivors are in `convert.go`, whose output the round-trip tests in the root package check but this package's own tests do not. `dialect/internal/runtime`, `dialect` and `internal/parser` are measured by the first weekly run. A pull request should not lower a package's efficacy below this table.
