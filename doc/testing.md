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

## Running the generated tests longer

`go test` draws a hundred inputs per test. Draw more, or replay a failure, with rapid's flags:

```sh
go test . -run '^Test(Property|Metamorphic)_' -rapid.checks=20000
go test . -run '^TestMetamorphic_' -rapid.seed=<seed printed by the failure>
```

The `Property` workflow (`.github/workflows/property.yml`) draws 20,000 inputs per test every night with a new seed. A failure writes the shrunk input under `testdata/rapid/`, which is ignored by git and uploaded by the workflow.

## Fuzzing

The fuzz targets parse files and queries this package did not write: `FuzzCSVReader` (`internal/reader`), `FuzzParseFormats` and `FuzzParseBinary` (`internal/parser`), `FuzzCodecReader` (`internal/codec`), `FuzzTranslate` and `FuzzTranslationPrepares` (`dialect`), and `FuzzParse` and `FuzzLex` (`dialect/internal/parser`). `go test` runs their seed corpus; the `Fuzz` workflow searches each for ten minutes every night.

```sh
go test ./internal/parser -run '^$' -fuzz '^FuzzParseFormats$' -fuzztime 1m
```
