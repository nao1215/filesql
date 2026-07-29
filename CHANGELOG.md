# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Dialect queries can now call the scalar functions that were previously missing. Shared: `LEAST` and `GREATEST`. MySQL: `REVERSE`, `FIND_IN_SET`, `FIELD`, `ELT`, `MONTHNAME`, `DAYNAME`, `LAST_DAY`, `UNIX_TIMESTAMP`, `FROM_UNIXTIME`. PostgreSQL: `MD5`, `ASCII`, `CHR`, `TRANSLATE`. GoogleSQL: `FORMAT_DATE`, `FORMAT_DATETIME`, `FORMAT_TIMESTAMP`, `PARSE_DATE`, `PARSE_DATETIME`, `PARSE_TIMESTAMP`, `UNIX_SECONDS`, `UNIX_MILLIS`, `UNIX_MICROS`, `TIMESTAMP_SECONDS`, `TIMESTAMP_MILLIS`, `TIMESTAMP_MICROS`, `TO_HEX`, `IS_NAN`, `SAFE_ADD`, `SAFE_SUBTRACT`, `SAFE_MULTIPLY`, `SAFE_NEGATE`. Each previously failed with `no such function`.
- `REGEXP_REPLACE` accepts PostgreSQL's fourth flags argument: `g` replaces every match, its absence replaces only the first, and `i` matches case insensitively. The three-argument form keeps replacing every match.

### Fixed
- **A cast now follows the source dialect's rules instead of SQLite's type affinity.** Mapping `CAST(x AS type)` onto SQLite's own `CAST` changed results silently: `CAST(1.9 AS SIGNED)` truncated to 1 where every dialect rounds to 2, `CAST('abc' AS INTEGER)` answered 0 where PostgreSQL and GoogleSQL raise, an invalid date or UUID or JSON document passed straight through, `'true'::boolean` collapsed to 0, and the length and scale of `CHAR(n)` and `DECIMAL(p,s)` were discarded. A query written to validate its input therefore reported success on exactly the rows it was meant to reject. Each dialect now converts with its own semantics: PostgreSQL and GoogleSQL raise on a value the target type cannot represent, MySQL coerces the way MySQL does (a numeric prefix or 0 for a string, NULL for an invalid date), and PostgreSQL rounds halves to even while MySQL and GoogleSQL round them away from zero. A target type this package does not model still falls back to a plain SQLite `CAST`.
- **Operators now mean what they mean in the source dialect.** `/` is floating-point division in MySQL and GoogleSQL, so `5/2` was answering 2 instead of 2.5 and every average or ratio came out truncated. `LIKE` is case-sensitive in PostgreSQL and GoogleSQL, but SQLite's folds ASCII case, so a filter matched rows it should not have and PostgreSQL's `ILIKE` was indistinguishable from `LIKE` (`ILIKE` now also folds non-ASCII characters, which the old rewrite could not). MySQL reads `||` as a logical OR under its default `sql_mode` and `<=>` as null-safe equality; PostgreSQL's `^` is exponentiation. MySQL `HEX(255)` is `FF`, not the bytes of the string `255`.
- **`SAFE_CAST` returns NULL when the cast fails.** It was rewritten to a plain `CAST`, so an invalid value produced SQLite's fallback (0 for a numeric target, the original string for a date one) rather than the NULL that is the entire reason to write `SAFE_CAST`.

## [0.19.0] - 2026-07-29

### Added
- **Queries can now be written in MySQL, PostgreSQL, or GoogleSQL syntax.** Configure the builder with `WithDialect(dialect.MySQL)`, `dialect.PostgreSQL`, or `dialect.GoogleSQL` (BigQuery / Cloud Spanner), and filesql translates each query to SQLite before running it. Loading files always uses SQLite regardless of the dialect, so only the queries you write are affected. Translation is best-effort compatibility: common incompatibilities are rewritten (identifier quoting, `DATE_ADD`/`DATE_SUB`, `EXTRACT`, `::`/`SAFE_CAST` casts, `ILIKE`, `POSITION`, `SUBSTRING ... FROM ... FOR`, `STRING_AGG`, the `~`/`!~` regex operators, and more), function gaps are filled by helper functions registered into the driver (`NOW`, `DATE_FORMAT`, `TO_CHAR`, `SPLIT_PART`, `SAFE_DIVIDE`, `REGEXP_CONTAINS`, `GENERATE_UUID`, ...), constructs with no SQLite equivalent (for example `QUALIFY`, `DISTINCT ON`, and `ARRAY`/`STRUCT` types) return a clear error, and anything else is passed through to SQLite. The new `dialect` package (`Dialect`, `Parse`, `Translate`, `RegisterFunctions`, `RegisterTranslator`) exposes the translation layer for direct use. A non-SQLite dialect cannot be combined with auto-save in this release.

### Internal
- Added `context`-based query calls and `rows.Err()` checks to the runnable README/API examples, and applied `gofmt` to the standalone `examples/` module.

## [0.18.0] - 2026-07-19

### Added
- **`prep` is now part of this repository and ships as a first-class companion package.** It preprocesses and validates CSV, TSV, LTSV, JSON, JSONL, Parquet, and XLSX inputs with struct tags, then hands cleaned rows back as an `io.Reader` ready for `filesql`.
- **`frame` is now part of this repository and ships as a first-class companion package.** It provides immutable, Go-style in-memory transforms for small and medium datasets: filtering, mutation, joins, concatenation, grouping, sorting, and missing-value handling.
- **The parser stack is now maintained in-repo again.** `filesql`, `prep`, and `frame` all share the same parser implementation, including JSON/JSONL, Parquet, XLSX, ACH, and Fedwire support.

### Changed
- **filesql now uses the in-repository parser packages instead of the external `github.com/nao1215/fileparser` module.** The public surface stays centered on `filesql.Open`, `OpenContext`, `LoadInto`, and the builder APIs, but parser-backed behavior now ships and evolves in the same release unit.
- **Top-level project docs were rewritten to match the current shape of the repository.** `README.md`, `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, the bug report template, and the pull request template now describe the library, `prep`, and `frame` in the same direct tone as the related `gup`, `sqly`, and `atago` projects.
- **README examples now cover `prep` and `frame` with runnable sample code.** The documented cleanup and in-memory transform flows are backed by tests so the examples stay valid as the APIs evolve.

### Internal
- Added README example tests, parser legacy-compatibility coverage, and smaller test cleanups that removed dead helpers and tightened example error handling.
- Removed the stale Cursor-specific `.cursorrules` file from the repository.

## [0.17.2] - 2026-07-06

### Fixed
- **A zero-padded numeric code no longer loses its leading zeros on import.** Type inference classified a column as INTEGER when every value looked like an integer, so a ZIP code, product ID, or bank code such as `02134` was stored as `2134` and `00501` as `501`. An integer literal with a redundant leading zero (an optional sign, then a `0` followed by more digits) is now classified as TEXT, since both SQLite INTEGER and float64 would drop the leading zero. A lone `0` stays an integer, and a column that also contains a non-numeric value was already TEXT. (PR [#149](https://github.com/nao1215/filesql/pull/149), [eeb798e](https://github.com/nao1215/filesql/commit/eeb798e), Ref [nao1215/sqly](https://github.com/nao1215/sqly))

## [0.17.1] - 2026-07-06

### Fixed
- **A leading Unicode byte-order mark in a text input is now honored instead of corrupting the import.** A UTF-8 BOM (written by Excel, Notepad, and PowerShell) was kept as part of the first CSV/TSV column name and as the first LTSV label, so a query on the plain name failed with `no such column`, and on JSON/JSONL the BOM made the parse fail outright. UTF-16 input fared worse: the null bytes were read as single-byte data and surfaced as a column-count mismatch. Text readers (CSV, TSV, LTSV, JSON, JSONL) now strip a UTF-8 BOM and transcode UTF-16 (LE or BE) to UTF-8 before parsing. Input without a recognized BOM passes through unchanged, so ordinary UTF-8 is untouched and a non-Unicode legacy encoding keeps its original bytes rather than being lossily replaced. Binary formats (Parquet, XLSX) and record formats (ACH, Fedwire) are not decoded. (PR [#147](https://github.com/nao1215/filesql/pull/147), [fca33e4](https://github.com/nao1215/filesql/commit/fca33e4), Ref [nao1215/sqly](https://github.com/nao1215/sqly))

## [0.17.0] - 2026-07-04

### Added
- **`WithMalformedRowPolicy` lets callers choose how a ragged CSV/TSV row is handled.** A row whose field count differs from the header can now stop the import (`MalformedRowStop`, the default), be dropped (`MalformedRowSkip`), or be reshaped to the header width by padding short rows with empty strings and truncating long rows (`MalformedRowFill`). The policy applies to delimited text only; XLSX, LTSV, Parquet, and JSON/JSONL have no per-row field-count mismatch. (PR [#145](https://github.com/nao1215/filesql/pull/145), [3a625dc](https://github.com/nao1215/filesql/commit/3a625dc), Ref [nao1215/sqly#731](https://github.com/nao1215/sqly/issues/731))

### Fixed
- **A ragged CSV/TSV row no longer silently drops data.** A row with fewer or more fields than the header made `encoding/csv` return a field-count error, which the streaming loader masked by creating an empty single-column table, losing both the malformed row and the well-formed rows before it. The default policy now aborts the import with `ErrColumnMismatch` instead of producing an empty table. (PR [#145](https://github.com/nao1215/filesql/pull/145), [3a625dc](https://github.com/nao1215/filesql/commit/3a625dc), Ref [nao1215/sqly#731](https://github.com/nao1215/sqly/issues/731))

## [0.16.0] - 2026-06-29

### Fixed
- **`ReadOnlyDB` now actually enforces read-only access.** Previously `Query`/`QueryRow` passed SQL through unchecked, so a `DELETE ... RETURNING` executed via the Query path mutated data, and write detection only matched the leading keyword, so `/*comment*/ DELETE` or `WITH ... DELETE` slipped past `Exec`. Statements are now scanned for write keywords at the top level — skipping comments, string literals, quoted identifiers, and CTE bodies — and rejected on the `Query`/`QueryRow` and `Exec` paths of both `ReadOnlyDB` and `ReadOnlyTx`. SQLite-specific mutators are also blocked: an assigning `PRAGMA` (e.g. `PRAGMA foreign_keys = ON`), `ATTACH`/`DETACH`, `VACUUM`, `ANALYZE`, and `REINDEX`, while reading PRAGMAs such as `PRAGMA table_info(...)` stay allowed. (PR [#138](https://github.com/nao1215/filesql/pull/138), [ea75ee2](https://github.com/nao1215/filesql/commit/ea75ee2))
- **The `*sql.DB` returned by `Open`/`OpenContext` is now safe to share across goroutines.** It was backed by a single in-memory SQLite connection reused for every pooled connection, so sharing it across goroutines could race and crash (the README warned about segmentation faults). It now uses a uniquely named shared-cache in-memory database, so each pooled connection opens its own connection to the same data and `database/sql` manages them; queries issued while iterating `*sql.Rows` keep working. Auto-save keeps its single-connection design, now documented on `EnableAutoSave`. (PR [#138](https://github.com/nao1215/filesql/pull/138), [ea75ee2](https://github.com/nao1215/filesql/commit/ea75ee2))

### Changed
- README and translations now describe the implementation accurately: files are loaded into an in-memory SQLite database (rather than queried "directly"), the concurrency section matches the shared-cache behavior, and a new "Memory and streaming" note states which formats stream in chunks (CSV, TSV, JSON arrays) and which are read fully into memory (LTSV, non-array JSON, Parquet, Excel). Updated across all 7 languages. (PR [#138](https://github.com/nao1215/filesql/pull/138))

### Internal
- Extracted the SQL write-detection logic into a focused `internal/sqlguard` package, brought `make lint` to green, and added a multilingual README drift-detection test (`doc_sync_test.go`) plus race tests for the concurrent and nested-query paths. (PR [#138](https://github.com/nao1215/filesql/pull/138))

## [0.15.0] - 2026-06-29

### Fixed
- Parquet export and reload now preserve SQL `NULL`. A null cell was written as an empty string and reloaded as an empty string, so `NULL` and `""` became indistinguishable after a round-trip. The Parquet writer now stores a real null, and the streaming reader reloads it as SQL `NULL`. Other formats are unaffected, since they carry no null concept. (PR [#136](https://github.com/nao1215/filesql/pull/136), Ref [nao1215/sqly#686](https://github.com/nao1215/sqly/issues/686))

## [0.14.1] - 2026-06-28

### Dependencies
- `modernc.org/sqlite`: 1.51.0 → 1.53.0 (PR [#134](https://github.com/nao1215/filesql/pull/134), [238acf7](https://github.com/nao1215/filesql/commit/238acf7))
- `github.com/pierrec/lz4/v4`: 4.1.26 → 4.1.27 (PR [#131](https://github.com/nao1215/filesql/pull/131), [f5cd865](https://github.com/nao1215/filesql/commit/f5cd865))
- `actions/checkout`: 6 → 7 (PR [#133](https://github.com/nao1215/filesql/pull/133), [ba55c2a](https://github.com/nao1215/filesql/commit/ba55c2a))

## [0.14.0] - 2026-06-01

### Fixed
- LTSV imports now reject a label repeated within a single record (for example `x:1\tx:2`) instead of silently keeping only the last value and dropping the earlier one. Both the streaming parser and the chunked parser fail with a `duplicate column name` error, matching the CSV/TSV parsers, so LTSV imports stay lossless. A label repeated across separate records (the normal column-per-row case) still parses. (Ref [nao1215/sqly#467](https://github.com/nao1215/sqly/issues/467))

### Dependencies
- `github.com/nao1215/fileparser`: 0.5.1 → 0.5.2, which rejects duplicate labels within an LTSV record on the shared parser path.

## [0.13.0] - 2026-05-31

### Added
- `LoadInto(ctx, db, paths...)` and `(*DBBuilder).LoadInto(ctx, db)` load files into an existing `*sql.DB` instead of creating a new in-memory database. This lets callers combine file-derived tables with a database they already manage (for example a long-lived session that imports files repeatedly) without copying the data through a second database. A table whose name matches a loaded file is replaced (last-wins); other tables are left untouched, and the caller's database is never closed. `Open`/`OpenContext` behavior is unchanged. (PR [#129](https://github.com/nao1215/filesql/pull/129), [5afc764](https://github.com/nao1215/filesql/commit/5afc764))

## [0.12.1] - 2026-05-30

### Dependencies
- `modernc.org/sqlite`: 1.50.0 → 1.51.0 (PR #125, [3380ff1](https://github.com/nao1215/filesql/commit/3380ff1))
- `github.com/klauspost/compress`: 1.18.5 → 1.18.6 (PR #121, [e7e1f8a](https://github.com/nao1215/filesql/commit/e7e1f8a))
- `github.com/apache/thrift`: 0.20.0 → 0.23.0 (indirect) (PR #122, [e190ff7](https://github.com/nao1215/filesql/commit/e190ff7))

### Changed
- **Bumped pinned `github.com/nao1215/filesql` version in the `examples/` modules (PR #123, [ff22bc9](https://github.com/nao1215/filesql/commit/ff22bc9))**: Dependabot updated the filesql version pinned by the example modules across 8 directories
- **Consolidated contributor guidance**: Repository guidance was reduced to a single source of truth and the duplicate helper files were turned into short pointers.

## [0.12.0] - 2026-02-14

### Added
- **Fedwire (Legacy Wire) File Support ([e7e2189](https://github.com/nao1215/filesql/commit/e7e2189))**: Complete legacy Fedwire message file support (**Experimental**)
  - **File format**: Tag-value text format (`.fed`) used by the Federal Reserve's large-value real-time gross settlement system
  - **Flat table structure**: All FEDWireMessage fields (~326 columns) flattened into a single `{filename}_message` table with 1 row per file
  - **All fields as TEXT**: Wire format stores amounts as fixed-width strings; all columns use `TEXT` type to preserve formatting
  - **Full round-trip support**: Parse → query/modify via SQL → export back to valid `.fed` format
  - **Registry-backed TableSet**: `registerWireTableSet` / `getWireTableSet` / `UnregisterWireTableSet` / `ClearWireTableSetRegistry` for managing original Wire structures needed for round-trip export
  - **`WireTableInfo` struct**: Provides `MessageTable()` and `AllTableNames()` methods for programmatic table name discovery
  - **`GetWireTableInfos()`**: Returns `[]WireTableInfo` for all registered Fedwire files
  - **`IsWireBaseTableName()`**: Checks if a table name matches the Fedwire `_message` suffix convention
  - **`DumpFedWire()` / `DumpFedWireWithTableSet()`**: Export Fedwire tables from database back to `.fed` files
  - **`OutputFormatFedWire`**: New output format enum for auto-save and dump operations
  - **`ErrWire`**: Sentinel error for Fedwire operation failures
  - **`FileTypeFedWire`**: New file type constant with `String()`, `extension()`, `baseType()` support
  - **Streaming support**: `streamFedWireFileToDatabase()` for file-path input, `streamWireFileToDatabase()` for `io.Reader` input
  - **AddFS support**: Fedwire files in `fs.FS` (including `embed.FS`) are properly detected and loaded
  - **Auto-save integration**: `performFedWireAutoSave()` and `overwriteOriginalFiles()` handle `.fed` files alongside ACH and tabular formats
  - **Test coverage**: Unit tests for file detection, parsing, registry, SQL queries, round-trip, and export

### Fixed
- **Windows file lock in AddFS ([e7e2189](https://github.com/nao1215/filesql/commit/e7e2189))**: Added `closer: file` to `readerInput` in `file_processor.go` so that FS-opened files are properly closed after streaming. Previously, `TempDir RemoveAll` cleanup failed on Windows because files remained open
- **Dead code removal in builder.go ([e7e2189](https://github.com/nao1215/filesql/commit/e7e2189))**: Removed unused `processFSToReaders` method (~85 lines) and `deduplicateCompressedFiles` wrapper from `DBBuilder`. The actual code path uses `fileProcessor.processFSToReaders()`. Updated `builder_test.go` accordingly

### Changed
- **Documentation Updates**: Added Fedwire Support sections to all README files (7 languages: EN, ES, FR, JA, KO, RU, ZH-CN)
  - Supported formats table updated with `.fed` extension
  - Experimental status warning, table structure explanation, TEXT column rationale, limitations, security considerations, and code examples
- **Auto-save cleanup refactoring**: Renamed `cleanupACHRegistry()` to `cleanupTableSetRegistries()` to handle both ACH and Fedwire registry cleanup on connection close
- **ACH table detection improvement**: `dumpSQLiteDatabase()` now verifies registry presence before treating `_message` suffix tables as ACH, preventing false positives with Fedwire's `_message` tables

### Dependencies
- `github.com/nao1215/fileparser`: v0.4.0 → v0.5.1 (adds Wire subpackage for Fedwire parsing)
- `github.com/moov-io/wire`: v0.15.7 (new indirect dependency via fileparser)

## [0.11.0] - 2026-02-13

### Added
- **JSON / JSONL File Support ([e45329d](https://github.com/nao1215/filesql/commit/e45329d))**: Complete JSON and JSON Lines file format support with SQLite `json_extract()` integration
  - **JSON format**: Array root → one row per element, Object root → single row. Raw JSON stored in `data TEXT` column
  - **JSONL format**: One row per line with `bufio.Reader` (no line size limit). Empty lines silently skipped, invalid lines rejected with line number
  - **Query with `json_extract()`**: Access nested fields via SQLite's built-in JSON functions
    - Example: `SELECT json_extract(data, '$.name') FROM my_table`
    - Example: `SELECT json_extract(data, '$.address.city') FROM my_table`
  - **Compression support**: All 8 compression formats supported for both JSON and JSONL (`.json.gz`, `.json.bz2`, `.json.xz`, `.json.zst`, `.json.z`, `.json.snappy`, `.json.s2`, `.json.lz4`, and corresponding `.jsonl.*` variants)
  - **Streaming chunk processing**: `processJSONInChunks` uses `json.Decoder` to stream array elements one at a time without loading the entire array into memory, preventing OOM for large JSON files. `processJSONLInChunks` provides true line-by-line streaming
  - **Trailing data validation**: Rejects malformed JSON with trailing garbage after array (e.g., `[{"a":1}] garbage`)
  - **18 new `FileType` constants**: `FileTypeJSON`, `FileTypeJSONL`, plus 16 compressed variants
  - **Test coverage**: Unit tests, integration tests with `json_extract()` queries, compressed format tests (gzip, zstd, snappy, s2, lz4, zlib), 85.0% overall coverage

### Fixed
- **Missing `parserFileType` mappings ([e45329d](https://github.com/nao1215/filesql/commit/e45329d))**: Added missing zlib, snappy, s2, lz4 mappings for CSV, TSV, LTSV, Parquet, XLSX in `parser_bridge.go`. Previously these compressed variants would fall through to `Unsupported`
- **Pre-existing lint issues**: Fixed `prealloc` warnings in `builder.go` and `file.go`, removed unused `colType` parameter from `newColumnInfoWithType`

### Changed
- **Documentation Updates**: Added JSON/JSONL sections to all README files (7 languages: EN, ES, FR, JA, KO, RU, ZH-CN)
  - Supported formats table updated with `.json` and `.jsonl` base formats and all compression variants
  - Usage examples with `json_extract()` queries for flat and nested JSON structures
- **Test coverage for `parserFileType`**: Added 38 test cases covering ZLIB, SNAPPY, S2, LZ4 for all existing formats plus all 18 JSON/JSONL mappings

### Dependencies
- `github.com/nao1215/fileparser`: v0.3.1 → v0.4.0 (adds JSON/JSONL parsing support)
- `modernc.org/sqlite`: 1.40.1 → 1.45.0
- `github.com/klauspost/compress`: 1.18.2 → 1.18.4
- `github.com/pierrec/lz4/v4`: 4.1.22 → 4.1.25

## [0.10.0] - 2025-12-18

### Added
- **Custom Logger Support**: Flexible logging system with slog integration
  - **`Logger` interface**: Simple logging interface with `Debug`, `Info`, `Warn`, `Error`, and `With` methods
  - **`ContextLogger` interface**: Extended logging interface with context-aware methods (`DebugContext`, `InfoContext`, `WarnContext`, `ErrorContext`)
  - **`NewSlogAdapter()`**: Adapter to use standard library `slog.Logger` with filesql's `Logger` interface
  - **`NewSlogContextAdapter()`**: Adapter for context-aware logging with `slog.Logger`
  - **`WithLogger()`**: Builder method to inject custom logger into the build and open process
  - **`nopLogger`**: Zero-overhead no-op logger implementation used as default (benchmarked at ~0.2 ns/op)
  - Logging throughout build, validation, and database opening operations
  - Comprehensive test coverage and benchmarks for all logger implementations

### Changed
- **Documentation Updates**: Added Custom Logger section to all README files (7 languages: EN, ES, FR, JA, KO, RU, ZH-CN)
  - Usage examples with slog integration
  - Logger and ContextLogger interface definitions
  - Performance benchmark comparison table

## [0.9.0] - 2025-12-18

### Added
- **Read-Only Database Mode**: New `ReadOnlyDB` wrapper for safe read-only access to databases
  - `NewReadOnlyDB(db)`: Wraps existing `*sql.DB` to prevent write operations
  - `ReadOnlyDB.Query()`, `QueryContext()`, `QueryRow()`, `QueryRowContext()`: Read operations work normally
  - `ReadOnlyDB.Exec()`, `ExecContext()`: Returns `ErrReadOnly` for write operations (INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, REPLACE, UPSERT)
  - `ReadOnlyDB.Prepare()`, `PrepareContext()`: Rejects preparation of write statements
  - `ReadOnlyDB.Begin()`, `BeginTx()`: Returns `ReadOnlyTx` for read-only transactions
  - `ReadOnlyDB.Ping()`, `PingContext()`, `Close()`, `DB()`: Standard database operations
  - `ReadOnlyStmt`: Read-only prepared statement wrapper
  - `ReadOnlyTx`: Read-only transaction wrapper with same protections
  - `DBBuilder.OpenReadOnly(ctx)`: Convenience method to open database in read-only mode
  - `ErrReadOnly`: Sentinel error for rejected write operations
  - Useful for audit scenarios where data viewing without modification risk is required

- **ACHTableInfo Struct**: New struct for managing ACH table name information
  - `ACHTableInfo.BaseName`: The base table name derived from ACH filename
  - `ACHTableInfo.FileHeaderTable()`: Returns `{baseName}_file_header`
  - `ACHTableInfo.BatchesTable()`: Returns `{baseName}_batches`
  - `ACHTableInfo.EntriesTable()`: Returns `{baseName}_entries`
  - `ACHTableInfo.AddendaTable()`: Returns `{baseName}_addenda`
  - `ACHTableInfo.IATBatchesTable()`: Returns `{baseName}_iat_batches`
  - `ACHTableInfo.IATEntriesTable()`: Returns `{baseName}_iat_entries`
  - `ACHTableInfo.IATAddendaTable()`: Returns `{baseName}_iat_addenda`
  - `ACHTableInfo.AllTableNames()`: Returns all possible table names for the base name
  - `GetACHTableInfos()`: Returns `[]ACHTableInfo` for all registered ACH files

### Changed
- **Internal ACH Function**: Made `GetACHBaseTableNames` private (`getACHBaseTableNames`) as it was only used internally
  - Use `GetACHTableInfos()` for public access to ACH table information


## [0.8.0] - 2025-12-11

### Added
- **New Compression Formats**: Added support for 4 new compression formats via fileparser v0.2.0
  - zlib (.z) - Standard DEFLATE compression
  - snappy (.snappy) - Google's high-speed compression
  - s2 (.s2) - Improved Snappy extension, faster
  - lz4 (.lz4) - Extremely fast compression


## [0.7.0] - 2025-12-11

### Changed
- Migrated from internal `github.com/nao1215/filesql/parser` to external `github.com/nao1215/fileparser` for file parsing
- Updated all internal references from `parser.` to `fileparser.`

### Removed
- Internal `parser` package (now using `github.com/nao1215/fileparser v0.1.0` as external dependency)

## [0.6.0] - 2025-12-09

### Added
- **Public Parser Package ([6271e5ef](https://github.com/nao1215/filesql/commit/6271e5ef))**: Exposed the internal parser as a public API for use in external projects
  - **New `parser` package**: Standalone file parsing without SQLite dependency
    - `parser.Parse()`: Parse CSV, TSV, LTSV, XLSX, and Parquet files from `io.Reader`
    - `parser.DetectFileType()`: Automatic file type detection from file path
    - `parser.BaseFileType()`: Get base file type from potentially compressed file types
  - **Type exports**: `TableData`, `ColumnType`, `FileType` types for working with parsed data
  - **Parquet support**: Full Parquet parsing with `parser/parquet.go`
  - **XLSX support**: Excel file parsing with `parser/xlsx.go`
  - **Comprehensive test coverage**: 90%+ coverage for the parser package
- **ORM Integration Examples ([281ede2](https://github.com/nao1215/filesql/commit/281ede2))**: Added example code for popular Go ORMs and query builders
  - **GORM**: Full GORM integration example with model definitions
  - **Bun**: Bun ORM example with struct scanning
  - **Ent**: Facebook's Ent framework example with generated code
  - **sqlx**: sqlx example with struct tags
  - **sqlc**: sqlc example with generated type-safe queries
  - **Squirrel**: Squirrel query builder example
  - **Basic**: Standard library database/sql example
  - **Multi-format**: Example combining CSV, TSV, and LTSV files
- **FileType.String() Method**: Added `fmt.Stringer` implementation for `FileType` enum
  - Human-readable format names for logging and debugging
  - Returns names like "CSV", "TSV", "LTSV", "XLSX", "Parquet", etc.

### Changed
- **Documentation Updates**: Enhanced README files across all 7 languages
  - Added fileprep project reference ([e3705a7](https://github.com/nao1215/filesql/commit/e3705a7))
  - Fixed project link formatting ([dea615e](https://github.com/nao1215/filesql/commit/dea615e))
  - Updated fileprep section ([d7905b0](https://github.com/nao1215/filesql/commit/d7905b0))

### Technical Details
- **Architecture**: Parser package enables lightweight file parsing without database overhead
- **Compatibility**: Parser package can be used independently of the main filesql package
- **Testing**: Added comprehensive test suites for parser, types, and error handling

## [0.5.0] - 2025-12-06

### Added
- **Benchmark Tests ([2852ea2](https://github.com/nao1215/filesql/commit/2852ea2))**: Added benchmark infrastructure for performance testing
  - New `make benchmark` target in Makefile for running benchmark tests
  - Benchmark tests isolated with `//go:build benchmark` tag to prevent execution during regular tests
  - `BenchmarkOpenContext` and `BenchmarkOpenContextParallel` for measuring CSV loading performance

### Improved
- **Major Performance Optimization ([d20b3c8](https://github.com/nao1215/filesql/commit/d20b3c8), [e95a5bf](https://github.com/nao1215/filesql/commit/e95a5bf))**: Significantly improved file loading performance
  - **55% faster execution**: Reduced 100,000-row CSV loading time from ~960ms to ~430ms
  - **12% less memory**: Reduced memory usage from ~161MB to ~141MB
  - **Transaction batching**: Wrapped all INSERT operations in a single transaction to reduce SQLite disk sync operations
  - **Slice reuse**: Pre-allocate and reuse value slices in `insertChunkData()` to reduce allocations
  - **Pre-allocation in type inference**: Optimized `newColumnInfoList()` and `inferColumnsInfo()` with pre-allocated column value slices

### Fixed
- **Data Integrity in Chunk Insertion ([b191d93](https://github.com/nao1215/filesql/commit/b191d93))**: Fixed potential data corruption issues in `insertChunkData()`
  - **Stale value prevention**: Fixed issue where records with fewer columns than headers could retain stale values from previous rows
  - **Extra column detection**: Added validation to fail fast when records have more columns than headers, preventing silent data truncation

### Changed
- **Documentation Updates ([17a42fa](https://github.com/nao1215/filesql/commit/17a42fa))**: Added benchmark results to all README files (7 languages)
  - Performance metrics: ~430ms execution time, ~141MB memory for 100,000-row CSV

### Dependencies
- `github.com/klauspost/compress`: 1.18.1 → 1.18.2

## [0.4.6] - 2025-11-27

### Added
- **Header-Only File Support (PR #67, [5de8801](https://github.com/nao1215/filesql/commit/5de8801))**: Files with headers but no data records are now supported
  - CSV, TSV, Parquet, and XLSX formats can now be loaded with only header rows
  - Creates empty SQLite tables with correct column names (all columns as TEXT type)
  - Useful for schema definition files and template files
  - Example: A CSV file containing only `id,name,age` will create a table with those columns but zero rows

### Fixed
- **LTSV Error Handling**: Improved error messages for invalid LTSV data
  - Now correctly returns `"no valid LTSV keys found"` error instead of silently creating empty tables
  - LTSV format requires `key:value` pairs, so header-only concept does not apply

### Changed
- **Dependencies**: Updated library dependencies
  - `modernc.org/sqlite`: 1.40.0 → 1.40.1
  - `github.com/klauspost/compress`: 1.18.0 → 1.18.1
  - `github.com/xuri/excelize/v2`: 2.9.1 → 2.10.0
  - `golang.org/x/crypto`: Security update
  - `actions/checkout`: 4 → 6

## [0.4.5] - 2025-09-17

### Fixed
- **Table Name Sanitization**: Fixed SQL syntax errors caused by special characters in file names
  - Applied `sanitizeTableName()` to all table name generation paths
  - Hyphens, spaces, and special characters are now automatically converted to underscores
  - Example: `"user-data.csv"` → table `"user_data"`, `"my file.csv"` → table `"my_file"`
  - Updated test expectations to match sanitized table names

### Improved
- **API Documentation**: Enhanced documentation for public APIs to clarify table name sanitization
  - Updated `Open()`, `OpenContext()`, and `DBBuilder.Open()` method documentation
  - Added examples showing special character conversion in table names
  - Improved `sanitizeTableName()` function documentation with detailed transformation rules
- **Development Experience**: Optimized test execution time for local development
  - Added GitHub Actions environment checks to skip slow tests locally
  - Reduced local test execution time by 63% (from ~55s to ~20s)
  - Maintained full test coverage in CI/CD while improving developer productivity

### Technical Details
- **Breaking Change Prevention**: Preserved existing `tableFromFilePath()` behavior for backward compatibility
- **Test Coverage**: Maintained 80.7% test coverage with updated test expectations
- **Performance**: No impact on runtime performance, only development-time improvements

## [0.4.4] - 2025-09-03

### Added
- **Memory Management System (PR #49, [d128a27](https://github.com/nao1215/filesql/commit/d128a27))**: Comprehensive memory optimization for large file processing
  - Introduced `MemoryPool` for efficient reuse of byte slices, record slices, and string slices
  - Added `MemoryLimit` with configurable thresholds and graceful degradation
  - Implemented automatic memory monitoring with adaptive chunk size reduction
  - Enhanced XLSX processing with chunked streaming and memory-optimized operations
  - Added comprehensive test coverage (800+ lines) with benchmarks and concurrent access validation
- **Compression Handler (PR #48, [ac04ae9](https://github.com/nao1215/filesql/commit/ac04ae9))**: Factory pattern for file compression handling
  - Unified compression/decompression interface supporting gzip, bzip2, xz, and zstd formats
  - Clean resource management with automatic cleanup functions
  - Comprehensive test suite with end-to-end compression validation
  - Performance benchmarks for different compression algorithms

### Changed
- **Architecture Refactoring (PR #47, [c228ffd](https://github.com/nao1215/filesql/commit/c228ffd))**: Split DBBuilder into focused processors following Single Responsibility Principle
  - Created dedicated `FileProcessor` for file-specific operations
  - Introduced `StreamProcessor` for streaming data processing
  - Added `Validator` for centralized validation logic
  - Improved code maintainability and testability through separation of concerns
- **API Breaking Change**: Exported `Record` type (was previously unexported `record`)
  - Fixed lint issues with exported methods returning unexported types
  - Added comprehensive documentation for migration guidance

### Fixed
- **Memory Pool Resource Management**: Fixed critical backing array tracking issue
  - Resolved potential memory corruption when slice capacity exceeded original allocation
  - Implemented proper resource cleanup with original slice tracking
- **Performance Optimization**: Reduced `runtime.ReadMemStats` call frequency
  - Changed from every 100 records to every 1000 records (10x performance improvement)
  - Added detailed comments explaining the performance trade-offs

### Technical Improvements
- **Enhanced Documentation**: Added comprehensive godoc comments for all new types
  - `MemoryPool` and `MemoryLimit` usage examples and thread safety guarantees
  - Performance notes and best practices for memory management
- **Code Quality**: Replaced magic numbers with named constants throughout memory management
- **Integer Overflow Safety**: Enhanced overflow protection with detailed documentation for edge cases
- **Test Coverage**: Maintained 81.2% test coverage with extensive memory management test suite

## [0.4.3] - 2025-09-02

### Fixed
- **DBBuilder Refactoring (PR #45, [6379425](https://github.com/nao1215/filesql/commit/6379425))**: Major architectural improvements for better maintainability
  - Refactored DBBuilder implementation for cleaner code structure
  - Improved error handling and validation in builder pattern
  - Enhanced code organization and readability

### Technical Improvements
- **Development Tooling Refresh (PR #44, [2575759](https://github.com/nao1215/filesql/commit/2575759))**: Updated local development configuration used during unit testing
  - Improved the test-oriented local tooling setup
  - Enhanced the development environment used by contributors
- **Integration Testing Expansion (PR #43, [48eadbe](https://github.com/nao1215/filesql/commit/48eadbe))**: Added comprehensive integration test coverage
  - Enhanced test coverage with real-world usage scenarios
  - Improved reliability and robustness validation
- **Sample Data Addition (PR #41, [0adba40](https://github.com/nao1215/filesql/commit/0adba40))**: Added sample CSV files for testing and demonstration
  - Enhanced testing capabilities with realistic sample data
  - Improved documentation with practical examples

## [0.4.2] - 2025-09-01

### Changed
- **Type Detection Optimization (PR #39, [4480577](https://github.com/nao1215/filesql/commit/4480577))**: Improved column type inference performance
  - Optimized type detection algorithms for faster processing
  - Enhanced performance when analyzing large datasets
  - Reduced overhead in column type classification
- **Code Refactoring (PR #37, [f78146e](https://github.com/nao1215/filesql/commit/f78146e))**: Cleaned up codebase and improved maintainability
  - Removed unused code and dead functions
  - Simplified internal logic for better readability
  - Refactored complex functions into smaller, more focused units
- **Development Guidelines ([1774b7d](https://github.com/nao1215/filesql/commit/1774b7d))**: Updated CHANGELOG maintenance rules
  - Enhanced documentation for commit reference formatting
  - Improved traceability with GitHub links to commits and PRs

### Fixed
- **Chunk Size Configuration (PR #38, [9cda8b6](https://github.com/nao1215/filesql/commit/9cda8b6))**: Fixed incorrect chunk size settings
  - Resolved issues with chunk size configuration in streaming operations
  - Improved memory efficiency with proper chunk size handling
- **Test Stability (PR #36, [9fa5dbc](https://github.com/nao1215/filesql/commit/9fa5dbc))**: Fixed broken and flaky tests
  - Resolved intermittent test failures
  - Improved test reliability across different environments
  - Enhanced test isolation for parallel execution

### Technical Improvements
- Updated benchmark code to use Go 1.22+ range syntax for cleaner iteration patterns
- Improved overall code quality through refactoring and optimization
- Enhanced development workflow with better documentation standards

## [0.4.1] - 2025-08-31

### Added
- **CI/CD Automation ([11e05c7](https://github.com/nao1215/filesql/commit/11e05c7))**: Enhanced development workflow with automated processes
  - **GitHub Actions integration ([d7bfa9a](https://github.com/nao1215/filesql/commit/d7bfa9a))**: Added pull request automation and code review workflows
  - **Automated release process ([83e3bd5](https://github.com/nao1215/filesql/commit/83e3bd5))**: Auto-release workflow triggered by tag creation
  - **Comprehensive development tooling**: Streamlined the local development experience
- **Contributor Experience Expansion ([775b058](https://github.com/nao1215/filesql/commit/775b058))**: Expanded repository setup and contributor documentation
  - **Development guidelines**: Created detailed contributing guides in 7 languages (EN, JA, ES, FR, RU, KO, ZH-CN)
  - **Coding standards documentation**: Added broader contributor guidance
  - **International contributor support**: Multi-language documentation for the global development team
- **Enhanced Edge Case Testing ([81239fb](https://github.com/nao1215/filesql/commit/81239fb))**: Expanded test coverage for robustness
  - **Error handling validation**: Additional tests for edge cases and error conditions
  - **Stream processing edge cases**: Enhanced testing for unusual input scenarios
  - **Builder pattern validation**: More comprehensive validation of configuration edge cases

### Changed
- **Testing Framework Modernization ([13070fa](https://github.com/nao1215/filesql/commit/13070fa))**: Migrated to testify for improved test maintainability
  - **Reduced test code complexity**: Replaced verbose manual assertions with concise testify assertions
  - **Improved test readability**: Cleaner test structure using `assert` and `require` functions
  - **Enhanced test reliability**: Better error messages and assertion failures with testify
  - **Code reduction**: Significantly reduced test code lines (over 600 lines removed) while maintaining coverage
- **Test Stability Improvements ([1176e12](https://github.com/nao1215/filesql/commit/1176e12))**: Enhanced test reliability and performance
  - **Fixed flaky tests**: Resolved intermittent test failures in concurrent scenarios
  - **Local development optimization**: Added conditions to skip heavy tests in local environments
  - **Better test isolation**: Improved test independence and parallel execution safety

### Dependencies
- **Added**: `github.com/stretchr/testify v1.11.1` for enhanced testing capabilities

## [0.4.0] - 2025-08-30

### Added
- **Excel (XLSX) Support ([942e1d5](https://github.com/nao1215/filesql/commit/942e1d5))**: Complete Microsoft Excel XLSX file support with 1-sheet-1-table architecture
  - **Multi-sheet processing**: Each Excel sheet becomes a separate SQL table with naming format `{filename}_{sheetname}`
  - **Full-featured XLSX integration**: 
    - Header row processing from first row of each sheet
    - Support for compressed XLSX files (`.xlsx.gz`, `.xlsx.bz2`, `.xlsx.xz`, `.xlsx.zst`)
    - Multi-sheet JOIN operations across different sheets in the same workbook
    - Export functionality to XLSX format with table names automatically becoming sheet names
  - **XLSX streaming parser**: Memory-efficient processing using `excelize.Rows()` iterator
    - Eliminated double memory allocation for better performance
    - Added duplicate header validation for parity with CSV/TSV parsers
    - Streaming parser processes first sheet only (use `Open`/`OpenContext` for multi-sheet support)
- **Enhanced Security**: Safe SQL identifier handling
  - `quoteIdent()` function for proper SQLite identifier escaping
  - Sanitized table name generation with `sanitizeTableName()` for all file types
  - Protection against SQL injection through identifier names

### Fixed
- **Critical Windows Compatibility (commit 3e8f4b2)**: Fixed Windows test failures in `TestIntegrationWithEmbedFS`
  - Replaced `filepath.Join()` with forward slashes for embed.FS paths to prevent Windows path separator issues
  - Fixed similar issues in `example_test.go` for consistent cross-platform behavior
- **Excel Column Limit Bug (commit 7a9c3f1)**: Fixed 26+ column support in Excel export operations
  - Replaced arithmetic-based column naming (`'A'+i`) with `excelize.CoordinatesToCellName()`
  - Now supports unlimited columns: 27th column becomes `AA`, 28th becomes `AB`, etc.
  - Proper error handling for coordinate generation failures
- **Case-Insensitive File Detection (commit 4d6e8a3)**: Enhanced compression file detection
  - Made `isCompressedFile()` case-insensitive to match other file type detection functions
  - Files like `.CSV.GZ`, `.TSV.BZ2` now properly detected alongside `.csv.gz`, `.tsv.bz2`
- **Compressed File Path Handling (commit 9b2f5c8)**: Fixed table name derivation for compressed XLSX files
  - Files like `data.xlsx.gz` now correctly produce table name `data` instead of `data.xlsx`
  - Improved logic: first strips compression extension, then strips file extension
- **XLSX Streaming Performance (commit 6c4a7e1)**: Major optimization in XLSX streaming parser
  - **Eliminated double memory allocation**: Removed `io.ReadAll()` + `GetRows()` pattern
  - **True streaming implementation**: Direct use of `excelize.OpenReader()` + `Rows()` iterator
  - **Memory usage reduction**: 50-70% less memory usage for large XLSX files
  - **Improved error handling**: Better error messages with row/column context

### Changed
- **Comprehensive Documentation Updates (commit f1e9d4a)**: Updated all README files across 7 languages (EN, JA, ES, FR, RU, KO, ZH-CN)
  - **Corrected Parquet status (commit 2b7c5e9)**: Updated "planned but not implemented" to "implemented with caveats"
  - **Added Excel (XLSX) documentation (commit 8a3f1d6)**: Comprehensive sections with examples, architecture diagrams, and usage patterns
  - **Fixed XLSX streaming descriptions (commit 5c9b2a4)**: Clarified that XLSX files are fully loaded and all sheets are processed
  - **Enhanced export examples (commit 7e4f8c1)**: Added Parquet and XLSX export examples with proper annotations
  - **Multi-language consistency (commit 3d8e5b7)**: Ensured technical accuracy across all language versions
- **Enhanced Builder Pattern (commit 9f2a6c3)**: Improved table name sanitization and validation
  - Base table names for XLSX files are now sanitized before sheet name concatenation
  - Better handling of special characters and invalid identifiers in file paths

### Breaking Changes
**XLSX File Behavior Change (commit a4e7b9d)**: 
- XLSX files now create **multiple tables** (one per sheet) instead of a single table
- Table names follow the `{filename}_{sheetname}` pattern (e.g., `sales_Q1`, `sales_Q2`)
- This enables full utilization of multi-sheet Excel workbooks but changes the table structure

### Migration Notes
For users upgrading from v0.3.x:
1. **XLSX files**: Expect multiple tables instead of one. Update queries to reference specific sheet tables.
2. **Streaming parsers**: XLSX streaming parsers now process only the first sheet. Use `Open`/`OpenContext` for multi-sheet support.
3. **Table names**: XLSX-derived table names now include sheet names. Update any hardcoded table references.

## [0.3.0] - 2025-08-30

### Added
- **Parquet file format support ([2b77692](https://github.com/nao1215/filesql/commit/2b77692))**: Complete Apache Parquet integration with streaming capabilities
  - **Full Parquet read/write functionality**: Complete implementation using Apache Arrow Go library (v18)
    - `writeParquetData()` function with schema inference and data conversion
    - `parseParquet()` and `parseCompressedParquet()` for reading Parquet files
    - Support for both uncompressed and externally compressed Parquet files (.parquet.gz, .parquet.bz2, .parquet.xz, .parquet.zst)
  - **Parquet streaming support**: Memory-efficient processing for large Parquet files
    - `parseParquetStream()` method for streaming Parquet data from io.Reader
    - `processParquetInChunks()` for chunked processing with configurable batch sizes
    - `bytesReaderAt` helper for random access requirements
  - **Export functionality**: Parquet output format in database dump operations
    - `OutputFormatParquet` enum value for export configuration
    - Integration with existing `DumpDatabase()` function and `DumpOptions`
    - Maintains schema and data type information during export
- **Comprehensive Parquet testing**: Extensive test coverage for all Parquet functionality
  - Integration tests for Parquet read/write operations with real data
  - Streaming functionality tests with chunked processing
  - Compressed Parquet file handling tests
  - Cross-format compatibility tests (CSV → Parquet → SQLite)

### Changed
- **Unified streaming architecture**: All file formats now use consistent streaming approach
  - Consolidated file processing pipeline through `streamReaderToSQLite()`
  - Removed format-specific processing functions in favor of unified stream handling
  - Enhanced memory efficiency across all supported formats (CSV, TSV, LTSV, Parquet)
- **Enhanced test coverage**: Improved from 73.5% to 80.7% coverage (exceeding 80% target)
  - Added comprehensive tests for dump options functionality
  - Enhanced column inference testing with mixed data types
  - Added LTSV chunk processing tests for better coverage
  - Expanded Parquet-specific test scenarios

### Fixed
- **Code quality improvements ([5d5f337](https://github.com/nao1215/filesql/commit/5d5f337))**: Resolved all linting issues (13 total issues fixed)
  - **errcheck**: Fixed unchecked error returns with proper error handling
  - **gofmt**: Applied consistent code formatting across all files
  - **gosec**: Addressed security issues with appropriate nolint annotations for test files
  - **noctx**: Updated database operations to use context-aware methods (`BeginTx`, `ExecContext`)
- **Concurrent access simplification ([cabb4cc](https://github.com/nao1215/filesql/commit/cabb4cc))**: Removed complex goroutine usage in favor of simpler, more reliable patterns
  - Simplified database connection management per user feedback
  - Enhanced test reliability and reduced race condition potential
- **Memory management**: Improved resource cleanup in Parquet processing
  - Proper memory allocator usage with Apache Arrow
  - Better error handling for Parquet file operations
  - Enhanced cleanup of temporary resources during streaming

## [0.2.0] - 2025-08-27

### Added
- **Major architecture enhancement**: Stream processing support and domain model restructuring
- **Stream processing capabilities ([e1ad820](https://github.com/nao1215/filesql/commit/e1ad820))**: Complete stream-based file loading for improved memory efficiency
  - `AddReader()` method in Builder pattern for stream input support
  - Chunked reading for local files to handle large datasets efficiently
  - Memory-optimized processing for both local files and streaming data
  - Stream-friendly auto-save functionality with proper resource management
- **Integration testing framework ([a3f3d77](https://github.com/nao1215/filesql/commit/a3f3d77))**: Comprehensive BDD-style integration tests using Ginkgo/Gomega
  - Full end-to-end behavior validation for library functionality
  - Stream processing integration tests with various data sources
  - Auto-save functionality testing across different scenarios
  - Cross-platform compatibility verification

### Changed
- **Domain model architecture restructuring ([bcb92f5](https://github.com/nao1215/filesql/commit/bcb92f5))**: Breaking change for improved maintainability
  - Moved all model types from `domain/model` package to main `filesql` package
  - Simplified import structure and reduced package complexity
  - Enhanced type organization and accessibility for library users
  - Streamlined API with consolidated model definitions
- **Enhanced file loading system**: Improved file processing with stream support
  - Unified file loading approach supporting both file paths and streams
  - Better memory management for large file processing
  - Enhanced chunked reading implementation for local files
  - Improved error handling and resource cleanup

### Fixed
- **Auto-save functionality ([54b9336](https://github.com/nao1215/filesql/commit/54b9336))**: Resolved limitations and edge cases in auto-save operations
  - Fixed auto-save behavior with stream inputs and temporary files
  - Improved handling of auto-save with various input sources
  - Enhanced error recovery and cleanup during auto-save operations
  - Better validation for auto-save configuration consistency
- **Stream processing stability**: Enhanced reliability of stream-based operations
  - Proper resource management for stream readers
  - Improved error handling in chunked reading scenarios
  - Fixed memory leaks in stream processing pipeline

## [0.1.0] - 2025-08-26

### Added
- **Initial major feature release ([31cabc4](https://github.com/nao1215/filesql/commit/31cabc4))**: Library with comprehensive Builder pattern and auto-save functionality
- **Builder pattern architecture ([9238c13](https://github.com/nao1215/filesql/commit/9238c13))**: Complete implementation of extensible Builder pattern for flexible configuration
  - `NewBuilder()` provides fluent API for database construction
  - `AddPath()` method for adding individual files and directories
  - `AddFS()` method for embedded filesystem support (go:embed compatibility)
  - `EnableAutoSave()` and `EnableAutoSaveOnCommit()` for automatic data persistence
  - `Build()` method with comprehensive validation and error checking
  - Chainable method design for clean, readable configuration code
- **go:embed and fs.FS support**: Full integration with Go's embedded filesystem capabilities
  - Works seamlessly with `//go:embed` directive for embedded data files
  - Custom `fs.FS` implementation support for advanced use cases
  - Automatic temporary file management for embedded content
  - Cross-platform embedded file handling
- **Advanced auto-save functionality**: Comprehensive automatic data persistence system
  - **Two timing modes**: Save on database close (`OnClose`) or transaction commit (`OnCommit`)
  - **Overwrite mode**: Automatically saves back to original file locations when output directory is empty
  - **Directory mode**: Saves to specified backup directory with original file names
  - **Format preservation**: Maintains original file formats (CSV, TSV, LTSV) and compression
  - **Configurable compression**: Support for gzip, bzip2, xz, and zstd compression options
  - **Transaction integration**: Seamless integration with database transaction lifecycle

### Changed
- **Enhanced driver interface (commit 5c3a8f2)**: Breaking change with auto-save configuration support
  - Extended `Connection` struct with auto-save capabilities and original path tracking
  - Updated `Connector` interface to support Builder-generated configurations
  - DSN format extended to include JSON-encoded auto-save configuration via base64 encoding
- **Enhanced export system (commit 7f2e9a6)**: Improved table export with comprehensive format support
  - Extended `DumpOptions` with detailed format and compression configuration
  - Enhanced compression detection and writer creation pipeline
  - Improved error handling with proper resource cleanup and partial file removal
  - Better cross-platform file path handling and sanitization

### Fixed
- **Auto-save overwrite mode (commit 2e8f4d9)**: Fixed critical issue where overwrite mode incorrectly used current working directory
  - Now properly uses original input file locations for file overwrites
  - Maintains correct directory structure and file naming conventions
  - Preserves original file formats and compression settings automatically
- **Builder validation (commit 4b6a3c7)**: Enhanced configuration validation with detailed error reporting
- **Memory management (commit 9d1f5e8)**: Improved cleanup of temporary files created from embedded filesystems

## [0.0.4] - 2025-08-24

### Added
- **Version 0.0.4 release ([45f3e78](https://github.com/nao1215/filesql/commit/45f3e78))**: Minor version update with maintenance improvements

### Changed
- Project maintenance and version management updates

## [0.0.3] - 2025-08-24

### Added
- **Enhanced security compliance ([c74d1eb](https://github.com/nao1215/filesql/commit/c74d1eb))**: Added gosec security linter to the build process
  - Comprehensive security analysis for potential vulnerabilities
  - File permission restrictions (0600 for files, 0750 for directories)
  - Protection against SQL injection and file inclusion vulnerabilities
- **Duplicate validation system**: Implemented robust duplicate detection mechanisms
  - **Table name validation**: Prevents multiple files from creating tables with identical names
  - **Column name validation**: Detects and rejects files with duplicate column headers
  - **Cross-directory validation**: Ensures uniqueness across multiple input paths
  - **Compression preference logic**: Automatically prefers uncompressed files over compressed versions
- **Comprehensive test coverage expansion**: Significantly increased driver package coverage
  - Driver package coverage increased from 73.5% to 83.9%
  - Added extensive transaction testing, connection management, and error handling tests
  - Enhanced export functionality testing and helper method validation
  - Overall project coverage maintained at 80.4%

### Changed
- **Major driver.go refactoring ([33583ce](https://github.com/nao1215/filesql/commit/33583ce))**: Complete architectural reorganization for improved maintainability
  - **Method decomposition**: Split complex methods into focused, single-responsibility functions
    - `loadFileDirectly` → `loadSinglePath`, `validatePath`
    - `loadSingleFile` → `parseFileToTable`, `loadTableIntoDatabase`
    - `collectDirectoryFiles` → `readDirectoryEntries`, `shouldSkipFile`, `handleTableNameConflict`
    - `loadMultiplePaths` → `collectAllFiles`, `collectFilesFromPath`, `collectSingleFile`
  - **Database operations unification**: Centralized query execution and statement handling
    - `executeQuery`: Unified interface for all database queries
    - `executeStatement`: Consistent statement execution with proper context support
    - `scanStringValues`: Standardized database response processing
  - **CSV export enhancement**: Modular CSV generation pipeline
    - `writeCSVFile`, `writeDataRows`, `convertRowToCSVRecord`: Clean separation of concerns
    - Improved error handling and resource management
  - **Enhanced documentation**: Comprehensive package and method documentation
    - Detailed usage examples and feature descriptions
    - Clear API documentation for all public interfaces
- **Improved error handling consistency**: Standardized error formatting and path validation
- **Cross-platform compatibility improvements**: Enhanced Windows/Unix path handling compatibility

### Fixed
- **Security vulnerabilities**: Addressed all gosec security findings
  - **G104 (Unhandled Errors)**: Proper error handling in all file and database operations
  - **G201/G202 (SQL Injection)**: Secure SQL query construction with parameterization
  - **G301/G302/G306 (File Permissions)**: Restricted file and directory permissions for security
  - **G304 (File Inclusion)**: Safe file path handling with proper validation
- **Cross-platform path issues**: Fixed Windows filepath separator compatibility
  - Normalized path comparisons using `filepath.Clean()` for consistent behavior
  - Unified output path formatting in examples and tests
  - Resolved GitHub Actions Windows test failures
- **Code quality improvements**: 
  - All linting issues resolved with stricter gosec configuration
  - Proper code formatting with gofmt
  - Performance optimizations (replaced `fmt.Sprintf` with `strconv.Itoa` where appropriate)

### Technical Details
- **Security hardening**: Comprehensive security audit and remediation
- **Architecture improvement**: Clean code principles applied throughout driver implementation
- **Testing enhancement**: Robust test suite covering edge cases and error scenarios
- **Documentation quality**: Improved code documentation and usage examples
- **Platform compatibility**: Verified compatibility across Linux, macOS, and Windows environments

## [0.0.2] - 2025-08-24

### Added
- **OpenContext function ([79621f8](https://github.com/nao1215/filesql/commit/79621f8))**: Added `OpenContext(ctx context.Context, paths ...string)` function for context-aware database opening
  - Enables timeout control and cancellation support
  - Provides better resource management and operation control
  - Maintains backward compatibility by making `Open()` call `OpenContext()` internally
- **Comprehensive test coverage**: Added extensive tests for OpenContext functionality
  - Context timeout scenarios
  - Context cancellation handling
  - Concurrent access testing
  - Error handling validation
- **Example documentation**: Added `ExampleOpenContext` demonstrating proper usage with timeouts

### Changed
- **Updated all README files ([7d73c70](https://github.com/nao1215/filesql/commit/7d73c70))**: Modified all 7 language versions to use OpenContext in examples
  - English (README.md)
  - Japanese (doc/ja/README.md)
  - Russian (doc/ru/README.md)
  - Chinese Simplified (doc/zh-cn/README.md)
  - Korean (doc/ko/README.md)
  - Spanish (doc/es/README.md)
  - French (doc/fr/README.md)
- **Improved database operations**: All examples now demonstrate proper context usage
  - Added timeout configuration in examples
  - Replaced `context.Background()` with reusable context variables
  - Enhanced error handling patterns

### Fixed
- **Linting issues**: Resolved all golangci-lint warnings
  - Fixed context usage in tests to use `t.Context()` where appropriate
  - Adopted Go 1.22+ integer range loops syntax (`for i := range numGoroutines`)
  - Improved error wrapping with `%w` format verb instead of `%v`
  - Ensured proper code formatting with gofmt

### Technical Details
- **Go version compatibility**: Leverages Go 1.24 features as specified in go.mod
- **Test improvements**: Enhanced test reliability and coverage
- **Code quality**: Maintained 79.3% test coverage
- **Documentation consistency**: Ensured all language versions provide equivalent information

## [0.0.1] - 2025-08-23

### Added
- Initial release of filesql library
- Support for CSV, TSV, and LTSV file formats
- Compression support for .gz, .bz2, .xz, .zst files
- SQLite3-based in-memory database engine
- Multi-file and directory loading capabilities
- Cross-platform compatibility (Linux, macOS, Windows)
- Database export functionality via `DumpDatabase`
- Comprehensive test suite
- Multi-language documentation (7 languages)
- Standard database/sql interface implementation

[Unreleased]: https://github.com/nao1215/filesql/compare/v0.19.0...HEAD
[0.19.0]: https://github.com/nao1215/filesql/compare/v0.18.0...v0.19.0
[0.18.0]: https://github.com/nao1215/filesql/compare/v0.17.2...v0.18.0
[0.17.2]: https://github.com/nao1215/filesql/compare/v0.17.1...v0.17.2
[0.17.1]: https://github.com/nao1215/filesql/compare/v0.17.0...v0.17.1
[0.17.0]: https://github.com/nao1215/filesql/compare/v0.16.0...v0.17.0
[0.16.0]: https://github.com/nao1215/filesql/compare/v0.15.0...v0.16.0
[0.12.1]: https://github.com/nao1215/filesql/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/nao1215/filesql/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/nao1215/filesql/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/nao1215/filesql/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/nao1215/filesql/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/nao1215/filesql/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/nao1215/filesql/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/nao1215/filesql/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/nao1215/filesql/compare/v0.4.6...v0.5.0
[0.4.6]: https://github.com/nao1215/filesql/compare/v0.4.5...v0.4.6
[0.4.5]: https://github.com/nao1215/filesql/compare/v0.4.4...v0.4.5
[0.4.4]: https://github.com/nao1215/filesql/compare/v0.4.3...v0.4.4
[0.4.3]: https://github.com/nao1215/filesql/compare/v0.4.2...v0.4.3
[0.4.2]: https://github.com/nao1215/filesql/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/nao1215/filesql/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/nao1215/filesql/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/nao1215/filesql/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/nao1215/filesql/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/nao1215/filesql/compare/v0.0.4...v0.1.0
[0.0.4]: https://github.com/nao1215/filesql/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/nao1215/filesql/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/nao1215/filesql/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/nao1215/filesql/releases/tag/v0.0.1
