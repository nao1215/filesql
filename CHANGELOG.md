# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- A CSV, TSV, or LTSV whose lines end with a lone carriage return now imports its rows instead of loading empty ([#279](https://github.com/nao1215/filesql/issues/279)). The CSV and LTSV readers understand LF and CRLF, so a file written with the classic Mac OS 9 convention was one very long line: the data was folded into the column names and the table came out with zero rows, at no error. The line ending is now decided from the first 64 KiB of the file, counting only what sits outside quotes: a file is read this way when that window holds a carriage return outside quotes and no line feed outside them, and only carriage returns outside quotes are translated, so a quoted one stays data in either kind of file. A first record longer than that window is left as it is rather than guessed at.

## [0.39.1] - 2026-08-08

### Fixed

- The duplicate-header check folds ASCII case only, and keeps its two comparisons apart. v0.39.0 folded with `strings.ToLower` over a trimmed name, which refused two headers SQLite accepts: `ä` beside `Ä`, whose case SQLite does not fold, and `" A"` beside `"a"`, which differ by whitespace and by case at once so that neither rule matches on its own. A name is now compared with surrounding whitespace removed, and again with ASCII case ignored, as two separate questions — the rule the export side of nao1215/sqly was written against, and the one SQLite itself applies.

## [0.39.0] - 2026-08-08

### Added

- `DBBuilder.SkippedRows` reports what `MalformedRowSkip` discarded, per table, with the count and the number of data rows it was choosing from. Skipping is an instruction from the caller, but an instruction that reports nothing left an import that dropped one row and one that dropped most of the file looking identical — and a write-back afterwards makes either one permanent. A load that dropped nothing is not listed, so a non-empty result is always worth reporting to a user.

### Fixed

- ACH and Fedwire write-back refuses a value too wide for its fixed-width record instead of cutting it to fit ([#257](https://github.com/nao1215/filesql/issues/257)). The two halves of the same edit behaved differently: an amount past its field failed the write with a FieldError, while a name past its field was shortened by the writer's own formatter and written at no error, so the file on disk held data the session never asked for — `individual_name` cut at 22 characters, `company_name` at 16, a Fedwire originator name at 35. No width is hardcoded: each field formatter returns exactly the number of characters its record holds, so its output is the specification's own answer. The Fedwire side walks the message rather than listing its fields, since the library defines close to three hundred of them, and only refuses a value whose formatted form is a strict prefix of it — the shape truncation leaves.
- A value that only a TEXT column holds losslessly keeps its column TEXT wherever it sits in the file ([#255](https://github.com/nao1215/filesql/issues/255)). Three kinds of value are damaged by a numeric column — a zero-padded code, an integer literal past int64, and Go-only numeric syntax — and the classifier already refused to call any of them numeric. It only ever saw a sample: at most 1000 values per column, taken from the first chunk. A code arriving after that met a column that was already INTEGER, and SQLite's affinity rewrote it on the way in, so `007` came back as `7` and an account number past int64 as `1.104032026e+19`, at exit 0 with nothing on stderr. Whether a column is INTEGER or REAL is still decided from a sample; whether a cell survives is now asked of every value, and a chunk that widens a column rebuilds the table before its rows are inserted.

### Documentation

- README describes what column-type inference guarantees and what it does not ([#256](https://github.com/nao1215/filesql/issues/256)). Decimal spelling is not preserved: `2.50` loads as the REAL `2.5`, `1.00` as `1`, and `1e3` as `1000`. Keeping the spelling would mean a TEXT column, and SQLite compares a TEXT column against a number as text, so `WHERE amount > 9.5` over `9.00` and `10.00` matches nothing — the arithmetic is worth more than the formatting. The rule is now written down beside the three cases that do force TEXT, rather than left to be discovered.

## [0.38.0] - 2026-08-08

### Added

- `DumpOptions.WithEncoding` writes csv, tsv, and ltsv output in Shift-JIS, EUC-JP, ISO-2022-JP, or UTF-16 as well as UTF-8. The read side has understood these encodings for a long time and the write side had no matching option, so a caller that decoded a legacy source before loading had no way to get one back: every save wrote UTF-8, which meant an in-place save silently changed the file's encoding on disk and the caller's next read of the same file returned mojibake. Output is UTF-8 unless the option says otherwise, and the UTF-16 encodings write a byte-order mark so the read side recognizes them without being told. A value the encoding cannot write fails the save with `ErrEncoding` and leaves the destination untouched, rather than being replaced with a substitute character. Parquet and XLSX carry their own encoding and are unaffected.

### Fixed

- ACH write-back rebuilds each batch control from the entries, so an edited amount can be written at all. `File.Create` builds the file control from the batch controls and leaves the batch controls alone — that is `Batch.Create`'s job — so each batch kept the control the original file arrived with, and every amount edit failed the write with "TotalDebitEntryDollarAmount calculated N is out-of-balance with batch control M". Editing the control column instead did not help, because the recalculation it was waiting for never ran. Control records are derived values: the write recalculates them, for IAT batches too, and an edit to a control column is overwritten rather than honored.

- `STRING_AGG(DISTINCT x, s)` is answered at translate time in the PostgreSQL and GoogleSQL dialects instead of reaching SQLite. Both dialects accept the form and SQLite cannot express it, because its DISTINCT aggregates take exactly one argument and the separator has nowhere to go. The call used to reach the engine and fail with "DISTINCT aggregates must have exactly one argument", which describes SQLite's parser rather than the query the caller wrote, so the construct was neither translated, rejected, nor runnable. A separator of `','` is now dropped, since that is already what `group_concat` joins with and the answer is unchanged; any other separator is refused by name, the way MySQL's `GROUP_CONCAT(DISTINCT x SEPARATOR s)` already was. An aggregate `ORDER BY` is carried into the rewritten call.

## [0.37.1] - 2026-08-07

### Removed

- The second XLSX loader in `builder.go`, which no production path reached. `Open` and `OpenContext` load a workbook through the streaming processor; `streamXLSXFileToSQLite` and the three helpers under it were a parallel implementation only their own tests exercised. That is how the duplicate column rule came to differ by format in the first place — the copy nobody ran was free to disagree with the one that did. The behaviors those tests covered (a multi-sheet workbook, an empty one, invalid bytes, and a table name already taken) are covered against the loader that runs.

## [0.37.0] - 2026-08-07

### Changed

- One rule for duplicate column names, whatever format the header arrived in. Names are compared with surrounding whitespace removed, which is what reading a CSV has always done; a workbook was the exception on both counts. The loader that reads a workbook checked nothing, so `name` beside ` name ` became two columns there and a duplicate everywhere else, while an exact duplicate reached SQLite and came back as its own "duplicate column name" wrapped in a database-operation error — an error no caller can match with `errors.Is(err, ErrDuplicateColumn)`. A workbook now fails the same check a CSV does, with the same error and the offending column named.

## [0.36.0] - 2026-08-07

### Changed

- A text source that is not valid UTF-8 is rejected instead of loaded. SQLite stores TEXT as UTF-8, so bytes in a legacy encoding were stored verbatim and read back as mojibake: `LENGTH` counted the wrong number of characters, `LIKE` and `UPPER` worked on fragments of characters, and the load reported success. A Shift-JIS CSV exported from Excel is the ordinary way to reach this. The load now fails with `ErrInvalidUTF8`, naming the offending byte and its offset, and the caller transcodes before loading. Validation is not detection: nothing in the byte stream says which encoding it is, and a wrong guess would be the same silent corruption in another shape. It covers CSV, TSV, LTSV, JSON, and JSONL, whose readers already share one entry point; UTF-16 with a byte-order mark is still transcoded as before, and valid UTF-8 passes through byte for byte.

### Fixed

- A duplicate column name error says which column it means. The name was printed unquoted, so a header with two unnamed columns (`a,,`) produced `duplicate column name:` with nothing after the colon, and neither the name nor its position was recoverable from the message. It now reads `duplicate column name: "" (column 3)`.

## [0.35.2] - 2026-08-06

### Fixed

- Column type detection no longer declares a type the storage will not match. `strconv.ParseFloat` accepts Go source syntax that SQLite's numeric affinity does not convert, so a digit-separating underscore (`1_000`) or a hexadecimal float (`0x1p4`) made the column `REAL` while every value in it was stored as text. A datetime alongside a number did the same: the numeric type won on confidence and the datetime was stored as text under it. Both are TEXT now, which is what the storage always was. A caller reading the schema to plan a numeric comparison had no way to detect the difference.

## [0.35.1] - 2026-08-06

### Fixed

- A select item whose expression was rewritten and which already carried an
  implicit alias is no longer given a second name. `SELECT CONCAT(a,b) z`
  became `SELECT strict_concat(a,b) z AS "CONCAT(a,b) z"`, which does not
  parse, so a query that worked in 0.34.0 failed in 0.35.0 with a syntax error
  near `AS`. The alias detection read a closing parenthesis as an operator and
  so took the name after it for part of the expression; a bare word after `)`
  is an alias. Only the implicit form was affected — `AS z` was always
  detected — and only for expressions a dialect rewrites.

## [0.35.0] - 2026-08-06

### Fixed

- A Parquet file's declared column types now reach SQLite instead of every
  column arriving as TEXT. Parquet states the type of each column in its own
  schema, and the streaming import ignored it, so a `DOUBLE` price and an
  `INT64` quantity became text and carried TEXT affinity into every comparison:
  `MAX(price)` returned the lexicographically largest value, `ORDER BY price`
  sorted digit strings, and `WHERE price > 100` compared `100` as `'100'`. The
  same rows loaded from CSV, where types are inferred from the values, answered
  those queries correctly, so one file gave two answers depending on its format.
  The Arrow schema now decides the column type, which is also the only way to
  tell a `STRING` column of zip codes from an `INT64` one. `parser.Parse` reads
  the schema the same way.

- A Parquet export writes each column as the type its values call for rather
  than as `STRING`. Parquet readers trust the schema, so a numeric column
  written as digit strings reached the next tool as text. A column whose values
  are not all numeric is still written as `STRING`, because SQLite types values
  rather than columns and a dump has to carry back what the rows held; a column
  with no rows takes the declared type of the table it came from.

- A dialect rewrite no longer renames the caller's result columns. SQLite names
  an unaliased result column after the text of the expression that produced it,
  so rewriting the expression renamed the column: PostgreSQL's `SELECT
  amt::text` came back as `postgresql_cast(amt, 'text')`, and that name reached
  the caller as a CSV header and a JSON key. MySQL's `/` and `CONCAT`,
  GoogleSQL's `SAFE_CAST`, and every other rewritten select item had the same
  problem. A rewritten item now carries its original text as an alias, so
  `amt::text` stays `amt::text`. An item the query already named, a bare column
  reference, and a `*` are left alone.

## [0.34.0] - 2026-08-05

### Added

- `Dialect.DisplayName()` returns a dialect spelled the way its own project
  spells it: `SQLite`, `MySQL`, `PostgreSQL`, `GoogleSQL`. The wire value is a
  lowercase identifier, right for a flag value and wrong in a sentence, so every
  caller that printed one for a person kept its own table of names and drifted
  from `Dialects()` as soon as a dialect was added. A dialect installed with
  `RegisterTranslator` reads back as its wire value, so any dialect with a name
  has one to print; the zero `Dialect` returns `""`.

## [0.33.0] - 2026-08-05

### Changed

- A file that fails to load is reported once instead of three times. The wrapper
  said `filesql: parsing failed: failed to stream file rm.csv: filesql: column
  count mismatch: row 1 has 2 fields, want 3` — two framing verbs for one event
  and the package's own name twice — and a caller that had already named the
  file added a third mention of the path, because there was no way to reach the
  cause without it. The failure is now a `*ParseError` carrying `Source` and
  `Err`: its message names the input once, `errors.Is` still finds `ErrParsing`
  and whatever sentinel the cause holds, and a caller that loads one file at a
  time can read `Err` and say the path itself. The reader that produced the
  cause no longer announces `parsing failed` a second time either, so a cause
  that already carried `ErrParsing` is reported once rather than nested inside
  itself.

## [0.32.1] - 2026-08-04

### Fixed

- Stop discarding an input because another input shares its base name.
  Deduplication — which exists so a dataset offered both plain and compressed is
  read once — was keyed on the derived table name, so `a/users.csv` and
  `b/users.csv` looked like one source and one of them was dropped without a
  word. Files are now the same source only when they are in the same place: the
  path with any compression suffix removed. `dir/users.csv` still displaces
  `dir/users.csv.gz`; `a/users.csv` and `b/users.csv.gz` are two inputs and both
  are loaded.
- Return the surviving inputs in the order they were given. The result was built
  by ranging over a map, so the order changed between runs of the same command.
  Everything downstream reads it: which input a `LoadInto` leaves in place under
  its last-wins rule, which malformed file a failing load names, and the order
  the duplicate-table check works through.

Loading a directory tree that holds two files of the same base name in different
subdirectories now fails with `ErrDuplicateTable` instead of silently loading
one of them. That is a behavior change, and it is the point: the previous
outcome was to lose a file and not say which. `LoadInto` and `LoadIntoTx` keep
their last-wins contract for a same-named table — what changed is that both
inputs are now read at all.

## [0.32.0] - 2026-08-04

### Added

- Excel sheet visibility policy. `DBBuilder.WithExcelSheetPolicy` decides which
  sheets of a workbook a load reads: `ExcelSheetPolicyAll` (the default, and the
  behavior every previous release had) reads them all, and
  `ExcelSheetPolicyVisibleOnly` reads only the sheets the workbook shows. The
  policy applies to every source — a path, a directory, an embedded filesystem,
  a reader, and a compressed workbook alike — because all of them now select
  sheets through one function instead of each calling the sheet list itself.
  Table names are worked out after the policy has run, so a hidden sheet that
  would sanitize to the same table as a visible one is not a collision when it
  is not loaded.
- `ExcelSheetsInFile` and `ExcelSheetsInReader` report a workbook's sheets and
  whether it shows each one, without loading anything. A caller that has to
  explain which sheets a policy left behind needs the same view filesql uses,
  and reimplementing it would let the two drift.
- `parser.WithExcelSheetPolicy` applies the same setting to `parser.Parse`,
  which reads one sheet: under the visible-only policy it takes the first sheet
  the workbook shows rather than the first one stored.

Excel separates "hidden", which a reader can undo from the sheet tabs, from
"very hidden", which only the VBA editor can. excelize reports one boolean
covering both, so filesql does not tell them apart: the visible-only policy
leaves out either kind and claims nothing about which it was.

## [0.30.4] - 2026-08-03

### Fixed

- Report the remaining cleanup failures instead of discarding them: closing a
  compressing writer (which is what flushes it, so its failure can mean a
  truncated file), closing the per-sheet insert statement of an XLSX load, and
  removing the staged file an atomic write leaves behind. Each is joined onto
  the operation's own error, so a caller whose write already failed still learns
  the output is unusable or that a temporary file is still on disk.

## [0.30.3] - 2026-08-03

### Fixed

- Report a failed rollback instead of discarding it. A load that fails and then
  cannot be undone previously returned only the parse or schema error, so a
  caller could not tell that the database had been left mid-load. The rollback
  failure is now joined onto the cause and both are reachable with `errors.Is`
  and `errors.As`.
- End a load's transaction exactly once. A failure while preparing an insert
  statement rolled the transaction back and then let the outer handler roll it
  back again; the second call could only report `sql.ErrTxDone`, which was
  discarded.
- Report a prepared statement that fails to close. It holds a connection, so the
  effect used to surface later as an unrelated stall rather than as this load's
  failure.
- Treat a rollback that reports `sql.ErrTxDone` under a canceled context as
  cancellation rather than a broken transaction, since `database/sql` has
  already rolled the transaction back in that case.

### Added

- `ErrCleanup`, the sentinel marking a failure that happened while releasing or
  undoing something after an operation finished. Use `errors.Is(err,
  filesql.ErrCleanup)` to tell "the load failed" from "the load failed and
  something was left behind".

## [0.30.2] - 2026-08-02

### Fixed

- Defer ACH and Fedwire registry publication until the transaction that creates
  their SQLite tables commits successfully.
- Make `MalformedRowFill` pad short CSV/TSV rows while rejecting long rows
  instead of discarding source fields.
- Handle empty JSON and JSONL inputs in the same streaming load path without a
  second read.

## [0.30.1] - 2026-08-02

### Added

- **Caller-controlled transactions for loading.** `DBBuilder.LoadIntoTx` loads CSV, TSV, LTSV, JSON, JSONL, XLSX, ACH, and FedWire inputs into a caller-provided `*sql.Tx`. The loader does not commit or open nested transactions, so callers can combine multiple loads with their own schema changes and roll everything back together. Existing `LoadInto` behavior for standalone database loads is unchanged.

## [0.30.0] - 2026-08-02

### Breaking Changes

The public API of the root package went from 159 top-level declarations to 123, and the `FileType` constants from 65 to 10. Everything removed was either unreachable from outside the package or reachable and unusable; nothing that a caller could act on was taken away.

- **The 56 fused `FileType` constants are gone.** `FileTypeCSVGZ`, `FileTypeTSVBZ2`, `FileTypeJSONLLZ4` and the rest named a format-and-codec combination. `FileType` now names formats only: `FileTypeCSV`, `FileTypeTSV`, `FileTypeLTSV`, `FileTypeParquet`, `FileTypeXLSX`, `FileTypeJSON`, `FileTypeJSONL`, `FileTypeACH`, `FileTypeFedWire`, `FileTypeUnsupported`.
- **`prep` no longer re-exports the parser's 56 compressed constants**; it re-exports formats only. `parser.FileType` is unchanged and still has them.
- **`MemoryPool`, `MemoryLimit`, `MemoryStatus`, `MemoryInfo`, `Record`, `TableName`, `ChunkSize`, `NewTableName`, `NewChunkSize`** and their methods are unexported. None appeared in any exported signature or struct field, so no call could have passed one in.
- **`ErrorContext`, `NewErrorContext`, `WithTable`, `WithDetails`, `ErrorContext.Error` are removed.** No error this package returns was built through them.
- **`ValidationPeekSize`, `MaxSampleSize`, `MinConfidenceThreshold`, `EarlyTerminationThreshold`, `MinDatetimeLength`, `MaxDatetimeLength`, `SamplingStratificationFactor`, `MinRealThreshold`** are unexported. `DefaultRowsPerChunk`, `DefaultChunkSize`, and `MinChunkSize` remain — they document what `SetDefaultChunkSize` does with its argument.

### Migration Notes

For users upgrading from v0.29.x:

1. **`AddReader` with a compressed source.** Replace the fused constant with the format plus `WithCompression`. The three-argument call is unchanged, so a reader of uncompressed bytes needs no edit.

   ```go
   // before
   builder.AddReader(gz, "users", filesql.FileTypeCSVGZ)

   // after
   builder.AddReader(gz, "users", filesql.FileTypeCSV, filesql.WithCompression(filesql.CompressionGZ))
   ```

2. **File paths are unaffected.** `AddPath("data.csv.gz")` and `Open("data.csv.gz")` still infer both format and codec from the name.

3. **`errors.Is` now reaches every sentinel an error names.** Code that matched on message text to work around this can use `errors.Is` instead. Nothing that worked before stops working: the messages are unchanged.

4. **Excel workbooks of several sheets can now be saved back in place.** A save that previously failed with "only a workbook of one sheet can be written back to itself" now succeeds. A save whose tables would collide on one sheet name is refused rather than silently dropping a table.

### Fixed
- `errors.Is` reached only one of the sentinels an error named. A failure was wrapped as `fmt.Errorf("%w: ...: %s", Sentinel, err.Error())` in 115 places, so the `%s` rendered everything below that frame to text: a save that failed because bzip2 has no writer ended its message with "filesql: unsupported file format: bzip2 compression is not supported for writing" and yet did not satisfy `errors.Is(err, ErrUnsupportedFormat)`, leaving a caller to match on the string to tell "this codec cannot be written" from "the compressor failed". Which sentinel survived depended on which frame happened to be outermost, which is not something a caller can reason about. Those sites now wrap with `%w`. No message changes: `fmt` renders `%w` through the error's own `Error` method, so the text is identical — the whole suite passed unaltered except the one assertion that had been pinning the old behavior on purpose.
- Saving a workbook whose tables collide on a sheet name lost one of them without saying so. Excel caps a sheet name at 31 runes, so two tables of a workbook whose names agree for the first 31 and differ after arrive at the same sheet; excelize answers `NewSheet` for an existing name with that sheet's index rather than an error, so the second table overwrote the first's sheet, one table's rows were gone from the file, and the save reported success. The collision is now refused with an error naming both tables and the sheet, and the workbook is left as it was — the same stance overwrite mode already takes for a format it cannot write.
- Overwriting an Excel workbook in place renamed its sheet, and the rename accumulated. A workbook `book.xlsx` holding a sheet `Orders` loads as the table `book_Orders`, and the save wrote the table's name back as the sheet name, so the file came back with its sheet called `book_Orders`. Loading that gave `book_book_Orders`, then `book_book_book_Orders`, growing by the file's name on every round until Excel's 31-rune sheet name limit truncated it and the sheet stopped being recognizable. A sheet now goes back under the name it came in with. The reverse mapping is not exact — `sanitizeTableName` is not injective, so `Q1 Sales` and `Q1-Sales` both load as `Q1_Sales` and only that form can be recovered — but it is stable, which is what stops the accumulation.

### Added
- An Excel workbook of more than one sheet can be saved back to itself. Auto-save in overwrite mode refused it with "only a workbook of one sheet can be written back to itself", because the writer wrote one sheet per file; a caller who opened a two-sheet workbook with auto-save could not save at all. Every table of the workbook is now written back as its own sheet in one staged write, in a fixed order so the same workbook saved twice is the same file, and through the source's own codec when it was compressed.
- `TestLoadMemoryFootprint`, behind the `benchmark` build tag, reports what loading a CSV actually costs, and the README's "Memory and streaming" section now carries the figure it produces. The number this package had been tracking was `B/op` from `go test -benchmem`, which counts every byte a load ever allocated, garbage included; at around 141MB for the 100,000-row fixture it says nothing about how much memory is held at once, which is the question a caller has. Measured instead: the Go heap stays flat at about 24MB whether the file is 16MB or 131MB, because loading is chunked and the parser holds roughly a chunk; resident memory grows by about 2x the file's size, because the rows live in the in-memory SQLite database rather than on the Go heap. The test prints the table the figure comes from, so it can be re-derived rather than trusted.

### Changed
- `Record`, `TableName`, and `ChunkSize`, with `NewTableName` and `NewChunkSize`, are no longer public. None of them appeared in any exported signature or struct field, so a caller could construct one and then find nothing that would accept it. `Record`'s own doc comment recorded why it was exported in v0.5.0 — "to fix lint issues with exported methods returning unexported types" — and no exported method returns it any more, so the reason had lapsed.
- The memory machinery is no longer public. `MemoryPool`, `MemoryLimit`, `MemoryStatus`, `MemoryInfo`, and their 20-odd methods and constants described a tuning surface a caller could not reach: the only pool and limit this package builds are the ones `newStreamingParser` makes at a hardcoded 1MB and 512MB, and nothing on `DBBuilder` accepts another. `SetDefaultChunkSize` remains the way to tune loading. `MaxSampleSize`, `MinConfidenceThreshold`, `EarlyTerminationThreshold`, `MinDatetimeLength`, `MaxDatetimeLength`, `SamplingStratificationFactor`, and `MinRealThreshold` were internal knobs of column type inference in the same way and are now unexported too; `ValidationPeekSize` was referenced by nothing at all, not even a test, and is gone. `TableName.Sanitize` and `ChunkSize.IsValid` follow, having had no caller outside this package.
- `ErrorContext`, `NewErrorContext`, `WithTable`, `WithDetails`, and `ErrorContext.Error` are removed. Nothing in this package ever called them — the type existed with tests and an example written for it, and no error this package returns was ever built through it. The sentinel errors (`ErrDuplicateColumn`, `ErrParsing`, and the rest) are unchanged and remain the way to inspect a failure.
- `FileType` names a format and no longer fuses in the codec wrapping it, so it has 10 constants instead of 65. 56 of the old ones named a combination — `FileTypeCSVGZ`, `FileTypeCSVBZ2`, and the same eight codecs for TSV, LTSV, Parquet, XLSX, JSON, and JSONL — which made them the single largest part of this package's public API, and the naming was ambiguous where the two axes collided: `FileTypeJSONLZ4` was lz4-compressed JSON while `FileTypeJSONLLZ4` was lz4-compressed JSONL, one `L` apart with nothing in the name to say which reading was right. `AddReader` was the only caller that needed them, because an `io.Reader` carries no path to infer a codec from; it now takes options, and the codec is stated with `WithCompression(CompressionGZ)`. The existing three-argument call is unchanged, so a caller reading uncompressed bytes needs no edit. `prep` re-exported 56 of the parser's compressed constants under a second spelling and now re-exports formats only; `parser.FileType` keeps its fused constants, since that is the lower level where a format-and-codec name is the honest description of what it dispatches on, and `parser.BaseFileType` folds one back.
- `stream.go`'s `createDecompressedReader` was 70 lines of eight arms naming seven formats each — a second implementation of what `CompressionHandler.CreateReader` already did from a `CompressionType` — and is now one line that calls it. One behavior follows from that: for an uncompressed source it returns a no-op close function rather than `nil`, which is `CreateReader`'s convention, so a caller can invoke it unconditionally.

## [0.29.0] - 2026-07-30

### Fixed
- An LTSV value lost the whitespace around it. The reader trimmed the value, while the LTSV writer wrote it and CSV and TSV kept theirs, so `v:  padded  ` loaded as `padded`: a dump and reload through LTSV lost the spaces, and the same data read from LTSV and from CSV disagreed. LTSV defines a value as everything up to the next tab or newline and says nothing about trimming, so the value is now read as those bytes. The trailing side needed the line-level trim narrowed to the line terminator, which had been taking the last field's trailing spaces with it. A label is still trimmed: LTSV restricts one to letters, digits, underscore, dot, and hyphen, so a space around a label is malformed either way.
- An XLSX dump failed for a table whose name Excel cannot use as a sheet name. A table name comes from a file name, so one longer than 31 characters or holding one of `: \ / ? * [ ]` is ordinary input — a table named after `monthly_sales_report_2026_q3_final.csv` could not be exported to XLSX at all, and the error came from the Excel library rather than this package. The name is now adapted the way sqly already adapted it: the forbidden characters become underscores, the name is cut to 31 runes, and apostrophes are trimmed from the edges. A table whose name Excel cannot hold does not read back under it, because no name would; the alternative was refusing to write the file.

## [0.28.0] - 2026-07-30

### Fixed
- A row whose only column was empty disappeared from a CSV or TSV dump. Written plainly, a record of one empty field is a blank line, and a blank line is not a record — a reader skips it. A one-column table of five rows, two of them empty, was dumped and read back as three, and the dump reported success. Such a record is now written as `""`, which says "one field, and it is empty" and cannot be read as anything else. A record of several columns is unaffected, because its delimiters already say how many fields there are.

## [0.27.0] - 2026-07-30

### Fixed
- Auto-save in overwrite mode wrote a new file instead of the one it was overwriting. `EnableAutoSave("")` handed the whole database to `DumpDatabase` with one output directory and the format from its options, which defaults to CSV. A `.tsv` source therefore got a new `.csv` beside it holding the change while the `.tsv` the caller asked to overwrite still held the old rows; the same for `.ltsv`, for a compressed source like `.csv.gz`, and for a workbook, which became one CSV per sheet. Sources in different directories all landed next to whichever was loaded first, and a table created during the session was written out as a file of its own. Each table is now written back to the file it came from, in that file's own format and compression, and only those files are written. A source in a format this package reads but does not write (JSON, JSONL), and a workbook of more than one sheet, now fail the save instead of turning into something else on disk.
- An LTSV file's columns came out in a different order on every load. LTSV has no header line, so the columns are the labels its records carry; those were collected into a map and then read back out of it, and Go randomizes map iteration. The same file answered `SELECT *` as `id,name` one run and `name,id` the next, a dump of it wrote its columns in a different order each time, and any query written against the column positions was unreliable. The columns are now the labels in the order they first appear. Both the whole-file and the chunked load path had their own copy of this.

## [0.26.0] - 2026-07-30

### Fixed
- An XLSX dump could not be written at all. v0.24.0 staged every table dump in a temporary file next to its destination, and Excel was still saved with `SaveAs`, which picks the container format from the file extension — the staged name ends in `.tmp1234567`, which is not a workbook format, so every uncompressed `OutputFormatXLSX` dump failed with "unsupported workbook file format". A compressed one was written, but its sheet was named after the staged file, so reading it back produced a table called `seed__seed_xlsx_gz` instead of `seed`. Every format now writes to the writer it is handed and takes the table name as a parameter, so no format decides anything from a name that belongs to a temporary file.
- A Parquet dump reported failure after writing the file correctly. The Parquet writer closes the destination it is given when it can, so the staged file was already closed when the dump came to close it and check that error, and a complete dump failed with "file already closed". The destination is now passed as a writer that exposes only `Write`.
- A sheet named after its own workbook no longer doubles up in the table name. A sheet's name is appended to the file's name because one workbook holds several sheets, which turned a dumped table read back into `people_people`, and meant a save could not overwrite the file it came from. The suffix is now omitted when it would only repeat the file name; a workbook whose sheet name differs is unaffected.
- A BLOB was dumped as the decimal bytes of a Go slice. The dump used `fmt`'s `%v` as its catch-all, which prints a Go value rather than a data value, so a BLOB holding "hello" was written as `[104 101 108 108 111]` in CSV, TSV, LTSV, Parquet, and XLSX alike, and reading the dump back gave that text instead of the bytes. Every value type a driver can return is now named explicitly.
- A column declared `DATE`, `DATETIME`, or `TIMESTAMP` was dumped in Go's default time layout. The driver parses the stored text into a `time.Time` for those declared types, and the conversion is one-way: `2026-07-30` was written as `2026-07-30 00:00:00 +0000 UTC`, so a dump and reload silently changed the cell. Those columns are now read as text, which keeps whatever the cell holds, including an integer stored in a date column.
- An LTSV dump dropped a value that LTSV cannot hold. LTSV ends a field at a tab and a record at a newline, and defines no escape for either, so a value containing one was written out as a file that parses as something else: the tab opened a second field, which has no label and which the reader discards without a word, and the newline split the record in two. Such a value, and a column name containing a colon, tab, or newline, are now refused with an error that names the column and suggests CSV or TSV. Because the dump is staged, a refused write leaves the destination as it was.
- A dump error no longer loses the failure inside it. The three places that report a failed table, ACH, or Fedwire export attached the inner error as text, so `errors.Is` against the format's own sentinel — `ErrUnsupportedFormat` for a value a format cannot hold, for instance — could not see past the outer `ErrIOOperation`. They now wrap both.
- A table with no rows could not be dumped to Parquet. The Parquet writer refused an empty row set with `ErrEmptyData`, while CSV, TSV, and XLSX wrote their header and nothing else — so a `DELETE` that emptied a table made the dump fail, and an auto-save therefore kept the rows that had just been deleted. An emptied table is now written as a schema with no row groups, which reads back as the same table with its columns and no rows. LTSV stays the exception: it carries a label on every row and so has no header to record columns in.
- A dump wrote outside the directory it was given. A table name is an arbitrary SQL identifier, and the output path was built by joining it to the output directory, which resolves a path separator or a parent reference in it: a table created as `../escaped` had its dump written next to the directory rather than in it. A table whose name is not usable as a file name is now refused, naming the table, and both path separators are refused on every platform so the same database dumped on Linux and on Windows agrees on which tables it can write.

## [0.25.0] - 2026-07-29

### Fixed
- **MySQL and GoogleSQL `CONCAT` swallowed a NULL argument.** Both return NULL when any argument is NULL; SQLite's own `concat()` treats a NULL as an empty string, and the call was passed straight through. `CONCAT(first_name, middle_name)` with a NULL middle name answered the first name where MySQL and BigQuery answer NULL, so a query written to detect incomplete rows reported them as complete. Both dialects now route the call to a NULL-propagating helper. PostgreSQL's `concat()` genuinely does ignore NULLs, so it is left on SQLite's, and `CONCAT_WS`, which skips NULLs in MySQL too, is unchanged.


## [0.24.0] - 2026-07-29

### Fixed
- **A compressed dump that failed to finish was written out as if it had succeeded.** The compression writer's `Close`, which is where a gzip, xz, zstd, zlib, snappy, s2, or lz4 stream writes its trailer, ran in a deferred call whose error was discarded. A dump that could not finish its archive therefore reported success and left a truncated file that no longer decompresses. Both that error and the file's own `Close` error are now returned.
- **A failed table dump destroyed the file it was overwriting.** v0.23.0 staged the ACH and Fedwire writes; the CSV, TSV, LTSV, Parquet, and XLSX path still opened the destination with `os.Create`, which truncates before a byte has been produced. A format the writer rejects, or an I/O failure partway through, left the destination empty — the caller's own source file when the dump is a write-back over it. Every dump now writes to a temporary file beside its destination and moves it into place only on success, so the two paths behave the same way. Windows refuses to rename over a destination another handle still has open, and refuses to rename it out of the way too, which is exactly a save that overwrites a file this package is streaming from; there the staged bytes are copied over it after a copy of it has been taken, and that copy is restored if the write fails, so a refused commit still leaves the data that was there.

## [0.23.0] - 2026-07-29

### Fixed
- **A rejected ACH or Fedwire write destroyed the file it was overwriting.** Both encoders validate while they encode, so a value the format cannot hold — an amount wider than its field, a malformed amount string — is rejected partway through the write. The destination was opened with `os.Create` first, which truncates, and for an in-place save that destination is the caller's own source file: an 891-byte ACH file came back 0 bytes, and Fedwire went further and deleted the file outright, because its failure path removed the output path as a "partial file". The write is now staged in a temporary file next to the destination and renamed over it only after the encoder and the close both succeed, so a rejected write costs nothing. An existing destination keeps its permissions; a new file is created 0644.

## [0.22.0] - 2026-07-29

### Fixed
- **A file name written in a non-Latin script keeps its table name.** Table names were sanitized against the ASCII letter range, so every other letter was deleted: `売上.csv` became `sheet`, `Данные.csv` became `sheet`, and `café.csv` became `caf`. Two such files in one database therefore collided on the same fallback name and only one of them loaded. A table name is always emitted double-quoted, so the restriction bought nothing; the sanitizer now judges letters and digits by Unicode category and keeps combining marks, so `SELECT * FROM "売上"` works. Excel sheet names, which go through the same sanitizer, are fixed with it. A name whose characters are all dropped still falls back to `sheet`, and punctuation, symbols, and quotes are still removed.

## [0.21.0] - 2026-07-29

### Added
- More of each dialect's string and JSON spellings translate. MySQL: `CHAR_LENGTH`, `CHARACTER_LENGTH`, `ORD`, `JSON_UNQUOTE`, and `TRIM(BOTH 'x' FROM s)`. PostgreSQL: `BTRIM`, `OVERLAY(x PLACING y FROM n FOR m)`, `JSONB_ARRAY_LENGTH`, `JSON_ARRAY_LENGTH`, `CHAR_LENGTH`, and the same `TRIM` form. GoogleSQL: `JSON_VALUE`, `JSON_QUERY` (which keeps its result in JSON text, unlike `JSON_VALUE`), `JSON_EXTRACT_SCALAR`, `BYTE_LENGTH`, and `CHAR_LENGTH`. `UNION DISTINCT` is accepted under MySQL and GoogleSQL, where SQLite rejects the keyword and its plain `UNION` already deduplicates.
- A PostgreSQL array literal (`ARRAY[...]`) is reported by name instead of failing on the bracket, which said nothing useful.

### Fixed
- **MySQL `LENGTH` counts bytes.** SQLite's counts characters, so `LENGTH('あい')` answered 2 where MySQL answers 6, and the query succeeded either way.
- **A window or filter clause no longer breaks the operator rewrites.** `SUM(x) OVER (ORDER BY id) / 2` failed to parse under MySQL and GoogleSQL, and `x / COUNT(*) OVER ()` attached the `OVER` clause to the division helper instead of the count. The operand scanners walk back from an operator to the primary expression beside it, and a windowed aggregate ends in the clause's own parentheses, which they mistook for the whole operand. `FILTER (WHERE ...)` and `OVER window_name` had the same problem. Introduced in 0.20.0 with the division and `LIKE` rewrites.
- **The `INTERVAL` amount can be any expression.** `DATE_ADD(d, INTERVAL n DAY)` with a column or an expression as the amount is valid MySQL but was rejected with "INTERVAL value must be a numeric literal".

## [0.20.0] - 2026-07-29

### Added
- Dialect queries can now call the scalar functions that were previously missing. Shared: `LEAST` and `GREATEST`. MySQL: `REVERSE`, `FIND_IN_SET`, `FIELD`, `ELT`, `MONTHNAME`, `DAYNAME`, `LAST_DAY`, `UNIX_TIMESTAMP`, `FROM_UNIXTIME`. PostgreSQL: `MD5`, `ASCII`, `CHR`, `TRANSLATE`. GoogleSQL: `FORMAT_DATE`, `FORMAT_DATETIME`, `FORMAT_TIMESTAMP`, `PARSE_DATE`, `PARSE_DATETIME`, `PARSE_TIMESTAMP`, `UNIX_SECONDS`, `UNIX_MILLIS`, `UNIX_MICROS`, `TIMESTAMP_SECONDS`, `TIMESTAMP_MILLIS`, `TIMESTAMP_MICROS`, `TO_HEX`, `IS_NAN`, `SAFE_ADD`, `SAFE_SUBTRACT`, `SAFE_MULTIPLY`, `SAFE_NEGATE`. Each previously failed with `no such function`.
- `REGEXP_REPLACE` accepts PostgreSQL's fourth flags argument: `g` replaces every match, its absence replaces only the first, and `i` matches case insensitively. The three-argument form keeps replacing every match.

### Fixed
- **A cast now follows the source dialect's rules instead of SQLite's type affinity.** Mapping `CAST(x AS type)` onto SQLite's own `CAST` changed results silently: `CAST(1.9 AS SIGNED)` truncated to 1 where every dialect rounds to 2, `CAST('abc' AS INTEGER)` answered 0 where PostgreSQL and GoogleSQL raise, an invalid date or UUID or JSON document passed straight through, `'true'::boolean` collapsed to 0, and the length and scale of `CHAR(n)` and `DECIMAL(p,s)` were discarded. A query written to validate its input therefore reported success on exactly the rows it was meant to reject. Each dialect now converts with its own semantics: PostgreSQL and GoogleSQL raise on a value the target type cannot represent, MySQL coerces the way MySQL does (a numeric prefix or 0 for a string, NULL for an invalid date), and PostgreSQL rounds halves to even while MySQL and GoogleSQL round them away from zero. A target type this package does not model still falls back to a plain SQLite `CAST`.
- **The aggregates SQLite lacks are now translated.** GoogleSQL `COUNTIF`, `LOGICAL_AND`, `LOGICAL_OR`, and `ANY_VALUE`; PostgreSQL `BOOL_AND`, `BOOL_OR`, and `EVERY`; MySQL `ANY_VALUE` and `STD`; and the `STDDEV`/`VARIANCE` family in all three (each dialect's default estimator is respected: sample in PostgreSQL and GoogleSQL, population in MySQL). The driver has no aggregate registration hook, so each is rewritten into an equivalent SQLite expression. MySQL `GROUP_CONCAT` also keeps its `SEPARATOR` when an `ORDER BY` is present, where SQLite previously read the separator as another sort term and silently joined with a comma; combining `DISTINCT` with `SEPARATOR` now reports that SQLite cannot express it instead of failing with an argument-count message.
- **PostgreSQL `SIMILAR TO` and numeric `TO_CHAR` work.** `SIMILAR TO` had no SQLite equivalent, and `TO_CHAR` gave up and returned NULL on a numeric template such as `'9,999.99'`.
- **Date arithmetic clamps a month-end overflow and keeps a date a date.** SQLite's `datetime()` modifier rolls an out-of-range day forward, so `DATE_ADD('2026-01-31', INTERVAL 1 MONTH)` answered 2026-03-03 where all three dialects clamp to 2026-02-28, and it always rendered a time, so adding a day to a date grew a `00:00:00`. `INTERVAL` now also accepts the `WEEK` and `QUARTER` units and a signed literal such as `INTERVAL -1 DAY`, all of which were rejected outright.
- **The date functions that had no translation now work.** MySQL `TIMESTAMPDIFF` and `TIMESTAMPADD`, its typed date literals (`DATE '2026-01-01'`), and its `POSITION(x IN y)` and `SUBSTRING(x FROM n)` spellings; PostgreSQL's typed date literals and its `x + INTERVAL '1 day'` arithmetic, which is the only date arithmetic that dialect has; GoogleSQL's `DATE_TRUNC(value, PART)` argument order along with `TIMESTAMP_TRUNC` and `DATETIME_TRUNC`, `EXTRACT(DATE FROM ...)` and `EXTRACT(TIME FROM ...)`, and the parenthesized `CURRENT_DATE()` spelling that MySQL and GoogleSQL use.
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

[Unreleased]: https://github.com/nao1215/filesql/compare/v0.30.1...HEAD
[0.30.1]: https://github.com/nao1215/filesql/compare/v0.30.0...v0.30.1
[0.30.0]: https://github.com/nao1215/filesql/compare/v0.29.0...v0.30.0
[0.29.0]: https://github.com/nao1215/filesql/compare/v0.28.0...v0.29.0
[0.28.0]: https://github.com/nao1215/filesql/compare/v0.27.0...v0.28.0
[0.27.0]: https://github.com/nao1215/filesql/compare/v0.26.0...v0.27.0
[0.26.0]: https://github.com/nao1215/filesql/compare/v0.25.0...v0.26.0
[0.25.0]: https://github.com/nao1215/filesql/compare/v0.24.0...v0.25.0
[0.24.0]: https://github.com/nao1215/filesql/compare/v0.23.0...v0.24.0
[0.23.0]: https://github.com/nao1215/filesql/compare/v0.22.0...v0.23.0
[0.22.0]: https://github.com/nao1215/filesql/compare/v0.21.0...v0.22.0
[0.21.0]: https://github.com/nao1215/filesql/compare/v0.20.0...v0.21.0
[0.20.0]: https://github.com/nao1215/filesql/compare/v0.19.0...v0.20.0
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
