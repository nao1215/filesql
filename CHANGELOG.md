# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `parser.ErrTSVSyntax` reports tab-separated input that does not describe a table, which is the sentinel the TSV fix below needs; `ErrCSVSyntax` has been the one for CSV, and without a counterpart the two formats reported the same fault in two ways.

- `DumpOptions.WithLineEnding` sets the line terminator of csv, tsv, and ltsv output, and writing a table back in place with `EnableAutoSave("")` takes that file's own terminator without being asked ([#269](https://github.com/nao1215/filesql/issues/269)). A save kept a source's compression and its text encoding but wrote every record with `\n`, so a CRLF file saved in place came back LF throughout: a caller who edited one row got a file whose every line had changed, which is a whole-file diff in a repository configured for CRLF and a file the tools reading it no longer saw as they had. The terminator is read from the file about to be replaced — through its codec, so a `.csv.gz` is read as the text inside it — and a file with mixed terminators keeps whichever the majority of its lines use, so one stray ending cannot rewrite the rest. The whole file is counted, through a fixed buffer rather than in memory, and a line break inside a quoted CSV field is field data rather than a terminator: a workbook-style export with CRLF between records and LF inside a quoted address is a CRLF file. A dump to a new destination writes `\n` unless `WithLineEnding(LineEndingCRLF)` says otherwise, which is what every save wrote before this existed. `parser.WriteTSVRecordLineEnding` is the same choice for a caller writing TSV records directly. Parquet and XLSX are not line-based and are unaffected.

### Changed

- The README and the write-back functions say that an ACH or Fedwire write-back rewrites the whole file ([#395](https://github.com/nao1215/filesql/issues/395)). Both formats are written from the parsed structure rather than patched, so records nobody edited can come back formatted differently: loading one of the ACH files this repository ships and writing it straight back changes 891 bytes into 950, because the file header and file control in it are written short and the write-back writes every record at its full width, and a Fedwire file comes back with its tags in the order the format defines rather than the order the file had them. The values are the same either way. The serialization belongs to the libraries this package reads and writes those formats with, so the behavior is documented rather than changed: keeping a caller's byte-for-byte formatting would mean writing a NACHA and a Fedwire serializer here. What a caller can rely on is pinned instead: a test per format writes a file back with no edit in it, reloads it, and requires every column of every table to hold what the first load held.

- The README says what chunk size does not change and what it does. A column's type is the same at any chunk size, but a column that reads as a number and turns out to be text is created numeric and rebuilt as TEXT when the text arrives, and the rows already stored then carry SQLite's spelling of the number rather than the file's: with `SetDefaultChunkSize(1)`, a file holding `1`, `2.50` and `abc` gives back `1.0` and `2.5` where the default chunk size gives `1` and `2.50` ([#377](https://github.com/nao1215/filesql/issues/377)). The file's spelling is lost at the insert that precedes the rebuild rather than in the rebuild, so no rendering of a stored number recovers it; storing every row as text and narrowing at the end would, at the cost of a full table copy on every chunked load and a transient doubling of the database's memory, against a documented resident cost of about 2x the file size. The limit is documented while that trade is decided. The Concurrency section also now says that loading is the one thing the shared database is not safe for: creating a table takes a schema lock, so two loads into one database at the same time leave one reporting `database schema is locked` with its table not created ([#383](https://github.com/nao1215/filesql/issues/383)).

### Fixed

- `prep` matches a struct field to a column whatever the header's case ([#407](https://github.com/nao1215/filesql/issues/407)). A field `Name` derives the column name `name` and the header was compared as written, so a file whose header says `Name` — which is what a spreadsheet writes — matched nothing and was refused with an error that listed the column it called missing among the columns the input has. The same file loads through `filesql.Open` without trouble, because SQLite compares identifiers without regard to case, so one half of the library read `Name` and `name` as one column and the other half as two. Both sides of the match are now folded, which is the rule the loader's duplicate-column check already follows; an explicit `name:"..."` tag folds with them. A header holding two columns that differ only in case is still refused before this, by the parser, so nothing becomes ambiguous.

- GoogleSQL's `STRUCT(...)` and `ARRAY(...)` are refused with the error the typed spellings already gave ([#405](https://github.com/nao1215/filesql/issues/405)). `STRUCT<a INT64>(1)` was rejected and `STRUCT(1 AS a)` was not, so the ordinary way to write a struct reached SQLite as a function call and failed with `near "AS": syntax error`, a message about a token the caller did write. `ARRAY(SELECT ...)` had the same shape and failed with `no such function: ARRAY`. Both are now refused by name, which is what `dialect/doc.go` says the rejection class is for: SQLite has neither type, so there is nothing to translate them into and the caller learns that instead of reading an engine message. A column or alias named `struct` or `array` is untouched, since the rejection needs a parenthesis or a type parameter after the word.

- The string helpers count characters, not bytes ([#400](https://github.com/nao1215/filesql/issues/400), [#401](https://github.com/nao1215/filesql/issues/401), [#402](https://github.com/nao1215/filesql/issues/402)). Three of them measured a string in bytes where all three dialects measure in characters, so text outside ASCII came back wrong in three different ways. `LPAD` and `RPAD` cut a multibyte character in half: `LPAD('日本', 4, '*')` returned the bytes `E697A5E6`, which is not valid UTF-8 at all, and this package's own reader refuses those bytes on the way back in. `LOCATE` and `STRPOS` returned a byte offset where the engines return a character position, so `LOCATE('本', '日本語')` was 4 where MySQL says 2, and `LOCATE`'s start position was counted in bytes too; `INSTR` and `POSITION` answer the same question correctly, so which spelling a query used decided whether it was right. `INITCAP` read an accented letter as a word separator, so `école du soir` became `éCole Du Soir` rather than `École Du Soir`. All three now work over runes, which costs a scan: the same 20,000-row query measures 5.10ms before and 5.99ms after for `LPAD`, and 2.93ms before and 3.49ms after for `LOCATE`.

- `UPPER` and `LOWER` fold the whole of Unicode in the MySQL, PostgreSQL and GoogleSQL dialects ([#402](https://github.com/nao1215/filesql/issues/402)). They reached SQLite untouched, and SQLite's own folding stops at ASCII, so `UPPER('école')` came back `éCOLE` while `'École' LIKE 'école'` was already correctly true — the `LIKE` rewrite exists for exactly this reason, and the two disagreed about the same pair of strings. The calls are now rewritten onto helpers of this package's own, the way `LIKE` and `CONCAT` already are. The SQLite dialect is unchanged and still gets SQLite's functions, since a caller writing SQLite wants SQLite's answers. This costs a call across the driver boundary per value: `SELECT COUNT(UPPER(v))` over 20,000 rows measures 1.62ms before and 3.72ms after. It is the same trade the `LIKE` rewrite already makes for the same reason, and it buys the answer every one of these engines gives.

- The streaming loader closes the decompression reader it opens ([#399](https://github.com/nao1215/filesql/issues/399)). Two places wrote `defer handleCloseError(closeFunc)`, and that helper returned a function rather than performing the close, so the defer ran the call that builds the closure and threw the closure away: every load of a `.gz`, `.zst`, `.zlib`, `.lz4`, `.s2` or `.snappy` source left its decoder to the garbage collector instead of releasing it when the load ended. The helper now performs the close and is deferred as `closeQuietly(closeFunc)`, which makes the two call sites correct as they were written and leaves no way to write the wrong thing at a fourth one. staticcheck reported this as SA9010 on main; `make lint` does not run staticcheck, `make lint-staticcheck` does.

- `parser.Parse` refuses a TSV row that does not fit its header ([#397](https://github.com/nao1215/filesql/issues/397)). A row with more cells than the header names, or fewer, was returned as it stood, so `TableData.Records` no longer lined up with `TableData.Headers` or `TableData.ColumnTypes` and a caller reading a record by header position indexed past the header list. `prep` then called such a row valid and wrote it back unchanged, so the pipeline its documentation recommends — hand the reader `prep` returns to `AddReader` — produced a file that failed to load with `filesql: column count mismatch`. The same shape in CSV was already refused, by the CSV reader itself, and the same file loaded through `filesql.Open` was already refused: the row is now refused in the one place that took it, with an error naming the line and both counts. The streaming loader is unchanged and still applies `MalformedRowPolicy`, which is what `Skip` and `Fill` need in order to see the row at all.

- A cast of a value past the integer range answers what the dialect answers ([#389](https://github.com/nao1215/filesql/issues/389)). `CAST(1e30 AS SIGNED)`, `(1e30)::bigint` and `CAST(1e30 AS INT64)` all returned `-9223372036854775808`: the conversion rounded the value and handed it to Go's float-to-integer conversion, which the language specification leaves implementation-defined for a value the integer type cannot hold, so a positive number came back as the most negative one with no error — the silent coercion the cast helpers exist to replace, and not a stable answer either, since it need not be the same on every platform this builds for. The range is now checked before the conversion, and each dialect gives its own answer: MySQL clamps to the bound of the type, which is what it answers with a warning, while PostgreSQL and GoogleSQL raise `ErrInvalidCast`, which `SAFE_CAST` turns into NULL. NaN takes the same two answers, and the bound itself is handled as the arithmetic requires: no float64 holds the largest integer, so the first float outside the range is 2^63, while -2^63 is exact and converts. A digit string too long for a float64 is part of the same fix: `strconv.ParseFloat` answers one with an infinity and `ErrRange`, which was read as a parse failure and sent the value down MySQL's numeric-prefix path, where a 400-digit number came back as 0. A digit string one past the range is answered from the integer parse rather than through a float, because no float64 tells `-9223372036854775809` from the bound itself.

- MySQL's `/` divides the expression to its left rather than the value beside it ([#393](https://github.com/nao1215/filesql/issues/393)). It is the defect below seen through the division pass, which picked its left operand the same way `DIV` did: `7 % 4 / 2` answered 1 where MySQL answers 1.5, because the remainder was taken of the quotient instead of the quotient of the remainder. A chain of multiplications and divisions has the same value however it is grouped, which is why this shows through a remainder alone. The operand now extends over the operators of that precedence level for `/` in MySQL and GoogleSQL; the two `^` operators keep taking one primary, since MySQL's bitwise XOR and PostgreSQL's exponentiation both bind tighter than `*`.

- MySQL's `DIV` divides the expression to its left rather than the value beside it ([#392](https://github.com/nao1215/filesql/issues/392)). MySQL puts `*`, `/`, `%`, `DIV` and `MOD` on one precedence level and associates them left to right, so `8 * 5 DIV 2` is `(8 * 5) DIV 2`; the translation took only the primary expression before the operator, and the query answered 16 where MySQL answers 20, `100 / 5 DIV 2` answered 50 where MySQL answers 10, and `8 % 5 DIV 2` answered 0 where MySQL answers 1. The left operand now extends back over the operators of that level, and is parenthesized in the output so the division pass that runs later sees one expression and cannot regroup it in turn. An operator that binds less tightly is still left outside, so `2 + 8 DIV 2` is 6 as before.

- MySQL's `MOD` operator is translated instead of being handed to SQLite ([#390](https://github.com/nao1215/filesql/issues/390)). `SELECT 7 MOD 2` came back as `near "2": syntax error`, an engine message naming a token the caller did write, while `DIV` — the same family of operator, in the same query — was already rewritten. `MOD` is now written as SQLite's `%`, which is the same operation at the same precedence and with the same sign rule, so nothing about the expression changes but its spelling. The function spelling `MOD(a, b)` already worked and is left alone; a call is told from an operator the way MySQL's own parser tells them apart, by whether the parenthesis follows the name with nothing between.

- A damaged UTF-16 file fails the load rather than arriving with characters it never held ([#385](https://github.com/nao1215/filesql/issues/385)). Text input is read through a decoder chosen by the byte-order mark, and that decoder answers a code unit it cannot use — half of a surrogate pair, or a file whose last unit is cut short by a truncated download — with U+FFFD: the load succeeded, the table held a replacement character no consumer can tell from one the file really contained, and nothing said so. It is the silent corruption the UTF-8 side already refuses, reached through the decoder rather than through a guess about the encoding, so a UTF-16 stream is now checked before it is decoded and a stream that is not well formed fails with `ErrEncoding` naming the byte offset it stopped at. The check reads the byte order from the same mark the rest of the package does, judges the bytes without editing them, and carries a unit and a pair split across two reads into the next one, so the verdict does not depend on where the reads land. A U+FFFD the file really holds is still data, which is why the check runs before the decoder rather than after it, and a surrogate pair — every emoji in a UTF-16 file, including the ones filesql itself writes with `WithEncoding(EncodingUTF16LE)` — still loads as the one character it encodes.

- An XLSX date whose format is led by a color loads as a date ([#386](https://github.com/nao1215/filesql/issues/386)). A custom number format may start with a color, a condition, or a locale, and the rule that keeps an elapsed duration from being read as a calendar day — `[h]:mm` of 1.5 is 36 hours, not a day and a half after the epoch — looked for an h, m or s anywhere inside a bracket. Two of Excel's color names hold one: `[Magenta]mm/dd/yy` and `[White]mm/dd/yy` were read as elapsed durations, so the same serial in the same column arrived as `2023-03-15` in an uncolored row and as `03/15/23` in a colored one, and the column became format-dependent text that `ORDER BY` sorts lexically and no comparison against an ISO literal matches. Excel writes an elapsed unit as one of h, m or s repeated and nothing else, so a bracket is now judged by its whole content: `[hh]` and `[mm]` are elapsed units, and a color, a condition, or a locale is not.

- An XLSX date serial at or below 60 loads as the day the workbook shows ([#387](https://github.com/nao1215/filesql/issues/387)). The 1900 date system counts serial 1 as January 1, 1900 and keeps a February 29, 1900 that no calendar has, so counting plain days from 1899-12-30 is right from serial 61 on and a day early before it: a cell the workbook shows as `01-01-00` was stored as `1899-12-31` and one shown as `02-28-00` as `1900-02-27`, which is every date in January and February 1900 read as the day before the one in the file, with nothing about the result to say so. The two serials that name no day are no longer converted at all: serial 60 is the phantom leap day, and a serial below 1 is before the system starts, so both keep the text the workbook shows rather than being moved onto a neighboring date and making two different cells one. A workbook counting from 1904 has neither problem and is untouched. The correction sits at the one call that converts a serial, and can go away if the library filesql reads workbooks with ever agrees with its own rendering of the same cell.

- Committing from more than one goroutine with `EnableAutoSaveOnCommit` no longer hangs or crashes the process ([#378](https://github.com/nao1215/filesql/issues/378)). Every commit writes the whole database out through the connector's own anchor connection, and the write was started wherever the commit happened: two goroutines committing at once wrapped that one connection in two `sql.DB` pools and drove statements into it side by side, which a SQLite connection does not allow. The pair either blocked forever on the connection's own mutex or faulted inside the SQLite library, and neither is something a caller can catch or retry — a segmentation fault takes the process down, and the README tells callers the database is safe to share across goroutines. The saves are now serialized on a mutex of their own, so a second committing goroutine waits for the first save to finish and then makes its own; a save is a whole-database write, so it was always a serialization point. The lock is separate from the one guarding the connector's fields, which no save now holds while it runs.

- `min` and `max` measure a string field by its length ([#382](https://github.com/nao1215/filesql/issues/382)). Both parsed the cell as a number whatever the field's type, so a string field tagged `validate:"min=3"` reported every name as "value must be a valid number" and, with `WithValidRowsOnly`, dropped every row of the file. The dialect `prep` documents is go-playground/validator, where these two tags mean a magnitude for a number and a length for a string, and `prep`'s own `len` already counted characters, so the two disagreed about the same string in the same struct. The field's Go type now picks the meaning, characters are counted as runes rather than bytes, and the message names what was measured. A numeric field is unchanged.

- A UTF-8 byte-order mark is no longer part of the first column's name ([#381](https://github.com/nao1215/filesql/issues/381)). `filesql.Open` stripped it before parsing, `parser.Parse` and everything above it did not, so a CSV exported by a spreadsheet — which is where the mark comes from — was refused by `prep` with "struct field names a column the input does not have", naming a column that printed exactly like the one the input had. The mark is now consumed for every text format once the input is decompressed, so `prep` and the parser agree with the loader about what a file's columns are called. Parquet and XLSX carry their own container and are untouched, and input with no mark passes through byte for byte.

- Two different integers past 2^53 are two values in `frame` ([#379](https://github.com/nao1215/filesql/issues/379)). Every numeric type was rendered through `float64` to give one canonical spelling, which is what makes `1`, `1.0` and `int64(1)` one value; two integers that round to the same float64 were then one value as well, so `Distinct` dropped a row that was not a duplicate, `GroupBy` merged two groups, and `Join` matched a key against a different number and produced a row that looked like a match. A 19-digit id — a Snowflake id, an account number — is exactly the case. An integer a float64 holds exactly still takes the float spelling, so the cross-type equality is unchanged; one past that takes its own decimal text. `frame`'s aggregates still convert to float64, which they document.

- `Join` no longer overwrites a left column with the right column it renamed ([#380](https://github.com/nao1215/filesql/issues/380)). A right column whose name collides with a left one is renamed by prefixing `right_`, and nothing asked whether that name was free: a left frame already holding `right_v` lost it to the right frame's `v`, with `Columns` naming `right_v` twice and one value gone from every row. Two right columns could collide with each other the same way. The prefix is now repeated until the name is unused, so a join keeps every column of both frames and never names one twice.

- MySQL's `LIKE` folds case the way MySQL does and reads a trailing escape as a character ([#366](https://github.com/nao1215/filesql/issues/366), [#368](https://github.com/nao1215/filesql/issues/368)). It stayed on SQLite's own `LIKE`, with an `ESCAPE '\'` clause appended to supply the default escape MySQL has and SQLite does not. That covered the escape and left two gaps beside it: SQLite's `LIKE` matches nothing at all for a pattern ending in the escape character, so a row holding exactly `A\` was dropped by `LIKE 'A\'`, and its folding stops at ASCII, so `É` did not match `é` under a collation that folds them. Both were already solved in the helper that PostgreSQL and GoogleSQL route through, so MySQL now routes through it too — the same helper, with folding turned on to match MySQL's default collation. A pattern that names its own escape character is still left to SQLite, which handles it natively.

- MySQL's `0x` literal is refused rather than read as a number ([#367](https://github.com/nao1215/filesql/issues/367)). MySQL calls it a binary string: `SELECT 0x41` prints `A`, and `WHERE s = 0x616263` compares against `abc`. SQLite reads the same token as the number 65, so the value came back as a number and the comparison quietly became a numeric one, matching different rows with nothing to say so. Rewriting the literal to the string it stands for would fix that and break the other reading just as quietly — `0x10 + 1` is 17 in MySQL and would have become a string — because which reading applies depends on where the literal sits, which a token rewrite cannot see. A construct with two meanings and no way to tell them apart is what `ErrUnsupportedSyntax` is for: the error names both spellings, `x'41'` for the string and the decimal for the number, and the caller writes the one they meant.

- `parser.Parse` reads a JSON document the way the SQL loader reads the same bytes ([#358](https://github.com/nao1215/filesql/issues/358), [#359](https://github.com/nao1215/filesql/issues/359)). A document holding `null` was refused with "empty JSON array", naming an array the document does not contain, because `null` unmarshals into a slice as the empty slice and the array branch was chosen by whether the unmarshal succeeded rather than by what the document opens with; the parser's own doc says a primitive root becomes one row, and `filesql.Open` on the same bytes gives that row. An empty array `[]` was refused too, where loading the same bytes gives a table with no rows — an array with nothing in it is a document that says there is nothing, not one that cannot be read.

- LTSV labels differing only in case are one column across records as well as within one ([#357](https://github.com/nao1215/filesql/issues/357)). A label was tracked as written when the column list was built, so `id:1` on one line and `ID:2` on the next were two columns with each row filling only its own, and the table SQLite was then asked to create came back as `duplicate column name: ID` — an error naming neither the file nor the rule, and a raw engine message where every other format gives `filesql: duplicate column name`. Both the parser and the streaming loader now fold the label the way they already fold it within a record, and the way SQLite compares the column names it ends up holding; the spelling kept is the one that named the column first.

- An XLSX row carrying more cells than its header names is refused rather than truncated ([#356](https://github.com/nao1215/filesql/issues/356)). The extra cells were dropped with no error, no skipped-row count, and nothing else to say it had happened, which is data in a column the header does not name being discarded silently — the opposite of what `MalformedRowFill` documents for a long record, and of what CSV does with the same shape. A short row is still padded, because a workbook stores no cell for a trailing empty one, so a row ending in blanks is written short and means what the padding says.

- `frame` types a value the way the SQL loader types it ([#352](https://github.com/nao1215/filesql/issues/352), [#353](https://github.com/nao1215/filesql/issues/353)). Two spellings disagreed in opposite directions. The numeric syntax that Go's parsers accept and SQLite's affinity does not convert — `1_0.5`, `0x1.8p1` — was a real in `frame` and text in the loader, and one such value pulled its whole column with it, so the same file read through `frame` held float64 where a query over the loaded table held the string. In the other direction `+7` and `-0` stayed text in `frame`, because the integer conversion asked the value to render back to the same string, where the loader stores the integers 7 and 0. `parser` now applies the loader's guard, and the conversion keeps the quantity rather than the spelling for both types; a spelling that does change the value keeps its whole column text, which the column's type decides before the conversion is reached.

- `Distinct`, `GroupBy` and `Join` agree on when two values are one value ([#354](https://github.com/nao1215/filesql/issues/354), [#355](https://github.com/nao1215/filesql/issues/355)). `Distinct` and `GroupBy` keyed a row by every cell formatted with `%v` and joined by a null byte, so the integer 7 and the text `7` were one row, a missing value merged with the text `<nil>`, `true` merged with `"true"`, and a value carrying a null byte reached across into its neighbor — rows the documented rule calls different were silently dropped, and two groups became one. `Join` had the opposite fault: its index was keyed by the interface value, so 1 and 1.0 did not match although `frame/doc.go` says a join matches them and `Distinct` collapses them. All three now go through one function that writes a value's identity — one tag per kind, one canonical spelling shared by every numeric type, and a length in front of each part so no value can reach into the next.

- `GroupBy` returns its groups in the order they first appear ([#354](https://github.com/nao1215/filesql/issues/354)). They were sorted by the internal group key, which is an encoding of a value rather than the value, so the order a caller saw was neither the frame's nor the values' and changed with the encoding. First appearance is as deterministic and is what the rows themselves say.

- A PostgreSQL block comment nests, so a comment does not end at the first close inside it ([#362](https://github.com/nao1215/filesql/issues/362)). The tokenizer ended every block comment at the first `*/`, so the text between an inner close and the outer one became part of the statement: `SELECT 1 /* /* inner */ + 1 -- */` answered 2 where PostgreSQL's nesting rule answers 1, which means a query that comments out a clause with a nested comment silently ran with that clause back in. Nesting is now tracked for PostgreSQL alone, and MySQL and GoogleSQL, whose comments do not nest, are unchanged. The comment body is rewritten on the way out so it cannot end the SQLite comment it is written into, since SQLite comments do not nest either.

- The escapes that name a character by its number are decoded ([#363](https://github.com/nao1215/filesql/issues/363), [#364](https://github.com/nao1215/filesql/issues/364)). `\xHH`, `\ooo`, `\uXXXX` and `\UXXXXXXXX` fell through to the lenient rule for an unknown escape, which drops the backslash and keeps what follows, so a PostgreSQL `E'\x41'` and a GoogleSQL `'\x41'` both became the three characters `x41` instead of the one character `A`. A comparison against such a literal silently matched different rows than the same query does on the engine it was written for. MySQL is unchanged: it defines no numeric escapes, so `'\x41'` there is still `x41`.

- A GoogleSQL triple-quoted string holds its content bare ([#365](https://github.com/nao1215/filesql/issues/365)). `'''abc'''` was read as an ordinary string whose doubled quotes were SQL-style escapes, so the literal evaluated to `'abc'` — five characters with a quote on each end — and a WHERE clause comparing a column to one matched nothing at all. Both triple-quote spellings are now recognized, including the line breaks and single quotes such a string exists to carry. A prefix combines with them the way GoogleSQL writes it: `r'''..'''`, `b""".."""`, and both at once as `rb` or `br`, which used to read the opening quotes as an empty string and leave the rest of the literal as stray tokens.

- A blob literal that is not hexadecimal is refused instead of producing SQL that does not parse ([#370](https://github.com/nao1215/filesql/issues/370)). `X''''` translated to `x'''` and `x'41''42'` to `x'41'42'`, so the caller got a syntax error from SQLite about text they had not written. The content of a blob literal is now checked where the literal is read, and the error names the offset the way the other lexical errors do.

- Two tokens the input kept apart stay apart when the translation is written back ([#371](https://github.com/nao1215/filesql/issues/371)). MySQL reads a double-quoted literal as a string, so `SELECT X"41"` is a word and a string; rendered with nothing between them the two read as `x'41'`, a blob literal, and the query answered from a value nobody had written, with no error anywhere. Values and names written next to each other are now separated by a space, which cannot change what SQL means.

- `FuzzTranslate` checks a property that can fail ([#369](https://github.com/nao1215/filesql/issues/369)). Its stability check fed the translated output through `Translate(SQLite, ...)`, which returns its input untouched, so it compared a string with itself and passed whatever the translation produced. It now requires the output to be deterministic and to lex as the SQLite it claims to be — a property that found the two rendering defects above within seconds of being turned on.

- An auto-save database is a pooled database like any other, and no longer crashes the process ([#348](https://github.com/nao1215/filesql/issues/348)). Every connection `database/sql` asked the auto-save connector for was a wrapper around one shared `driver.Conn`, and each wrapper ran the save and closed that connection when the pool closed it: the first close saved and closed the real connection, and the next one ran the save against a connection that was already gone, which is a SIGSEGV inside the SQLite driver rather than an error a caller can recover from. Nothing exotic was needed to reach it — a query issued while an earlier query's rows are still open makes the pool open a second connection, and closing the database then closes both — and eight goroutines reading the same database reported a data race for the same reason, although README states the returned `*sql.DB` is safe to share. The connector now opens a real connection per pooled connection against the same shared-cache in-memory database the files were loaded into, which is what the non-auto-save path already did, and holds one connection of its own so the data survives the pool trimming its idle connections. The save runs once, from that connection, when `database/sql` closes the connector — the moment the caller closes the database, and after every pooled connection is gone. Loading is also cheaper: the previous arrangement closed the loader database and streamed every file a second time into the fresh connection, and the data is now simply kept.

- `EnableAutoSaveOnCommit` saves a statement that no transaction wrapped ([#349](https://github.com/nao1215/filesql/issues/349)). The commit hook is the only place the save ran, and `database/sql` reaches it only for an explicit transaction, so a plain `db.Exec` was committed as far as SQLite was concerned and never written: the database held the new value, the file held the old one, `Close` returned nil, and nothing anywhere said the change had been dropped. Commit timing now means the save runs after each committed transaction and again when the database closes, so it saves more often than `EnableAutoSave` and never less. Closing also covers the writes that never pass through the connection's `ExecContext`, such as a statement prepared from the database, which a commit hook alone could not.

- A save in place writes back the text encoding the file already had ([#350](https://github.com/nao1215/filesql/issues/350)). It wrote plain UTF-8 for every source, so a UTF-16 file — which the read side recognizes by its byte-order mark — was replaced with bytes no other reader of that file would accept, and a UTF-8 file that carried a mark lost it, which is how a spreadsheet program recognizes the file it wrote. Either way every byte of the file changed while the caller had edited one row, against the mode's own contract that what comes back is the file the caller had with their edit in it. The four encodings the read side accepts without being told — UTF-8, UTF-8 with a mark, UTF-16LE and UTF-16BE — are now read off the leading bytes and written back. The UTF-16 case also lost its CRLF terminators, because the terminator count ran over raw bytes where a UTF-16 `\r\n` is `\r\x00\n\x00` and the byte before every `\n` is `\x00`; counting now runs over decoded text. An export is unaffected and still writes what `DumpOptions` says.

- A save in place keeps a lone carriage return as the line terminator ([#351](https://github.com/nao1215/filesql/issues/351)). The parser reads a file terminated by `\r` as lines rather than as one very long line, but the terminator count only looked for `\n`, so such a file reported LF and came back rewritten line by line — including when the caller ran no statement at all, which turned opening a file to read it into a whole-file diff. The count now recognizes all three terminators and the majority rule decides among them, the way it already decided between LF and CRLF. `WithLineEnding` still offers LF and CRLF alone: a caller choosing the terminator of new output has no reason to ask for `\r`, and an in-place save reads the terminator from the file rather than from the options.

- A column's type follows from every value it holds rather than from its first chunk ([#344](https://github.com/nao1215/filesql/issues/344)). Types were inferred from the first 1000 rows, and a later chunk could only widen the column for the three values that a numeric column visibly damages, so ordinary text on row 1001 of an otherwise integer column left the column INTEGER holding a thousand integers and one text cell. The same multiset of values then answered the same query differently depending on which row the odd value sat on: `count(*) WHERE v > 100` gave 998 with the text value on row 2 and 901 with it on row 1001, and `ORDER BY v` sorted lexicographically in one case and numerically in the other, with nothing about either result looking wrong. The type is now accumulated over every row read, along a chain where an integer is held by REAL and anything is held by TEXT, so a chunk boundary cannot change the answer and a column only ever widens — a table already holding rows cannot narrow one. The rebuild that a widening chunk already triggered is unchanged, as is what each of the three types stores. Parquet, ACH and Fedwire bring their own schema and are not inferred, so they are unaffected.

- A numeric column that holds any decimal is REAL ([#345](https://github.com/nao1215/filesql/issues/345)). Decimals had to reach a tenth of the column to be counted, so a handful among integers lost the vote and the column was declared INTEGER: SQLite rewrote `4.0` to the integer `4`, stored `2.5` as a real under a declaration that said otherwise, and applied integer division to the whole column. Adding one more decimal row flipped the column to REAL and changed the arithmetic of rows nobody had touched — over ten odd integers plus one decimal, `5 / 2` answered 2 and `sum(v) / count(v)` answered 13; with a second decimal in the file the same rows answered 2.5 and 12.5. There is no reading of the data under which a decimal belongs in an INTEGER column, which is the reasoning the neighboring rule for datetimes beside numbers already followed. Inference also reads the whole column instead of a sample of it, since a sample made the answer depend on which values the sampler happened to look at.

- `DBBuilder.SetDefaultChunkSize` reaches the loader ([#343](https://github.com/nao1215/filesql/issues/343)). The size was stored on the builder and read by nothing, so every load used the 1000-row default however the option was set: a caller lowering it to bound memory on a large file, or raising it to reduce per-chunk overhead, got neither, and the call was accepted and returned the builder for chaining while changing nothing. The README documents it as the way to tune chunked loading and `ExampleDBBuilder_SetDefaultChunkSize` presents it as working, so the implementation is what was wrong. A size of zero or less is still ignored.

- `parser` types a column the way the SQL loader types the same file ([#346](https://github.com/nao1215/filesql/issues/346)). It inferred from the first 1000 records by a majority vote, and `frame` is built on that inference, so the two halves of filesql disagreed: five integers and one text value were TEXT to `OpenContext` and INTEGER to `frame`, whose rows then held `int64` for five values and `string` for the sixth, and a text value past row 1001 was never read at all. The README says `frame` applies the same rules to its own values, and `frame.Row` is offered on the understanding that a column has one type — which a column answering `Int` for some of its rows and `String` for others does not. The rule is now the loader's: every record is read, every kind of value present counts, a number carrying surrounding whitespace keeps the column text because a fixed-width padded code is what that costs, and a datetime beside a number gives text where the vote used to answer datetime. A differential test loads the same inputs through both and requires the Go type of every value to match, since two implementations of one rule drift apart.

- A line break inside a quoted CSV cell survives a save ([#325](https://github.com/nao1215/filesql/issues/325)). It was rewritten to whatever the file's row terminator is, so editing one row changed the contents of a multi-line cell in a row nobody touched: a CRLF file whose cell held `x\ny` came back holding `x\r\ny`, and an LF file whose cell held `x\r\ny` came back holding `x\ny`. A multi-line cell is what a spreadsheet export produces for an address or a comment, and those files are usually the CRLF ones. Two halves hid each other, so the case where the cell's break matched the file's terminator passed by accident and either fix alone would have broken it. On the write side `csv.Writer`'s `UseCRLF` rewrote every line feed it emitted, inside a quoted field as well as between records; records are now staged one at a time so the terminator is this package's choice. On the read side `encoding/csv` removes a carriage return before a line feed inside quotes, which is documented and cannot be configured away, so `parser.CSVReader` reads CSV the way `parser.TSVReader` already reads TSV. It agrees with `encoding/csv` on everything else, which a differential test pins, and loading is no slower: the same time, memory and allocation count as before.

- A file written in a non-UTF-8 encoding says so when it is loaded back ([#329](https://github.com/nao1215/filesql/issues/329)). `WithEncoding` writes Shift-JIS, EUC-JP and ISO-2022-JP; filesql reads UTF-8 only, which the README and the option's godoc now say. Shift-JIS and EUC-JP already failed clearly, their bytes not being valid UTF-8, but ISO-2022-JP is seven-bit and passed that check, then failed as `column count mismatch` — the escape sequences were read as text, so the record really did have the wrong number of fields, and the complaint was about the caller's data on a file filesql had just written from it. Input carrying an ISO-2022-JP designator is now named as such. A bare escape byte is not enough to act on, since it is a legal character that may appear in data.

- A `LoadInto` that fails partway leaves the caller's database as it was ([#332](https://github.com/nao1215/filesql/issues/332)). The table was created — and an existing one of the same name dropped — outside the transaction the rows were inserted in, so a failed load rolled the rows back and kept the empty table. A reload that failed left the caller holding a table that answers queries and returns nothing, where their rows had been: the error said the load failed and the database said the file was empty, which is the shape a program logs and carries on past. The drop and the create now join the same transaction as the inserts. Each input is atomic on its own; inputs are still not atomic with respect to each other, so when the third of three files fails the first two are loaded, and `LoadIntoTx` remains the way to require the whole set or none of it.

- An escaped wildcard in a `LIKE` pattern matches the character rather than acting as a wildcard ([#328](https://github.com/nao1215/filesql/issues/328)). `WHERE code LIKE 'a\%b'` asks for the one row whose code is the literal text `a%b`; under `WithDialect` it returned every row in the table for MySQL and GoogleSQL, and none for PostgreSQL, with `NOT LIKE` failing the same way inverted. Escaping a wildcard is the normal way to search for a value containing `%` or `_`, and an over-match is the worse of the two failures because it hands back rows the caller filtered out and nothing about the result looks wrong. Three things were missing, one per path: the MySQL and GoogleSQL literal reader dropped the backslash of `\%` and `\_`, which both dialects keep precisely so a pattern survives being written as a string; SQLite has no default escape character, so MySQL's `LIKE` now carries the `ESCAPE '\'` it means; and the matcher the PostgreSQL and GoogleSQL translations route through had no notion of escaping at all. A pattern that already names its own escape character is untouched, an unescaped wildcard still matches everything, and `\Z` now reads as ASCII 26 the way MySQL says rather than as the letter Z.

- `DumpDatabase` no longer blocks forever on a database that allows one open connection ([#324](https://github.com/nao1215/filesql/issues/324)). It took a connection out of the pool that it never read from and held it for the whole dump, so every query the dump made waited for the connection the dump itself was sitting on: the call returned no error, never timed out, and a caller who ran it in a goroutine leaked one. A limit of one is not an unusual choice — `LoadInto`, `DBBuilder.LoadInto`, and the README all ask for `db.SetMaxOpenConns(1)` on a `sql.Open("sqlite", ":memory:")` database, because SQLite's `:memory:` is private per connection — so the documented way to load files into a database you own produced exactly the database this package's own export function hung on. The dump runs its queries through the pool and needs no connection of its own; what the acquisition was really doing, failing before the output directory is created when the database cannot be read at all, is now a ping, which gives the connection straight back.

- A `SIMILAR TO` pattern that ends in a backslash no longer matches the wrong thing. The translation wrote the trailing backslash through, where it escaped the anchor the translation appends: `a\` became the regular expression `^a\$`, which matches the literal text `a$` and not the backslash the pattern ends with. A trailing escape now escapes itself.

### Breaking Changes

- `DefaultRowsPerChunk` and `MinChunkSize` are removed. `DefaultRowsPerChunk` and `DefaultChunkSize` were two exported names for the same 1000, the second documented as an alias of the first, which left a caller no way to tell which one to reach for; `DefaultChunkSize` is the one that pairs with `SetDefaultChunkSize` and is what remains. `MinChunkSize` named a floor a caller cannot act on: `SetDefaultChunkSize` already ignores anything at or below zero, and the loader clamps on its own. A caller using either name replaces it with `DefaultChunkSize`, or with the literal 1 where the minimum was meant.

- `prep` refuses a struct field that names a column the input does not have, with the new `ErrUnknownColumn` ([#330](https://github.com/nao1215/filesql/issues/330)). Such a field was filled with the zero value and then validated, so a field named `Emails` against a column named `email` produced `row 1, column "emails": value is required` on a file whose every row has an email — the error pointed at the data instead of at the mapping, and a caller had no way to tell the two apart. The same mistake made `prep` do nothing at all for JSON and JSONL, whose rows arrive as a single `data` column: a struct written against the object's own keys matched nothing, so no preprocessor ran and every field came back reported as a missing required value. All unmatched fields are named in one error rather than one per row. A struct covering a subset of the columns is still accepted, since extra columns are ordinary, and a field carrying `prep:"default=..."` is still accepted without a column, since the default is where its value comes from. A caller relying on an absent column arriving as an empty string has to declare the field with a default or drop it.

- `ErrPermissionDenied`, `ErrMemoryLimit` and `ErrContextCancelled` are removed ([#331](https://github.com/nao1215/filesql/issues/331)). No code path ever returned them, so `errors.Is` against any of the three answered false forever — worse than the sentinel not existing, because it reads like a supported way to ask a question and quietly answers wrong. Two of them have a standard library answer that already works: an unreadable file satisfies `errors.Is(err, fs.ErrPermission)`, because the operating system's error is wrapped all the way up, and a canceled load reports `context.Canceled` or `context.DeadlineExceeded`. Nothing detects a memory limit, so that one named a condition this package does not have. A test now fails when an exported sentinel is declared and never wrapped, so the catalog cannot drift again.

- `ACHTableInfo` and `WireTableInfo` are removed, along with their table-name methods, and `IsACHBaseTableName` and `IsWireBaseTableName` are no longer exported. The two types were what the registry API returned; that API went in v0.43.0 and the types stayed behind, leaving them orphaned — nothing in this package returns one, accepts one, or constructs one, so a caller could only build one by hand to compute a string it already knew. The two predicates are how a dump decides which of its tables belong to an ACH or Fedwire file, and that decision is not one a caller outside this package can act on, since it also needs the source metadata the database holds. A caller that was appending the suffixes itself keeps doing so: they are `_file_header`, `_batches`, `_entries`, `_addenda`, `_iat_batches`, `_iat_entries`, `_iat_addenda` for ACH and `_message` for Fedwire, and none of them has changed.

### Documentation

- staticcheck runs off the pull request path instead of in lint. It builds SSA and computes facts for every package in the import graph, and this module's graph holds machine-generated Go whose largest function is 7,517 lines, so a run whose cache misses takes tens of minutes while the other forty-five linters finish in thirteen seconds together. Any change to `go.sum` invalidated that cache, which is how every dependency update came to fail lint for a reason unrelated to its diff. The checks are kept rather than dropped: `.golangci.staticcheck.yml` enables staticcheck alone, a workflow runs it on pushes to main with an hour to work in, and `make lint-staticcheck` runs it on demand. What changed is that it no longer stands between a pull request and its merge.

- Which save keeps a source's line terminator is written down as the mode it belongs to ([#326](https://github.com/nao1215/filesql/issues/326)). The README, the `WithLineEnding` godoc, and the `EnableAutoSave` godoc described the detection as a property of overwriting a file loaded from a path, but only writing back in place — `EnableAutoSave("")` — reads it. `DumpDatabase` and `EnableAutoSave("./dir")` are exports and write `\n` even when the directory named is the one a source came from, which is what an export should do: output that changed with whatever already sat in the destination would write different bytes on its second run. A caller who reads "save it back where it came from" as passing the source directory was told their CRLF file would be preserved and it was not; `WithLineEnding(LineEndingCRLF)` is how an export is asked for CRLF. All three paths are now pinned by a test side by side, so they cannot drift apart again.

### Changed

- `github.com/nao1215/fileparser` is no longer a dependency. It was the module this repository's `parser` package was forked from, it has since been archived, and the only thing still importing it was one test comparing the fork against its origin. That comparison can no longer tell anyone anything: the reference is frozen, and `parser` has already diverged from it on purpose. Nothing in a build ever reached it — it was a test-only requirement — so no caller's build changes, and the archived module stops holding the floor under `excelize`, `moov-io/ach` and the rest at the versions it happened to pin.

- Dependencies: `github.com/moov-io/ach` 1.61.3 → 1.63.3, `github.com/moov-io/wire` 0.15.8 → 0.16.0, `modernc.org/sqlite` 1.55.0 → 1.56.0, `github.com/klauspost/compress` 1.19.1 → 1.19.2, `github.com/pierrec/lz4/v4` 4.1.27 → 4.1.28, `github.com/parquet-go/parquet-go` 0.30.1 → 0.32.0, `github.com/stretchr/testify` 1.11.1 → 1.12.0, `golang.org/x/text` 0.40.0 → 0.41.0.

## [0.43.1] - 2026-08-09

### Fixed

- The reserved `_filesql_` table prefix is reserved in both directions. v0.43.0 began hiding those names from `DumpDatabase` and from the table listings this package returns, but still loaded a file called `_filesql_report.csv` into a table under the prefix: that table existed and answered queries while being absent from every listing and from any dump, so its rows were silently left out of an export. An input that would land in the namespace is now refused with `ErrReservedTableName`, naming the table and the prefix, which is how SQLite answers for its own `sqlite_` prefix. The comparison folds ASCII case, because the LIKE that hides these names does: `_FILESQL_report` loaded and then vanished the same way. A name that merely resembles the prefix, such as `filesql_report`, is a normal table.

## [0.43.0] - 2026-08-09

### Added

- `frame.Row` reads a callback's row without guessing what the load typed it as ([#277](https://github.com/nao1215/filesql/issues/277)). A CSV column of digits arrives as `int64`, so the natural `row["id"] == "1"` matched nothing and reported nothing — a filter that looks correct returning an empty result. `frame.Row(row).String("id")` answers with the value as the file spelled it, `Int` and `Float` with it as a number, and each returns false rather than a zero value when the column is absent, holds nil, or holds something it cannot represent, so a mistyped column name shows up in the predicate. The callbacks still take `map[string]any`, which converts to `Row` for free.

### Documentation

- What the `frame` type inference preserves is written down ([#276](https://github.com/nao1215/filesql/issues/276)). The quantity survives and the way it was written does not, so `1`, `1.0` and `1.00` are one value: `Distinct` collapses them to a single row and `Join` matches them to each other. Keeping the three apart would mean keeping the column as text, where `9.00` does not compare as less than `10.00`. A zero-padded code and an integer past `int64` are the values kept as text instead, because converting those changes what they are rather than how they look, and they stay distinct — that half was fixed in v0.41.1.

### Fixed

- An ACH or Fedwire dump writes the file its own database was loaded from ([#210](https://github.com/nao1215/filesql/issues/210)). The structure a dump needs lived in a process-global map keyed by the base table name alone, so `/a/payment.ach` and `/b/payment.ach` were one key: loading the second replaced the first, and dumping the first database applied its rows to the second file's structure — an error when the shapes disagreed, and silently the wrong output when they happened to line up. Each database now records its own sources in a reserved table, `_filesql_sources`, so two databases cannot collide, nothing has to be released when a database closes, and a rolled-back load discards the metadata with the tables it describes. Table names beginning with `_filesql_` are reserved for this package and are hidden from `DumpDatabase` and from the table listings filesql returns.

### Breaking Changes

- `DumpACH` and `DumpFedWire` need the source file to still be readable. Neither format can be rebuilt from its SQL tables alone, so the export reads the original and applies the edits to it; a missing or unreadable source now fails with `ErrSourceUnavailable` naming the file. A database loaded from an `io.Reader` records no source and cannot be exported this way: parse the reader with `parser/ach` or `parser/wire` and call `DumpACHWithTableSet` or `DumpFedWireWithTableSet`.

- The registry API is removed because there is no registry left to manage: `GetACHTableInfos`, `UnregisterACHTableSet`, `ClearACHTableSetRegistry`, `GetWireTableInfos`, `UnregisterWireTableSet`, `ClearWireTableSetRegistry`, the `PendingRegistries` type with its `PublishRegistries` method, and `DBBuilder.LoadIntoTxWithPending`. A caller of `LoadIntoTxWithPending` calls `LoadIntoTx` and drops the publish step; the metadata is written inside the transaction and commits or rolls back with it. A caller of the unregister and clear functions drops the call: closing the database releases the metadata with it.

## [0.42.0] - 2026-08-09

### Breaking Changes

- `frame.DataFrame.Select` and `Drop` return `(*DataFrame, error)` ([#272](https://github.com/nao1215/filesql/issues/272), [#282](https://github.com/nao1215/filesql/issues/282), [#283](https://github.com/nao1215/filesql/issues/283)). Both silently ignored a column that does not exist, so a typo returned a frame quietly missing a column, or one still holding the column the caller believed was dropped — while `Sort`, `GroupBy`, `Rename` and `Concat` all refused the same typo. `Select` also accepted a repeated name and produced a frame whose two representations disagreed: `Columns` and `ToCSV` kept both, `ToRecords` kept one, because a row is a map and a map cannot hold the name twice. Both now refuse what they cannot do, which is the contract the rest of the package already had. A caller updates `df.Select(...)` to take the error the sibling methods already return.

### Fixed

- An Excel date cell imports as an ISO 8601 date rather than as whatever its number format rendered ([#270](https://github.com/nao1215/filesql/issues/270)). A workbook stores a date as a serial number and a format, so the same day arrived as `03-15-23`, `2023-03-15`, or `Mar 15` depending on how the sheet was formatted: `ORDER BY` sorted such a column lexically and a comparison against an ISO literal never matched. The value now comes from the stored serial, in the form the datetime inference already recognizes, and a cell whose time of day is midnight is written as a plain date. A cell is a date because its number format names a calendar day — the type attribute in the XML says nothing, since the value is an ordinary number — so a number formatted as a number, text that merely looks like a date, a time of day, and an elapsed duration (`[h]:mm` of 1.5 is 36 hours, not a day and a half after the epoch) are all untouched. The workbook's own epoch is honored, so a file counting from 1904 does not import four years early.

- `DropNA` and `FillNA` agree on what is missing ([#271](https://github.com/nao1215/filesql/issues/271)). `DropNA` counted an empty string as missing and `FillNA` counted only a real nil, so on the same frame `DropNA` removed a row that `FillNA` would not fill — and a caller who filled a frame to make it safe for later processing was left holding the cell that made it unsafe. A CSV has no null, so `""` is how a missing value arrives from the format most frames are read from, and it now counts for both. A column `FillNAByColumn` was not given a value for keeps its cells rather than having them normalized to nil.

- `Concat` and `ConcatAll` agree on what frames go together ([#275](https://github.com/nao1215/filesql/issues/275), [#284](https://github.com/nao1215/filesql/issues/284)). `Concat` compared column slices positionally and refused frames whose columns were the same set in a different order, reporting `different columns` about columns that were the same, while `ConcatAll` accepted that very pair; it now compares the set, since a row is a map keyed by column name and there is nothing to reconcile, and the result keeps the receiver's order. `ConcatAll` dropped a nil frame silently, producing a result quietly missing that data, while `Concat` rejected the same nil — a nil is almost always a constructor whose error was mishandled, so both now refuse it.

- A `frame` aggregate says when it has no answer instead of inventing one ([#273](https://github.com/nao1215/filesql/issues/273), [#281](https://github.com/nao1215/filesql/issues/281)). `Sum` over a column with no number in it returned 0 for every group, which is what a real total of zero looks like, and `Mean` returned nil; both now refuse the column by name, while a group inside a numeric column that holds no value gets nil rather than 0. `Min` and `Max` over a text column returned nil, discarding an answer the data plainly held: they now follow SQLite's ordering, where a number sorts before any text and text compares lexically, so the minimum of `banana` and `apple` is `apple`.

### Breaking Changes

- `AggSum` returns nil rather than 0.0 for a group with no numeric value, and `AggMin`/`AggMax` return a string for a group holding only text. A caller asserting `float64` on those results must handle nil and string. This is what makes "nothing here was a number" distinguishable from "the numbers added to zero".

## [0.41.1] - 2026-08-09

### Fixed

- `frame.ToTSV` writes tab-separated records rather than CSV with its comma changed ([#280](https://github.com/nao1215/filesql/issues/280)). TSV has no quoting, so a value holding a tab came out wrapped in double quotes that a TSV reader takes for two more characters, with the tab inside them still a field boundary: the file had the wrong shape and the quotes were data. It now goes through the same literal writer the rest of filesql uses, which refuses a value the format cannot hold instead of writing something else.

- An LTSV record whose labels differ only in ASCII case is refused by name ([#267](https://github.com/nao1215/filesql/issues/267)). LTSV carries its labels on every record rather than in a header, so its duplicate check is its own, and that one compared them exactly: `A:1\ta:2` passed it and failed at SQLite instead, which folds ASCII case — a raw `duplicate column name` from a failed CREATE TABLE, three wraps deep, with no `ErrDuplicateColumn` to match. The labels are now compared the way the header formats compare theirs, folded to ASCII only, so `ä` and `Ä` stay two labels.

- A value the file quoted with surrounding whitespace keeps it, whatever it looks like ([#266](https://github.com/nao1215/filesql/issues/266)). SQLite's numeric affinity converts `" 5 "` to 5 on the way into a numeric column, so the spaces the quotes made part of the value were gone, while the text column beside it kept its own — the same input preserved or rewritten depending on what it looked like, and a fixed-width padded code (`"  42"`) rewritten silently. Such a value is now text, which is the rule already applied to a zero-padded code and an integer past int64: a value SQLite's affinity would change on the way in has no numeric form that holds it.

- A zero-padded code survives the `frame` package ([#274](https://github.com/nao1215/filesql/issues/274), [#278](https://github.com/nao1215/filesql/issues/278)). The parser's own type inference had none of the guards the SQLite load path was given: it called `007` an integer, so a DataFrame stored 7, and a round trip through `NewDataFrame` and `ToCSV` wrote back a file whose codes had lost their zeros. `Distinct` merged `007` into `7`, and `Join` matched a `007` account to a `7` account — a row pair in neither input. A zero-padded literal is now text wherever the parser classifies a value, and a `frame` column called numeric keeps a value the conversion would change, since the type is decided from a sample and a code can arrive below it. Decimal scale is unchanged: `1.50` is the real 1.5 here as everywhere else in filesql, because the quantity survives and only the way it was written does not.

## [0.41.0] - 2026-08-09

### Added

- MySQL's logical and bitwise operators that SQLite does not share are translated ([nao1215/sqly#893](https://github.com/nao1215/sqly/issues/893)). `&&` becomes `AND`. `!` becomes a parenthesized `NOT`: MySQL's `!` binds tighter than a comparison while SQLite's `NOT` binds looser, so a bare `NOT` would turn `!a = b` into a negation of the comparison. `^` has no SQLite operator at all and becomes a `mysql_bit_xor` helper call rather than the `(a|b)&~(a&b)` expansion, which would evaluate each operand twice; its arithmetic is unsigned, as MySQL's bitwise operators are, and the result comes back as the same 64 bits in SQLite's only integer, which is signed.
- BigQuery's `SAFE.` call prefix is translated ([nao1215/sqly#894](https://github.com/nao1215/sqly/issues/894)). `SAFE.DIVIDE(1, 0)` is now the same query as `SAFE_DIVIDE(1, 0)`; BigQuery's own documentation writes these functions with the prefix. A `SAFE.` prefix on any other function is refused rather than dropped.

### Fixed

- Constructs SQLite cannot represent are refused by name instead of reaching its parser ([nao1215/sqly#895](https://github.com/nao1215/sqly/issues/895)). A GoogleSQL array literal came back as `no such column: 1,2,3`, because SQLite reads `[...]` as identifier quoting, and a PostgreSQL `generate_series` as `no such table`, which reads as a missing input file. Both now say which construct is unsupported. MySQL's `XOR` is refused for the same reason: its precedence sits between `OR` and `AND`, which SQLite has no operator for, so its operands are not the primaries a rewrite can pick out and translating it would reassociate the expression. A `SAFE.` prefix on a function with no safe form is refused rather than dropped, since dropping it would answer with the plain function, which raises where the caller asked for a NULL.

## [0.40.1] - 2026-08-09

### Fixed

- A Parquet double holding an infinity imports as a REAL instead of as text. The column type comes from the Parquet schema, but the value reached SQLite as the text `%g` renders, and `+Inf` is not a number to SQLite's REAL affinity: the cell was stored as TEXT inside a column declared REAL, so `typeof()` answered `text` for a value the file held as a double, and comparisons on it ordered lexically. An infinity now renders as `9e999`, which SQLite parses back to one. A NaN imports as NULL, because SQLite has no NaN at all — a computed one is NULL there — and the word would leave the same text-in-a-REAL-column mismatch.

## [0.40.0] - 2026-08-09

### Added

- `parser.TSVReader` and `parser.WriteTSVRecord` read and write tab-separated records literally, and `parser.ErrTSVUnrepresentable` reports a value the format cannot hold. They are what the TSV fix below is built on, and they are exported because the package is the one that defines what TSV means here.

### Fixed

- A TSV value containing a double quote imports, and a TSV dump writes it back unchanged ([#268](https://github.com/nao1215/filesql/issues/268)). TSV was read with a CSV reader, which brought CSV's quote handling with it, so a value as ordinary as `5'9" tall` failed the whole import with `bare " in non-quoted-field`. IANA's text/tab-separated-values has no quoting: a field is the bytes between two tabs, and a double quote there is an ordinary character. Both halves now agree on that — the reader splits on tabs and keeps what is between them, and the writer joins with tabs and refuses a value holding a tab or a line break, which the format cannot represent, rather than quoting it into something the reader would hand back with the quotes attached. A blank line in a one-column TSV is that column's empty value, which is how a dump writes it; with more columns it is skipped, as a CSV reader skips it.
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

[Unreleased]: https://github.com/nao1215/filesql/compare/v0.43.1...HEAD
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
