// Package filesql provides a file-based SQL driver implementation that enables
// querying CSV, TSV, LTSV, JSON, JSONL, Parquet, and Excel (XLSX) files using
// SQLite3 SQL syntax.
//
// filesql allows you to treat structured text files as SQL databases without
// any data import or transformation steps. It uses SQLite3 as an in-memory
// database engine, providing full SQL capabilities including JOINs, aggregations,
// window functions, and CTEs.
//
// ACH (NACHA) and Fedwire files are also loaded; see the ACH and Fedwire section
// below.
//
// # Features
//
//   - Query CSV, TSV, LTSV, JSON, JSONL, Parquet, and Excel (XLSX) files using
//     standard SQL
//   - Automatic handling of compressed files: gzip (.gz), bzip2 (.bz2), xz
//     (.xz), zstandard (.zst), zlib (.z), snappy (.snappy), s2 (.s2) and
//     LZ4 (.lz4)
//   - Support for multiple input sources (files, directories, io.Reader, embed.FS)
//   - Efficient streaming for large files with configurable chunk sizes
//   - Cross-platform compatibility (Linux, macOS, Windows)
//   - Optional auto-save functionality to persist changes
//   - Optional MySQL, PostgreSQL, and GoogleSQL query dialects (queries are
//     translated to SQLite; see WithDialect and the dialect subpackage)
//
// # Basic Usage
//
// The simplest way to use filesql is with the Open or OpenContext functions:
//
//	db, err := filesql.Open("data.csv")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	rows, err := db.Query("SELECT * FROM data WHERE age > 25")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer rows.Close()
//
// # Advanced Usage
//
// For more complex scenarios, use the Builder pattern:
//
//	db, err := filesql.NewBuilder().
//	    AddPath("users.csv").
//	    AddPath("orders.tsv").
//	    EnableAutoSave("./output").
//	    Open(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
// # Column Types
//
// CSV, TSV, LTSV and XLSX carry no types, so the values decide whether a column
// is INTEGER, REAL or TEXT. Parquet, ACH and Fedwire bring their own schema and
// are not inferred; a Parquet UINT64 column loads as TEXT, since the upper half
// of its range is past what SQLite's INTEGER holds exactly.
//
// The type follows from every value in the column, wherever the value sits and
// however large the file is. One value of these four kinds anywhere in the file
// makes the column TEXT, because a numeric column would damage it:
//   - a leading zero, such as 007, which INTEGER drops
//   - an integer past int64, such as 11040320260000000000, which float64 renders
//     as 1.104032026e+19
//   - a decimal a float64 cannot hold, such as 1e400 or 1e-400, which would be
//     stored as an infinity or as an exact zero
//   - a spelling Go parses and SQLite's affinity does not convert, such as 1_000
//     or 0x1p4
//   - the padding a fixed-width code carries, such as "  42"
//
// One decimal makes a numeric column REAL. An INTEGER column turns the column's
// arithmetic into integer division, so 5 / 2 answers 2 rather than 2.5. The
// exception is a column where a decimal meets an integer past 2^53 that REAL
// would round to a neighboring double: that column is TEXT, while the same
// integers with no decimal beside them stay INTEGER and exact.
//
// Decimal formatting is not preserved: 2.50 loads as the REAL 2.5, 1.00 as 1,
// and 1e3 as 1000. The quantity is kept and the spelling is not, because a TEXT
// column is compared against a number as text, so WHERE amount > 9.5 over "9.00"
// and "10.00" would return nothing at all. Keep the source file when the
// original spelling matters. A dump keeps the type rather than the spelling: a
// REAL column is written with a decimal point, so 1.00 comes back as 1.0 and
// loads as REAL again, and an infinity is written as 9e999 and loads back as the
// infinity.
//
// A column whose values are all datetimes is recognized as one and stored as
// TEXT; the parser package names that column DATETIME, since it reports what was
// recognized rather than what SQLite stores. A text cell is stored as the file
// wrote it, so a column written as 1/2/2024, 2024/01/02 or 02.01.2024 is
// recognized as a datetime and still needs converting before date() or
// strftime() can answer about it: those read ISO 8601 and "YYYY-MM-DD HH:MM:SS".
// An XLSX date cell is the one that is rewritten, into ISO 8601, because it
// holds a serial number and a number format rather than text.
//
// A workbook draws a number the way its format says, and the drawing is not the
// value: a number of more than fifteen significant digits is drawn rounded to
// fifteen, and a whole-number format draws 1234.5 as 1235. A cell a sheet draws
// as a plain number loads the number the file stores, so an 18-digit identifier
// keeps its digits. A cell drawn as something else -- a percentage, a
// thousands-separated amount, an accounting figure, a fraction -- loads as the
// text the sheet shows, since that is what the cell means to a reader.
//
// A blank cell -- empty, or nothing but whitespace -- in an INTEGER or REAL
// column is a missing number and is stored as NULL, which is what makes MAX
// answer the largest value rather than the blank, AVG divide by the values that
// are there, COUNT(column) count them, and WHERE column IS NULL find the rows
// that have none. A blank cell in a TEXT column is what it is, which is a value
// the file holds; a column recognized as DATETIME is stored as TEXT and follows
// that rule. A number written with spaces around it is not blank and is not a
// number either: the padding is data a fixed-width code column depends on, so
// one keeps the column TEXT.
//
// # Memory and Streaming
//
// Data is loaded into an in-memory SQLite database. CSV, TSV, JSONL, JSON arrays
// and Parquet arrive in chunks while loading; LTSV, a JSON document that is not
// an array, XLSX, ACH and Fedwire are read in full before they are turned into
// rows. A chunked format still holds one record at a time, since a record has to
// be complete before it can be read. A Parquet file named by path is read where
// it lies, since the format reads at an offset and a file already serves that; a
// Parquet reader passed to DBBuilder.AddReader is buffered whole instead,
// because a stream cannot go back and the format is read back to front.
//
// One record is held whole while it is read, so a record longer than 64 MiB is
// refused rather than buffered: a delimited record, an LTSV record, a JSONL
// line, one element of a JSON array, a JSON document that is not an array --
// which is one record, since it becomes one row holding the whole of it -- and
// an ACH record. A file cannot cost more than its own size either way; what the bound
// is for is a source that is a stream, where a record with no terminator would
// otherwise ask for everything the sender chooses to send. The JSON refusal
// lands within one of the decoder's own reads of the bound rather than on it, so
// an unterminated element reads about twice the bound and not the whole stream.
//
// The rows end up in SQLite rather than on the Go heap, so the heap is not where
// the cost is: loading CSVs of 16 MB through 131 MB kept the Go heap flat at
// about 24 MB while resident memory grew by about twice the file's size. For a
// chunked format, budget from the file size: over 200,000 rows, CSV and Parquet
// each cost about 2.1 times the file.
//
// A workbook is read whole, so what it costs follows the cells it holds rather
// than the size of the file, which is a zip and shrinks with how well the sheet
// compressed. The same 200,000 rows cost about 26 times the file as a wide
// workbook and about 37 times as a workbook of one column, and against the file
// alone the ratio reaches 135 times for a small one-column workbook. Budget an
// XLSX load from the cells rather than from the file, and bound the decompressed
// size before loading a workbook that came from somewhere else, since nothing
// here does. Every figure is printed by
// "go test -tags benchmark -run TestLoadMemoryFootprint -v ." and
// "go test -tags benchmark -run TestLoadMemoryFootprintByFormat -v .", which
// print the tables they are drawn from so they can be re-derived rather than
// taken on trust.
//
// A damaged Parquet file can cost more than any of that. A page header states
// how large the page's statistics are and the reader allocates that before
// reading them, so a damaged 473-byte file costs 98 MB before it is refused; the
// number sits inside a column chunk, where this package cannot check it first.
// Do not point a memory-constrained process at Parquet files you did not write.
//
// DBBuilder.SetDefaultChunkSize tunes chunked loading. It changes when rows
// reach the database, not what reaches it: a column's type and the text of every
// cell are the same at any chunk size, because a file whose later chunk needs a
// wider type is read again under the types the whole file calls for. A reader
// passed to DBBuilder.AddReader cannot be read twice, so it is staged as text
// and typed once it has all been read, at the cost of one copy of the table
// inside SQLite. The final cost is still dominated by the size of the database:
// chunking reduces loader overhead, it does not make a large dataset free.
//
// A blank line is not a record in CSV, in LTSV or in a sheet. In TSV it is one
// in a one-column file, where it is that column's empty value: TSV has no quote
// to write an empty field with, so an empty line is the only spelling left and a
// reader that skipped it would drop the row. An XLSX row holding no cell at all
// is skipped, so a workbook whose used range reaches far down the sheet holds no
// record for the rows between its header and a stray cell near the bottom. A row
// whose cells are there and empty is a record, which is what a sheet's own XML
// separates from the space under its data.
//
// # Concurrency
//
// The *sql.DB returned by Open and OpenContext is safe to share across
// goroutines: a shared-cache in-memory database is used so pooled connections
// see the same tables. Auto-save does not change that, since EnableAutoSave
// saves once when Close returns and EnableAutoSaveOnCommit saves one at a time.
// A DBBuilder is not safe to share; build one per goroutine.
//
// Loading into one database from several goroutines works. Creating a table
// takes a write lock and SQLite refuses a second holder rather than queueing it,
// so a load that meets another load's lock waits it out and starts over, for up
// to five seconds, before it returns the database's own error. Paths and
// directories are covered whatever database they load into. A reader passed to
// DBBuilder.AddReader is not fully covered, because starting over would have
// nothing left to read: only the steps before the reading are tried again.
// LoadIntoTx is not retried at all, since the transaction belongs to the caller
// and one transaction belongs to one goroutine.
//
// # Transactions
//
// One transaction runs at a time. SQLite serializes them itself and the driver
// waits for its turn inside itself, with no context in that wait, so a second
// transaction used to park until the first ended whatever deadline its caller
// had put on the work. It now queues here instead: BeginTx waits for the
// transaction before it and returns the context's error if the wait outlives the
// context. Statements outside a transaction are not queued, since a query beside
// an open transaction is an ordinary thing to write; what that leaves is the
// wait this package cannot reach. While a transaction that has written is open,
// and while a rows iterator is still open, a statement touching the same table
// waits inside SQLite for it, and no context ends that wait -- so close a rows
// iterator when you are done with it, and do not leave a writing transaction
// open across work that queries the same tables. DumpDatabase reads every table
// and is subject to the same rule.
//
// SQLite runs serializable transactions and has no other level, so a sql.TxOptions
// naming one of the weaker levels is refused when the transaction begins rather
// than taken and quietly downgraded; sql.LevelDefault and sql.LevelSerializable
// are the two a transaction can ask for. A transaction begun with ReadOnly set
// is held under SQLite's query_only pragma for as long as it runs, so a write
// inside it fails and the permission comes back when it commits or rolls back.
//
// A transaction begun by running BEGIN as a statement rather than through
// database/sql belongs to whichever pooled connection ran it, and the next
// statement may reach a different one, so the rows are not where the caller
// expects and are discarded when that connection closes. Auto-save counts such a
// transaction as open and refuses to save while it is, the same as one begun
// with BeginTx, and saves at a COMMIT run as a statement just as it does at
// tx.Commit. A savepoint taken while no transaction is open begins one, so it
// counts the same way and the RELEASE naming it ends it; a savepoint taken
// inside a transaction is not one of its own, so releasing that one leaves the
// transaction around it open. A comment before the keyword does not hide it.
//
// # Table Naming
//
// Table names are automatically derived from file paths:
//   - "users.csv" becomes table "users"
//   - "data.tsv.gz" becomes table "data"
//   - "/path/to/logs.ltsv" becomes table "logs"
//   - "sales.xlsx" with multiple sheets becomes tables "sales_Sheet1", "sales_Sheet2", etc.
//
// # Excel Sheet Visibility
//
// Every sheet of a workbook is loaded by default, whether or not the workbook
// shows it. Use DBBuilder.WithExcelSheetPolicy with ExcelSheetPolicyVisibleOnly
// to load only the shown ones, and ExcelSheetsInFile to report what a workbook
// holds without loading it.
//
// The policy applies to every source alike: a path, a directory, an embedded
// filesystem, a reader, and a compressed workbook. Table names are worked out
// after it has run, so a hidden sheet that would sanitize to the same table as a
// visible one is not a collision when it is not loaded.
//
// Excel separates "hidden", which a reader can undo from the sheet tabs, from
// "very hidden", which only the VBA editor can. The library this package reads
// workbooks with reports one boolean covering both, so the two are not told
// apart and ExcelSheetPolicyVisibleOnly leaves out either kind.
//
// # Data Modifications
//
// INSERT, UPDATE, and DELETE operations affect only the in-memory database.
// Original files remain unchanged unless auto-save is enabled.
//
// # Saving Changes
//
// There are three ways to write the database out. DumpDatabase and
// DumpDatabaseContext export it to files when an explicit step is wanted;
// DBBuilder.EnableAutoSave writes when Close runs; DBBuilder.EnableAutoSaveOnCommit
// writes after each committed transaction and again at close, so a statement run
// outside a transaction is not lost. A transaction still open when Close runs has
// been neither committed nor rolled back, so the save is skipped and Close says so.
//
// DumpOptions decides the format, the compression, the text encoding and the
// line terminator of csv, tsv and ltsv output. Output is UTF-8 unless
// DumpOptions.WithEncoding says otherwise; this package reads UTF-8 only, so a
// file written in another encoding is for other tools, and transcoding it is the
// caller's step before loading it back. A value the encoding has no way to write
// fails the save with ErrEncoding and leaves the destination as it was, rather
// than writing a substitute character. ISO-2022-JP additionally refuses an
// escape, U+001B, which it reads as the start of a character-set designator
// rather than as data.
//
// A text format holds characters rather than bytes, so a value or a column name
// the database holds that is not valid UTF-8 fails the save with
// ErrUnsupportedFormat for CSV, TSV, LTSV and XLSX, naming the field and
// pointing at Parquet, which stores bytes and reads them back unchanged. Such a
// value reaches a table by being bound as a Go []byte, or through a CAST or a
// blob literal in a statement run during the session; a value read from a file
// is checked when it is loaded.
//
// Records end with "\n" unless DumpOptions.WithLineEnding says otherwise, with
// one exception: EnableAutoSave("") writes a table back to the file it was
// loaded from and keeps that file's own terminator, so a CRLF file edited in
// place stays CRLF and the rows nobody touched keep the ending they had. A file
// with mixed terminators keeps whichever one the majority of its lines use.
// Every other save is an export and writes what the options say whatever sits in
// the destination, EnableAutoSave("./dir") included. Parquet and XLSX carry
// their own encoding and are not line-based, so they ignore both options.
//
// A workbook written back to itself keeps what a table does not hold. The save
// writes onto the workbook it is replacing, so a sheet ExcelSheetPolicyVisibleOnly
// left out stays, and so do the column widths, merged ranges and comments of the
// sheets it did load. Each table goes back to the rows it came from, which for a
// sheet holding a row with no cell in it -- under a title, or between two blocks
// of records -- are not the rows from the top. A cell already holding the value
// the save would write is left as it is, so a date cell nothing edited keeps the
// serial and the number format that make a spreadsheet read it as a day. A cell
// the save does write takes the type its column has: a value of an INTEGER or
// REAL column is written as a number, so the column stays one a spreadsheet
// sums, while a value this package keeps as text -- a zero-padded code, a
// literal past int64 -- is written as text.
//
// A Parquet file written back to itself keeps the schema it declared. Nothing
// in the database remembers it -- a BOOLEAN column loads as the integers 1 and
// 0, a DATE and a TIMESTAMP as the integer they store, a DECIMAL and a UUID as
// their text -- so the file is read for its types before it is replaced, and
// each value is rebuilt from the text the column holds. A column keeps its type
// only when every one of its values comes back as itself; one that does not is
// a value the caller set that the type cannot hold, and it is written the way
// an export writes it rather than narrowed into a different number. A required
// column holding a null the caller set becomes optional, which is the one thing
// the data demands. Some types are rendered by a load in a form that says less
// than the value they store, and those keep what an export writes. A list or a
// map is rendered as text. An INT96 is rendered as nanoseconds since the Unix
// epoch rather than the Julian day it holds, and a FLOAT16 is widened to 32
// bits.
//
// # ACH and Fedwire
//
// ACH (.ach) and Fedwire (.fed) files are loaded, queried and written back like
// any other format, but an exported file still needs domain knowledge from the
// caller: both formats carry rules about what a valid file is, and this package
// checks only the ones the format itself defines.
//
// Control records are derived rather than stored. Writing an ACH file rebuilds
// each batch control and the file control from the entries, so an edit to
// total_debit, total_credit, entry_hash or entry_addenda_count is overwritten by
// the recalculation. The batch_index, entry_index and addenda_index columns say
// which record a row updates rather than holding a value of their own, and a
// write refuses a row whose coordinates name a record that is not there or one
// another row has already named.
//
// A Fedwire write is verified before it reaches the caller's file: the message
// is written to a buffer, read back, and compared column by column with the
// table it was written from, and a field that did not survive refuses the write
// by name, leaving the file as it was. One field reaches this today,
// remittance_originator_address_line_four, which the underlying library writes
// from line one. Fedwire files must be the delimiter-separated form; a
// fixed-width file is refused, naming every record it could not read.
//
// A write-back rewrites the whole file, since both formats are written from the
// parsed structure rather than patched, so records the caller did not edit can
// come back formatted differently while holding the same values. Writing needs
// the source file, because fields no table exposes exist only in the original:
// DumpACH and DumpFedWire read the file the tables were loaded from and fail
// with ErrSourceUnavailable, naming it, when it is gone or unreadable. A
// database loaded from an io.Reader has no such file; parse the reader with the
// parser/ach or parser/wire package and use DumpACHWithTableSet or
// DumpFedWireWithTableSet.
//
// Each database records its own source, so two databases loaded from files that
// share a name in different directories each export their own data. The record
// lives in a reserved table named _filesql_sources. Table names beginning with
// _filesql_ belong to this package: they are hidden from a dump and from the
// table listings filesql returns, and an input that would load into one is
// refused with ErrReservedTableName. A name beginning with sqlite_ is refused
// the same way, because SQLite keeps that prefix for its own tables. Both
// comparisons ignore ASCII case, as SQLite does.
//
// # Cancellation
//
// OpenContext, DBBuilder.Open, DBBuilder.OpenReadOnly, LoadInto, LoadIntoTx and
// DumpDatabaseContext take a context; Open and DumpDatabase are the same calls
// with a background one, so they cannot be canceled. A load stops soon after
// its context ends, and whatever the database said on the way out, the error it
// returns matches context.Canceled or context.DeadlineExceeded. Soon is the next
// read for a source that is a stream, and the next chunk for one that is an open
// file, which is left unwrapped so that a Parquet file is still read where it
// lies rather than copied whole. A read already blocked inside the source cannot
// be interrupted, so a sender that stops mid-body without closing needs a
// deadline on the connection instead.
//
// # SQL Syntax
//
// Since filesql uses SQLite3 as its underlying engine, all SQL syntax follows
// SQLite3's SQL dialect. This includes support for:
//   - Common Table Expressions (CTEs)
//   - Window functions
//   - JSON functions
//   - Date and time functions
//   - And all other SQLite3 features
//
// # Column Name Handling
//
// A table holds at most 2000 columns, which is SQLite's own limit and is fixed
// when SQLite is compiled. A wider file is refused with ErrUnsupportedFormat,
// naming the limit and the width the file has.
//
// A byte-order mark at the front of a text file belongs to the encoding rather
// than to the first column name, however many the file carries: a file marked
// twice names its first column the same as a file marked once. A U+FEFF
// anywhere else is a character the file wrote and is kept, in a later column's
// name and inside a value alike. Dumping a table whose first column name begins
// with one is refused with ErrUnsupportedFormat for CSV, TSV and LTSV, which
// write that name where a reader takes it for the encoding mark; XLSX and
// Parquet keep it.
//
// A header cell that is empty names nothing, so the column takes the name of its
// position: "a,,c" loads as a, column_2 and c. The generated name is moved along
// -- column_2_2, column_2_3 -- when the file wrote a column of that name itself.
// A header cell holding a space is a name the file wrote and is kept as it is.
// Dumping a table that holds a column with no name is refused with
// ErrUnsupportedFormat for CSV, TSV and XLSX, which write their names in a
// header row where an empty cell reads back as a positional name; LTSV writes
// the empty label beside each value and Parquet holds the name in its schema, so
// both carry it.
//
// A header that names one column twice is refused with ErrDuplicateColumn. Two
// names are the same column if either of two separate rules says so, and the
// rules are applied one at a time rather than combined:
//
//   - Two names that differ only in ASCII letter case are one column, which is
//     SQLite's rule, since SQLite is what ends up holding them: "ID" and "id"
//     are a duplicate. The folding stops at ASCII, as SQLite's does, so "ä" and
//     "Ä" remain two columns.
//   - Two names that are identical after leading and trailing whitespace is
//     trimmed are one column, which is filesql's own rule: " name " and "name"
//     are one name typed twice.
//
// Because the two are separate, neither is applied on top of the other: " A"
// beside "a" is accepted, since trimming alone does not make them equal and
// folding alone does not either, and SQLite likewise keeps them as two columns.
//
// LTSV carries its labels on every record rather than in a header, so the check
// runs per record; a record holding "A:1\ta:2" is refused for the same reason.
// Two rules above read differently there, because a label is trimmed as it is
// read: a space around one is malformed either way, and a value is not trimmed,
// so the same data written as LTSV and as CSV agrees on the values whatever the
// labels carry. So the trimming is not a rule applied beside the folding but
// something that has already happened when the folding runs, and " A:1\ta:2" is
// one label typed twice where " A" beside "a" in a CSV header is two columns.
// And a label that is empty names a column the empty string rather than taking
// the name of its position: a position names a column of a header, and an LTSV
// record does not have to carry the labels the record before it carried, so
// there is no position for a name to come from. Dumping to LTSV refuses a column
// name either rule would change -- one holding a colon, a tab or a newline, and
// one beginning or ending in whitespace -- rather than writing a label that
// reads back as a different name.
//
// For complete SQL syntax documentation, see: https://www.sqlite.org/lang.html
//
// # Query Dialects
//
// By default queries use SQLite3 syntax. To accept a different dialect, configure
// the builder with WithDialect:
//
//	db, err := filesql.NewBuilder().
//	    AddPath("users.csv").
//	    WithDialect(dialect.PostgreSQL).
//	    Open(ctx)
//	// db.Query("SELECT name::text FROM users WHERE name ILIKE 'a%'")
//
// The supported dialects are MySQL, PostgreSQL, and GoogleSQL (BigQuery / Cloud
// Spanner). Queries are translated to SQLite before execution on a best-effort
// basis: common incompatibilities are rewritten, constructs with no SQLite
// equivalent are rejected with a clear error, and everything else is passed
// through. Loading files always uses SQLite regardless of the dialect. See the
// dialect subpackage for the full list of translations and their limitations.
package filesql
