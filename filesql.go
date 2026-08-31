package filesql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nao1215/filesql/internal/reader"
	"github.com/nao1215/filesql/internal/writer"
	"github.com/nao1215/filesql/parser"
)

// Open creates an SQL database from the files at the given paths.
//
//	db, err := filesql.Open("data.csv")
//	if err != nil {
//		return err
//	}
//	defer db.Close()
//
//	rows, err := db.Query("SELECT * FROM data WHERE age > 25")
//
// A path is a file, a compressed file ("data.csv.gz", "archive.tsv.bz2"), or a
// directory, which is loaded recursively.
//
// A file becomes a table named after it, with the extensions dropped and
// characters SQL cannot hold in a bare identifier replaced by underscores:
// "users.csv" and "data.tsv.gz" become "users" and "data", "user-data.csv"
// becomes "user_data", and "my file.csv" becomes "my_file".
//
// The files are never modified. Changes live in the database until
// DumpDatabase writes them out, or until an auto-save configured through
// NewBuilder does.
func Open(paths ...string) (*sql.DB, error) {
	return OpenContext(context.Background(), paths...)
}

// OpenContext is Open with a context, for a load that has to time out or be
// canceled: a large file, or a server that abandons the request.
//
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//
//	db, err := filesql.OpenContext(ctx, "large-dataset.csv")
func OpenContext(ctx context.Context, paths ...string) (*sql.DB, error) {
	// Open validates the paths before it loads them.
	return NewBuilder().AddPaths(paths...).Open(ctx)
}

// LoadInto loads the given file or directory paths into an existing database
// instead of creating a new one, and returns without closing it. This lets a
// caller combine file-derived tables with a database it already manages (for
// example a long-lived session that imports files repeatedly).
//
// A table whose name matches a loaded file is replaced, so reloading is
// idempotent (last-wins for same-named inputs); other tables in db are left
// untouched. Each input is loaded atomically, so a file that fails partway
// leaves the database as it was, including the table it was replacing. For an in-memory database, pin the pool to a single connection
// (db.SetMaxOpenConns(1)) because SQLite ":memory:" is private per connection.
// See (*DBBuilder).LoadInto for the full semantics and for loading readers or
// filesystems.
//
// Example:
//
//	db, _ := sql.Open("sqlite", ":memory:")
//	db.SetMaxOpenConns(1)
//	if err := filesql.LoadInto(ctx, db, "users.csv", "orders.csv"); err != nil {
//		return err
//	}
func LoadInto(ctx context.Context, db *sql.DB, paths ...string) error {
	return NewBuilder().AddPaths(paths...).LoadInto(ctx, db)
}

// DumpDatabase saves all database tables to files in the specified directory.
//
// Basic usage:
//
//	err := filesql.DumpDatabase(db, "./output")
//
// This will save all tables as CSV files in the output directory.
//
// Advanced usage with options:
//
//	// Default: Export as CSV files
//	err := DumpDatabase(db, "./output")
//
//	// Export as TSV files with gzip compression
//	options := NewDumpOptions().
//		WithFormat(OutputFormatTSV).
//		WithCompression(CompressionGZ)
//	err := DumpDatabase(db, "./output", options)
//
// A destination that already exists as a symbolic link is followed: the file it
// names receives the rows and the link stays a link.
//
// A table is written to a file named after it, so a table whose name cannot be a
// file name is refused rather than written: the two path separators, the
// characters < > : " | ? *, a control character, a name ending in a dot or a
// space, and the names Windows reserves for devices -- CON, PRN, AUX, NUL, COM1
// to COM9 and LPT1 to LPT9, with or without an extension. The same set is
// refused on every platform, so a database dumped on Linux and on Windows agrees
// about which tables it can write. Rename the table before dumping it. A name
// derived from a file cannot reach that set except as a device name, since only
// letters, digits, marks and underscore survive; a name given to
// DBBuilder.AddReader or to CREATE TABLE can.
//
// A table whose name a load would spell differently is refused for the same
// reason: a load names a table after the file and spells a space, a hyphen and
// a dot as an underscore, so "with space" would come back as with_space, and
// "a b" beside "a-b" would be two files and one table name. Rename the table
// before dumping it.
//
// A value the output format cannot hold is refused rather than rewritten, with
// ErrUnsupportedFormat and the advice to dump the table as CSV: a tab or a line
// break in TSV, those and a colon in an LTSV label, and in XLSX a control
// character other than tab, line feed and carriage return, which is what an XML
// 1.0 document has no way to spell, or a last column with no name, which a
// worksheet does not store. A table with no rows is refused for LTSV alone,
// which has no header to carry the columns and would leave an empty file --
// and an empty file is not a table, so it blocks the load of every file beside
// it. The other formats keep the columns of an empty table.
//
// A table whose column names a load would refuse is refused too, with
// ErrDuplicateColumn: this package reads " a", "a" and "a " as one name, and
// SQLite does not, so a table holding two of them is one only SQLite can hold.
//
// It exports without a deadline. DumpDatabaseContext takes one.
func DumpDatabase(db *sql.DB, outputDir string, opts ...DumpOptions) error {
	return DumpDatabaseContext(context.Background(), db, outputDir, opts...)
}

// DumpDatabaseContext is DumpDatabase with a context, so an export can be
// canceled or given a deadline.
//
// The export stops at the next table, row or write after the context ends. A
// table already written stays written, since the tables are separate files and
// nothing undoes one: what a canceled export leaves is the tables it had
// finished. The table it was in the middle of leaves nothing, because each file
// is staged and put in place only once it is whole.
func DumpDatabaseContext(ctx context.Context, db *sql.DB, outputDir string, opts ...DumpOptions) error {
	// A nil database is what a caller holds after an error they did not check,
	// and reaching into it here would take their process down over a mistake this
	// package answers with an error everywhere else.
	if db == nil {
		return fmt.Errorf("%w: database must be a non-nil *sql.DB", ErrNilInput)
	}

	// Use default options if none provided
	options := NewDumpOptions()
	if len(opts) > 0 {
		options = opts[0]
	}

	// Fail before creating the output directory when the database cannot be read
	// at all, so a dump that never starts leaves nothing behind.
	//
	// Ping rather than Conn: the dump runs its queries through the pool and needs
	// no connection of its own, and holding one deadlocks a database limited to a
	// single open connection — which is what LoadInto asks the caller for, since
	// SQLite's ":memory:" is private per connection. Every query below then waited
	// for the connection the dump itself was holding, with no error and no timeout.
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("%w: failed to get connection: %w", ErrDatabaseOperation, err)
	}

	// Use generic dump functionality for all connections
	return dumpSQLiteDatabase(ctx, db, outputDir, options)
}

// dumpSQLiteDatabase implements generic dump functionality for SQLite databases
func dumpSQLiteDatabase(ctx context.Context, db *sql.DB, outputDir string, options DumpOptions) error {
	// What there is to write is settled before the destination is touched, for
	// the reason the ping above exists: a dump that writes nothing should leave
	// nothing, and a database with no tables used to leave an empty directory
	// behind along with its error.
	tableNames, err := getSQLiteTableNames(ctx, db)
	if err != nil {
		return fmt.Errorf("%w: failed to get table names: %w", ErrDatabaseOperation, err)
	}

	if len(tableNames) == 0 {
		return ErrNoTables
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0750); err != nil {
		return fmt.Errorf("%w: failed to create output directory: %w", ErrIOOperation, err)
	}

	// A table only belongs to an ACH or Fedwire file when the database says it
	// was loaded from one. The suffix alone is not enough: a caller's own table
	// named orders_entries is not part of an ACH file.
	achBaseNames := fileSourceBaseNames(ctx, db, sourceFormatACH)
	wireBaseNames := fileSourceBaseNames(ctx, db, sourceFormatFedWire)

	writeBackTables := make(map[string]bool)
	for _, tableName := range tableNames {
		if baseName, isACH := isACHBaseTableName(tableName); isACH && slices.Contains(achBaseNames, baseName) {
			writeBackTables[tableName] = true
		}
		if baseName, isWire := isWireBaseTableName(tableName); isWire && slices.Contains(wireBaseNames, baseName) {
			writeBackTables[tableName] = true
		}
	}

	// Export ACH files
	for _, baseName := range achBaseNames {
		outputPath, err := dumpFilePath(outputDir, baseName, extACH)
		if err != nil {
			return err
		}
		if err := DumpACH(ctx, db, baseName, outputPath); err != nil {
			return fmt.Errorf("%w: failed to export ACH file %s: %w", ErrACH, baseName, err)
		}
	}

	// Export Fedwire files
	for _, baseName := range wireBaseNames {
		outputPath, err := dumpFilePath(outputDir, baseName, extFED)
		if err != nil {
			return err
		}
		if err := DumpFedWire(ctx, db, baseName, outputPath); err != nil {
			return fmt.Errorf("%w: failed to export Fedwire file %s: %w", ErrWire, baseName, err)
		}
	}

	// Export remaining tabular tables in the requested format
	for _, tableName := range tableNames {
		if writeBackTables[tableName] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := dumpSQLiteTable(ctx, db, tableName, outputDir, options); err != nil {
			return fmt.Errorf("%w: failed to export table %s: %w", ErrIOOperation, tableName, err)
		}
	}

	return nil
}

// getSQLiteTableNames retrieves all user-defined table names from SQLite database.
// Tables this package keeps for its own bookkeeping are excluded, so they appear
// neither in a dump nor in a listing shown to a caller.
func getSQLiteTableNames(ctx context.Context, db *sql.DB) ([]string, error) {
	// Both underscores are escaped: LIKE reads a bare one as a wildcard, so
	// 'sqlite_%' hid a caller's sqliteish table as readily as SQLite's own
	// sqlite_stat1, and a dump written from this list left that table out.
	query := `SELECT name FROM sqlite_master WHERE type='table'` +
		` AND name NOT LIKE 'sqlite\_%' ESCAPE '\'` +
		` AND name NOT LIKE '` + sourceTableLikePattern + `' ESCAPE '\'`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, name)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tableNames, nil
}

// dumpSQLiteTable exports a single table from SQLite database
func dumpSQLiteTable(ctx context.Context, db *sql.DB, tableName, outputDir string, options DumpOptions) error {
	// Get table columns
	columns, declTypes, err := getSQLiteTableColumns(ctx, db, tableName)
	if err != nil {
		return fmt.Errorf("%w: failed to get columns for table %s: %w", ErrDatabaseOperation, tableName, err)
	}

	// SQLite tells "a" from "a " and this package does not, so a table holding
	// both is one the load refuses -- and the dump wrote the two names into one
	// header and said nothing, leaving the caller to find out when they read
	// their own dump. The question the load asks is asked here, of the same
	// names, so the two cannot drift apart.
	if err := reader.ValidateColumnNames(columns); err != nil {
		return fmt.Errorf("%w in table %s", wrapReadError(err), quoteIdentifier(tableName))
	}

	// Query all data from table
	query := fmt.Sprintf("SELECT %s FROM %s", dumpSelectList(columns, declTypes), quoteIdentifier(tableName)) //nolint:gosec // Table and column names are quoted
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Create output file
	outputPath, err := dumpFilePath(outputDir, tableName, options.FileExtension())
	if err != nil {
		return err
	}

	// An export writes a new file, so there is no schema to keep faith with.
	return writeSQLiteTableData(outputPath, tableName, columns, rows, options, nil)
}

// dumpFilePath is the path a table's dump is written to, or an error when the
// table's name cannot be one.
//
// A table name is an arbitrary SQL identifier, so it can carry a path separator
// or a parent reference, and filepath.Join resolves those: a table created as
// "../escaped" had its dump written next to the output directory rather than in
// it, past whatever the caller had decided the dump was allowed to touch.
//
// What is refused is what no platform this package builds for can hold, judged
// the same way everywhere rather than by the rules of the running one, so the
// same database dumped on Linux and on Windows agrees on which tables it can
// write. Refusing rather than rewriting is what keeps two tables from colliding
// on one file.
func dumpFilePath(outputDir, tableName, ext string) (string, error) {
	name := tableName + ext
	path := filepath.Join(outputDir, name)
	if !usableAsFileName(name) || filepath.Dir(path) != filepath.Clean(outputDir) {
		return "", fmt.Errorf("%w: table %q cannot be dumped because its name is not usable as a file name inside %s",
			ErrInvalidData, tableName, outputDir)
	}
	// A table name is an arbitrary SQL identifier and a table name derived from
	// a file is not: the load spells a space, a hyphen and a dot as an
	// underscore and drops what neither is. So a name a load would spell
	// differently is refused here, rather than written into a file that comes
	// back under another name -- which is silent, and which two such tables
	// turn into a dump this package will not load at all: "a b" and "a-b" are
	// two files and one table name. Refusing is the same answer as above and
	// for the same reason, and the way out is the same: rename the table.
	if loaded := sanitizeTableName(tableFromFilePath(name)); loaded != tableName {
		return "", fmt.Errorf("%w: table %q cannot be dumped because a load of %q would name it %q; rename the table first",
			ErrInvalidData, tableName, name, loaded)
	}
	return path, nil
}

// reservedDeviceNames are the names Windows resolves to a device rather than to
// a file, with or without an extension: writing to one goes to the device and
// the rows go nowhere. They are matched without regard to case, which is how
// Windows matches them.
//
//nolint:gochecknoglobals // constant-like lookup table
var reservedDeviceNames = map[string]bool{
	"con": true, "prn": true, "aux": true, "nul": true,
	"com1": true, "com2": true, "com3": true, "com4": true, "com5": true,
	"com6": true, "com7": true, "com8": true, "com9": true,
	"lpt1": true, "lpt2": true, "lpt3": true, "lpt4": true, "lpt5": true,
	"lpt6": true, "lpt7": true, "lpt8": true, "lpt9": true,
}

// forbiddenFileNameRunes are the characters no file name may carry. The two
// separators are what a dump has to refuse anywhere, since either one makes the
// name a path; the rest are what Windows forbids, and a colon is the worst of
// them, because it names an alternate data stream rather than failing.
const forbiddenFileNameRunes = `/\<>:"|?*`

// usableAsFileName reports whether name can be a file name on every platform
// this package builds for.
func usableAsFileName(name string) bool {
	if name == "" || strings.ContainsAny(name, forbiddenFileNameRunes) {
		return false
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	// Windows strips a trailing dot or space, so such a name reaches the disk
	// as a different one and two tables can land on one file.
	if last := name[len(name)-1]; last == '.' || last == ' ' {
		return false
	}
	// A device name is reserved whatever follows the first dot, so CON.csv is
	// the console and not a file.
	stem := name
	if i := strings.IndexByte(stem, '.'); i >= 0 {
		stem = stem[:i]
	}
	return !reservedDeviceNames[strings.ToLower(stem)]
}

// timeDeclTypes are the declared column types the SQLite driver turns into a
// time.Time. The driver uppercases a declared type before matching it, and it
// parses the stored text against seven layouts, so what comes back no longer
// says which one it was written in.
var timeDeclTypes = map[string]bool{
	"DATE":      true,
	"DATETIME":  true,
	"TIMESTAMP": true,
}

// dumpSelectList builds the select list a dump reads through. A column whose
// declared type the driver converts to a time.Time is read as text instead.
//
// Why: a dump writes what the table holds, and the conversion is one-way. The
// driver parses "2026-07-30" into a time.Time, which formats back as
// "2026-07-30 00:00:00 +0000 UTC" — a Go value's default layout, not the value
// that was stored, and not something a reader turns back into the same cell.
// Asking SQLite for the text keeps whatever is in the cell, including an integer
// stored in a DATE column, and leaves NULL as NULL.
func dumpSelectList(columns, declTypes []string) string {
	if len(columns) == 0 {
		return "*"
	}

	parts := make([]string, 0, len(columns))
	for i, col := range columns {
		quoted := "`" + strings.ReplaceAll(col, "`", "``") + "`"
		if i < len(declTypes) && timeDeclTypes[strings.ToUpper(declTypes[i])] {
			parts = append(parts, "CAST("+quoted+" AS TEXT) AS "+quoted)
			continue
		}
		parts = append(parts, quoted)
	}
	return strings.Join(parts, ", ")
}

// getSQLiteTableColumns retrieves the column names of a table and the type each
// was declared with. The declared type is what decides whether the driver hands
// a value back converted; see dumpSelectList.
func getSQLiteTableColumns(ctx context.Context, db *sql.DB, tableName string) (columns, declTypes []string, err error) {
	query := fmt.Sprintf("PRAGMA table_info(%s)", quoteIdentifier(tableName))
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, dfltValue, pk any

		if err := rows.Scan(&cid, &name, &dataType, &notNull, &dfltValue, &pk); err != nil {
			return nil, nil, err
		}
		columns = append(columns, name)
		declTypes = append(declTypes, dataType)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	return columns, declTypes, nil
}

// writeSQLiteTableData writes table data to file with specified format. The
// write is staged and moved into place, so a format or I/O error partway through
// leaves an existing destination — which for a write-back is the caller's source
// file — exactly as it was.
func writeSQLiteTableData(outputPath, tableName string, columns []string, rows *sql.Rows, options DumpOptions, prior parquetPrior) error {
	return writeFileAtomically(outputPath, func(w io.Writer) error {
		return writeSQLiteTableDataTo(w, tableName, columns, rows, options, prior)
	})
}

// writeSQLiteTableDataTo writes table data to w in the requested format.
//
// Why every format writes to an io.Writer rather than to a path of its own: the
// caller stages the write, so the only path available here is a temporary one
// whose name means nothing. Excel reads both its container format and its sheet
// name out of the name it is given, so handing it the staged path produced a
// rejected save or a sheet named after the temporary file. tableName carries the
// one piece of naming a format legitimately needs.
//
// The named return lets the deferred close report its own failure. A compressor
// writes its trailer on Close, so a compressed output that failed to finish is
// only detectable there; dropping that error would commit a truncated archive
// over the destination as if it had been written.
func writeSQLiteTableDataTo(w io.Writer, tableName string, columns []string, rows *sql.Rows, options DumpOptions, prior parquetPrior) (err error) {
	// Parquet compresses per column internally, so an outer compressor would only
	// bloat the file and hide it behind a second extension.
	if options.Format == OutputFormatParquet && options.Compression != CompressionNone {
		return fmt.Errorf("%w: external compression not supported for Parquet format - use Parquet's built-in compression instead", ErrUnsupportedFormat)
	}

	compressed, closeWriter, err := createCompressedWriter(w, options.Compression)
	if err != nil {
		// The handler separates a codec with no writer from a compressor that
		// failed to start, and wrapping both in ErrCompression here undid that:
		// an output this build cannot write reported a compression failure and
		// matched both sentinels at once.
		return err
	}
	defer func() {
		// Closing a compressing writer is what flushes it, so a failure here can
		// mean a truncated file. Joining rather than dropping it means a caller
		// whose write already failed still learns the output is unusable.
		err = joinCleanup(err, closeWriter(), "finish writing "+tableName)
	}()

	// Parquet and XLSX state their own encoding, so running their bytes through
	// a transcoder would corrupt the container rather than translate it. Only the
	// text formats below are encoded.
	switch options.Format {
	case OutputFormatParquet:
		return writeParquetTableData(compressed, columns, rows, prior)
	case OutputFormatXLSX:
		return writeXLSXTableData(compressed, tableName, columns, rows)
	}

	// The encoder wraps inside the compressor: what a compressor stores is the
	// encoded text, so a reader decompresses and then decodes.
	encoded, encoder := options.Encoding.encodingWriter(compressed)
	defer func() {
		if encoder == nil {
			return
		}
		// Closing the encoder is what flushes a sequence it was still holding, so
		// dropping this error would commit a truncated file.
		if closeErr := encoder.Close(); closeErr != nil && err == nil {
			err = encodingError(options.Encoding, closeErr)
		}
	}()

	text, ok := textDumpFormats[options.Format]
	if !ok {
		return fmt.Errorf("%w: unsupported output format: %v", ErrUnsupportedFormat, options.Format)
	}
	writeErr := writeTextData(encoded, columns, rows, text, options.LineEnding)
	if writeErr != nil && encoder.encoderFailed() {
		return encodingError(options.Encoding, writeErr)
	}
	return writeErr
}

// createCompressedWriter creates an appropriate writer based on compression type
func createCompressedWriter(w io.Writer, compression CompressionType) (io.Writer, func() error, error) {
	handler := NewCompressionHandler(compression)
	return handler.CreateWriter(w)
}

// textDumpFormat is how one text output format is written, and the sentinel a
// value it cannot hold has always been reported with.
type textDumpFormat struct {
	// format is the writer package's name for this one.
	format writer.Format
	// sentinel is the error a refused value carried before this package had one
	// of its own for it, or nil where there was none. It is kept on the error so
	// a caller matching it goes on matching it.
	sentinel error
}

// textDumpFormats are the output formats written as text. Parquet and XLSX are
// not among them: they are typed container formats, written by their own
// functions from the driver's values rather than from rendered text.
//
//nolint:gochecknoglobals // constant-like lookup table
var textDumpFormats = map[OutputFormat]textDumpFormat{
	OutputFormatCSV:  {format: writer.FormatCSV},
	OutputFormatTSV:  {format: writer.FormatTSV, sentinel: parser.ErrTSVUnrepresentable},
	OutputFormatLTSV: {format: writer.FormatLTSV},
}

// writeTextData writes a table's rows to w in one of the text formats.
//
// The encoding is the writer package's; what is here is the part that is this
// package's own: reading a row out of the driver and rendering each value as
// the text a reader turns back into the same cell.
func writeTextData(w io.Writer, columns []string, rows *sql.Rows, text textDumpFormat, lineEnding LineEnding) error {
	out := writer.New(w, text.format, writer.Options{LineEnding: lineEnding.terminator()})

	// The header is written — or, for LTSV, checked and kept as the label of
	// each field — before the rows, so a table whose column name the format
	// cannot express is refused whether or not it has any rows.
	if err := out.Header(columns); err != nil {
		return dumpFormatError(err, text)
	}

	// Prepare for scanning
	values := make([]any, len(columns))
	scanArgs := make([]any, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	// One record is reused across rows: the writer has encoded it by the time
	// Record returns, so it keeps no reference to the strings in it.
	record := make([]string, len(columns))

	written := 0
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}
		for i, value := range values {
			record[i] = formatDumpValue(value)
		}
		if err := out.Record(record); err != nil {
			return dumpFormatError(err, text)
		}
		written++
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// LTSV carries its labels on every record rather than in a header, so a
	// table with no rows has nothing to write and the columns go with it: the
	// file came out empty, and an empty file is not a table, so the dump wrote
	// something this package refuses to read. The other formats keep the
	// columns -- a header line, a Parquet schema, a header row -- so this is
	// the one place a table with no rows cannot be said. Writing a line of bare
	// labels instead would load as one row of empty values, which is a row the
	// table did not have.
	if written == 0 && text.format == writer.FormatLTSV {
		return fmt.Errorf("%w: LTSV cannot hold a table with no rows, since it has no header to carry the columns; dump this table as CSV instead",
			ErrUnsupportedFormat)
	}
	// The writer buffers, so this is where a destination that could not take the
	// bytes — an encoder refusing a value it cannot write, for instance —
	// reports it. Nothing is buffered past this point.
	return dumpFormatError(out.Flush(), text)
}

// dumpFormatError gives a value the output format cannot hold this package's
// sentinel and the advice that goes with it.
//
// The writer package names no sentinel, because its callers have different
// ones for this. What a dump has to say is that the table is
// fine and the format is not, which is what ErrUnsupportedFormat means here,
// and which format to ask for instead.
//
// Which format to ask for follows from what could not be written. A value one
// text format cannot hold is CSV's to hold, because CSV quotes a field, so a
// tab, a line break and a carriage return are ordinary characters inside one.
// LTSV used to answer "CSV or TSV", which is wrong for all three of the
// characters it forbids in a value, since TSV forbids the same three. A value
// whose place in the file is the problem is nobody's to hold as text, CSV
// included, so that one asks for a typed container instead.
func dumpFormatError(err error, text textDumpFormat) error {
	var writeErr *writer.Error
	if !errors.As(err, &writeErr) {
		return err
	}
	var instead string
	switch writeErr.Kind {
	case writer.KindUnrepresentable:
		instead = "; dump this table as CSV instead"
	case writer.KindUnrepresentableAsText:
		instead = "; dump this table as XLSX or Parquet instead"
	case writer.KindUnrepresentableUnnamed:
		instead = "; dump this table as LTSV or Parquet instead"
	default:
		return err
	}
	if text.sentinel != nil {
		return fmt.Errorf("%w: %w: %s%s", ErrUnsupportedFormat, text.sentinel, writeErr.Error(), instead)
	}
	return fmt.Errorf("%w: %w%s", ErrUnsupportedFormat, err, instead)
}

// formatDumpValue renders one cell for a text-based dump.
//
// Every type a SQL driver may hand back is named, because fmt's %v as a
// catch-all prints a Go value rather than a data value: a BLOB came out as the
// decimal bytes of a Go slice ("[104 101 108 108 111]" for "hello"), which no
// reader turns back into the cell it came from. A NULL and an empty string both
// render empty, which is what the text formats can express.
func formatDumpValue(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		// Rendered the way an import reads it back: a whole number keeps a
		// decimal point, or the file reloads as an INTEGER column and integer
		// division answers a different question than this database would.
		return reader.SQLiteFloatText(v, 64)
	case bool:
		return strconv.FormatBool(v)
	case time.Time:
		// A dump reads date columns as text so this is not reached for them; it
		// stays a defined answer for a driver that converts something else.
		return v.Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%v", v)
	}
}
