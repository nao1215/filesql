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

// Open creates an SQL database from CSV, TSV, or LTSV files.
//
// Quick start:
//
//	db, err := filesql.Open("data.csv")
//	if err != nil {
//		return err
//	}
//	defer db.Close()
//
//	rows, err := db.Query("SELECT * FROM data WHERE age > 25")
//
// Parameters:
//   - paths: One or more file paths or directories
//   - Files: "users.csv", "products.tsv", "logs.ltsv"
//   - Compressed: "data.csv.gz", "archive.tsv.bz2"
//   - Directories: "/data/" (loads all CSV/TSV/LTSV files recursively)
//
// Table names:
//   - "users.csv" → table "users"
//   - "data.tsv.gz" → table "data"
//   - "/path/to/sales.csv" → table "sales"
//   - "user-data.csv" → table "user_data" (hyphens become underscores)
//   - "my file.csv" → table "my_file" (spaces become underscores)
//
// Special characters in file names are automatically sanitized for SQL safety.
//
// Note: Original files are never modified. Changes exist only in memory.
// To save changes, use DumpDatabase() function.
//
// Example with multiple files:
//
//	// Open a single CSV file
//	db, err := filesql.Open("data/users.csv")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
//
//	// Complex query with JOINs, aggregation, and window functions
//	rows, err := db.Query(`
//		SELECT
//			u.name,
//			u.department,
//			u.salary,
//			AVG(u.salary) OVER (PARTITION BY u.department) as dept_avg_salary,
//			RANK() OVER (PARTITION BY u.department ORDER BY u.salary DESC) as salary_rank,
//			COUNT(*) OVER (PARTITION BY u.department) as dept_size
//		FROM users u
//		WHERE u.salary > (
//			SELECT AVG(salary) * 0.8
//			FROM users
//			WHERE department = u.department
//		)
//		ORDER BY u.department, u.salary DESC
//	`)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer rows.Close()
//
//	// Process results
//	for rows.Next() {
//		var name, dept string
//		var salary, deptAvg float64
//		var rank, deptSize int
//		if err := rows.Scan(&name, &dept, &salary, &deptAvg, &rank, &deptSize); err != nil {
//			log.Fatal(err)
//		}
//		fmt.Printf("%s (%s): $%.2f (Rank: %d/%d, Dept Avg: $%.2f)\n",
//			name, dept, salary, rank, deptSize, deptAvg)
//	}
func Open(paths ...string) (*sql.DB, error) {
	return OpenContext(context.Background(), paths...)
}

// OpenContext is like Open but accepts a context for cancellation and timeout control.
//
// Use this when you need to:
//   - Set timeouts for loading large files
//   - Support cancellation in server applications
//   - Integrate with context-aware code
//
// Example with timeout:
//
//	// Open a single CSV file with timeout
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	db, err := filesql.OpenContext(ctx, "data/users.csv")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
//
//	// Complex query with JOINs, aggregation, and window functions
//	rows, err := db.QueryContext(ctx, `
//		SELECT
//			u.name,
//			u.department,
//			u.salary,
//			AVG(u.salary) OVER (PARTITION BY u.department) as dept_avg_salary,
//			RANK() OVER (PARTITION BY u.department ORDER BY u.salary DESC) as salary_rank,
//			COUNT(*) OVER (PARTITION BY u.department) as dept_size
//		FROM users u
//		WHERE u.salary > (
//			SELECT AVG(salary) * 0.8
//			FROM users
//			WHERE department = u.department
//		)
//		ORDER BY u.department, u.salary DESC
//	`)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer rows.Close()
//
//	// Process results
//	for rows.Next() {
//		var name, dept string
//		var salary, deptAvg float64
//		var rank, deptSize int
//		if err := rows.Scan(&name, &dept, &salary, &deptAvg, &rank, &deptSize); err != nil {
//			log.Fatal(err)
//		}
//		fmt.Printf("%s (%s): $%.2f (Rank: %d/%d, Dept Avg: $%.2f)\n",
//			name, dept, salary, rank, deptSize, deptAvg)
//	}
//
// OpenContext creates an SQL database from CSV, TSV, or LTSV files with context support.
//
// This function is similar to Open() but allows cancellation and timeout control through context.
// Table names are automatically generated from file names with special characters
// sanitized for SQL safety (e.g., hyphens become underscores: "data-file.csv" → "data_file").
//
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//
//	db, err := filesql.OpenContext(ctx, "large-dataset.csv")
//	if err != nil {
//		return err
//	}
//	defer db.Close()
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - paths: One or more file paths or directories to load
func OpenContext(ctx context.Context, paths ...string) (*sql.DB, error) {
	// Use builder pattern internally for backward compatibility
	builder := NewBuilder().AddPaths(paths...)

	// Build validates the paths
	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		return nil, err
	}

	// Open creates the database connection
	return validatedBuilder.Open(ctx)
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
	builder := NewBuilder().AddPaths(paths...)

	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		return err
	}

	return validatedBuilder.LoadInto(ctx, db)
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
func DumpDatabase(db *sql.DB, outputDir string, opts ...DumpOptions) error {
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
	if err := db.PingContext(context.Background()); err != nil {
		return fmt.Errorf("%w: failed to get connection: %w", ErrDatabaseOperation, err)
	}

	// Use generic dump functionality for all connections
	return dumpSQLiteDatabase(db, outputDir, options)
}

// dumpSQLiteDatabase implements generic dump functionality for SQLite databases
func dumpSQLiteDatabase(db *sql.DB, outputDir string, options DumpOptions) error {
	// What there is to write is settled before the destination is touched, for
	// the reason the ping above exists: a dump that writes nothing should leave
	// nothing, and a database with no tables used to leave an empty directory
	// behind along with its error.
	tableNames, err := getSQLiteTableNames(db)
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

	ctx := context.Background()

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
		if err := dumpSQLiteTable(db, tableName, outputDir, options); err != nil {
			return fmt.Errorf("%w: failed to export table %s: %w", ErrIOOperation, tableName, err)
		}
	}

	return nil
}

// getSQLiteTableNames retrieves all user-defined table names from SQLite database.
// Tables this package keeps for its own bookkeeping are excluded, so they appear
// neither in a dump nor in a listing shown to a caller.
func getSQLiteTableNames(db *sql.DB) ([]string, error) {
	ctx := context.Background()
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
func dumpSQLiteTable(db *sql.DB, tableName, outputDir string, options DumpOptions) error {
	// Get table columns
	columns, declTypes, err := getSQLiteTableColumns(db, tableName)
	if err != nil {
		return fmt.Errorf("%w: failed to get columns for table %s: %w", ErrDatabaseOperation, tableName, err)
	}

	// Query all data from table
	ctx := context.Background()
	query := fmt.Sprintf("SELECT %s FROM `%s`", dumpSelectList(columns, declTypes), tableName) //nolint:gosec // Table and column names are quoted and come from database metadata
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

	return writeSQLiteTableData(outputPath, tableName, columns, rows, options)
}

// dumpFilePath is the path a table's dump is written to, or an error when the
// table's name cannot be one.
//
// A table name is an arbitrary SQL identifier, so it can carry a path separator
// or a parent reference, and filepath.Join resolves those: a table created as
// "../escaped" had its dump written next to the output directory rather than in
// it, past whatever the caller had decided the dump was allowed to touch. Both
// separators are refused on every platform, not only the one the running OS
// honors, so the same database dumped on Linux and on Windows agrees on which
// tables it can write.
func dumpFilePath(outputDir, tableName, ext string) (string, error) {
	name := tableName + ext
	path := filepath.Join(outputDir, name)
	if strings.ContainsAny(name, `/\`) || filepath.Dir(path) != filepath.Clean(outputDir) {
		return "", fmt.Errorf("%w: table %q cannot be dumped because its name is not usable as a file name inside %s",
			ErrInvalidData, tableName, outputDir)
	}
	return path, nil
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
func getSQLiteTableColumns(db *sql.DB, tableName string) (columns, declTypes []string, err error) {
	ctx := context.Background()
	query := fmt.Sprintf("PRAGMA table_info(`%s`)", tableName)
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
func writeSQLiteTableData(outputPath, tableName string, columns []string, rows *sql.Rows, options DumpOptions) error {
	return writeFileAtomically(outputPath, func(w io.Writer) error {
		return writeSQLiteTableDataTo(w, tableName, columns, rows, options)
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
func writeSQLiteTableDataTo(w io.Writer, tableName string, columns []string, rows *sql.Rows, options DumpOptions) (err error) {
	// Parquet compresses per column internally, so an outer compressor would only
	// bloat the file and hide it behind a second extension.
	if options.Format == OutputFormatParquet && options.Compression != CompressionNone {
		return fmt.Errorf("%w: external compression not supported for Parquet format - use Parquet's built-in compression instead", ErrUnsupportedFormat)
	}

	compressed, closeWriter, err := createCompressedWriter(w, options.Compression)
	if err != nil {
		return fmt.Errorf("%w: failed to create writer: %w", ErrCompression, err)
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
		return writeParquetTableData(compressed, columns, rows)
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
	}

	if err := rows.Err(); err != nil {
		return err
	}
	// The writer buffers, so this is where a destination that could not take the
	// bytes — an encoder refusing a value it cannot write, for instance —
	// reports it. Nothing is buffered past this point.
	return dumpFormatError(out.Flush(), text)
}

// dumpFormatError gives a value the output format cannot hold this package's
// sentinel and the advice that goes with it.
//
// The writer package names no sentinel, because the three callers of it have
// three different ones for this. What a dump has to say is that the table is
// fine and the format is not, which is what ErrUnsupportedFormat means here,
// and which format to ask for instead.
//
// That advice is always CSV, because CSV is the only text format here that can
// hold what the others cannot: it quotes a field, so a tab, a line break and a
// carriage return are ordinary characters inside one. LTSV used to answer "CSV
// or TSV", which is wrong for all three of the characters it forbids in a
// value, since TSV forbids the same three. CSV itself never reaches this, there
// being no value it cannot write.
func dumpFormatError(err error, text textDumpFormat) error {
	var writeErr *writer.Error
	if !errors.As(err, &writeErr) || writeErr.Kind != writer.KindUnrepresentable {
		return err
	}
	if text.sentinel != nil {
		return fmt.Errorf("%w: %w: %s; dump this table as CSV instead", ErrUnsupportedFormat, text.sentinel, writeErr.Error())
	}
	return fmt.Errorf("%w: %w; dump this table as CSV instead", ErrUnsupportedFormat, err)
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
