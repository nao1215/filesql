package filesql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nao1215/filesql/internal/reader"
	"github.com/nao1215/filesql/parser"
	"github.com/parquet-go/parquet-go"
	"github.com/xuri/excelize/v2"
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

	writer, closeWriter, err := createCompressedWriter(w, options.Compression)
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
		return writeParquetTableData(writer, columns, rows)
	case OutputFormatXLSX:
		return writeXLSXTableData(writer, tableName, columns, rows)
	}

	// The encoder wraps inside the compressor: what a compressor stores is the
	// encoded text, so a reader decompresses and then decodes.
	encoded, encoder := options.Encoding.encodingWriter(writer)
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

	var writeErr error
	switch options.Format {
	case OutputFormatCSV:
		writeErr = writeCSVData(encoded, columns, rows, options.LineEnding)
	case OutputFormatTSV:
		writeErr = writeTSVData(encoded, columns, rows, options.LineEnding)
	case OutputFormatLTSV:
		writeErr = writeLTSVData(encoded, columns, rows, options.LineEnding)
	default:
		return fmt.Errorf("%w: unsupported output format: %v", ErrUnsupportedFormat, options.Format)
	}
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

// loneEmptyField is what a CSV record of one empty field is written as.
//
// Written plainly it is a blank line, and a blank line is not a CSV record: a reader
// skips it, so a one-column table's empty rows disappeared and the dump reported
// success. The quotes say "one field, and it is empty", which cannot be read as
// anything else. encoding/csv's writer does not quote an empty field — it has no
// way to know it is the only one on the line — so this record is written around
// it.
const loneEmptyField = `""`

// writeDelimitedData writes data in CSV or TSV format based on delimiter
func writeDelimitedData(writer io.Writer, columns []string, rows *sql.Rows, delimiter rune, lineEnding LineEnding) error {
	// Records are staged one at a time so the terminator is this package's
	// choice rather than the csv writer's. UseCRLF would do it, but it rewrites
	// every line feed the writer emits, inside a quoted field as well as between
	// records: a cell holding a line break came back holding a different one, so
	// saving a file changed a row nobody edited.
	var staged bytes.Buffer
	csvWriter := csv.NewWriter(&staged)
	if delimiter != csvDelimiter {
		csvWriter.Comma = delimiter
	}
	terminator := lineEnding.terminator()

	writeStaged := func(record []string) error {
		staged.Reset()
		if err := csvWriter.Write(record); err != nil {
			return err
		}
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			return err
		}
		line := strings.TrimSuffix(staged.String(), "\n")
		_, err := io.WriteString(writer, line+terminator)
		return err
	}

	// writeRecord writes one record, taking the lone empty field around the csv
	// writer. Flushing first keeps the two writers' output in order.
	writeRecord := func(record []string) error {
		// TSV is written literally; see parser.WriteTSVRecord. A blank line is
		// already the one-column empty value there, so it needs no form of its own.
		if delimiter == tsvDelimiter {
			return parser.WriteTSVRecordLineEnding(writer, record, terminator)
		}
		if len(record) != 1 || record[0] != "" {
			return writeStaged(record)
		}
		_, err := io.WriteString(writer, loneEmptyField+terminator)
		return err
	}

	// Write header
	if err := writeRecord(columns); err != nil {
		return err
	}

	// Prepare for scanning
	values := make([]any, len(columns))
	scanArgs := make([]any, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Write data rows
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		record := make([]string, len(columns))
		for i, value := range values {
			record[i] = formatDumpValue(value)
		}

		if err := writeRecord(record); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}
	// Nothing is buffered past this point: each record is flushed into the
	// staging buffer and written on from there, so a failure of the writer
	// underneath — an encoder refusing a value it cannot write, for instance —
	// has already been returned by the write that caused it.
	return nil
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

// writeCSVData writes data in CSV format
func writeCSVData(writer io.Writer, columns []string, rows *sql.Rows, lineEnding LineEnding) error {
	return writeDelimitedData(writer, columns, rows, csvDelimiter, lineEnding)
}

// writeTSVData writes data in TSV format
func writeTSVData(writer io.Writer, columns []string, rows *sql.Rows, lineEnding LineEnding) error {
	return writeDelimitedData(writer, columns, rows, tsvDelimiter, lineEnding)
}

// writeLTSVData writes data in LTSV format
func writeLTSVData(writer io.Writer, columns []string, rows *sql.Rows, lineEnding LineEnding) error {
	// A label is read up to the first colon, so a colon in a column name would
	// make the rest of the name part of the value. Checked once, ahead of the
	// rows, because it does not depend on them.
	for _, col := range columns {
		if err := checkLTSVLabel(col); err != nil {
			return err
		}
	}

	// Prepare for scanning
	values := make([]any, len(columns))
	scanArgs := make([]any, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Write data rows
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		// Build LTSV record
		parts := make([]string, 0, len(columns))
		for i, col := range columns {
			value := formatDumpValue(values[i])
			if err := checkLTSVValue(col, value); err != nil {
				return err
			}
			parts = append(parts, col+":"+value)
		}

		line := strings.Join(parts, "\t") + lineEnding.terminator()
		if _, err := writer.Write([]byte(line)); err != nil {
			return err
		}
	}

	return rows.Err()
}

// ltsvForbidden names the characters that end a field or a record in LTSV, which
// is why a value cannot carry one.
var ltsvForbidden = []struct {
	char rune
	name string
}{
	{char: '\t', name: "tab"},
	{char: '\n', name: "newline"},
	{char: '\r', name: "carriage return"},
}

// checkLTSVValue refuses a value LTSV has no way to hold.
//
// LTSV separates fields with a tab and records with a newline, and defines no
// escape for either. Writing one anyway produced a file that parses as something
// else: a tab inside a value opened a second field, which has no label and which
// a reader drops without a word, and a newline split the record in two. Failing
// here costs nothing, because the dump is staged and the destination is only
// replaced on success — where silently dropping the value cost the value.
func checkLTSVValue(column, value string) error {
	for _, f := range ltsvForbidden {
		if strings.ContainsRune(value, f.char) {
			return fmt.Errorf("%w: LTSV cannot hold a value that contains a %s, and column %q holds a %s; dump this table as CSV or TSV instead",
				ErrUnsupportedFormat, f.name, column, f.name)
		}
	}
	return nil
}

// checkLTSVLabel refuses a column name that would not read back as a label. A
// colon is what separates a label from its value, so it is forbidden here on top
// of the characters no field may carry.
func checkLTSVLabel(column string) error {
	if strings.ContainsRune(column, ':') {
		return fmt.Errorf("%w: an LTSV label cannot contain a colon, and column %q holds a colon; dump this table as CSV or TSV instead",
			ErrUnsupportedFormat, column)
	}
	for _, f := range ltsvForbidden {
		if strings.ContainsRune(column, f.char) {
			return fmt.Errorf("%w: an LTSV label cannot contain a %s, and column %q holds a %s; dump this table as CSV or TSV instead",
				ErrUnsupportedFormat, f.name, column, f.name)
		}
	}
	return nil
}

// writeParquetTableData writes SQLite table data to Parquet format
func writeParquetTableData(w io.Writer, columns []string, rows *sql.Rows) error {
	if len(columns) == 0 {
		return fmt.Errorf("%w: no columns defined", ErrEmptyData)
	}

	// The declared types are read before the scan loop. Draining Rows closes it,
	// and ColumnTypes on a closed Rows fails, which would leave a table with no
	// rows with nothing to take its schema from.
	declared := make([]string, len(columns))
	if types, err := rows.ColumnTypes(); err == nil {
		for i, ct := range types {
			if i < len(declared) {
				declared[i] = ct.DatabaseTypeName()
			}
		}
	}

	// Read all rows into memory first. The raw driver values are kept rather than
	// their rendered text because Parquet is a typed format: which Parquet type
	// each column is written as is decided from the values themselves, below.
	var allRows [][]any

	// Prepare for scanning
	values := make([]any, len(columns))
	scanArgs := make([]any, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("%w: failed to scan row: %w", ErrDatabaseOperation, err)
		}
		row := make([]any, len(columns))
		copy(row, values)
		allRows = append(allRows, row)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: error iterating rows: %w", ErrDatabaseOperation, err)
	}

	return writeParquetData(w, columns, allRows, declared)
}

// parquetKind is the Parquet type one column is written as.
type parquetKind int

const (
	parquetString parquetKind = iota
	parquetInt
	parquetFloat
)

// parquetColumnKind decides how one column is written. A column is numeric only
// when every value in it is, so a column that mixes a number with text is
// written as STRING rather than losing the text: SQLite types values, not
// columns, and a dump has to carry back whatever the rows actually held.
//
// The declared type decides an empty column, and only an empty one. It is what a
// table emptied by the session still knows about itself, so an auto-save keeps
// the schema instead of rewriting every column as STRING; for a column with rows
// the values are the better witness, because a query's result columns carry no
// declared type at all.
func parquetColumnKind(rows [][]any, col int, declaredType string) parquetKind {
	kind := parquetKind(-1)
	for _, row := range rows {
		// A blank cell says nothing about the column's type. SQLite stores a
		// blank in a numeric column as the empty string, since "" has no numeric
		// value to convert to, and letting that decide the column wrote a column
		// of numbers as text the moment one row was missing an entry.
		if col >= len(row) || row[col] == nil || row[col] == "" {
			continue
		}
		var cell parquetKind
		switch row[col].(type) {
		case int64:
			cell = parquetInt
		case float64:
			cell = parquetFloat
		default:
			return parquetString
		}
		switch {
		case kind < 0:
			kind = cell
		case kind != cell:
			// int64 and float64 in one column widen to float64, which holds both
			// without changing how the column compares.
			kind = parquetFloat
		}
	}
	if kind >= 0 {
		return kind
	}
	switch strings.ToUpper(declaredType) {
	case "INTEGER", "INT", "BIGINT":
		return parquetInt
	case "REAL", "FLOAT", "DOUBLE":
		return parquetFloat
	default:
		return parquetString
	}
}

// parquetNodeFor is the Parquet schema node a parquetKind is written as. Every
// column is optional, so a stored null survives the round-trip.
func parquetNodeFor(kind parquetKind) parquet.Node {
	switch kind {
	case parquetInt:
		return parquet.Optional(parquet.Leaf(parquet.Int64Type))
	case parquetFloat:
		return parquet.Optional(parquet.Leaf(parquet.DoubleType))
	default:
		return parquet.Optional(parquet.String())
	}
}

// orderedGroup is parquet.Group with its fields kept in the order the table
// declares its columns; parquet.Group alone sorts them by name, and a dump
// that reordered its columns would hand every downstream reader a different
// table than the one it was asked to write.
type orderedGroup struct {
	parquet.Group
	names []string
}

// Fields returns the group's fields in declaration order.
func (g orderedGroup) Fields() []parquet.Field {
	sorted := g.Group.Fields()
	byName := make(map[string]parquet.Field, len(sorted))
	for _, field := range sorted {
		byName[field.Name()] = field
	}
	fields := make([]parquet.Field, 0, len(g.names))
	for _, name := range g.names {
		fields = append(fields, byName[name])
	}
	return fields
}

// parquetCellValue renders one cell as the column's Parquet value. ok is false
// for a null: a nil value is the SQL NULL the row carried, and a blank in a
// numeric column is written as the null it means, since a number has no
// spelling for a blank. A text column keeps its empty string. A value that does
// not match a numeric column's kind cannot occur, because parquetColumnKind
// only chooses a numeric kind when every value in the column is numeric.
func parquetCellValue(kind parquetKind, value any) (parquet.Value, bool, error) {
	if value == nil {
		return parquet.Value{}, false, nil
	}
	if text, isText := value.(string); isText && text == "" && kind != parquetString {
		return parquet.Value{}, false, nil
	}
	switch kind {
	case parquetInt:
		n, ok := value.(int64)
		if !ok {
			return parquet.Value{}, false, fmt.Errorf("%w: %T in an integer column", ErrInvalidData, value)
		}
		return parquet.Int64Value(n), true, nil
	case parquetFloat:
		switch v := value.(type) {
		case float64:
			return parquet.DoubleValue(v), true, nil
		case int64:
			return parquet.DoubleValue(float64(v)), true, nil
		default:
			return parquet.Value{}, false, fmt.Errorf("%w: %T in a real column", ErrInvalidData, value)
		}
	default:
		return parquet.ByteArrayValue([]byte(formatDumpValue(value))), true, nil
	}
}

// writeParquetData writes data to Parquet format. A nil cell in rows is stored
// as a Parquet null, so a SQL NULL survives the round-trip instead of collapsing
// into an empty string. declared holds each column's declared SQL type, which
// decides a column that has no rows to speak for it.
//
// Each column is written as the Parquet type its values call for rather than as
// STRING. Parquet states the type of every column in its schema and readers
// trust it, so writing a numeric column as digit strings hands the next tool a
// column it will compare and sort as text.
//
// A table with no rows is written as a schema with no row groups, which is a
// valid Parquet file: the other formats write their header and nothing else, and
// a dump that refused to write an emptied table let an auto-save keep the rows
// the caller had deleted.
func writeParquetData(w io.Writer, columns []string, rows [][]any, declared []string) error {
	if len(columns) == 0 {
		return fmt.Errorf("%w: no columns defined", ErrEmptyData)
	}

	kinds := make([]parquetKind, len(columns))
	group := orderedGroup{Group: make(parquet.Group, len(columns)), names: columns}
	for i, col := range columns {
		declaredType := ""
		if i < len(declared) {
			declaredType = declared[i]
		}
		kinds[i] = parquetColumnKind(rows, i, declaredType)
		group.Group[col] = parquetNodeFor(kinds[i])
	}
	schema := parquet.NewSchema("table", group)

	// The writer takes an io.Writer and never closes it, so the caller keeps
	// ownership of the destination.
	writer := parquet.NewGenericWriter[any](w, schema)

	// Rows are built as Parquet values directly rather than deconstructed from
	// Go values by reflection, which reads a zero value in an optional column
	// as a null and would turn a stored 0 or "" into a missing cell.
	buf := make([]parquet.Row, 0, min(len(rows), 1024))
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if _, err := writer.WriteRows(buf); err != nil {
			return fmt.Errorf("%w: failed to write rows to parquet: %w", ErrIOOperation, err)
		}
		buf = buf[:0]
		return nil
	}
	for _, row := range rows {
		out := make(parquet.Row, len(columns))
		for i := range columns {
			var value any
			if i < len(row) {
				value = row[i]
			}
			cell, present, err := parquetCellValue(kinds[i], value)
			if err != nil {
				return err
			}
			if present {
				out[i] = cell.Level(0, 1, i)
			} else {
				out[i] = parquet.NullValue().Level(0, 0, i)
			}
		}
		buf = append(buf, out)
		if len(buf) == cap(buf) {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}

	// Close writes the footer, so this is where an incomplete file shows up.
	// For a table with no rows it writes the schema and the footer, which is
	// the whole file.
	if err := writer.Close(); err != nil {
		return fmt.Errorf("%w: failed to close parquet writer: %w", ErrIOOperation, err)
	}
	return nil
}

// xlsxSheet is one sheet of a workbook being written. Rows are opened when the
// sheet is reached rather than up front, because a *sql.Rows holds a cursor and
// only one can be read at a time.
type xlsxSheet struct {
	// name is the sheet name, already adapted to what Excel accepts.
	name string
	// open yields the sheet's columns and rows.
	open func() ([]string, *sql.Rows, error)
}

// writeXLSXTableData writes SQLite table data to Excel XLSX format as a
// single-sheet workbook named after the table.
func writeXLSXTableData(w io.Writer, tableName string, columns []string, rows *sql.Rows) error {
	// The sheet name is what a reader turns back into a table name, so it comes
	// from the table this dump is of, adapted to what Excel accepts.
	return writeXLSXWorkbook(w, []xlsxSheet{{
		name: excelSheetName(tableName),
		open: func() ([]string, *sql.Rows, error) { return columns, rows, nil },
	}})
}

// writeXLSXWorkbook writes sheets as one workbook. A workbook overwritten in
// place goes through here with every one of its sheets, so a file of several
// sheets comes back whole rather than being refused or flattened to one.
func writeXLSXWorkbook(w io.Writer, sheets []xlsxSheet) error {
	return writeXLSXWorkbookOnto(w, nil, sheets)
}

// writeXLSXWorkbookOnto writes sheets into base, or into a new workbook when
// base is nil.
//
// A save that replaces a workbook writes onto the workbook it is replacing, so
// what this package does not hold survives the save: a sheet the sheet policy
// chose not to load used to be deleted from the caller's file, and a column
// width, a merged range and a comment were gone from the sheets it did load.
// Only the rows of a sheet a table was loaded from are rewritten, since those
// rows are what the table is.
func writeXLSXWorkbookOnto(w io.Writer, base *excelize.File, sheets []xlsxSheet) error {
	if len(sheets) == 0 {
		return fmt.Errorf("%w: no sheets to write", ErrEmptyData)
	}

	f := base
	if f == nil {
		f = excelize.NewFile()
	}
	defer func() {
		_ = f.Close() // Ignore close error
	}()

	for _, sheet := range sheets {
		had, err := xlsxSheetBefore(f, sheet.name, base != nil)
		if err != nil {
			return err
		}
		written, err := writeXLSXSheet(f, sheet, had.values)
		if err != nil {
			return err
		}
		if err := trimXLSXSheet(f, sheet.name, had.extent, written); err != nil {
			return err
		}
	}

	// excelize starts a workbook with a default sheet. It is only ours to remove
	// once a sheet of our own exists, and not at all if a sheet reused its name,
	// and never when the workbook came from the caller rather than from here.
	if _, err := f.GetSheetIndex(defaultSheetName); err == nil && base == nil {
		hasOwn := false
		for _, sheet := range sheets {
			if sheet.name == defaultSheetName {
				hasOwn = true
				break
			}
		}
		if !hasOwn {
			if err := f.DeleteSheet(defaultSheetName); err != nil {
				return fmt.Errorf("failed to delete default sheet: %w", err)
			}
		}
	}

	// Why Write and not SaveAs: SaveAs picks the container format from the file
	// extension, and the caller stages the write, so the only name available here
	// carries a temporary suffix that Excel rejects. Any compression the caller
	// asked for is already wrapped around w.
	if err := f.Write(w); err != nil {
		return fmt.Errorf("%w: failed to write Excel file: %w", ErrIOOperation, err)
	}

	return nil
}

// unchangedXLSXCell reports whether the cell at row and column, both counted
// from one, already reads as value in the sheet a save is writing onto.
//
// A cell holds more than the text a table can carry. It may hold a formula and
// the value that formula last evaluated to, and it may hold a date as a serial
// number with a format that renders it. Writing the loaded text back over such
// a cell says nothing the cell did not already say and takes the rest with it:
// the formula became an empty cell, so the workbook lost the rule that produced
// its numbers, and the date became text, so the column no longer sorted or
// calculated as dates. Leaving the cell alone keeps both. A cell whose value did
// change is written, which is the point of the save, and a formula that no
// longer produces the value it is next to cannot stay.
func unchangedXLSXCell(before [][]string, row, column int, value string) bool {
	if row > len(before) {
		return false
	}
	cells := before[row-1]
	if column > len(cells) {
		// A workbook stores no cell for a trailing empty one, so a row that ends
		// before this column holds nothing here, and nothing is what an empty
		// value would write.
		return value == ""
	}
	return cells[column-1] == value
}

// xlsxExtent is how far a sheet's values reached before it was rewritten.
type xlsxExtent struct {
	rows    int
	columns int
}

// xlsxSheetPrior is a sheet as it stood before a save wrote onto it.
type xlsxSheetPrior struct {
	// extent is how far its values reached, so what the table no longer covers
	// can be removed afterwards.
	extent xlsxExtent
	// values is the sheet as the loader read it, which is what a cell has to be
	// compared against to tell an edit from a value that came out of the file
	// unchanged. It is nil for a workbook being built from nothing.
	values [][]string
}

// xlsxSheetBefore reads a sheet about to be rewritten. Nothing is removed up
// front: writing over a cell keeps the style it carries, where clearing the
// sheet first would take the styles, the merged ranges and the comments with
// it. A sheet the workbook does not have yet has nothing to read, and neither
// does a workbook this package is building from nothing.
//
// The values are read the way the loader read them, dates included, so a cell
// that comes back the same string it went in as is recognizable as untouched.
func xlsxSheetBefore(f *excelize.File, sheetName string, ontoExisting bool) (xlsxSheetPrior, error) {
	if !ontoExisting {
		return xlsxSheetPrior{}, nil
	}
	if _, err := f.GetSheetIndex(sheetName); err != nil {
		return xlsxSheetPrior{}, nil //nolint:nilerr // A sheet that is not there yet; writeXLSXSheet creates it.
	}
	rows, err := f.GetRows(sheetName)
	if err != nil {
		return xlsxSheetPrior{}, fmt.Errorf("failed to read sheet %s: %w", sheetName, err)
	}
	prior := xlsxSheetPrior{
		extent: xlsxExtent{rows: len(rows)},
		values: reader.NormalizeXLSXDates(f, sheetName, rows),
	}
	for _, row := range rows {
		prior.extent.columns = max(prior.extent.columns, len(row))
	}
	return prior, nil
}

// trimXLSXSheet removes what the rewritten table no longer covers: rows below
// its last one and columns to the right of its last. Removing from the far end
// inward keeps the indexes gathered before the write correct as the sheet
// shrinks. A sheet that grew has nothing to trim.
func trimXLSXSheet(f *excelize.File, sheetName string, had xlsxExtent, wrote xlsxExtent) error {
	for row := had.rows; row > wrote.rows; row-- {
		if err := f.RemoveRow(sheetName, row); err != nil {
			return fmt.Errorf("failed to remove row %d of sheet %s: %w", row, sheetName, err)
		}
	}
	for column := had.columns; column > wrote.columns; column-- {
		name, err := excelize.ColumnNumberToName(column)
		if err != nil {
			return fmt.Errorf("failed to name column %d of sheet %s: %w", column, sheetName, err)
		}
		if err := f.RemoveCol(sheetName, name); err != nil {
			return fmt.Errorf("failed to remove column %s of sheet %s: %w", name, sheetName, err)
		}
	}
	return nil
}

// writeXLSXSheet adds one sheet to f and fills it. A cell whose value matches
// what before already holds is left alone.
func writeXLSXSheet(f *excelize.File, sheet xlsxSheet, before [][]string) (xlsxExtent, error) {
	columns, rows, err := sheet.open()
	if err != nil {
		return xlsxExtent{}, err
	}
	if rows != nil {
		defer rows.Close()
	}
	if len(columns) == 0 {
		return xlsxExtent{}, fmt.Errorf("%w: no columns defined", ErrEmptyData)
	}

	if sheet.name != defaultSheetName {
		if _, err := f.NewSheet(sheet.name); err != nil {
			return xlsxExtent{}, fmt.Errorf("failed to create sheet %s: %w", sheet.name, err)
		}
	}

	// Set headers
	for i, col := range columns {
		if unchangedXLSXCell(before, 1, i+1, col) {
			continue
		}
		cell, err := excelize.CoordinatesToCellName(i+1, 1)
		if err != nil {
			return xlsxExtent{}, fmt.Errorf("failed to generate cell name for column %d: %w", i+1, err)
		}
		if err := f.SetCellValue(sheet.name, cell, col); err != nil {
			return xlsxExtent{}, fmt.Errorf("failed to set header %s: %w", col, err)
		}
	}

	// Prepare for scanning rows
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Write data rows
	rowIndex := 2 // Start from row 2 (after header)
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return xlsxExtent{}, fmt.Errorf("failed to scan row: %w", err)
		}

		for i, val := range values {
			// Every cell is written as text, the same string the text formats
			// produce, so one table dumped twice does not disagree with itself.
			cellValue := formatDumpValue(val)
			if unchangedXLSXCell(before, rowIndex, i+1, cellValue) {
				continue
			}
			cell, err := excelize.CoordinatesToCellName(i+1, rowIndex)
			if err != nil {
				return xlsxExtent{}, fmt.Errorf("failed to generate cell name for column %d, row %d: %w", i+1, rowIndex, err)
			}
			if err := f.SetCellValue(sheet.name, cell, cellValue); err != nil {
				return xlsxExtent{}, fmt.Errorf("failed to set cell value at %s: %w", cell, err)
			}
		}
		rowIndex++
	}

	if err := rows.Err(); err != nil {
		return xlsxExtent{}, fmt.Errorf("error reading rows: %w", err)
	}

	return xlsxExtent{rows: rowIndex - 1, columns: len(columns)}, nil
}
