package filesql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/nao1215/filesql/parser"
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
		// 'g' with -1 digits is the shortest form that reads back as the same
		// float64, which is what %v produced for the values it did get right.
		return strconv.FormatFloat(v, 'g', -1, 64)
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

// bytesReaderAt implements io.ReaderAt for byte slices
type bytesReaderAt struct {
	data []byte
}

func (b *bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(b.data)) {
		return 0, io.EOF
	}

	n := copy(p, b.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Size returns the size of the data
func (b *bytesReaderAt) Size() int64 {
	return int64(len(b.data))
}

// Seek implements io.Seeker interface (required for ReaderAtSeeker)
func (b *bytesReaderAt) Seek(offset int64, whence int) (int64, error) {
	// bytesReaderAt doesn't maintain position state, so Seek is not meaningful
	// However, we implement it to satisfy the ReaderAtSeeker interface
	switch whence {
	case io.SeekStart:
		return offset, nil
	case io.SeekCurrent:
		return 0, nil // We don't track current position
	case io.SeekEnd:
		return int64(len(b.data)) + offset, nil
	default:
		return 0, fmt.Errorf("%w: invalid whence value", ErrInvalidData)
	}
}

// Read implements io.Reader interface (required for ReaderAtSeeker)
func (b *bytesReaderAt) Read(p []byte) (int, error) {
	// For ReaderAtSeeker, we implement a basic Read that starts from beginning
	return b.ReadAt(p, 0)
}

// arrowCellIsNull reports whether a cell has no value SQLite can store: a
// Parquet null, or a NaN, which SQLite has no representation for at all — a
// computed NaN is NULL there, so NULL is what the value already means in the
// destination. Left as text it would sit in a column declared REAL as the word
// "NaN".
func arrowCellIsNull(arr arrow.Array, index int64) bool {
	if arr.IsNull(int(index)) {
		return true
	}
	switch a := arr.(type) {
	case *array.Float32:
		return math.IsNaN(float64(a.Value(int(index))))
	case *array.Float64:
		return math.IsNaN(a.Value(int(index)))
	default:
		return false
	}
}

// sqliteFloatText renders a float at bitSize so SQLite's REAL affinity converts
// it back to the same number, which "%g" does not for the three values that have
// no decimal spelling.
//
// The column is declared REAL from the Parquet schema, and SQLite applies that
// affinity to the text an import binds: "+Inf" is not a number to it, so the
// cell was stored as TEXT inside a REAL column and typeof() answered "text" for
// a value the file held as a double. "9e999" overflows to infinity when SQLite
// parses it, which is the only spelling that survives.
//
// NaN renders as empty, the same as a null, because SQLite has no NaN at all: a
// computed one becomes NULL there, so NULL is what the value already means in
// the destination. Keeping the word would leave the same TEXT-in-a-REAL-column
// mismatch this exists to remove.
func sqliteFloatText(f float64, bitSize int) string {
	// A literal SQLite overflows to an infinity while parsing it. There is no
	// spelling of the value itself that its REAL affinity accepts.
	const infinityLiteral = "9e999"
	switch {
	case math.IsInf(f, 1):
		return infinityLiteral
	case math.IsInf(f, -1):
		return "-" + infinityLiteral
	case math.IsNaN(f):
		return ""
	}
	return strconv.FormatFloat(f, 'g', -1, bitSize)
}

// extractValueFromArrowArray extracts a value from an Arrow array at the given index
func extractValueFromArrowArray(arr arrow.Array, index int64) string {
	if arr.IsNull(int(index)) {
		return ""
	}

	switch a := arr.(type) {
	case *array.Boolean:
		if a.Value(int(index)) {
			return "1"
		}
		return "0"

	case *array.Int8:
		return strconv.Itoa(int(a.Value(int(index))))
	case *array.Int16:
		return strconv.Itoa(int(a.Value(int(index))))
	case *array.Int32:
		return strconv.Itoa(int(a.Value(int(index))))
	case *array.Int64:
		return strconv.FormatInt(a.Value(int(index)), 10)

	case *array.Uint8:
		return strconv.FormatUint(uint64(a.Value(int(index))), 10)
	case *array.Uint16:
		return strconv.FormatUint(uint64(a.Value(int(index))), 10)
	case *array.Uint32:
		return strconv.FormatUint(uint64(a.Value(int(index))), 10)
	case *array.Uint64:
		return strconv.FormatUint(a.Value(int(index)), 10)

	case *array.Float32:
		return sqliteFloatText(float64(a.Value(int(index))), 32)
	case *array.Float64:
		return sqliteFloatText(a.Value(int(index)), 64)

	case *array.String:
		return a.Value(int(index))
	case *array.Binary:
		return string(a.Value(int(index)))

	case *array.Date32:
		// Convert days since epoch to string representation
		days := a.Value(int(index))
		return fmt.Sprintf("%d", days)
	case *array.Date64:
		// Convert milliseconds since epoch to string representation
		millis := a.Value(int(index))
		return fmt.Sprintf("%d", millis)

	case *array.Timestamp:
		// Convert timestamp to string
		ts := a.Value(int(index))
		return fmt.Sprintf("%d", ts)

	default:
		// For unsupported types, try to convert to string representation
		return fmt.Sprintf("%v", arr.GetOneForMarshal(int(index)))
	}
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
	// their rendered text because Parquet is a typed format: which Arrow type each
	// column is written as is decided from the values themselves, below.
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

// parquetKind is the Arrow type one column is written as.
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
		if col >= len(row) || row[col] == nil {
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

// arrowTypeFor is the Arrow type a parquetKind is written as.
func arrowTypeFor(kind parquetKind) arrow.DataType {
	switch kind {
	case parquetInt:
		return arrow.PrimitiveTypes.Int64
	case parquetFloat:
		return arrow.PrimitiveTypes.Float64
	default:
		return arrow.BinaryTypes.String
	}
}

// appendParquetValue appends one cell to the builder for its column. A nil value
// is the SQL NULL the row carried; anything that does not match the column's
// chosen kind cannot occur, because parquetColumnKind only chooses a numeric
// kind when every value in the column is numeric.
func appendParquetValue(b array.Builder, kind parquetKind, value any) error {
	if value == nil {
		b.AppendNull()
		return nil
	}
	switch kind {
	case parquetInt:
		ib, ok := b.(*array.Int64Builder)
		if !ok {
			return fmt.Errorf("%w: expected an Int64Builder for an integer column", ErrInvalidData)
		}
		n, ok := value.(int64)
		if !ok {
			return fmt.Errorf("%w: %T in an integer column", ErrInvalidData, value)
		}
		ib.Append(n)
	case parquetFloat:
		fb, ok := b.(*array.Float64Builder)
		if !ok {
			return fmt.Errorf("%w: expected a Float64Builder for a real column", ErrInvalidData)
		}
		switch v := value.(type) {
		case float64:
			fb.Append(v)
		case int64:
			fb.Append(float64(v))
		default:
			return fmt.Errorf("%w: %T in a real column", ErrInvalidData, value)
		}
	default:
		sb, ok := b.(*array.StringBuilder)
		if !ok {
			return fmt.Errorf("%w: expected a StringBuilder for a text column", ErrInvalidData)
		}
		sb.Append(formatDumpValue(value))
	}
	return nil
}

// writeOnly exposes only Write, so a library that would close or seek the
// destination it is given cannot reach past what it was handed.
type writeOnly struct {
	w io.Writer
}

func (o writeOnly) Write(p []byte) (int, error) {
	return o.w.Write(p)
}

// writeParquetData writes data to Parquet format. A nil cell in rows is stored
// as a Parquet null, so a SQL NULL survives the round-trip instead of collapsing
// into an empty string. declared holds each column's declared SQL type, which
// decides a column that has no rows to speak for it.
//
// Each column is written as the Arrow type its values call for rather than as
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
	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		declaredType := ""
		if i < len(declared) {
			declaredType = declared[i]
		}
		kinds[i] = parquetColumnKind(rows, i, declaredType)
		fields[i] = arrow.Field{
			Name:     col,
			Type:     arrowTypeFor(kinds[i]),
			Nullable: true, // allow a stored null so SQL NULL survives the round-trip
		}
	}
	schema := arrow.NewSchema(fields, nil)

	// Create Arrow record batch builder
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// Add data to builders
	for _, row := range rows {
		for i := range columns {
			var value any
			if i < len(row) {
				value = row[i]
			}
			if err := appendParquetValue(builder.Field(i), kinds[i], value); err != nil {
				return err
			}
		}
	}

	// Build record
	record := builder.NewRecord()
	defer record.Release()

	// Create Parquet writer
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	// writeOnly hides the destination's Close from the Parquet writer, which
	// closes its sink when it can. The caller owns the file and closes it after
	// checking that error; a Parquet-side close leaves it with "file already
	// closed" and turns a good dump into a failure.
	writer, err := pqarrow.NewFileWriter(schema, writeOnly{w}, nil, arrowProps)
	if err != nil {
		return fmt.Errorf("%w: failed to create parquet writer: %w", ErrIOOperation, err)
	}
	// An empty record has no row group to write; Close still writes the schema and
	// the footer, which is the whole file for a table with no rows.
	if record.NumRows() > 0 {
		if err := writer.Write(record); err != nil {
			_ = writer.Close() // Release the writer; the write error is the one to report
			return fmt.Errorf("%w: failed to write record to parquet: %w", ErrIOOperation, err)
		}
	}

	// Close writes the footer, so this is where an incomplete file shows up.
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
	if len(sheets) == 0 {
		return fmt.Errorf("%w: no sheets to write", ErrEmptyData)
	}

	f := excelize.NewFile()
	defer func() {
		_ = f.Close() // Ignore close error
	}()

	for _, sheet := range sheets {
		if err := writeXLSXSheet(f, sheet); err != nil {
			return err
		}
	}

	// excelize starts a workbook with a default sheet. It is only ours to remove
	// once a sheet of our own exists, and not at all if a sheet reused its name.
	if _, err := f.GetSheetIndex(defaultSheetName); err == nil {
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

// writeXLSXSheet adds one sheet to f and fills it.
func writeXLSXSheet(f *excelize.File, sheet xlsxSheet) error {
	columns, rows, err := sheet.open()
	if err != nil {
		return err
	}
	if rows != nil {
		defer rows.Close()
	}
	if len(columns) == 0 {
		return fmt.Errorf("%w: no columns defined", ErrEmptyData)
	}

	if sheet.name != defaultSheetName {
		if _, err := f.NewSheet(sheet.name); err != nil {
			return fmt.Errorf("failed to create sheet %s: %w", sheet.name, err)
		}
	}

	// Set headers
	for i, col := range columns {
		cell, err := excelize.CoordinatesToCellName(i+1, 1)
		if err != nil {
			return fmt.Errorf("failed to generate cell name for column %d: %w", i+1, err)
		}
		if err := f.SetCellValue(sheet.name, cell, col); err != nil {
			return fmt.Errorf("failed to set header %s: %w", col, err)
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
			return fmt.Errorf("failed to scan row: %w", err)
		}

		for i, val := range values {
			cell, err := excelize.CoordinatesToCellName(i+1, rowIndex)
			if err != nil {
				return fmt.Errorf("failed to generate cell name for column %d, row %d: %w", i+1, rowIndex, err)
			}
			// Every cell is written as text, the same string the text formats
			// produce, so one table dumped twice does not disagree with itself.
			cellValue := formatDumpValue(val)

			if err := f.SetCellValue(sheet.name, cell, cellValue); err != nil {
				return fmt.Errorf("failed to set cell value at %s: %w", cell, err)
			}
		}
		rowIndex++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading rows: %w", err)
	}

	return nil
}
