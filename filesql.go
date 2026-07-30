package filesql

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
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
// untouched. For an in-memory database, pin the pool to a single connection
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
	// Use default options if none provided
	options := NewDumpOptions()
	if len(opts) > 0 {
		options = opts[0]
	}

	// Get the underlying connection
	conn, err := db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("%w: failed to get connection: %s", ErrDatabaseOperation, err.Error())
	}
	defer conn.Close()

	// Use generic dump functionality for all connections
	return dumpSQLiteDatabase(db, outputDir, options)
}

// dumpSQLiteDatabase implements generic dump functionality for SQLite databases
func dumpSQLiteDatabase(db *sql.DB, outputDir string, options DumpOptions) error {
	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0750); err != nil {
		return fmt.Errorf("%w: failed to create output directory: %s", ErrIOOperation, err.Error())
	}

	// Get all table names
	tableNames, err := getSQLiteTableNames(db)
	if err != nil {
		return fmt.Errorf("%w: failed to get table names: %s", ErrDatabaseOperation, err.Error())
	}

	if len(tableNames) == 0 {
		return ErrNoTables
	}

	// Detect ACH tables and group them by base name
	achBaseNames := make(map[string]bool)
	registryBackedTables := make(map[string]bool)
	for _, tableName := range tableNames {
		if baseName, isACH := IsACHBaseTableName(tableName); isACH {
			// Only treat as ACH table if we have a registered TableSet
			if getACHTableSet(baseName) != nil {
				achBaseNames[baseName] = true
				registryBackedTables[tableName] = true
			}
		}
	}

	// Detect Fedwire tables and group them by base name
	wireBaseNames := make(map[string]bool)
	for _, tableName := range tableNames {
		if baseName, isWire := IsWireBaseTableName(tableName); isWire {
			// Only treat as wire table if we have a registered TableSet
			if getWireTableSet(baseName) != nil {
				wireBaseNames[baseName] = true
				registryBackedTables[tableName] = true
			}
		}
	}

	ctx := context.Background()

	// Export ACH files
	for baseName := range achBaseNames {
		outputPath, err := dumpFilePath(outputDir, baseName, extACH)
		if err != nil {
			return err
		}
		if err := DumpACH(ctx, db, baseName, outputPath); err != nil {
			return fmt.Errorf("%w: failed to export ACH file %s: %w", ErrACH, baseName, err)
		}
	}

	// Export Fedwire files
	for baseName := range wireBaseNames {
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
		if registryBackedTables[tableName] {
			continue
		}
		if err := dumpSQLiteTable(db, tableName, outputDir, options); err != nil {
			return fmt.Errorf("%w: failed to export table %s: %w", ErrIOOperation, tableName, err)
		}
	}

	return nil
}

// getSQLiteTableNames retrieves all user-defined table names from SQLite database
func getSQLiteTableNames(db *sql.DB) ([]string, error) {
	ctx := context.Background()
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
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
		return fmt.Errorf("%w: failed to get columns for table %s: %s", ErrDatabaseOperation, tableName, err.Error())
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
		return fmt.Errorf("%w: failed to create writer: %s", ErrCompression, err.Error())
	}
	defer func() {
		if closeErr := closeWriter(); closeErr != nil && err == nil {
			err = fmt.Errorf("%w: failed to finish writing %s: %s", ErrCompression, tableName, closeErr.Error())
		}
	}()

	// Write data based on format
	switch options.Format {
	case OutputFormatCSV:
		return writeCSVData(writer, columns, rows)
	case OutputFormatTSV:
		return writeTSVData(writer, columns, rows)
	case OutputFormatLTSV:
		return writeLTSVData(writer, columns, rows)
	case OutputFormatParquet:
		return writeParquetTableData(writer, columns, rows)
	case OutputFormatXLSX:
		return writeXLSXTableData(writer, tableName, columns, rows)
	default:
		return fmt.Errorf("%w: unsupported output format: %v", ErrUnsupportedFormat, options.Format)
	}
}

// createCompressedWriter creates an appropriate writer based on compression type
func createCompressedWriter(w io.Writer, compression CompressionType) (io.Writer, func() error, error) {
	handler := NewCompressionHandler(compression)
	return handler.CreateWriter(w)
}

// writeDelimitedData writes data in CSV or TSV format based on delimiter
func writeDelimitedData(writer io.Writer, columns []string, rows *sql.Rows, delimiter rune) error {
	csvWriter := csv.NewWriter(writer)
	if delimiter != csvDelimiter {
		csvWriter.Comma = delimiter
	}
	defer csvWriter.Flush()

	// Write header
	if err := csvWriter.Write(columns); err != nil {
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

		if err := csvWriter.Write(record); err != nil {
			return err
		}
	}

	return rows.Err()
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
func writeCSVData(writer io.Writer, columns []string, rows *sql.Rows) error {
	return writeDelimitedData(writer, columns, rows, csvDelimiter)
}

// writeTSVData writes data in TSV format
func writeTSVData(writer io.Writer, columns []string, rows *sql.Rows) error {
	return writeDelimitedData(writer, columns, rows, tsvDelimiter)
}

// writeLTSVData writes data in LTSV format
func writeLTSVData(writer io.Writer, columns []string, rows *sql.Rows) error {
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

		line := strings.Join(parts, "\t") + "\n"
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
		return fmt.Sprintf("%g", a.Value(int(index)))
	case *array.Float64:
		return fmt.Sprintf("%g", a.Value(int(index)))

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

	// Read all rows into memory first. nulls runs parallel to rows and marks the
	// cells that were SQL NULL, so the Parquet writer can store a real null rather
	// than collapsing it into an empty string.
	var allRows [][]string
	var allNulls [][]bool

	// Prepare for scanning
	values := make([]any, len(columns))
	scanArgs := make([]any, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("%w: failed to scan row: %s", ErrDatabaseOperation, err.Error())
		}

		row := make([]string, len(columns))
		nullRow := make([]bool, len(columns))
		for i, value := range values {
			if value == nil {
				nullRow[i] = true
				continue
			}
			row[i] = formatDumpValue(value)
		}
		allRows = append(allRows, row)
		allNulls = append(allNulls, nullRow)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: error iterating rows: %s", ErrDatabaseOperation, err.Error())
	}

	return writeParquetData(w, columns, allRows, allNulls)
}

// writeOnly exposes only Write, so a library that would close or seek the
// destination it is given cannot reach past what it was handed.
type writeOnly struct {
	w io.Writer
}

func (o writeOnly) Write(p []byte) (int, error) {
	return o.w.Write(p)
}

// writeParquetData writes data to Parquet format. nulls, when non-nil, marks the
// cells to store as a Parquet null; rows[r][c] is ignored for a cell marked null.
func writeParquetData(w io.Writer, columns []string, rows [][]string, nulls [][]bool) error {
	if len(rows) == 0 {
		return fmt.Errorf("%w: no data to write", ErrEmptyData)
	}
	if len(columns) == 0 {
		return fmt.Errorf("%w: no columns defined", ErrEmptyData)
	}

	// Create Arrow schema - for simplicity, treat all columns as strings
	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		fields[i] = arrow.Field{
			Name:     col,
			Type:     arrow.BinaryTypes.String,
			Nullable: true, // allow a stored null so SQL NULL survives the round-trip
		}
	}
	schema := arrow.NewSchema(fields, nil)

	// Create Arrow record batch builder
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// Add data to builders
	for r, row := range rows {
		for i, value := range row {
			if i < len(columns) {
				strBuilder, ok := builder.Field(i).(*array.StringBuilder)
				if !ok {
					return fmt.Errorf("%w: failed to cast field %d to StringBuilder", ErrInvalidData, i)
				}
				if nulls != nil && r < len(nulls) && i < len(nulls[r]) && nulls[r][i] {
					strBuilder.AppendNull()
				} else {
					strBuilder.Append(value)
				}
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
		return fmt.Errorf("%w: failed to create parquet writer: %s", ErrIOOperation, err.Error())
	}
	if err := writer.Write(record); err != nil {
		_ = writer.Close() // Release the writer; the write error is the one to report
		return fmt.Errorf("%w: failed to write record to parquet: %s", ErrIOOperation, err.Error())
	}

	// Close writes the footer, so this is where an incomplete file shows up.
	if err := writer.Close(); err != nil {
		return fmt.Errorf("%w: failed to close parquet writer: %s", ErrIOOperation, err.Error())
	}

	return nil
}

// writeXLSXTableData writes SQLite table data to Excel XLSX format
func writeXLSXTableData(w io.Writer, tableName string, columns []string, rows *sql.Rows) error {
	if len(columns) == 0 {
		return fmt.Errorf("%w: no columns defined", ErrEmptyData)
	}

	// Create new Excel file
	f := excelize.NewFile()
	defer func() {
		_ = f.Close() // Ignore close error
	}()

	// The sheet name is what a reader turns back into a table name, so it comes
	// from the table this dump is of.
	sheetName := tableName
	if sheetName == "" {
		sheetName = defaultSheetName
	}

	// Create new sheet (replace default Sheet1)
	if sheetName != defaultSheetName {
		_, err := f.NewSheet(sheetName)
		if err != nil {
			return fmt.Errorf("failed to create sheet %s: %w", sheetName, err)
		}
		// Delete default sheet
		if err := f.DeleteSheet(defaultSheetName); err != nil {
			return fmt.Errorf("failed to delete default sheet: %w", err)
		}
	}

	// Set headers
	for i, col := range columns {
		cell, err := excelize.CoordinatesToCellName(i+1, 1)
		if err != nil {
			return fmt.Errorf("failed to generate cell name for column %d: %w", i+1, err)
		}
		if err := f.SetCellValue(sheetName, cell, col); err != nil {
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

			if err := f.SetCellValue(sheetName, cell, cellValue); err != nil {
				return fmt.Errorf("failed to set cell value at %s: %w", cell, err)
			}
		}
		rowIndex++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading rows: %w", err)
	}

	// Why Write and not SaveAs: SaveAs picks the container format from the file
	// extension, and the caller stages the write, so the only name available here
	// carries a temporary suffix that Excel rejects. Any compression the caller
	// asked for is already wrapped around w.
	if err := f.Write(w); err != nil {
		return fmt.Errorf("%w: failed to write Excel file: %s", ErrIOOperation, err.Error())
	}

	return nil
}
