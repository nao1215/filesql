package filesql

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	pqfile "github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
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
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	// Use generic dump functionality for all connections
	return dumpSQLiteDatabase(db, outputDir, options)
}

// dumpSQLiteDatabase implements generic dump functionality for SQLite databases
func dumpSQLiteDatabase(db *sql.DB, outputDir string, options DumpOptions) error {
	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0750); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Get all table names
	tableNames, err := getSQLiteTableNames(db)
	if err != nil {
		return fmt.Errorf("failed to get table names: %w", err)
	}

	if len(tableNames) == 0 {
		return errors.New("no tables found in database")
	}

	// Export each table
	for _, tableName := range tableNames {
		if err := dumpSQLiteTable(db, tableName, outputDir, options); err != nil {
			return fmt.Errorf("failed to export table %s: %w", tableName, err)
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
	columns, err := getSQLiteTableColumns(db, tableName)
	if err != nil {
		return fmt.Errorf("failed to get columns for table %s: %w", tableName, err)
	}

	// Query all data from table
	ctx := context.Background()
	query := fmt.Sprintf("SELECT * FROM `%s`", tableName) //nolint:gosec // Table name is validated and comes from database metadata
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Create output file
	fileName := tableName + options.FileExtension()
	outputPath := filepath.Join(outputDir, fileName)

	return writeSQLiteTableData(outputPath, columns, rows, options)
}

// getSQLiteTableColumns retrieves column names for a specific table
func getSQLiteTableColumns(db *sql.DB, tableName string) ([]string, error) {
	ctx := context.Background()
	query := fmt.Sprintf("PRAGMA table_info(`%s`)", tableName)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, dfltValue, pk any

		if err := rows.Scan(&cid, &name, &dataType, &notNull, &dfltValue, &pk); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

// writeSQLiteTableData writes table data to file with specified format
func writeSQLiteTableData(outputPath string, columns []string, rows *sql.Rows, options DumpOptions) error {
	// Create the file
	file, err := os.Create(outputPath) //nolint:gosec // Output path is constructed from validated directory and table name
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", outputPath, err)
	}
	defer file.Close()

	// Create writer with compression if needed
	writer, closeWriter, err := createCompressedWriter(file, options.Compression)
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}
	defer closeWriter()

	// Write data based on format
	switch options.Format {
	case OutputFormatCSV:
		return writeCSVData(writer, columns, rows)
	case OutputFormatTSV:
		return writeTSVData(writer, columns, rows)
	case OutputFormatLTSV:
		return writeLTSVData(writer, columns, rows)
	case OutputFormatParquet:
		return writeParquetTableData(outputPath, columns, rows, options.Compression)
	default:
		return fmt.Errorf("unsupported output format: %v", options.Format)
	}
}

// createCompressedWriter creates an appropriate writer based on compression type
func createCompressedWriter(file *os.File, compression CompressionType) (io.Writer, func() error, error) {
	switch compression {
	case CompressionNone:
		return file, func() error { return nil }, nil
	case CompressionGZ:
		gzWriter := gzip.NewWriter(file)
		return gzWriter, gzWriter.Close, nil
	case CompressionBZ2:
		// bzip2 doesn't have a writer in the standard library
		return nil, nil, errors.New("bzip2 compression is not supported for writing")
	case CompressionXZ:
		xzWriter, err := xz.NewWriter(file)
		if err != nil {
			return nil, nil, err
		}
		return xzWriter, xzWriter.Close, nil
	case CompressionZSTD:
		zstdWriter, err := zstd.NewWriter(file)
		if err != nil {
			return nil, nil, err
		}
		return zstdWriter, zstdWriter.Close, nil
	default:
		return nil, nil, fmt.Errorf("unsupported compression type: %v", compression)
	}
}

// writeDelimitedData writes data in CSV or TSV format based on delimiter
func writeDelimitedData(writer io.Writer, columns []string, rows *sql.Rows, delimiter rune) error {
	csvWriter := csv.NewWriter(writer)
	if delimiter != CSVDelimiter {
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
			if value == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprintf("%v", value)
			}
		}

		if err := csvWriter.Write(record); err != nil {
			return err
		}
	}

	return rows.Err()
}

// writeCSVData writes data in CSV format
func writeCSVData(writer io.Writer, columns []string, rows *sql.Rows) error {
	return writeDelimitedData(writer, columns, rows, CSVDelimiter)
}

// writeTSVData writes data in TSV format
func writeTSVData(writer io.Writer, columns []string, rows *sql.Rows) error {
	return writeDelimitedData(writer, columns, rows, TSVDelimiter)
}

// writeLTSVData writes data in LTSV format
func writeLTSVData(writer io.Writer, columns []string, rows *sql.Rows) error {
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
		var parts []string
		for i, col := range columns {
			value := ""
			if values[i] != nil {
				value = fmt.Sprintf("%v", values[i])
			}
			parts = append(parts, fmt.Sprintf("%s:%s", col, value))
		}

		line := strings.Join(parts, "\t") + "\n"
		if _, err := writer.Write([]byte(line)); err != nil {
			return err
		}
	}

	return rows.Err()
}

// parseParquet parses Parquet file with compression support
func (f *file) parseParquet() (*table, error) {
	// For Parquet files, we need direct file access
	// Compressed Parquet files are not common, but if needed, we'd decompress first
	if f.isCompressed() {
		// For compressed Parquet files, decompress to temp file first
		return f.parseCompressedParquet()
	}

	// Open the file directly
	pqFile, err := os.Open(f.path)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer pqFile.Close()

	// Get file size (not needed for current implementation but kept for completeness)
	_, err = pqFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file size: %w", err)
	}

	// Create parquet file reader
	pqReader, err := pqfile.NewParquetReader(pqFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pqReader.Close()

	// Create arrow file reader
	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	// Read all record batches using the table reader approach
	ctx := context.Background()
	table, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}
	defer table.Release()

	var allRecords []record
	var headerSlice header

	if table.NumRows() == 0 {
		return nil, fmt.Errorf("no records found in parquet file: %s", f.path)
	}

	// Initialize header from table schema
	schema := table.Schema()
	headerSlice = make(header, schema.NumFields())
	for i, field := range schema.Fields() {
		headerSlice[i] = field.Name
	}

	// Read data by converting table to record batches
	tableReader := array.NewTableReader(table, 0) // Read all rows at once
	defer tableReader.Release()

	for tableReader.Next() {
		batch := tableReader.Record()

		// Convert each row in the batch
		numRows := batch.NumRows()
		for i := range numRows {
			row := make(record, batch.NumCols())
			for j, col := range batch.Columns() {
				value := extractValueFromArrowArray(col, i)
				row[j] = value
			}
			allRecords = append(allRecords, row)
		}
	}

	if err := tableReader.Err(); err != nil {
		return nil, fmt.Errorf("error reading table records: %w", err)
	}

	if len(allRecords) == 0 {
		return nil, fmt.Errorf("no records found in parquet file: %s", f.path)
	}

	tableName := tableFromFilePath(f.path)
	return newTable(tableName, headerSlice, allRecords), nil
}

// parseCompressedParquet handles compressed Parquet files
func (f *file) parseCompressedParquet() (*table, error) {
	reader, closer, err := f.openReader()
	if err != nil {
		return nil, fmt.Errorf("failed to open compressed file: %w", err)
	}
	defer closer()

	// Read all data into memory for compressed files
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read compressed parquet data: %w", err)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("empty parquet file: %s", f.path)
	}

	// Create a bytes reader for the parquet data
	bytesReader := &bytesReaderAt{data: data}

	// Create parquet file reader from bytes
	pqReader, err := pqfile.NewParquetReader(bytesReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader from bytes: %w", err)
	}
	defer pqReader.Close()

	// Create arrow file reader
	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	// Read all record batches using the table reader approach
	ctx := context.Background()
	table, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}
	defer table.Release()

	var allRecords []record
	var headerSlice header

	if table.NumRows() == 0 {
		return nil, fmt.Errorf("no records found in compressed parquet file: %s", f.path)
	}

	// Initialize header from table schema
	schema := table.Schema()
	headerSlice = make(header, schema.NumFields())
	for i, field := range schema.Fields() {
		headerSlice[i] = field.Name
	}

	// Read data by converting table to record batches
	tableReader := array.NewTableReader(table, 0) // Read all rows at once
	defer tableReader.Release()

	for tableReader.Next() {
		batch := tableReader.Record()

		// Convert each row in the batch
		numRows := batch.NumRows()
		for i := range numRows {
			row := make(record, batch.NumCols())
			for j, col := range batch.Columns() {
				value := extractValueFromArrowArray(col, i)
				row[j] = value
			}
			allRecords = append(allRecords, row)
		}
	}

	if err := tableReader.Err(); err != nil {
		return nil, fmt.Errorf("error reading table records: %w", err)
	}

	if len(allRecords) == 0 {
		return nil, fmt.Errorf("no records found in parquet file: %s", f.path)
	}

	tableName := tableFromFilePath(f.path)
	return newTable(tableName, headerSlice, allRecords), nil
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
		return 0, errors.New("invalid whence value")
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
func writeParquetTableData(outputPath string, columns []string, rows *sql.Rows, compression CompressionType) error {
	if len(columns) == 0 {
		return errors.New("no columns defined")
	}

	// For Parquet format, compression is handled at the file level, not stream level
	// We ignore the compression parameter for now as Parquet has its own compression
	if compression != CompressionNone {
		return errors.New("external compression not supported for Parquet format - use Parquet's built-in compression instead")
	}

	// Read all rows into memory first
	var allRows [][]string

	// Prepare for scanning
	values := make([]any, len(columns))
	scanArgs := make([]any, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		row := make([]string, len(columns))
		for i, value := range values {
			if value == nil {
				row[i] = ""
			} else {
				row[i] = fmt.Sprintf("%v", value)
			}
		}
		allRows = append(allRows, row)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	return writeParquetData(outputPath, columns, allRows)
}

// writeParquetData writes data to Parquet format
func writeParquetData(outputPath string, columns []string, rows [][]string) error {
	if len(rows) == 0 {
		return errors.New("no data to write")
	}
	if len(columns) == 0 {
		return errors.New("no columns defined")
	}

	// Create output file
	file, err := os.Create(outputPath) //nolint:gosec
	if err != nil {
		return fmt.Errorf("failed to create parquet file: %w", err)
	}
	defer file.Close()

	// Create Arrow schema - for simplicity, treat all columns as strings
	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		fields[i] = arrow.Field{
			Name: col,
			Type: arrow.BinaryTypes.String,
		}
	}
	schema := arrow.NewSchema(fields, nil)

	// Create Arrow record batch builder
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// Add data to builders
	for _, row := range rows {
		for i, value := range row {
			if i < len(columns) {
				strBuilder, ok := builder.Field(i).(*array.StringBuilder)
				if !ok {
					return fmt.Errorf("failed to cast field %d to StringBuilder", i)
				}
				strBuilder.Append(value)
			}
		}
	}

	// Build record
	record := builder.NewRecord()
	defer record.Release()

	// Create Parquet writer
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	writer, err := pqarrow.NewFileWriter(schema, file, nil, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer writer.Close()

	// Write record to Parquet file
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record to parquet: %w", err)
	}

	// Flush and close writer explicitly
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}

	return nil
}
