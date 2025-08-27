// Package filesql provides file-based SQL driver implementation.
// It enables reading CSV, TSV, and LTSV files as SQL databases.
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
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/nao1215/filesql/domain/model"
	"github.com/ulikunitz/xz"
)

// Open opens a database connection using the filesql driver.
//
// The filesql driver uses SQLite3 as an in-memory database engine to provide SQL capabilities
// for structured text files. This allows you to query CSV, TSV, LTSV files and their compressed
// variants using standard SQL syntax.
//
// Supported file formats:
//   - CSV files (.csv)
//   - TSV files (.tsv)
//   - LTSV files (.ltsv)
//   - Compressed versions of above (.gz, .bz2, .xz, .zst)
//
// The paths parameter can be a mix of:
//   - Individual files (CSV, TSV, LTSV, or their compressed versions)
//   - Directories (all supported files within will be loaded recursively)
//
// Each file will be loaded as a separate table in the database with the table name
// derived from the filename (without extension).
//
// Important constraints:
//   - INSERT, UPDATE, and DELETE operations are applied only to the in-memory database
//   - Original input files are never modified by these operations
//   - To persist changes, use the DumpDatabase function to export modified data
//
// Example usage:
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

// OpenContext opens a database connection using the filesql driver with context support.
//
// The filesql driver uses SQLite3 as an in-memory database engine to provide SQL capabilities
// for structured text files. This allows you to query CSV, TSV, LTSV files and their compressed
// variants using standard SQL syntax.
//
// Supported file formats:
//   - CSV files (.csv)
//   - TSV files (.tsv)
//   - LTSV files (.ltsv)
//   - Compressed versions of above (.gz, .bz2, .xz, .zst)
//
// The paths parameter can be a mix of:
//   - Individual files (CSV, TSV, LTSV, or their compressed versions)
//   - Directories (all supported files within will be loaded recursively)
//
// Each file will be loaded as a separate table in the database with the table name
// derived from the filename (without extension).
//
// Important constraints:
//   - INSERT, UPDATE, and DELETE operations are applied only to the in-memory database
//   - Original input files are never modified by these operations
//   - To persist changes, use the DumpDatabase function to export modified data
//
// Example usage:
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

// Type aliases for dump options from model package
type (
	// DumpOptions represents options for dumping database
	DumpOptions = model.DumpOptions
	// OutputFormat represents the output file format
	OutputFormat = model.OutputFormat
	// CompressionType represents the compression type
	CompressionType = model.CompressionType
)

// Re-export constants for easier use
const (
	// OutputFormatCSV represents CSV output format
	OutputFormatCSV = model.OutputFormatCSV
	// OutputFormatTSV represents TSV output format
	OutputFormatTSV = model.OutputFormatTSV
	// OutputFormatLTSV represents LTSV output format
	OutputFormatLTSV = model.OutputFormatLTSV

	// CompressionNone represents no compression
	CompressionNone = model.CompressionNone
	// CompressionGZ represents gzip compression
	CompressionGZ = model.CompressionGZ
	// CompressionBZ2 represents bzip2 compression
	CompressionBZ2 = model.CompressionBZ2
	// CompressionXZ represents xz compression
	CompressionXZ = model.CompressionXZ
	// CompressionZSTD represents zstd compression
	CompressionZSTD = model.CompressionZSTD
)

// NewDumpOptions creates new DumpOptions with default values (CSV format, no compression)
var NewDumpOptions = model.NewDumpOptions

// DumpDatabase exports all tables from the database to a directory.
//
// By default, exports as CSV files without compression. You can optionally provide
// DumpOptions to customize the output format and compression.
//
// Note: filesql uses SQLite3 internally as an in-memory database. Any modifications
// made through UPDATE, DELETE, or INSERT operations are not persisted to the original
// files. If you need to persist changes, use DumpDatabase to export the modified data.
//
// Example usage:
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
		var notNull, dfltValue, pk interface{}

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

// writeCSVData writes data in CSV format
func writeCSVData(writer io.Writer, columns []string, rows *sql.Rows) error {
	csvWriter := csv.NewWriter(writer)
	defer csvWriter.Flush()

	// Write header
	if err := csvWriter.Write(columns); err != nil {
		return err
	}

	// Prepare for scanning
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
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

// writeTSVData writes data in TSV format
func writeTSVData(writer io.Writer, columns []string, rows *sql.Rows) error {
	csvWriter := csv.NewWriter(writer)
	csvWriter.Comma = '\t'
	defer csvWriter.Flush()

	// Write header
	if err := csvWriter.Write(columns); err != nil {
		return err
	}

	// Prepare for scanning
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
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

// writeLTSVData writes data in LTSV format
func writeLTSVData(writer io.Writer, columns []string, rows *sql.Rows) error {
	// Prepare for scanning
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
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
