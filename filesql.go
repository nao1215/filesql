// Package filesql provides file-based SQL driver implementation.
// It enables reading CSV, TSV, and LTSV files as SQL databases.
package filesql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/nao1215/filesql/domain/model"
	filesqldriver "github.com/nao1215/filesql/driver"
)

const (
	// DriverName is the name for the filesql driver
	DriverName = "filesql"
)

// Register registers the filesql driver with database/sql
func Register() {
	sql.Register(DriverName, filesqldriver.NewDriver())
}

func init() {
	// Auto-register the driver on import
	Register()
}

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

	// Use Raw to get the underlying driver connection
	return conn.Raw(func(driverConn interface{}) error {
		if filesqlConn, ok := driverConn.(*filesqldriver.Connection); ok {
			return filesqlConn.DumpWithOptions(outputDir, options)
		}
		return filesqldriver.ErrNotFilesqlConnection
	})
}
