package filesql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"modernc.org/sqlite"
)

// directConnector implements driver.Connector to wrap an existing driver.Conn
type directConnector struct {
	conn driver.Conn
}

func (dc *directConnector) Connect(_ context.Context) (driver.Conn, error) {
	return dc.conn, nil
}

func (dc *directConnector) Driver() driver.Driver {
	return &sqlite.Driver{}
}

// OutputFormat represents the output file format
type OutputFormat int

const (
	// OutputFormatCSV represents CSV output format
	OutputFormatCSV OutputFormat = iota
	// OutputFormatTSV represents TSV output format
	OutputFormatTSV
	// OutputFormatLTSV represents LTSV output format
	OutputFormatLTSV
	// OutputFormatParquet represents Parquet output format
	OutputFormatParquet
	// OutputFormatXLSX represents Excel XLSX output format
	OutputFormatXLSX
)

// String returns the string representation of OutputFormat
func (f OutputFormat) String() string {
	switch f {
	case OutputFormatCSV:
		return "csv"
	case OutputFormatTSV:
		return "tsv"
	case OutputFormatLTSV:
		return "ltsv"
	case OutputFormatParquet:
		return "parquet"
	case OutputFormatXLSX:
		return "xlsx"
	default:
		return "csv"
	}
}

// Extension returns the file extension for the format
func (f OutputFormat) Extension() string {
	switch f {
	case OutputFormatCSV:
		return ".csv"
	case OutputFormatTSV:
		return ".tsv"
	case OutputFormatLTSV:
		return ".ltsv"
	case OutputFormatParquet:
		return ".parquet"
	case OutputFormatXLSX:
		return ".xlsx"
	default:
		return ".csv"
	}
}

// CompressionType represents the compression type
type CompressionType int

const (
	// CompressionNone represents no compression
	CompressionNone CompressionType = iota
	// CompressionGZ represents gzip compression
	CompressionGZ
	// CompressionBZ2 represents bzip2 compression
	CompressionBZ2
	// CompressionXZ represents xz compression
	CompressionXZ
	// CompressionZSTD represents zstd compression
	CompressionZSTD
)

// string constants for compression types
const (
	compressionGZStr   = "gz"
	compressionBZ2Str  = "bz2"
	compressionXZStr   = "xz"
	compressionZSTDStr = "zstd"
)

// String returns the string representation of CompressionType
func (c CompressionType) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionGZ:
		return compressionGZStr
	case CompressionBZ2:
		return compressionBZ2Str
	case CompressionXZ:
		return compressionXZStr
	case CompressionZSTD:
		return compressionZSTDStr
	default:
		return "none"
	}
}

// Extension returns the file extension for the compression type
func (c CompressionType) Extension() string {
	switch c {
	case CompressionNone:
		return ""
	case CompressionGZ:
		return ".gz"
	case CompressionBZ2:
		return ".bz2"
	case CompressionXZ:
		return ".xz"
	case CompressionZSTD:
		return ".zst"
	default:
		return ""
	}
}

// DumpOptions configures how database tables are exported to files.
//
// Example:
//
//	options := NewDumpOptions().
//		WithFormat(OutputFormatTSV).
//		WithCompression(CompressionGZ)
//
//	err := DumpDatabase(db, "./output", options)
type DumpOptions struct {
	// Format specifies the output file format
	Format OutputFormat
	// Compression specifies the compression type
	Compression CompressionType
}

// NewDumpOptions creates default export options (CSV, no compression).
//
// Modify with:
//   - WithFormat(): Change file format (CSV, TSV, LTSV)
//   - WithCompression(): Add compression (GZ, BZ2, XZ, ZSTD)
func NewDumpOptions() DumpOptions {
	return DumpOptions{
		Format:      OutputFormatCSV,
		Compression: CompressionNone,
	}
}

// WithFormat sets the output file format.
//
// Options:
//   - OutputFormatCSV: Comma-separated values
//   - OutputFormatTSV: Tab-separated values
//   - OutputFormatLTSV: Labeled tab-separated values
//   - OutputFormatParquet: Apache Parquet columnar format
func (o DumpOptions) WithFormat(format OutputFormat) DumpOptions {
	o.Format = format
	return o
}

// WithCompression adds compression to output files.
//
// Options:
//   - CompressionNone: No compression (default)
//   - CompressionGZ: Gzip compression (.gz)
//   - CompressionBZ2: Bzip2 compression (.bz2)
//   - CompressionXZ: XZ compression (.xz)
//   - CompressionZSTD: Zstandard compression (.zst)
func (o DumpOptions) WithCompression(compression CompressionType) DumpOptions {
	o.Compression = compression
	return o
}

// FileExtension returns the complete file extension including compression
func (o DumpOptions) FileExtension() string {
	baseExt := o.Format.Extension()
	compExt := o.Compression.Extension()
	return baseExt + compExt
}

// autoSaveTiming defines when auto-save should be triggered
type autoSaveTiming int

const (
	// autoSaveOnClose saves data when db.Close() is called (default)
	autoSaveOnClose autoSaveTiming = iota
	// autoSaveOnCommit saves data when transaction is committed
	autoSaveOnCommit
)

// autoSaveConfig holds configuration for automatic saving
type autoSaveConfig struct {
	// enabled indicates whether auto-save is enabled
	enabled bool
	// timing specifies when to save (on close or on commit)
	timing autoSaveTiming
	// outputDir is the directory where files will be saved (overwrites original files)
	outputDir string
	// options contains dump options for formatting
	options DumpOptions
}

// autoSaveConnector implements driver.Connector interface with auto-save support
type autoSaveConnector struct {
	sqliteConn     driver.Conn
	autoSaveConfig *autoSaveConfig
	originalPaths  []string
}

// Connect implements driver.Connector interface
func (c *autoSaveConnector) Connect(_ context.Context) (driver.Conn, error) {
	return &autoSaveConnection{
		conn:           c.sqliteConn,
		autoSaveConfig: c.autoSaveConfig,
		originalPaths:  c.originalPaths,
	}, nil
}

// Driver implements driver.Connector interface
func (c *autoSaveConnector) Driver() driver.Driver {
	return &sqlite.Driver{}
}

// autoSaveConnection wraps a database connection with auto-save functionality
type autoSaveConnection struct {
	conn           driver.Conn
	autoSaveConfig *autoSaveConfig
	originalPaths  []string
}

// Close implements driver.Conn interface with auto-save on close
func (c *autoSaveConnection) Close() error {
	// Perform auto-save if configured for close timing
	if c.autoSaveConfig != nil && c.autoSaveConfig.enabled && c.autoSaveConfig.timing == autoSaveOnClose {
		if err := c.performAutoSave(); err != nil {
			// Close the underlying connection first to avoid resource leaks
			closeErr := c.conn.Close()
			// Return the auto-save error as it's more important for the user
			if closeErr != nil {
				return fmt.Errorf("auto-save failed: %w (also failed to close connection: %w)", err, closeErr)
			}
			return fmt.Errorf("auto-save failed: %w", err)
		}
	}

	return c.conn.Close()
}

// Begin implements driver.Conn interface (deprecated, use BeginTx instead)
func (c *autoSaveConnection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx implements driver.ConnBeginTx interface
func (c *autoSaveConnection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if connBeginTx, ok := c.conn.(driver.ConnBeginTx); ok {
		tx, err := connBeginTx.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &autoSaveTransaction{
			tx:   tx,
			conn: c,
		}, nil
	}

	// Fallback for connections that don't support BeginTx
	tx, err := c.conn.Begin() //nolint:staticcheck // Need backward compatibility with older drivers
	if err != nil {
		return nil, err
	}
	return &autoSaveTransaction{
		tx:   tx,
		conn: c,
	}, nil
}

// Prepare implements driver.Conn interface
func (c *autoSaveConnection) Prepare(query string) (driver.Stmt, error) {
	return c.conn.Prepare(query)
}

// ExecContext implements driver.ExecerContext interface
func (c *autoSaveConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if execer, ok := c.conn.(driver.ExecerContext); ok {
		return execer.ExecContext(ctx, query, args)
	}
	// Fallback to deprecated Execer for backward compatibility
	if execer, ok := c.conn.(driver.Execer); ok { //nolint:staticcheck // Need backward compatibility
		dArgs := make([]driver.Value, len(args))
		for i, arg := range args {
			dArgs[i] = arg.Value
		}
		return execer.Exec(query, dArgs)
	}
	return nil, driver.ErrSkip
}

// QueryContext implements driver.QueryerContext interface
func (c *autoSaveConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if queryer, ok := c.conn.(driver.QueryerContext); ok {
		return queryer.QueryContext(ctx, query, args)
	}
	// Fallback to deprecated Queryer for backward compatibility
	if queryer, ok := c.conn.(driver.Queryer); ok { //nolint:staticcheck // Need backward compatibility
		dArgs := make([]driver.Value, len(args))
		for i, arg := range args {
			dArgs[i] = arg.Value
		}
		return queryer.Query(query, dArgs)
	}
	return nil, driver.ErrSkip
}

// autoSaveTransaction wraps a transaction with auto-save functionality
type autoSaveTransaction struct {
	tx   driver.Tx
	conn *autoSaveConnection
}

// Commit implements driver.Tx interface with auto-save on commit
func (t *autoSaveTransaction) Commit() error {
	// First commit the underlying transaction
	if err := t.tx.Commit(); err != nil {
		return err
	}

	// Perform auto-save if configured for commit timing
	if t.conn.autoSaveConfig != nil && t.conn.autoSaveConfig.enabled && t.conn.autoSaveConfig.timing == autoSaveOnCommit {
		if err := t.conn.performAutoSave(); err != nil {
			// Auto-save failed, but the transaction was already committed
			// Return the auto-save error to notify the user
			return fmt.Errorf("transaction committed successfully, but auto-save failed: %w", err)
		}
	}

	return nil
}

// Rollback implements driver.Tx interface
func (t *autoSaveTransaction) Rollback() error {
	return t.tx.Rollback()
}

// performAutoSave executes automatic saving using the configured settings
func (c *autoSaveConnection) performAutoSave() error {
	if c.autoSaveConfig == nil || !c.autoSaveConfig.enabled {
		return nil // No auto-save configured
	}

	// Create a temporary SQL DB to use DumpDatabase function
	tempDB := sql.OpenDB(&directConnector{conn: c.conn})

	outputDir := c.autoSaveConfig.outputDir
	if outputDir == "" {
		// Overwrite mode - save to original file locations
		return c.overwriteOriginalFiles(tempDB)
	}

	// Use the configured DumpOptions directly
	dumpOptions := c.autoSaveConfig.options

	// Use the existing DumpDatabase method
	return DumpDatabase(tempDB, outputDir, dumpOptions)
}

// overwriteOriginalFiles saves each table back to its original file location
func (c *autoSaveConnection) overwriteOriginalFiles(db *sql.DB) error {
	if len(c.originalPaths) == 0 {
		return errors.New("no original paths available for overwrite")
	}

	// For now, use the first original path's directory as output
	// This is a simplified implementation
	if len(c.originalPaths) > 0 {
		outputDir := filepath.Dir(c.originalPaths[0])
		return DumpDatabase(db, outputDir, c.autoSaveConfig.options)
	}

	return nil
}

// validateAutoSaveConfig validates that the auto-save configuration is compatible with the input sources
func (b *DBBuilder) validateAutoSaveConfig() error {
	// If auto-save is not enabled, no validation needed
	if b.autoSaveConfig == nil || !b.autoSaveConfig.enabled {
		return nil
	}

	// Check if overwrite mode (empty OutputDir) is being used with non-file inputs
	isOverwriteMode := b.autoSaveConfig.outputDir == ""
	hasNonFileInputs := len(b.readers) > 0 || len(b.filesystems) > 0

	if isOverwriteMode && hasNonFileInputs {
		var inputTypes []string

		if len(b.readers) > 0 {
			inputTypes = append(inputTypes, fmt.Sprintf("%d io.Reader(s)", len(b.readers)))
		}
		if len(b.filesystems) > 0 {
			inputTypes = append(inputTypes, fmt.Sprintf("%d filesystem(s)", len(b.filesystems)))
		}

		return fmt.Errorf(
			"auto-save overwrite mode (empty output directory) is not supported with %s. "+
				"Please specify an output directory using EnableAutoSave(\"/path/to/output\") "+
				"or use file paths instead of readers/filesystems",
			strings.Join(inputTypes, " and "))
	}

	return nil
}
