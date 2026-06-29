package filesql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"

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
	// OutputFormatACH represents ACH (NACHA) output format
	OutputFormatACH
	// OutputFormatFedWire represents Fedwire output format
	OutputFormatFedWire
)

// String returns the string representation of OutputFormat
func (f OutputFormat) String() string {
	switch f {
	case OutputFormatCSV:
		return formatCSVStr
	case OutputFormatTSV:
		return formatTSVStr
	case OutputFormatLTSV:
		return formatLTSVStr
	case OutputFormatParquet:
		return formatParquetStr
	case OutputFormatXLSX:
		return formatXLSXStr
	case OutputFormatACH:
		return formatACHStr
	case OutputFormatFedWire:
		return formatFedWireStr
	default:
		return formatCSVStr
	}
}

// Extension returns the file extension for the format
func (f OutputFormat) Extension() string {
	switch f {
	case OutputFormatCSV:
		return extCSV
	case OutputFormatTSV:
		return extTSV
	case OutputFormatLTSV:
		return extLTSV
	case OutputFormatParquet:
		return extParquet
	case OutputFormatXLSX:
		return extXLSX
	case OutputFormatACH:
		return extACH
	case OutputFormatFedWire:
		return extFED
	default:
		return extCSV
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
	// CompressionZLIB represents zlib compression
	CompressionZLIB
	// CompressionSNAPPY represents snappy compression
	CompressionSNAPPY
	// CompressionS2 represents s2 compression
	CompressionS2
	// CompressionLZ4 represents lz4 compression
	CompressionLZ4
)

// string constants for compression types
const (
	compressionNoneStr   = "none"
	compressionGZStr     = "gz"
	compressionBZ2Str    = "bz2"
	compressionXZStr     = "xz"
	compressionZSTDStr   = "zstd"
	compressionZLIBStr   = "zlib"
	compressionSNAPPYStr = "snappy"
	compressionS2Str     = "s2"
	compressionLZ4Str    = "lz4"
)

// string constants for output format names
const (
	formatCSVStr     = "csv"
	formatTSVStr     = "tsv"
	formatLTSVStr    = "ltsv"
	formatParquetStr = "parquet"
	formatXLSXStr    = "xlsx"
	formatACHStr     = "ach"
	formatFedWireStr = "fed"
)

// String returns the string representation of CompressionType
func (c CompressionType) String() string {
	switch c {
	case CompressionNone:
		return compressionNoneStr
	case CompressionGZ:
		return compressionGZStr
	case CompressionBZ2:
		return compressionBZ2Str
	case CompressionXZ:
		return compressionXZStr
	case CompressionZSTD:
		return compressionZSTDStr
	case CompressionZLIB:
		return compressionZLIBStr
	case CompressionSNAPPY:
		return compressionSNAPPYStr
	case CompressionS2:
		return compressionS2Str
	case CompressionLZ4:
		return compressionLZ4Str
	default:
		return compressionNoneStr
	}
}

// Extension returns the file extension for the compression type
func (c CompressionType) Extension() string {
	switch c {
	case CompressionNone:
		return ""
	case CompressionGZ:
		return extGZ
	case CompressionBZ2:
		return extBZ2
	case CompressionXZ:
		return extXZ
	case CompressionZSTD:
		return extZSTD
	case CompressionZLIB:
		return extZLIB
	case CompressionSNAPPY:
		return extSNAPPY
	case CompressionS2:
		return extS2
	case CompressionLZ4:
		return extLZ4
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
//   - CompressionBZ2: Bzip2 compression (.bz2) - read only, writing not supported
//   - CompressionXZ: XZ compression (.xz)
//   - CompressionZSTD: Zstandard compression (.zst)
//   - CompressionZLIB: Zlib compression (.z)
//   - CompressionSNAPPY: Snappy compression (.snappy)
//   - CompressionS2: S2 compression (.s2) - Snappy compatible
//   - CompressionLZ4: LZ4 compression (.lz4)
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
			// Clean up ACH and Fedwire TableSet registry entries for this connection
			c.cleanupTableSetRegistries()
			// Return the auto-save error as it's more important for the user
			if closeErr != nil {
				return fmt.Errorf("auto-save failed: %w (also failed to close connection: %w)", err, closeErr)
			}
			return fmt.Errorf("auto-save failed: %w", err)
		}
	}

	// Clean up ACH and Fedwire TableSet registry entries for this connection
	c.cleanupTableSetRegistries()

	return c.conn.Close()
}

// cleanupTableSetRegistries removes ACH and Fedwire TableSet entries for files loaded by this connection.
func (c *autoSaveConnection) cleanupTableSetRegistries() {
	for _, path := range c.originalPaths {
		if isACHFile(path) {
			baseTableName := sanitizeTableName(tableFromFilePath(path))
			UnregisterACHTableSet(baseTableName)
		}
		if isFedWireFile(path) {
			baseTableName := sanitizeTableName(tableFromFilePath(path))
			UnregisterWireTableSet(baseTableName)
		}
	}
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

	// Handle ACH format specially - need to export ACH files separately
	if dumpOptions.Format == OutputFormatACH {
		return c.performACHAutoSave(tempDB, outputDir)
	}

	// Handle Fedwire format specially - need to export Fedwire files separately
	if dumpOptions.Format == OutputFormatFedWire {
		return c.performFedWireAutoSave(tempDB, outputDir)
	}

	// Use the existing DumpDatabase method for other formats
	return DumpDatabase(tempDB, outputDir, dumpOptions)
}

// performACHAutoSave saves all ACH tables back to ACH files
func (c *autoSaveConnection) performACHAutoSave(db *sql.DB, outputDir string) error {
	ctx := context.Background()

	// Get all registered ACH base table names
	achBaseNames := getACHBaseTableNames()
	if len(achBaseNames) == 0 {
		return errors.New("no ACH tables found to save")
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0750); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Export each ACH file
	for _, baseName := range achBaseNames {
		outputPath := filepath.Join(outputDir, baseName+".ach")
		if err := DumpACH(ctx, db, baseName, outputPath); err != nil {
			return fmt.Errorf("failed to export ACH file %s: %w", baseName, err)
		}
	}

	return nil
}

// performFedWireAutoSave saves all Fedwire tables back to Fedwire files
func (c *autoSaveConnection) performFedWireAutoSave(db *sql.DB, outputDir string) error {
	ctx := context.Background()

	wireBaseNames := getWireBaseTableNames()
	if len(wireBaseNames) == 0 {
		return errors.New("no Fedwire tables found to save")
	}

	if err := os.MkdirAll(outputDir, 0750); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	for _, baseName := range wireBaseNames {
		outputPath := filepath.Join(outputDir, baseName+".fed")
		if err := DumpFedWire(ctx, db, baseName, outputPath); err != nil {
			return fmt.Errorf("failed to export Fedwire file %s: %w", baseName, err)
		}
	}

	return nil
}

// overwriteOriginalFiles saves each table back to its original file location
func (c *autoSaveConnection) overwriteOriginalFiles(db *sql.DB) error {
	if len(c.originalPaths) == 0 {
		return errors.New("no original paths available for overwrite")
	}

	ctx := context.Background()

	// Check if any original paths are ACH or Fedwire files
	for _, path := range c.originalPaths {
		if isACHFile(path) {
			baseTableName := sanitizeTableName(tableFromFilePath(path))
			if err := DumpACH(ctx, db, baseTableName, path); err != nil {
				return fmt.Errorf("failed to overwrite ACH file %s: %w", path, err)
			}
		}
		if isFedWireFile(path) {
			baseTableName := sanitizeTableName(tableFromFilePath(path))
			if err := DumpFedWire(ctx, db, baseTableName, path); err != nil {
				return fmt.Errorf("failed to overwrite Fedwire file %s: %w", path, err)
			}
		}
	}

	// For tabular files (CSV, TSV, etc.), use the directory-based approach
	// Filter out ACH and Fedwire paths which are already handled above
	tabularPaths := make([]string, 0)
	for _, path := range c.originalPaths {
		if !isACHFile(path) && !isFedWireFile(path) {
			tabularPaths = append(tabularPaths, path)
		}
	}

	if len(tabularPaths) > 0 {
		outputDir := filepath.Dir(tabularPaths[0])
		return DumpDatabase(db, outputDir, c.autoSaveConfig.options)
	}

	return nil
}
