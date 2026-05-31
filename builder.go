package filesql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"github.com/xuri/excelize/v2"
	"modernc.org/sqlite" // Direct SQLite driver usage
)

// DBBuilder configures and creates database connections from various data sources.
//
// Basic usage:
//
//	builder := filesql.NewBuilder().
//		AddPath("data.csv").
//		AddPath("users.tsv")
//
//	validatedBuilder, err := builder.Build(ctx)
//	if err != nil {
//		return err
//	}
//
//	db, err := validatedBuilder.Open(ctx)
//	defer db.Close()
//
// Supports:
//   - File paths (AddPath)
//   - Embedded filesystems (AddFS)
//   - io.Reader streams (AddReader)
//   - Auto-save functionality (EnableAutoSave)
//   - Read-only mode (OpenReadOnly)
type DBBuilder struct {
	// paths contains regular file paths
	paths []string
	// filesystems contains fs.FS instances
	filesystems []fs.FS
	// readers contains reader configurations
	readers []readerInput
	// collectedPaths contains all paths after Build validation
	collectedPaths []string
	// parsedTables contains tables parsed from streaming readers
	parsedTables []*table
	// autoSaveConfig contains auto-save settings
	autoSaveConfig *autoSaveConfig
	// defaultChunkSize is the default chunk size for reading large files (10MB)
	defaultChunkSize int
	// logger is the logger instance for internal logging
	logger Logger

	// Internal processors for handling different responsibilities
	validator       *validator
	fileProcessor   *fileProcessor
	streamProcessor *streamProcessor
}

// readerInput represents configuration for reading from io.Reader
type readerInput struct {
	// reader is the data source
	reader io.Reader
	// tableName is the name of the table to create
	tableName string
	// fileType specifies the file format using domain/model types
	fileType FileType
	// closer is an optional closer for the reader (set when the reader was opened internally, e.g. from AddFS).
	// User-provided readers (from AddReader) do not set this field.
	closer io.Closer
}

// NewBuilder creates a new database builder.
//
// Start here when you need:
//   - Multiple data sources (files, streams, embedded FS)
//   - Auto-save functionality
//   - Custom chunk sizes for large files
//   - More control than the simple Open() function
//
// Example:
//
//	builder := filesql.NewBuilder().
//		AddPath("data.csv").
//		EnableAutoSave("./backup")
func NewBuilder() *DBBuilder {
	chunkSize := DefaultChunkSize
	return &DBBuilder{
		paths:            make([]string, 0),
		filesystems:      make([]fs.FS, 0),
		readers:          make([]readerInput, 0),
		collectedPaths:   make([]string, 0),
		parsedTables:     make([]*table, 0),
		autoSaveConfig:   nil, // Default: no auto-save
		defaultChunkSize: chunkSize,
		logger:           newNopLogger(), // Default: no-op logger

		// Initialize internal processors
		validator:       newValidator(),
		fileProcessor:   newFileProcessor(chunkSize),
		streamProcessor: newStreamProcessor(chunkSize),
	}
}

// AddPath adds a file or directory to load.
//
// Examples:
//   - Single file: AddPath("users.csv")
//   - Compressed: AddPath("data.tsv.gz")
//   - Directory: AddPath("/data/") // loads all CSV/TSV/LTSV files
//
// Returns self for chaining.
func (b *DBBuilder) AddPath(path string) *DBBuilder {
	b.paths = append(b.paths, path)
	return b
}

// AddPaths adds multiple files or directories at once.
//
// Example:
//
//	builder.AddPaths("users.csv", "products.tsv", "/data/logs/")
//
// Returns self for chaining.
func (b *DBBuilder) AddPaths(paths ...string) *DBBuilder {
	b.paths = append(b.paths, paths...)
	return b
}

// AddReader adds data from an io.Reader (file, network stream, etc.).
//
// Parameters:
//   - reader: Any io.Reader (file, bytes.Buffer, http.Response.Body, etc.)
//   - tableName: Name for the SQL table (e.g., "users")
//   - fileType: Data format (FileTypeCSV, FileTypeTSV, FileTypeLTSV, etc.)
//
// Example:
//
//	resp, _ := http.Get("https://example.com/data.csv")
//	builder.AddReader(resp.Body, "remote_data", FileTypeCSV)
//
// Returns self for chaining.
func (b *DBBuilder) AddReader(reader io.Reader, tableName string, fileType FileType) *DBBuilder {
	b.readers = append(b.readers, readerInput{
		reader:    reader,
		tableName: tableName,
		fileType:  fileType,
	})
	return b
}

// SetDefaultChunkSize sets chunk size (number of rows) for large file processing.
//
// Default: 1000 rows. Adjust based on available memory and processing needs.
//
// Example:
//
//	builder.SetDefaultChunkSize(5000) // 5000 rows per chunk
//
// Returns self for chaining.
func (b *DBBuilder) SetDefaultChunkSize(size int) *DBBuilder {
	if size > 0 {
		b.defaultChunkSize = size
	}
	return b
}

// WithLogger sets a custom logger for internal operations.
//
// The logger interface is compatible with slog.Logger. You can use the provided
// SlogAdapter to wrap an existing slog.Logger, or implement your own Logger.
//
// Examples:
//
//	// Using slog
//	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
//	builder.WithLogger(filesql.NewSlogAdapter(logger))
//
//	// Using a custom logger
//	builder.WithLogger(myCustomLogger)
//
// Returns self for chaining.
func (b *DBBuilder) WithLogger(logger Logger) *DBBuilder {
	if logger != nil {
		b.logger = logger
	}
	return b
}

// AddFS adds files from an embedded filesystem (go:embed).
//
// Automatically finds all CSV, TSV, and LTSV files in the filesystem.
//
// Example:
//
//	//go:embed data/*.csv data/*.tsv
//	var dataFS embed.FS
//
//	builder.AddFS(dataFS)
//
// Returns self for chaining.
func (b *DBBuilder) AddFS(filesystem fs.FS) *DBBuilder {
	b.filesystems = append(b.filesystems, filesystem)
	return b
}

// EnableAutoSave automatically saves changes when the database is closed.
//
// Parameters:
//   - outputDir: Where to save files
//   - "" (empty): Overwrite original files
//   - "./backup": Save to backup directory
//
// Example:
//
//	builder.AddPath("data.csv").
//		EnableAutoSave("") // Auto-save to original file on db.Close()
//
// Returns self for chaining.
func (b *DBBuilder) EnableAutoSave(outputDir string, options ...DumpOptions) *DBBuilder {
	opts := NewDumpOptions()
	if len(options) > 0 {
		opts = options[0]
	}

	b.autoSaveConfig = &autoSaveConfig{
		enabled:   true,
		timing:    autoSaveOnClose, // Default to close-time saving
		outputDir: outputDir,
		options:   opts,
	}
	return b
}

// EnableAutoSaveOnCommit automatically saves changes after each transaction commit.
//
// Use this for real-time persistence. Note: May impact performance.
//
// Example:
//
//	builder.AddPath("data.csv").
//		EnableAutoSaveOnCommit("./output") // Save after each commit
//
// Returns self for chaining.
func (b *DBBuilder) EnableAutoSaveOnCommit(outputDir string, options ...DumpOptions) *DBBuilder {
	opts := NewDumpOptions()
	if len(options) > 0 {
		opts = options[0]
	}

	b.autoSaveConfig = &autoSaveConfig{
		enabled:   true,
		timing:    autoSaveOnCommit,
		outputDir: outputDir,
		options:   opts,
	}
	return b
}

// DisableAutoSave disables automatic saving (default behavior).
// Returns the builder for method chaining.
func (b *DBBuilder) DisableAutoSave() *DBBuilder {
	b.autoSaveConfig = nil
	return b
}

// Build validates all configured inputs and prepares the builder for opening a database.
// This method must be called before Open(). It performs the following operations:
//
// 1. Validates that at least one input source is configured
// 2. Checks existence and format of all file paths
// 3. Processes embedded filesystems by converting files to streaming readers
// 4. Validates that all files have supported extensions
//
// After successful validation, the builder is ready to create database connections
// with Open(). The context is used for file operations and can be used for cancellation.
//
// Returns the same builder instance for method chaining, or an error if validation fails.
func (b *DBBuilder) Build(ctx context.Context) (*DBBuilder, error) {
	b.logger.Debug("starting build", "paths", len(b.paths), "filesystems", len(b.filesystems), "readers", len(b.readers))

	// Validate that we have at least one input
	if len(b.paths) == 0 && len(b.filesystems) == 0 && len(b.readers) == 0 {
		return nil, fmt.Errorf("%w: at least one path must be provided", ErrNoFiles)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Use validator to validate auto-save config
	if err := b.validator.validateAutoSaveConfig(b.autoSaveConfig); err != nil {
		return nil, err
	}

	// Use file processor to collect paths
	collectedPaths, err := b.fileProcessor.collectFilesFromPaths(b.paths)
	if err != nil {
		return nil, err
	}
	b.collectedPaths = collectedPaths

	// Use file processor to handle filesystems
	fsReaders, err := b.fileProcessor.processFilesystemsToReaders(ctx, b.filesystems)
	if err != nil {
		return nil, err
	}
	b.readers = append(b.readers, fsReaders...)

	// Use validator to validate reader inputs
	for _, readerInput := range b.readers {
		if err := b.validator.validateReader(readerInput.reader, readerInput.tableName, readerInput.fileType); err != nil {
			return nil, err
		}
	}

	// Use validator to validate final state
	if err := b.validator.validateFinalState(b.collectedPaths, b.readers, b.paths); err != nil {
		return nil, err
	}

	// Pass logger to internal processors
	b.streamProcessor.setLogger(b.logger)
	b.fileProcessor.setLogger(b.logger)

	b.logger.Info("build completed", "collected_paths", len(b.collectedPaths), "readers", len(b.readers))
	return b, nil
}

// Open creates and returns a database connection using the configured and validated inputs.
// This method can only be called after Build() has been successfully executed.
// It creates an in-memory SQLite database and loads all configured files as tables using streaming.
//
// Table names are derived from file names without extensions:
// - "users.csv" becomes table "users"
// - "data.tsv.gz" becomes table "data"
// - "user-data.csv" becomes table "user_data" (hyphens become underscores)
// - "my file.csv" becomes table "my_file" (spaces become underscores)
//
// Special characters in file names are automatically sanitized for SQL safety.
//
// The returned database connection supports the full SQLite3 SQL syntax.
// Auto-save functionality is supported for both file paths and reader inputs.
// The caller is responsible for closing the connection when done.
//
// Returns a *sql.DB connection or an error if the database cannot be created.
func (b *DBBuilder) Open(ctx context.Context) (*sql.DB, error) {
	b.logger.Debug("opening database")

	// Use validator to validate inputs availability
	if err := b.validator.validateInputsAvailable(b.collectedPaths, b.readers); err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Use file processor to deduplicate compressed files
	b.collectedPaths = b.fileProcessor.deduplicateCompressedFiles(b.collectedPaths)

	b.logger.Debug("creating in-memory database")
	db, err := b.createInMemoryDatabase()
	if err != nil {
		b.logger.Error("failed to create database", "error", err)
		return nil, err
	}

	// Use stream processor for all streaming operations (now includes XLSX support)
	if err := b.streamProcessor.streamAllFilesToDatabase(ctx, db, b.collectedPaths); err != nil {
		b.logger.Error("failed to stream files", "error", err)
		_ = db.Close() // Ignore close error during error handling
		return nil, err
	}

	if err := b.streamProcessor.streamAllReadersToDatabase(ctx, db, b.readers); err != nil {
		b.logger.Error("failed to stream readers", "error", err)
		_ = db.Close() // Ignore close error during error handling
		return nil, err
	}

	if err := b.validateDatabaseConnection(ctx, db); err != nil {
		_ = db.Close() // Ignore close error during error handling
		return nil, err
	}

	db, err = b.setupAutoSaveIfNeeded(ctx, db)
	if err != nil {
		return nil, err
	}

	b.logger.Info("database opened successfully")
	return db, nil
}

// OpenReadOnly creates a read-only database connection.
// This is a convenience method that calls Open() and wraps the result in a ReadOnlyDB.
// All SELECT queries work normally, but write operations return ErrReadOnly.
//
// This is useful for audit scenarios where you want to query data without
// risk of accidental modification.
//
// Example:
//
//	builder := filesql.NewBuilder().
//		AddPath("payment.ach")
//
//	validatedBuilder, err := builder.Build(ctx)
//	if err != nil {
//		return err
//	}
//
//	rodb, err := validatedBuilder.OpenReadOnly(ctx)
//	if err != nil {
//		return err
//	}
//	defer rodb.Close()
//
//	// SELECT works
//	rows, _ := rodb.Query("SELECT * FROM payment_entries")
//
//	// Write operations are rejected
//	_, err = rodb.Exec("DELETE FROM payment_entries") // returns ErrReadOnly
func (b *DBBuilder) OpenReadOnly(ctx context.Context) (*ReadOnlyDB, error) {
	db, err := b.Open(ctx)
	if err != nil {
		return nil, err
	}
	return NewReadOnlyDB(db), nil
}

// LoadInto loads the builder's configured inputs into an existing database
// instead of creating a new in-memory one as Open does. Use it to combine
// file-derived tables with a caller-managed database, such as a long-lived
// session or a database that already holds other tables, without copying the
// data through a second database.
//
// Semantics:
//   - A table whose name matches a loaded file or sheet is replaced (dropped and
//     recreated), so reloading a file is idempotent and same-named inputs are
//     last-wins. Other tables in db are left untouched.
//   - The caller keeps ownership of db. LoadInto never closes it, even on error.
//   - For an in-memory database, pin the pool to one connection
//     (db.SetMaxOpenConns(1)). SQLite ":memory:" is private per connection, so a
//     multi-connection pool would not share the loaded tables.
//
// Auto-save is not supported because the caller owns the database lifecycle;
// configuring it returns an error.
//
// Example:
//
//	db, _ := sql.Open("sqlite", ":memory:")
//	db.SetMaxOpenConns(1)
//	builder, err := filesql.NewBuilder().AddPath("users.csv").Build(ctx)
//	if err != nil {
//		return err
//	}
//	if err := builder.LoadInto(ctx, db); err != nil {
//		return err
//	}
//	// db now has a "users" table alongside any tables it already had.
func (b *DBBuilder) LoadInto(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("%w: target database is nil", ErrDatabaseOperation)
	}
	if b.autoSaveConfig != nil && b.autoSaveConfig.enabled {
		return fmt.Errorf("%w: auto-save is not supported by LoadInto; the caller owns the database lifecycle", ErrDatabaseOperation)
	}

	if err := b.validator.validateInputsAvailable(b.collectedPaths, b.readers); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	b.collectedPaths = b.fileProcessor.deduplicateCompressedFiles(b.collectedPaths)

	// Replace same-named tables so loading into an existing database is last-wins
	// rather than erroring on or appending to a pre-existing table.
	b.streamProcessor.replaceExisting = true

	if err := b.streamProcessor.streamAllFilesToDatabase(ctx, db, b.collectedPaths); err != nil {
		return err
	}
	if err := b.streamProcessor.streamAllReadersToDatabase(ctx, db, b.readers); err != nil {
		return err
	}
	return nil
}

// deduplicateCompressedFiles removes compressed duplicates when uncompressed versions exist.
// DEPRECATED: This method has been moved to fileProcessor.deduplicateCompressedFiles()
// createInMemoryDatabase creates a new in-memory SQLite database connection.
func (b *DBBuilder) createInMemoryDatabase() (*sql.DB, error) {
	sqliteDriver := &sqlite.Driver{}
	conn, err := sqliteDriver.Open(":memory:")
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create in-memory database: %s", ErrDatabaseOperation, err.Error())
	}

	return sql.OpenDB(&directConnector{conn: conn}), nil
}

// validateDatabaseConnection validates the database connection is working.
func (b *DBBuilder) validateDatabaseConnection(ctx context.Context, db *sql.DB) error {
	if err := db.PingContext(ctx); err != nil {
		closeErr := db.Close()

		var allErrors []error
		allErrors = append(allErrors, err)
		if closeErr != nil {
			allErrors = append(allErrors, fmt.Errorf("failed to close database: %w", closeErr))
		}

		return errors.Join(allErrors...)
	}
	return nil
}

// setupAutoSaveIfNeeded sets up auto-save functionality if enabled.
func (b *DBBuilder) setupAutoSaveIfNeeded(ctx context.Context, db *sql.DB) (*sql.DB, error) {
	if b.autoSaveConfig == nil || !b.autoSaveConfig.enabled {
		return db, nil
	}

	if err := db.Close(); err != nil {
		return nil, fmt.Errorf("%w: failed to close intermediate database: %s", ErrDatabaseOperation, err.Error())
	}

	sqliteDriver := &sqlite.Driver{}
	freshConn, err := sqliteDriver.Open(":memory:")
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create fresh SQLite connection for auto-save: %s", ErrDatabaseOperation, err.Error())
	}

	connector := &autoSaveConnector{
		sqliteConn:     freshConn,
		autoSaveConfig: b.autoSaveConfig,
		originalPaths:  b.collectOriginalPaths(),
	}
	db = sql.OpenDB(connector)

	// Use stream processor for all streaming operations (now includes XLSX support)
	if err := b.streamProcessor.streamAllFilesToDatabase(ctx, db, b.collectedPaths); err != nil {
		_ = db.Close() // Ignore close error during error handling
		return nil, err
	}

	if err := b.streamProcessor.streamAllReadersToDatabase(ctx, db, b.readers); err != nil {
		_ = db.Close() // Ignore close error during error handling
		return nil, err
	}

	return db, nil
}

// streamXLSXFileToSQLite handles XLSX files by creating separate tables for each sheet
func (b *DBBuilder) streamXLSXFileToSQLite(ctx context.Context, db *sql.DB, reader io.Reader, filePath string) error {
	// Read all data into memory (XLSX requires random access)
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("%w: failed to read XLSX data: %s", ErrIOOperation, err.Error())
	}

	if len(data) == 0 {
		return fmt.Errorf("%w: empty XLSX file", ErrEmptyData)
	}

	// Open XLSX file from bytes
	xlsxFile, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("%w: failed to open XLSX file: %s", ErrParsing, err.Error())
	}
	defer func() {
		_ = xlsxFile.Close() // Ignore close error
	}()

	// Get all sheet names
	sheetNames := xlsxFile.GetSheetList()
	if len(sheetNames) == 0 {
		return fmt.Errorf("%w: no sheets found in XLSX file", ErrEmptyData)
	}

	// Base table name from file path (sanitize to ensure a valid identifier)
	baseTableName := sanitizeTableName(tableFromFilePath(filePath))

	// Process each sheet as a separate table
	for _, sheetName := range sheetNames {
		rows, err := xlsxFile.GetRows(sheetName)
		if err != nil {
			return fmt.Errorf("%w: failed to read sheet %s: %s", ErrParsing, sheetName, err.Error())
		}

		// Skip empty sheets
		if len(rows) == 0 {
			continue
		}

		// Create table name: filename_sheetname
		tableName := fmt.Sprintf("%s_%s", baseTableName, sanitizeTableName(sheetName))

		// Check if table already exists
		var tableExists int
		err = db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`,
			tableName,
		).Scan(&tableExists)
		if err != nil {
			return fmt.Errorf("%w: failed to check table existence: %s", ErrDatabaseOperation, err.Error())
		}

		if tableExists > 0 {
			return fmt.Errorf("%w: table '%s' already exists", ErrDuplicateTable, tableName)
		}

		// Process sheet data
		if err := b.createTableFromXLSXSheet(ctx, db, tableName, rows); err != nil {
			return fmt.Errorf("%w: failed to create table from sheet %s: %s", ErrDatabaseOperation, sheetName, err.Error())
		}
	}

	return nil
}

// createTableFromXLSXSheet creates a SQLite table from XLSX sheet data
func (b *DBBuilder) createTableFromXLSXSheet(ctx context.Context, db *sql.DB, tableName string, rows [][]string) error {
	if len(rows) == 0 {
		return fmt.Errorf("%w: no rows in sheet", ErrEmptyData)
	}

	// First row is header
	headers := rows[0]
	if len(headers) == 0 {
		return fmt.Errorf("%w: no columns in sheet header", ErrEmptyData)
	}

	// Check for duplicate column names
	columnsSeen := make(map[string]bool)
	for _, col := range headers {
		if columnsSeen[col] {
			return fmt.Errorf("%w: %s", errDuplicateColumnName, col)
		}
		columnsSeen[col] = true
	}

	// Collect data rows for type inference
	dataRows := make([][]string, 0, len(rows)-1)
	for i := 1; i < len(rows); i++ {
		dataRows = append(dataRows, rows[i])
	}

	// Create records for type inference
	records := make([]Record, len(dataRows))
	for i, row := range dataRows {
		// Pad row with empty strings if necessary
		paddedRow := make(Record, len(headers))
		for j := range headers {
			if j < len(row) {
				paddedRow[j] = row[j]
			} else {
				paddedRow[j] = ""
			}
		}
		records[i] = paddedRow
	}

	// Infer column types
	headerObj := header(headers)
	columnInfo := inferColumnsInfo(headerObj, records)

	// Create table
	if err := b.createSQLiteTable(ctx, db, tableName, columnInfo); err != nil {
		return fmt.Errorf("%w: failed to create SQLite table: %s", ErrDatabaseOperation, err.Error())
	}

	// Insert data
	if len(records) > 0 {
		if err := b.insertDataIntoTable(ctx, db, tableName, headers, records); err != nil {
			return fmt.Errorf("%w: failed to insert data: %s", ErrDatabaseOperation, err.Error())
		}
	}

	return nil
}

// createSQLiteTable creates a SQLite table with the given columns
func (b *DBBuilder) createSQLiteTable(ctx context.Context, db *sql.DB, tableName string, columnInfo []columnInfo) error {
	columns := make([]string, 0, len(columnInfo))
	for _, col := range columnInfo {
		columns = append(columns, fmt.Sprintf(`"%s" %s`, col.Name, col.Type.string()))
	}

	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (%s)`,
		tableName,
		strings.Join(columns, ", "),
	)

	_, err := db.ExecContext(ctx, query)
	return err
}

// insertDataIntoTable inserts records into the specified table
func (b *DBBuilder) insertDataIntoTable(ctx context.Context, db *sql.DB, tableName string, headers []string, records []Record) error {
	placeholders := make([]string, len(headers))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf( //nolint:gosec // SQL table name is validated, placeholders are safe
		`INSERT INTO "%s" VALUES (%s)`,
		tableName,
		strings.Join(placeholders, ", "),
	)

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("%w: failed to prepare insert statement: %s", ErrDatabaseOperation, err.Error())
	}
	defer stmt.Close()

	for _, record := range records {
		values := make([]any, len(record))
		for i, value := range record {
			values[i] = value
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("%w: failed to insert record: %s", ErrDatabaseOperation, err.Error())
		}
	}

	return nil
}

// createDecompressedReader creates a decompressed reader based on file extension
func (b *DBBuilder) createDecompressedReader(file *os.File, filePath string) (io.Reader, error) {
	factory := NewCompressionFactory()
	handler := factory.CreateHandlerForFile(filePath)

	reader, _, err := handler.CreateReader(file)
	if err != nil {
		return nil, err
	}

	return reader, nil
}

// collectOriginalPaths collects original file paths for overwrite mode
func (b *DBBuilder) collectOriginalPaths() []string {
	paths := make([]string, 0, len(b.collectedPaths))
	paths = append(paths, b.collectedPaths...)
	return paths
}
