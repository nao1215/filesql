// Package filesql provides file-based SQL driver implementation.
package filesql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/nao1215/filesql/domain/model"
	_ "modernc.org/sqlite" // Import SQLite driver for in-memory databases
)

// DBBuilder is a builder for creating database connections from files and embedded filesystems.
// It provides a flexible way to configure input sources before creating a database connection.
// Use NewBuilder to create a new instance, then chain method calls to configure it.
//
// The typical usage pattern is:
//
//	builder := filesql.NewBuilder().AddPath("data.csv").AddFS(embeddedFS)
//	validatedBuilder, err := builder.Build(ctx)
//	if err != nil {
//		return err
//	}
//	db, err := validatedBuilder.Open(ctx)
//	defer db.Close()
type DBBuilder struct {
	// paths contains regular file paths
	paths []string
	// filesystems contains fs.FS instances
	filesystems []fs.FS
	// readers contains reader configurations
	readers []ReaderInput
	// collectedPaths contains all paths after Build validation
	collectedPaths []string
	// parsedTables contains tables parsed from streaming readers
	parsedTables []*model.Table
	// autoSaveConfig contains auto-save settings
	autoSaveConfig *AutoSaveConfig
	// defaultChunkSize is the default chunk size for reading large files (10MB)
	defaultChunkSize int
}

// AutoSaveTiming specifies when automatic saving should occur
type AutoSaveTiming int

const (
	// AutoSaveOnClose saves data when db.Close() is called (default)
	AutoSaveOnClose AutoSaveTiming = iota
	// AutoSaveOnCommit saves data when transaction is committed
	AutoSaveOnCommit
)

// AutoSaveConfig holds configuration for automatic saving
type AutoSaveConfig struct {
	// Enabled indicates whether auto-save is enabled
	Enabled bool
	// Timing specifies when to save (on close or on commit)
	Timing AutoSaveTiming
	// OutputDir is the directory where files will be saved (overwrites original files)
	OutputDir string
	// Options contains dump options for formatting
	Options DumpOptions
}

// ReaderInput represents configuration for reading from io.Reader
type ReaderInput struct {
	// Reader is the data source
	Reader io.Reader
	// TableName is the name of the table to create
	TableName string
	// FileType specifies the file format using domain/model types
	FileType model.FileType
}

// NewBuilder creates a new database builder for configuring file inputs.
// The returned builder can be used to add file paths and embedded filesystems
// before building and opening a database connection.
//
// Example:
//
//	builder := filesql.NewBuilder()
//	builder.AddPath("users.csv")
//	builder.AddPath("orders.tsv")
//	validatedBuilder, err := builder.Build(ctx)
//	if err != nil {
//		return err
//	}
//	db, err := validatedBuilder.Open(ctx)
//	if err != nil {
//		return err
//	}
//	defer db.Close()
func NewBuilder() *DBBuilder {
	return &DBBuilder{
		paths:            make([]string, 0),
		filesystems:      make([]fs.FS, 0),
		readers:          make([]ReaderInput, 0),
		collectedPaths:   make([]string, 0),
		parsedTables:     make([]*model.Table, 0),
		autoSaveConfig:   nil,              // Default: no auto-save
		defaultChunkSize: 10 * 1024 * 1024, // 10MB default chunk size
	}
}

// AddPath adds a regular file or directory path to the builder.
// The path can be:
// - A single file with supported extensions (.csv, .tsv, .ltsv, and their compressed variants)
// - A directory path (all supported files will be loaded recursively)
//
// Supported file extensions: .csv, .tsv, .ltsv
// Supported compression: .gz, .bz2, .xz, .zst
//
// Returns the builder for method chaining.
func (b *DBBuilder) AddPath(path string) *DBBuilder {
	b.paths = append(b.paths, path)
	return b
}

// AddPaths adds multiple regular file or directory paths to the builder.
// This is a convenience method for adding multiple paths at once.
// Each path can be a file or directory, following the same rules as AddPath.
//
// Returns the builder for method chaining.
func (b *DBBuilder) AddPaths(paths ...string) *DBBuilder {
	b.paths = append(b.paths, paths...)
	return b
}

// AddReader adds an io.Reader as a data source to the builder.
// The reader's content will be streamed directly to the database during Open().
// You must specify the table name and file type explicitly for security.
//
// The fileType parameter should use the FileType constants from domain/model:
// - model.FileTypeCSV for CSV data
// - model.FileTypeTSV for TSV data
// - model.FileTypeLTSV for LTSV data
// - model.FileTypeCSVGZ for gzip-compressed CSV data
// - etc.
//
// Example:
//
//	file, err := os.Open("data.csv")
//	if err != nil {
//		return err
//	}
//	defer file.Close()
//
//	builder := filesql.NewBuilder().AddReader(file, "users", model.FileTypeCSV)
//
// Returns the builder for method chaining.
func (b *DBBuilder) AddReader(reader io.Reader, tableName string, fileType model.FileType) *DBBuilder {
	b.readers = append(b.readers, ReaderInput{
		Reader:    reader,
		TableName: tableName,
		FileType:  fileType,
	})
	return b
}

// SetDefaultChunkSize sets the default chunk size for reading large files.
// This affects both Reader inputs and file path inputs.
// The chunk size determines how much data is read into memory at once.
//
// Returns the builder for method chaining.
func (b *DBBuilder) SetDefaultChunkSize(size int) *DBBuilder {
	if size > 0 {
		b.defaultChunkSize = size
	}
	return b
}

// AddFS adds all supported files from an fs.FS filesystem to the builder.
// This method is particularly useful for embedded filesystems using go:embed.
// It automatically searches for all supported file types recursively:
// - Base formats: .csv, .tsv, .ltsv
// - Compressed variants: .gz, .bz2, .xz, .zst
//
// The filesystem will be processed during Build(), where matching files will be
// converted to streaming readers for direct database access.
//
// Example with embedded filesystem:
//
//	//go:embed data/*.csv data/*.tsv
//	var dataFS embed.FS
//
//	subFS, _ := fs.Sub(dataFS, "data")
//	builder := filesql.NewBuilder().AddFS(subFS)
//
// Returns the builder for method chaining.
func (b *DBBuilder) AddFS(filesystem fs.FS) *DBBuilder {
	b.filesystems = append(b.filesystems, filesystem)
	return b
}

// EnableAutoSave enables automatic saving when the database connection is closed.
// Files will be overwritten in their original locations with the current database content.
// This uses the same functionality as DumpDatabase() internally.
//
// The outputDir parameter specifies where to save the files. If empty, files will be
// saved to their original locations (overwrite mode).
//
// Example:
//
//	builder := filesql.NewBuilder().
//		AddPath("data.csv").
//		EnableAutoSave("./backup", NewDumpOptions()) // Save to backup directory on close
//
// Returns the builder for method chaining.
func (b *DBBuilder) EnableAutoSave(outputDir string, options ...DumpOptions) *DBBuilder {
	opts := NewDumpOptions()
	if len(options) > 0 {
		opts = options[0]
	}

	b.autoSaveConfig = &AutoSaveConfig{
		Enabled:   true,
		Timing:    AutoSaveOnClose, // Default to close-time saving
		OutputDir: outputDir,
		Options:   opts,
	}
	return b
}

// EnableAutoSaveOnCommit enables automatic saving when transactions are committed.
// Files will be overwritten in their original locations with the current database content.
// This provides more frequent saves but may impact performance for frequent commits.
//
// The outputDir parameter specifies where to save the files. If empty, files will be
// saved to their original locations (overwrite mode).
//
// Example:
//
//	builder := filesql.NewBuilder().
//		AddPath("data.csv").
//		EnableAutoSaveOnCommit("", NewDumpOptions()) // Overwrite original on each commit
//
// Returns the builder for method chaining.
func (b *DBBuilder) EnableAutoSaveOnCommit(outputDir string, options ...DumpOptions) *DBBuilder {
	opts := NewDumpOptions()
	if len(options) > 0 {
		opts = options[0]
	}

	b.autoSaveConfig = &AutoSaveConfig{
		Enabled:   true,
		Timing:    AutoSaveOnCommit,
		OutputDir: outputDir,
		Options:   opts,
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
	// Validate that we have at least one input
	if len(b.paths) == 0 && len(b.filesystems) == 0 && len(b.readers) == 0 {
		return nil, errors.New("at least one path must be provided")
	}

	// Reset collected paths
	b.collectedPaths = make([]string, 0)

	// Validate and collect regular paths
	for _, path := range b.paths {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("failed to load file: path does not exist: %s", path)
			}
			return nil, fmt.Errorf("failed to stat path %s: %w", path, err)
		}

		// If it's a directory, we accept it (will be processed later)
		// If it's a file, check if it has a supported extension
		if !info.IsDir() {
			if !model.IsSupportedFile(path) {
				return nil, fmt.Errorf("unsupported file type: %s", path)
			}
		}
		// Add to collected paths
		b.collectedPaths = append(b.collectedPaths, path)
	}

	// Process and validate FS inputs, converting to streaming readers
	for _, filesystem := range b.filesystems {
		if filesystem == nil {
			return nil, errors.New("FS cannot be nil")
		}

		// Process FS and create reader inputs
		fsReaders, err := b.processFSToReaders(ctx, filesystem)
		if err != nil {
			return nil, fmt.Errorf("failed to process FS input: %w", err)
		}
		b.readers = append(b.readers, fsReaders...)
	}

	// Validate Reader inputs for streaming processing
	for i := range b.readers {
		readerInput := &b.readers[i]
		if readerInput.Reader == nil {
			return nil, errors.New("reader cannot be nil")
		}
		if readerInput.TableName == "" {
			return nil, errors.New("table name must be specified for reader input")
		}
		if readerInput.FileType == model.FileTypeUnsupported {
			return nil, errors.New("file type must be specified for reader input")
		}
		// Reader inputs will be processed directly in Open() method using streaming
	}

	if len(b.collectedPaths) == 0 && len(b.readers) == 0 {
		return nil, errors.New("no valid input files found")
	}

	return b, nil
}

// Open creates and returns a database connection using the configured and validated inputs.
// This method can only be called after Build() has been successfully executed.
// It creates an in-memory SQLite database and loads all configured files as tables.
//
// Table names are derived from file names without extensions:
// - "users.csv" becomes table "users"
// - "data.tsv.gz" becomes table "data"
//
// The returned database connection supports the full SQLite3 SQL syntax.
// The caller is responsible for closing the connection when done.
//
// Returns a *sql.DB connection or an error if the database cannot be created.
func (b *DBBuilder) Open(ctx context.Context) (*sql.DB, error) {
	// Check that we have inputs available
	if len(b.collectedPaths) == 0 && len(b.readers) == 0 {
		return nil, errors.New("no valid input files found, did you call Build()?")
	}

	var db *sql.DB
	var err error

	// Case 1: We have only file paths, use existing DSN-based approach
	if len(b.collectedPaths) > 0 && len(b.readers) == 0 {
		// Create DSN with all collected paths and auto-save config
		dsn := strings.Join(b.collectedPaths, ";")

		// Append auto-save configuration to DSN if enabled
		if b.autoSaveConfig != nil && b.autoSaveConfig.Enabled {
			configJSON, err := json.Marshal(b.autoSaveConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize auto-save config: %w", err)
			}
			configEncoded := base64.StdEncoding.EncodeToString(configJSON)
			dsn += "?autosave=" + configEncoded
		}

		// Open database connection using driver
		db, err = sql.Open(DriverName, dsn)
		if err != nil {
			return nil, err
		}
	} else {
		// Case 2: We have reader inputs (with or without file paths)
		// Create in-memory SQLite database and stream data directly
		db, err = sql.Open("sqlite", ":memory:")
		if err != nil {
			return nil, fmt.Errorf("failed to create in-memory database: %w", err)
		}

		// Process file paths first if any
		if len(b.collectedPaths) > 0 {
			for _, path := range b.collectedPaths {
				file := model.NewFile(path)
				table, err := file.ToTable()
				if err != nil {
					_ = db.Close() // Ignore close error during error handling
					return nil, fmt.Errorf("failed to process file %s: %w", path, err)
				}

				if err := b.createTableFromModel(ctx, db, table); err != nil {
					_ = db.Close() // Ignore close error during error handling
					return nil, fmt.Errorf("failed to create table from file %s: %w", path, err)
				}
			}
		}

		// Process reader inputs using streaming
		for _, readerInput := range b.readers {
			if err := b.streamReaderToSQLite(ctx, db, readerInput); err != nil {
				_ = db.Close() // Ignore close error during error handling
				return nil, fmt.Errorf("failed to stream reader input for table '%s': %w", readerInput.TableName, err)
			}
		}
	}

	// Validate connection
	if err := db.PingContext(ctx); err != nil {
		closeErr := db.Close()

		// Collect all errors that occurred during error handling
		var allErrors []error
		allErrors = append(allErrors, err) // Original error
		if closeErr != nil {
			allErrors = append(allErrors, fmt.Errorf("failed to close database: %w", closeErr))
		}

		return nil, errors.Join(allErrors...)
	}
	return db, nil
}

// processFSToReaders processes all supported files from an fs.FS and creates ReaderInput
func (b *DBBuilder) processFSToReaders(_ context.Context, filesystem fs.FS) ([]ReaderInput, error) {
	readers := make([]ReaderInput, 0)

	// Search for all supported file patterns
	supportedPatterns := model.SupportedFileExtPatterns()

	// Collect all matching files
	allMatches := make([]string, 0)
	for _, pattern := range supportedPatterns {
		matches, err := fs.Glob(filesystem, pattern)
		if err != nil {
			return nil, fmt.Errorf("failed to search pattern %s: %w", pattern, err)
		}
		allMatches = append(allMatches, matches...)
	}

	// Also search recursively in subdirectories
	err := fs.WalkDir(filesystem, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if model.IsSupportedFile(path) {
			// Check if already found by glob patterns
			// Use path.Clean to normalize paths for comparison (fs.FS uses forward slashes)
			normalizedPath := filepath.ToSlash(path)
			found := false
			for _, existing := range allMatches {
				normalizedExisting := filepath.ToSlash(existing)
				if normalizedExisting == normalizedPath {
					found = true
					break
				}
			}
			if !found {
				allMatches = append(allMatches, path)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to walk filesystem: %w", err)
	}

	if len(allMatches) == 0 {
		return nil, errors.New("no supported files found in filesystem")
	}

	// Create ReaderInput for each matched file
	for _, match := range allMatches {
		// Open the file from FS
		file, err := filesystem.Open(match)
		if err != nil {
			return nil, fmt.Errorf("failed to open FS file %s: %w", match, err)
		}

		// Determine file type from extension using model.NewFile
		fileInfo := model.NewFile(match)
		fileType := fileInfo.Type()

		// Generate table name from file path (remove extension and clean up)
		tableName := model.TableFromFilePath(match)

		// Create ReaderInput
		readerInput := ReaderInput{
			Reader:    file,
			TableName: tableName,
			FileType:  fileType,
		}

		readers = append(readers, readerInput)
	}
	return readers, nil
}

// streamReaderToSQLite streams data from io.Reader directly to SQLite database
// This is the ideal approach that provides true streaming with chunk-based processing
func (b *DBBuilder) streamReaderToSQLite(ctx context.Context, db *sql.DB, input ReaderInput) error {
	// Create streaming parser for chunked processing
	parser := model.NewStreamingParser(input.FileType, input.TableName, b.defaultChunkSize)

	// Initialize the table schema (we need to peek at the first chunk to get headers)
	var tableCreated bool
	var insertStmt *sql.Stmt

	// Process data in chunks
	err := parser.ProcessInChunks(input.Reader, func(chunk *model.TableChunk) error {
		// Create table on first chunk
		if !tableCreated {
			if err := b.createTableFromChunk(ctx, db, chunk); err != nil {
				return fmt.Errorf("failed to create table: %w", err)
			}

			// Prepare insert statement
			var err error
			insertStmt, err = b.prepareInsertStatement(ctx, db, chunk) //nolint:sqlclosecheck // Statement is closed after processing
			if err != nil {
				return fmt.Errorf("failed to prepare insert statement: %w", err)
			}

			tableCreated = true
		}

		// Insert chunk data
		if err := b.insertChunkData(ctx, insertStmt, chunk); err != nil {
			return fmt.Errorf("failed to insert chunk data: %w", err)
		}

		return nil
	})

	// Clean up the prepared statement
	if insertStmt != nil {
		_ = insertStmt.Close() // Ignore close error during statement cleanup
	}

	if err != nil {
		return fmt.Errorf("streaming processing failed: %w", err)
	}

	return nil
}

// createTableFromChunk creates a SQLite table from a TableChunk
func (b *DBBuilder) createTableFromChunk(ctx context.Context, db *sql.DB, chunk *model.TableChunk) error {
	columnInfo := chunk.ColumnInfo()
	columns := make([]string, 0, len(columnInfo))
	for _, col := range columnInfo {
		columns = append(columns, fmt.Sprintf(`"%s" %s`, col.Name, col.Type.String()))
	}

	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (%s)`,
		chunk.TableName(),
		strings.Join(columns, ", "),
	)

	_, err := db.ExecContext(ctx, query)
	return err
}

// createTableFromModel creates a SQLite table from a model.Table and inserts all data
func (b *DBBuilder) createTableFromModel(ctx context.Context, db *sql.DB, table *model.Table) error {
	columnInfo := table.ColumnInfo()
	columns := make([]string, 0, len(columnInfo))
	for _, col := range columnInfo {
		columns = append(columns, fmt.Sprintf(`"%s" %s`, col.Name, col.Type.String()))
	}

	// Create table
	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (%s)`,
		table.Name(),
		strings.Join(columns, ", "),
	)

	if _, err := db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Insert all data
	headers := table.Header()
	placeholders := make([]string, len(headers))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	insertQuery := fmt.Sprintf( //nolint:gosec // Table name is from validated input
		`INSERT INTO "%s" VALUES (%s)`,
		table.Name(),
		strings.Join(placeholders, ", "),
	)

	stmt, err := db.PrepareContext(ctx, insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Insert all records
	for _, record := range table.Records() {
		values := make([]any, len(record))
		for i, value := range record {
			values[i] = value
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("failed to insert record: %w", err)
		}
	}

	return nil
}

// prepareInsertStatement prepares an insert statement for the table
func (b *DBBuilder) prepareInsertStatement(ctx context.Context, db *sql.DB, chunk *model.TableChunk) (*sql.Stmt, error) {
	headers := chunk.Headers()
	placeholders := make([]string, len(headers))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf(
		`INSERT INTO "%s" VALUES (%s)`,
		chunk.TableName(),
		strings.Join(placeholders, ", "),
	)

	return db.PrepareContext(ctx, query)
}

// insertChunkData inserts a chunk's worth of data using a prepared statement
func (b *DBBuilder) insertChunkData(ctx context.Context, stmt *sql.Stmt, chunk *model.TableChunk) error {
	for _, record := range chunk.Records() {
		values := make([]any, len(record))
		for i, value := range record {
			values[i] = value
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("failed to insert record: %w", err)
		}
	}

	return nil
}
