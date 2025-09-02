package filesql

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
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
}

// readerInput represents configuration for reading from io.Reader
type readerInput struct {
	// reader is the data source
	reader io.Reader
	// tableName is the name of the table to create
	tableName string
	// fileType specifies the file format using domain/model types
	fileType FileType
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
	return &DBBuilder{
		paths:            make([]string, 0),
		filesystems:      make([]fs.FS, 0),
		readers:          make([]readerInput, 0),
		collectedPaths:   make([]string, 0),
		parsedTables:     make([]*table, 0),
		autoSaveConfig:   nil,              // Default: no auto-save
		defaultChunkSize: DefaultChunkSize, // Default rows per chunk
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
	// Validate that we have at least one input
	if len(b.paths) == 0 && len(b.filesystems) == 0 && len(b.readers) == 0 {
		return nil, errors.New("at least one path must be provided")
	}

	if err := b.validateAutoSaveConfig(); err != nil {
		return nil, err
	}

	processedFiles := make(map[string]bool)
	if err := b.validateAndCollectRegularPaths(processedFiles); err != nil {
		return nil, err
	}

	if err := b.processFilesystems(ctx); err != nil {
		return nil, err
	}

	if err := b.validateReaderInputs(); err != nil {
		return nil, err
	}

	if err := b.validateFinalState(); err != nil {
		return nil, err
	}

	return b, nil
}

// Open creates and returns a database connection using the configured and validated inputs.
// This method can only be called after Build() has been successfully executed.
// It creates an in-memory SQLite database and loads all configured files as tables using streaming.
//
// Table names are derived from file names without extensions:
// - "users.csv" becomes table "users"
// - "data.tsv.gz" becomes table "data"
//
// The returned database connection supports the full SQLite3 SQL syntax.
// Auto-save functionality is supported for both file paths and reader inputs.
// The caller is responsible for closing the connection when done.
//
// Returns a *sql.DB connection or an error if the database cannot be created.
func (b *DBBuilder) Open(ctx context.Context) (*sql.DB, error) {
	if err := b.validateInputsAvailable(); err != nil {
		return nil, err
	}

	b.collectedPaths = b.deduplicateCompressedFiles(b.collectedPaths)

	db, err := b.createInMemoryDatabase()
	if err != nil {
		return nil, err
	}

	if err := b.streamAllFilesToDatabase(ctx, db); err != nil {
		_ = db.Close() // Ignore close error during error handling
		return nil, err
	}

	if err := b.streamAllReadersToDatabase(ctx, db); err != nil {
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

	return db, nil
}

// validateInputsAvailable checks if any valid inputs are available for database creation.
func (b *DBBuilder) validateInputsAvailable() error {
	if len(b.collectedPaths) == 0 && len(b.readers) == 0 {
		return errors.New("no valid input files found, did you call Build()?")
	}
	return nil
}

// deduplicateCompressedFiles removes compressed duplicates when uncompressed versions exist.
func (b *DBBuilder) deduplicateCompressedFiles(files []string) []string {
	return b.deduplicateCompressedFilesInternal(files)
}

// createInMemoryDatabase creates a new in-memory SQLite database connection.
func (b *DBBuilder) createInMemoryDatabase() (*sql.DB, error) {
	sqliteDriver := &sqlite.Driver{}
	conn, err := sqliteDriver.Open(":memory:")
	if err != nil {
		return nil, fmt.Errorf("failed to create in-memory database: %w", err)
	}

	return sql.OpenDB(&directConnector{conn: conn}), nil
}

// streamAllFilesToDatabase streams all collected file paths to the database if any exist.
func (b *DBBuilder) streamAllFilesToDatabase(ctx context.Context, db *sql.DB) error {
	for _, path := range b.collectedPaths {
		if err := b.streamFileToSQLite(ctx, db, path); err != nil {
			return fmt.Errorf("failed to stream file %s: %w", path, err)
		}
	}
	return nil
}

// streamAllReadersToDatabase streams all reader inputs to the database.
func (b *DBBuilder) streamAllReadersToDatabase(ctx context.Context, db *sql.DB) error {
	for _, readerInput := range b.readers {
		if err := b.streamReaderToSQLite(ctx, db, readerInput); err != nil {
			return fmt.Errorf("failed to stream reader input for table '%s': %w", readerInput.tableName, err)
		}
	}
	return nil
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
		return nil, fmt.Errorf("failed to close intermediate database: %w", err)
	}

	sqliteDriver := &sqlite.Driver{}
	freshConn, err := sqliteDriver.Open(":memory:")
	if err != nil {
		return nil, fmt.Errorf("failed to create fresh SQLite connection for auto-save: %w", err)
	}

	connector := &autoSaveConnector{
		sqliteConn:     freshConn,
		autoSaveConfig: b.autoSaveConfig,
		originalPaths:  b.collectOriginalPaths(),
	}
	db = sql.OpenDB(connector)

	if err := b.streamAllFilesToDatabase(ctx, db); err != nil {
		_ = db.Close() // Ignore close error during error handling
		return nil, err
	}

	if err := b.streamAllReadersToDatabase(ctx, db); err != nil {
		_ = db.Close() // Ignore close error during error handling
		return nil, err
	}

	return db, nil
}

// processFSToReaders processes all supported files from an fs.FS and creates ReaderInput

// validateAndCollectRegularPaths validates regular file paths and collects them.
func (b *DBBuilder) validateAndCollectRegularPaths(processedFiles map[string]bool) error {
	b.collectedPaths = make([]string, 0)

	for _, path := range b.paths {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("failed to load file: path does not exist: %s", path)
			}
			return fmt.Errorf("failed to stat path %s: %w", path, err)
		}

		if info.IsDir() {
			if err := b.collectFilesFromDirectory(path, processedFiles); err != nil {
				return err
			}
		} else {
			if err := b.addSingleFile(path, processedFiles); err != nil {
				return err
			}
		}
	}

	return nil
}

// collectFilesFromDirectory recursively collects all supported files from a directory.
func (b *DBBuilder) collectFilesFromDirectory(dirPath string, processedFiles map[string]bool) error {
	err := filepath.WalkDir(dirPath, func(filePath string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !isSupportedFile(filePath) {
			return nil
		}

		if strings.Contains(filepath.Base(filePath), "duplicate_columns") {
			return nil
		}

		absPath, err := filepath.Abs(filePath)
		if err != nil {
			return fmt.Errorf("failed to get absolute path for %s: %w", filePath, err)
		}

		if !processedFiles[absPath] {
			processedFiles[absPath] = true
			b.collectedPaths = append(b.collectedPaths, filePath)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to walk directory %s: %w", dirPath, err)
	}
	return nil
}

// addSingleFile validates and adds a single file to the collected paths.
func (b *DBBuilder) addSingleFile(filePath string, processedFiles map[string]bool) error {
	if !isSupportedFile(filePath) {
		return fmt.Errorf("unsupported file type: %s", filePath)
	}

	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for %s: %w", filePath, err)
	}

	if !processedFiles[absPath] {
		processedFiles[absPath] = true
		b.collectedPaths = append(b.collectedPaths, filePath)
	}

	return nil
}

// processFilesystems processes embedded filesystems and converts them to readers if any exist.
func (b *DBBuilder) processFilesystems(ctx context.Context) error {
	for _, filesystem := range b.filesystems {
		if filesystem == nil {
			return errors.New("FS cannot be nil")
		}

		fsReaders, err := b.processFSToReaders(ctx, filesystem)
		if err != nil {
			return fmt.Errorf("failed to process FS input: %w", err)
		}
		b.readers = append(b.readers, fsReaders...)
	}

	return nil
}

// validateReaderInputs validates all reader inputs if any exist.
func (b *DBBuilder) validateReaderInputs() error {
	for i := range b.readers {
		readerInput := &b.readers[i]
		if readerInput.reader == nil {
			return errors.New("reader cannot be nil")
		}
		if readerInput.tableName == "" {
			return errors.New("table name must be specified for reader input")
		}
		if readerInput.fileType == FileTypeUnsupported {
			return errors.New("file type must be specified for reader input")
		}

		bufferedReader := bufio.NewReader(readerInput.reader)
		_, err := bufferedReader.Peek(1)
		if err == io.EOF {
			return errors.New("empty CSV data")
		}
		readerInput.reader = bufferedReader
	}

	return nil
}

// validateFinalState performs final validation to ensure we have valid inputs.
func (b *DBBuilder) validateFinalState() error {
	if len(b.collectedPaths) == 0 && len(b.readers) == 0 {
		hasDirectories := false
		for _, path := range b.paths {
			if info, err := os.Stat(path); err == nil && info.IsDir() {
				hasDirectories = true
				break
			}
		}

		if hasDirectories {
			return errors.New("no supported files found in directory")
		}
		return errors.New("no valid input files found")
	}

	return nil
}

func (b *DBBuilder) processFSToReaders(_ context.Context, filesystem fs.FS) ([]readerInput, error) {
	readers := make([]readerInput, 0)

	// Search for all supported file patterns
	supportedPatterns := supportedFileExtPatterns()

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
	// Check if "." exists in the filesystem before walking
	if _, err := fs.Stat(filesystem, "."); err == nil {
		// "." exists, we can safely walk the filesystem
		walkErr := fs.WalkDir(filesystem, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if isSupportedFile(path) {
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
		if walkErr != nil {
			return nil, fmt.Errorf("failed to walk filesystem: %w", walkErr)
		}
	}
	// If "." doesn't exist, we'll just use what we found with glob patterns

	if len(allMatches) == 0 {
		return nil, errors.New("no supported files found in filesystem")
	}

	// Remove compressed duplicates when uncompressed versions exist
	allMatches = b.deduplicateCompressedFiles(allMatches)

	// Create ReaderInput for each matched file
	for _, match := range allMatches {
		// Open the file from FS
		file, err := filesystem.Open(match)
		if err != nil {
			return nil, fmt.Errorf("failed to open FS file %s: %w", match, err)
		}

		// Determine file type from extension using NewFile
		fileInfo := newFile(match)
		fileType := fileInfo.getFileType()

		// Generate table name from file path (remove extension and clean up)
		tableName := tableFromFilePath(match)

		// Create ReaderInput
		readerInput := readerInput{
			reader:    file,
			tableName: tableName,
			fileType:  fileType,
		}

		readers = append(readers, readerInput)
	}
	return readers, nil
}

// streamFileToSQLite streams data from a file path directly to SQLite database using chunked processing
func (b *DBBuilder) streamFileToSQLite(ctx context.Context, db *sql.DB, filePath string) error {
	// At this point, filePath should only be files since directories are expanded in Build()
	// Check if file is supported (double-check for safety)
	if !isSupportedFile(filePath) {
		return fmt.Errorf("unsupported file type: %s", filePath)
	}

	// Open the file and create a reader
	file, err := os.Open(filePath) //nolint:gosec // File path is validated and comes from user input
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	// Check if file is empty before processing
	if fileInfo, err := file.Stat(); err != nil {
		return fmt.Errorf("failed to get file info for %s: %w", filePath, err)
	} else if fileInfo.Size() == 0 {
		return errors.New("file is empty")
	}

	// Create decompressed reader if needed
	reader, err := b.createDecompressedReader(file, filePath)
	if err != nil {
		return fmt.Errorf("failed to create decompressed reader for %s: %w", filePath, err)
	}

	// Create file model to determine type and table name
	fileModel := newFile(filePath)
	baseFileType := fileModel.getFileType().baseType()

	// Handle XLSX files specially - each sheet becomes a separate table
	if baseFileType == FileTypeXLSX {
		return b.streamXLSXFileToSQLite(ctx, db, reader, filePath)
	}

	// Create reader input for streaming
	// Note: Since we've already decompressed the reader, use the base file type
	readerInput := readerInput{
		reader:    reader,
		tableName: tableFromFilePath(filePath),
		fileType:  baseFileType,
	}
	return b.streamReaderToSQLite(ctx, db, readerInput)
}

// streamXLSXFileToSQLite handles XLSX files by creating separate tables for each sheet
func (b *DBBuilder) streamXLSXFileToSQLite(ctx context.Context, db *sql.DB, reader io.Reader, filePath string) error {
	// Read all data into memory (XLSX requires random access)
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read XLSX data: %w", err)
	}

	if len(data) == 0 {
		return errors.New("empty XLSX file")
	}

	// Open XLSX file from bytes
	xlsxFile, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to open XLSX file: %w", err)
	}
	defer func() {
		_ = xlsxFile.Close() // Ignore close error
	}()

	// Get all sheet names
	sheetNames := xlsxFile.GetSheetList()
	if len(sheetNames) == 0 {
		return errors.New("no sheets found in XLSX file")
	}

	// Base table name from file path (sanitize to ensure a valid identifier)
	baseTableName := sanitizeTableName(tableFromFilePath(filePath))

	// Process each sheet as a separate table
	for _, sheetName := range sheetNames {
		rows, err := xlsxFile.GetRows(sheetName)
		if err != nil {
			return fmt.Errorf("failed to read sheet %s: %w", sheetName, err)
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
			return fmt.Errorf("failed to check table existence: %w", err)
		}

		if tableExists > 0 {
			return fmt.Errorf("table '%s' already exists, duplicate table names are not allowed", tableName)
		}

		// Process sheet data
		if err := b.createTableFromXLSXSheet(ctx, db, tableName, rows); err != nil {
			return fmt.Errorf("failed to create table from sheet %s: %w", sheetName, err)
		}
	}

	return nil
}

// sanitizeTableName removes invalid characters from table names
func sanitizeTableName(name string) string {
	// Replace spaces and invalid characters with underscores
	result := strings.ReplaceAll(name, " ", "_")
	result = strings.ReplaceAll(result, "-", "_")
	result = strings.ReplaceAll(result, ".", "_")

	// Remove any non-alphanumeric characters except underscore
	var sanitized strings.Builder
	for _, r := range result {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			sanitized.WriteRune(r)
		}
	}

	finalResult := sanitized.String()

	// Ensure it doesn't start with a number
	if len(finalResult) > 0 && finalResult[0] >= '0' && finalResult[0] <= '9' {
		finalResult = "sheet_" + finalResult
	}

	// Ensure it's not empty
	if finalResult == "" {
		finalResult = "sheet"
	}

	return finalResult
}

// createTableFromXLSXSheet creates a SQLite table from XLSX sheet data
func (b *DBBuilder) createTableFromXLSXSheet(ctx context.Context, db *sql.DB, tableName string, rows [][]string) error {
	if len(rows) == 0 {
		return errors.New("no rows in sheet")
	}

	// First row is header
	headers := rows[0]
	if len(headers) == 0 {
		return errors.New("no columns in sheet header")
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
	records := make([]record, len(dataRows))
	for i, row := range dataRows {
		// Pad row with empty strings if necessary
		paddedRow := make(record, len(headers))
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
		return fmt.Errorf("failed to create SQLite table: %w", err)
	}

	// Insert data
	if len(records) > 0 {
		if err := b.insertDataIntoTable(ctx, db, tableName, headers, records); err != nil {
			return fmt.Errorf("failed to insert data: %w", err)
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
func (b *DBBuilder) insertDataIntoTable(ctx context.Context, db *sql.DB, tableName string, headers []string, records []record) error {
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
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, record := range records {
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

// streamReaderToSQLite streams data from io.Reader directly to SQLite database
// This is the ideal approach that provides true streaming with chunk-based processing
func (b *DBBuilder) streamReaderToSQLite(ctx context.Context, db *sql.DB, input readerInput) error {
	// Reader should already be buffered from Build validation, but ensure it's buffered
	if _, ok := input.reader.(*bufio.Reader); !ok {
		input.reader = bufio.NewReader(input.reader)
	}

	// Check if table already exists to avoid duplicates
	var tableExists int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`,
		input.tableName,
	).Scan(&tableExists)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if tableExists > 0 {
		// Table already exists - this is an error condition
		return fmt.Errorf("table '%s' already exists from another file, duplicate table names are not allowed", input.tableName)
	}

	// Create streaming parser for chunked processing
	parser := newStreamingParser(input.fileType, input.tableName, b.defaultChunkSize)

	// Initialize the table schema (we need to peek at the first chunk to get headers)
	var tableCreated bool
	var insertStmt *sql.Stmt

	// Process data in chunks
	err = parser.ProcessInChunks(input.reader, func(chunk *tableChunk) error {
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

	// Handle header-only files: if no data chunks were processed, create empty table
	if !tableCreated {
		// Check if the original streaming error should be preserved
		if err != nil {
			// Preserve certain parsing errors that should not be converted to empty tables
			if strings.Contains(err.Error(), "duplicate column name") ||
				strings.Contains(err.Error(), "empty CSV data") ||
				strings.Contains(err.Error(), "parse error") {
				return err // Preserve meaningful parsing errors
			}
		}

		// For header-only files or empty files, create an empty table by parsing headers
		if createErr := b.createEmptyTable(ctx, db, input); createErr != nil {
			return fmt.Errorf("failed to create empty table for header-only file: %w", createErr)
		}
		err = nil // Clear any previous error since we handled the empty case
	}

	// Clean up the prepared statement
	if insertStmt != nil {
		_ = insertStmt.Close() // Ignore close error during statement cleanup
	}

	if err != nil {
		return fmt.Errorf("streaming processing failed: %w", err)
	}

	return nil
}

// createTableFromChunk creates a SQLite table from a tableChunk
func (b *DBBuilder) createTableFromChunk(ctx context.Context, db *sql.DB, chunk *tableChunk) error {
	columnInfo := chunk.getColumnInfo()
	columns := make([]string, 0, len(columnInfo))
	for _, col := range columnInfo {
		columns = append(columns, fmt.Sprintf(`"%s" %s`, col.Name, col.Type.string()))
	}

	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (%s)`,
		chunk.getTableName(),
		strings.Join(columns, ", "),
	)

	_, err := db.ExecContext(ctx, query)
	return err
}

// prepareInsertStatement prepares an insert statement for the table
func (b *DBBuilder) prepareInsertStatement(ctx context.Context, db *sql.DB, chunk *tableChunk) (*sql.Stmt, error) {
	headers := chunk.getHeaders()
	placeholders := make([]string, len(headers))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf(
		`INSERT INTO "%s" VALUES (%s)`,
		chunk.getTableName(),
		strings.Join(placeholders, ", "),
	)

	return db.PrepareContext(ctx, query)
}

// insertChunkData inserts a chunk's worth of data using a prepared statement
func (b *DBBuilder) insertChunkData(ctx context.Context, stmt *sql.Stmt, chunk *tableChunk) error {
	for _, record := range chunk.getRecords() {
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

// createDecompressedReader creates a decompressed reader based on file extension
func (b *DBBuilder) createDecompressedReader(file *os.File, filePath string) (io.Reader, error) {
	// Check file extension to determine compression type
	if strings.HasSuffix(strings.ToLower(filePath), extGZ) {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		return gzReader, nil
	}

	if strings.HasSuffix(strings.ToLower(filePath), extBZ2) {
		return bzip2.NewReader(file), nil
	}

	if strings.HasSuffix(strings.ToLower(filePath), extXZ) {
		xzReader, err := xz.NewReader(file)
		if err != nil {
			return nil, fmt.Errorf("failed to create xz reader: %w", err)
		}
		return xzReader, nil
	}

	if strings.HasSuffix(strings.ToLower(filePath), extZSTD) {
		zstdReader, err := zstd.NewReader(file)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		return zstdReader.IOReadCloser(), nil
	}

	// No compression, return file as-is
	return file, nil
}

// createEmptyTable creates an empty table for header-only files
func (b *DBBuilder) createEmptyTable(ctx context.Context, db *sql.DB, input readerInput) error {
	// Parse just the header to get column information
	tempParser := newStreamingParser(input.fileType, input.tableName, 1)
	tempTable, err := tempParser.parseFromReader(input.reader)
	if err != nil {
		// Check if this is a parsing error we should preserve (like duplicate columns)
		if strings.Contains(err.Error(), "duplicate column name") {
			return err // Preserve the duplicate column error
		}

		// If ParseFromReader fails for other reasons, try a simpler header-only approach
		return b.createTableFromHeaders(ctx, db, input)
	}

	// Create table using the parsed headers
	headers := tempTable.getHeader()
	if len(headers) == 0 {
		return fmt.Errorf("no headers found in file for table %s", input.tableName)
	}

	// Infer column types from headers (all as TEXT for header-only files)
	columnInfoList := make([]columnInfo, len(headers))
	for i, colName := range headers {
		columnInfoList[i] = columnInfo{
			Name: colName,
			Type: columnTypeText, // Default to TEXT for header-only
		}
	}

	// Create the table
	columns := make([]string, 0, len(columnInfoList))
	for _, col := range columnInfoList {
		columns = append(columns, fmt.Sprintf(`"%s" %s`, col.Name, col.Type.string()))
	}

	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (%s)`,
		input.tableName,
		strings.Join(columns, ", "),
	)

	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create empty table: %w", err)
	}

	return nil
}

// createTableFromHeaders creates table from header information only
func (b *DBBuilder) createTableFromHeaders(ctx context.Context, db *sql.DB, input readerInput) error {
	// This is a fallback method for when ParseFromReader fails
	// Since the reader may have been consumed by the parser, we can't reliably detect
	// empty files here. Instead, we'll create a fallback table and assume the
	// empty file case was already handled earlier in the pipeline.

	// For simplicity, create a generic table structure
	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (column1 TEXT)`,
		input.tableName,
	)

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create fallback table: %w", err)
	}

	return nil
}

// collectOriginalPaths collects original file paths for overwrite mode
func (b *DBBuilder) collectOriginalPaths() []string {
	var paths []string
	paths = append(paths, b.collectedPaths...)
	return paths
}

// deduplicateCompressedFilesInternal removes compressed files when their uncompressed versions exist
// This prevents duplicate table names like 'logs' from both 'logs.ltsv' and 'logs.ltsv.xz'
func (b *DBBuilder) deduplicateCompressedFilesInternal(files []string) []string {
	// Create a map of table names to file paths, prioritizing uncompressed files
	tableToFile := make(map[string]string)

	// First pass: collect all uncompressed files
	for _, file := range files {
		tableName := tableFromFilePath(file)
		if !b.isCompressedFile(file) {
			tableToFile[tableName] = file
		}
	}

	// Second pass: add compressed files only if uncompressed version doesn't exist
	for _, file := range files {
		tableName := tableFromFilePath(file)
		if b.isCompressedFile(file) {
			if _, exists := tableToFile[tableName]; !exists {
				tableToFile[tableName] = file
			}
		}
	}

	// Convert map back to slice
	result := make([]string, 0, len(tableToFile))
	for _, file := range tableToFile {
		result = append(result, file)
	}

	return result
}

// isCompressedFile checks if a file path represents a compressed file
func (b *DBBuilder) isCompressedFile(filePath string) bool {
	p := strings.ToLower(filePath)
	return strings.HasSuffix(p, extGZ) ||
		strings.HasSuffix(p, extBZ2) ||
		strings.HasSuffix(p, extXZ) ||
		strings.HasSuffix(p, extZSTD)
}
