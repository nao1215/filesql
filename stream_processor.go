package filesql

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/xuri/excelize/v2"
)

// Structured log attribute keys used across stream processing.
const (
	logKeyTable = "table"
	logKeySheet = "sheet"
)

// streamError wraps stream processing errors with sentinel errors for errors.Is() compatibility.
func streamError(sentinel error, format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{sentinel}, args...)...)
}

// streamProcessor handles streaming operations for database loading
type streamProcessor struct {
	chunkSize int
	logger    Logger
	// replaceExisting makes table creation drop a same-named table first instead
	// of erroring or appending. It is enabled for LoadInto (loading into a
	// caller-owned database, where last-wins replacement is the useful default)
	// and left false for Open (fresh in-memory database, no collisions).
	replaceExisting bool
}

// dropIfReplacing drops a same-named table when replaceExisting is set, so the
// following CREATE installs the file's schema and rows in place of any prior
// table. It is a no-op in Open mode.
func (sp *streamProcessor) dropIfReplacing(ctx context.Context, db *sql.DB, tableName string) error {
	if !sp.replaceExisting {
		return nil
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS "`+tableName+`"`); err != nil {
		return fmt.Errorf("%w: failed to drop existing table %q: %s", ErrDatabaseOperation, tableName, err.Error())
	}
	return nil
}

// newStreamProcessor creates a new stream processor instance
func newStreamProcessor(chunkSize int) *streamProcessor {
	return &streamProcessor{
		chunkSize: chunkSize,
		logger:    newNopLogger(),
	}
}

// setLogger sets the logger for the stream processor
func (sp *streamProcessor) setLogger(logger Logger) {
	if logger != nil {
		sp.logger = logger
	}
}

// streamAllFilesToDatabase streams all collected file paths to the database
func (sp *streamProcessor) streamAllFilesToDatabase(ctx context.Context, db *sql.DB, collectedPaths []string) error {
	sp.logger.Info("starting file streaming", "file_count", len(collectedPaths))
	for i, path := range collectedPaths {
		sp.logger.Debug("streaming file", "path", path, "index", i+1, "total", len(collectedPaths))
		if err := sp.streamFileToDatabase(ctx, db, path); err != nil {
			sp.logger.Error("failed to stream file", "path", path, "error", err)
			return streamError(ErrParsing, "failed to stream file %s: %v", path, err)
		}
	}
	sp.logger.Info("completed file streaming", "file_count", len(collectedPaths))
	return nil
}

// streamAllReadersToDatabase streams all reader inputs to the database
func (sp *streamProcessor) streamAllReadersToDatabase(ctx context.Context, db *sql.DB, readers []readerInput) error {
	if len(readers) == 0 {
		return nil
	}
	sp.logger.Info("starting reader streaming", "reader_count", len(readers))
	for i, ri := range readers {
		sp.logger.Debug("streaming reader", logKeyTable, ri.tableName, "file_type", ri.fileType.String(), "index", i+1, "total", len(readers))
		if err := sp.streamReaderToDatabase(ctx, db, ri); err != nil {
			sp.closeReaderInput(ri)
			sp.logger.Error("failed to stream reader", logKeyTable, ri.tableName, "error", err)
			return streamError(ErrParsing, "failed to stream reader input for table '%s': %v", ri.tableName, err)
		}
		sp.closeReaderInput(ri)
	}
	sp.logger.Info("completed reader streaming", "reader_count", len(readers))
	return nil
}

// closeReaderInput closes the underlying resource of a reader input if it was opened internally (e.g. from AddFS).
func (sp *streamProcessor) closeReaderInput(ri readerInput) {
	if ri.closer != nil {
		if err := ri.closer.Close(); err != nil {
			sp.logger.Debug("error closing reader input", logKeyTable, ri.tableName, "error", err)
		}
	}
}

// streamFileToDatabase streams data from a file path directly to SQLite database using chunked processing
func (sp *streamProcessor) streamFileToDatabase(ctx context.Context, db *sql.DB, filePath string) error {
	// Check if file is ACH format
	if isACHFile(filePath) {
		sp.logger.Debug("detected ACH file format", "path", filePath)
		return sp.streamACHFileToDatabase(ctx, db, filePath)
	}

	// Check if file is Fedwire format
	if isFedWireFile(filePath) {
		sp.logger.Debug("detected Fedwire file format", "path", filePath)
		return sp.streamFedWireFileToDatabase(ctx, db, filePath)
	}

	// Check if file is supported
	if !isSupportedFile(filePath) {
		sp.logger.Warn("unsupported file format", "path", filePath)
		return fmt.Errorf("%w: %s", ErrUnsupportedFormat, filePath)
	}

	// Open the file and create a reader
	file, err := os.Open(filePath) //nolint:gosec // File path is validated and comes from user input
	if err != nil {
		sp.logger.Error("failed to open file", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to open file %s: %s", ErrIOOperation, filePath, err.Error())
	}
	defer file.Close()

	// Check if file is empty before processing
	fileInfo, err := file.Stat()
	if err != nil {
		sp.logger.Error("failed to get file info", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to get file info for %s: %s", ErrIOOperation, filePath, err.Error())
	}
	if fileInfo.Size() == 0 {
		sp.logger.Warn("empty file detected", "path", filePath)
		return fmt.Errorf("%w: file is empty", ErrEmptyData)
	}
	sp.logger.Debug("file opened", "path", filePath, "size", fileInfo.Size())

	// Create file model to determine type and table name
	fileModel := newFile(filePath)
	baseFileType := fileModel.getFileType().baseType()
	sp.logger.Debug("detected file type", "path", filePath, "type", baseFileType.String())

	// Create decompressed reader if needed
	reader, closer, err := sp.createDecompressedReader(file, filePath)
	if err != nil {
		sp.logger.Error("failed to create decompressed reader", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to create decompressed reader for %s: %s", ErrCompression, filePath, err.Error())
	}
	if closer != nil {
		sp.logger.Debug("compression detected, created decompressed reader", "path", filePath)
	}
	defer func() {
		if closer != nil {
			if closeErr := closer(); closeErr != nil {
				sp.logger.Debug("error closing decompressed reader", "path", filePath, "error", closeErr)
			}
		}
	}()

	// Handle XLSX files specially - each sheet becomes a separate table
	if baseFileType == FileTypeXLSX {
		sp.logger.Debug("processing XLSX file with multiple sheets", "path", filePath)
		return sp.streamXLSXFileToDatabase(ctx, db, reader, filePath)
	}

	// Create reader input for streaming
	tableName := sanitizeTableName(tableFromFilePath(filePath))
	sp.logger.Debug("streaming file to table", "path", filePath, logKeyTable, tableName, "type", baseFileType.String())
	readerInput := readerInput{
		reader:    reader, // Use decompressed reader
		tableName: tableName,
		fileType:  baseFileType,
	}
	return sp.streamReaderToDatabase(ctx, db, readerInput)
}

// streamACHFileToDatabase handles ACH files by creating multiple tables
func (sp *streamProcessor) streamACHFileToDatabase(ctx context.Context, db *sql.DB, filePath string) error {
	sp.logger.Debug("processing ACH file", "path", filePath)

	// Open the file
	file, err := os.Open(filePath) //nolint:gosec // File path is validated and comes from user input
	if err != nil {
		sp.logger.Error("failed to open ACH file", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to open ACH file %s: %s", ErrIOOperation, filePath, err.Error())
	}
	defer file.Close()

	// Check if file is empty
	fileInfo, err := file.Stat()
	if err != nil {
		sp.logger.Error("failed to get ACH file info", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to get file info for %s: %s", ErrIOOperation, filePath, err.Error())
	}
	if fileInfo.Size() == 0 {
		sp.logger.Warn("empty ACH file detected", "path", filePath)
		return fmt.Errorf("%w: ACH file is empty", ErrEmptyData)
	}

	sp.logger.Debug("streaming ACH file to database", "path", filePath, "size", fileInfo.Size())
	return streamACHFileToDatabase(ctx, db, file, filePath, sp.replaceExisting)
}

// streamFedWireFileToDatabase handles Fedwire files by creating a single message table
func (sp *streamProcessor) streamFedWireFileToDatabase(ctx context.Context, db *sql.DB, filePath string) error {
	sp.logger.Debug("processing Fedwire file", "path", filePath)

	// Open the file
	file, err := os.Open(filePath) //nolint:gosec // File path is validated and comes from user input
	if err != nil {
		sp.logger.Error("failed to open Fedwire file", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to open Fedwire file %s: %s", ErrIOOperation, filePath, err.Error())
	}
	defer file.Close()

	// Check if file is empty
	fileInfo, err := file.Stat()
	if err != nil {
		sp.logger.Error("failed to get Fedwire file info", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to get file info for %s: %s", ErrIOOperation, filePath, err.Error())
	}
	if fileInfo.Size() == 0 {
		sp.logger.Warn("empty Fedwire file detected", "path", filePath)
		return fmt.Errorf("%w: Fedwire file is empty", ErrEmptyData)
	}

	sp.logger.Debug("streaming Fedwire file to database", "path", filePath, "size", fileInfo.Size())
	return streamWireFileToDatabase(ctx, db, file, filePath, sp.replaceExisting)
}

// streamReaderToDatabase streams data from io.Reader directly to SQLite database
func (sp *streamProcessor) streamReaderToDatabase(ctx context.Context, db *sql.DB, input readerInput) error {
	// Route ACH/Fedwire readers to dedicated handlers
	if input.fileType == FileTypeACH {
		return streamACHFileToDatabase(ctx, db, input.reader, input.tableName+extACH, sp.replaceExisting)
	}
	if input.fileType == FileTypeFedWire {
		return streamWireFileToDatabase(ctx, db, input.reader, input.tableName+extFED, sp.replaceExisting)
	}

	// Reader should already be validated at Build time, but ensure it's buffered
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
		return fmt.Errorf("%w: failed to check table existence: %s", ErrDatabaseOperation, err.Error())
	}

	if tableExists > 0 && !sp.replaceExisting {
		sp.logger.Warn("table already exists", logKeyTable, input.tableName)
		return fmt.Errorf("%w: table '%s' already exists from another file", ErrDuplicateTable, input.tableName)
	}
	// When replacing, createTableFromChunk drops the old table before recreating it.

	// Create streaming parser for chunked processing
	parser := newStreamingParser(input.fileType, input.tableName, sp.chunkSize)

	// Initialize the table schema (we need to peek at the first chunk to get headers)
	var tableCreated bool
	var insertStmt *sql.Stmt
	var tx *sql.Tx

	// Process data in chunks with transaction batching for performance
	var chunkCount int
	var totalRows int
	err = parser.ProcessInChunks(input.reader, func(chunk *tableChunk) error {
		chunkCount++
		// Create table on first chunk
		if !tableCreated {
			sp.logger.Debug("creating table", logKeyTable, input.tableName, "columns", len(chunk.getHeaders()))
			if err := sp.createTableFromChunk(ctx, db, chunk); err != nil {
				return fmt.Errorf("%w: failed to create table: %s", ErrDatabaseOperation, err.Error())
			}

			// Start transaction for bulk inserts - significantly improves performance
			// by reducing disk sync operations
			var err error
			tx, err = db.BeginTx(ctx, nil)
			if err != nil {
				return fmt.Errorf("%w: failed to begin transaction: %s", ErrDatabaseOperation, err.Error())
			}

			// Prepare insert statement within transaction
			insertStmt, err = sp.prepareInsertStatementTx(ctx, tx, chunk) //nolint:sqlclosecheck // Statement is closed after processing
			if err != nil {
				_ = tx.Rollback() //nolint:errcheck // Ignore rollback error during error handling
				return fmt.Errorf("%w: failed to prepare insert statement: %s", ErrDatabaseOperation, err.Error())
			}

			tableCreated = true
			sp.logger.Info("table created", logKeyTable, input.tableName)
		}

		// Insert chunk data
		rowsInChunk := len(chunk.getRecords())
		totalRows += rowsInChunk
		sp.logger.Debug("inserting chunk", logKeyTable, input.tableName, "chunk", chunkCount, "rows", rowsInChunk)
		if err := sp.insertChunkData(ctx, insertStmt, chunk); err != nil {
			return fmt.Errorf("%w: failed to insert chunk data: %s", ErrDatabaseOperation, err.Error())
		}

		return nil
	})

	// Handle transaction commit/rollback
	if tx != nil {
		if err != nil {
			sp.logger.Debug("rolling back transaction", logKeyTable, input.tableName, "error", err)
			_ = tx.Rollback() //nolint:errcheck // Ignore rollback error during error handling
		} else {
			if commitErr := tx.Commit(); commitErr != nil {
				return fmt.Errorf("%w: failed to commit transaction: %s", ErrDatabaseOperation, commitErr.Error())
			}
			sp.logger.Debug("committed transaction", logKeyTable, input.tableName, "chunks", chunkCount, "total_rows", totalRows)
		}
	}

	// Handle header-only files: if no data chunks were processed, create empty table
	if !tableCreated {
		if err != nil {
			// Preserve certain parsing errors that should not be converted to empty tables
			if strings.Contains(err.Error(), "duplicate column name") ||
				strings.Contains(err.Error(), "parse error") {
				return err
			}
			// For completely empty files (only newlines), propagate error instead of creating empty table
			if strings.Contains(err.Error(), "empty") {
				return err
			}
			// For LTSV with no valid key:value pairs, propagate the error
			if strings.Contains(err.Error(), "no valid LTSV") {
				return err
			}
		}

		// For header-only files, try to create an empty table by parsing headers
		if createErr := sp.createEmptyTable(ctx, db, input); createErr != nil {
			// If createEmptyTable also fails, this indicates a truly empty file
			if err != nil {
				return err // Return the original processing error
			}
			return fmt.Errorf("%w: failed to create empty table for header-only file: %s", ErrDatabaseOperation, createErr.Error())
		}
		err = nil // Clear any previous error since we handled the header-only case
	}

	// Clean up the prepared statement
	if insertStmt != nil {
		_ = insertStmt.Close() // Ignore close error during statement cleanup
	}

	if err != nil {
		return fmt.Errorf("%w: streaming processing failed: %s", ErrParsing, err.Error())
	}

	return nil
}

// createTableFromChunk creates a SQLite table from a tableChunk
func (sp *streamProcessor) createTableFromChunk(ctx context.Context, db *sql.DB, chunk *tableChunk) error {
	columnInfo := chunk.getColumnInfo()
	columns := make([]string, 0, len(columnInfo))
	for _, col := range columnInfo {
		columns = append(columns, fmt.Sprintf(`"%s" %s`, col.Name, col.Type.string()))
	}

	if err := sp.dropIfReplacing(ctx, db, chunk.getTableName()); err != nil {
		return err
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
func (sp *streamProcessor) prepareInsertStatement(ctx context.Context, db *sql.DB, chunk *tableChunk) (*sql.Stmt, error) {
	query := sp.buildInsertQuery(chunk)
	return db.PrepareContext(ctx, query)
}

// prepareInsertStatementTx prepares an insert statement within a transaction
func (sp *streamProcessor) prepareInsertStatementTx(ctx context.Context, tx *sql.Tx, chunk *tableChunk) (*sql.Stmt, error) {
	query := sp.buildInsertQuery(chunk)
	return tx.PrepareContext(ctx, query)
}

// buildInsertQuery builds the INSERT query string for a chunk
func (sp *streamProcessor) buildInsertQuery(chunk *tableChunk) string {
	headers := chunk.getHeaders()
	placeholders := make([]string, len(headers))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	return fmt.Sprintf(
		`INSERT INTO "%s" VALUES (%s)`,
		chunk.getTableName(),
		strings.Join(placeholders, ", "),
	)
}

// insertChunkData inserts a chunk's worth of data using a prepared statement.
// Performance is optimized by reusing a single values slice to reduce allocations.
func (sp *streamProcessor) insertChunkData(ctx context.Context, stmt *sql.Stmt, chunk *tableChunk) error {
	records := chunk.getRecords()
	if len(records) == 0 {
		return nil
	}

	// Use header count as the authoritative column count to ensure consistency
	colCount := len(chunk.getHeaders())
	values := make([]any, colCount)
	nulls := chunk.getNulls()

	for rowIdx, record := range records {
		// Fail fast if record has more columns than headers to prevent silent data truncation
		if len(record) > colCount {
			return fmt.Errorf("%w: record has more columns (%d) than headers (%d)", ErrColumnMismatch, len(record), colCount)
		}

		// Fill values slice based on header count, handling records with fewer columns
		// by setting missing columns to nil (NULL in SQLite)
		for i := range colCount {
			switch {
			case nulls != nil && rowIdx < len(nulls) && i < len(nulls[rowIdx]) && nulls[rowIdx][i]:
				values[i] = nil // a source NULL (e.g. a Parquet null) inserts as SQL NULL
			case i < len(record):
				values[i] = record[i]
			default:
				values[i] = nil
			}
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("%w: failed to insert record: %s", ErrDatabaseOperation, err.Error())
		}
	}

	return nil
}

// createEmptyTable creates an empty table for header-only files
func (sp *streamProcessor) createEmptyTable(ctx context.Context, db *sql.DB, input readerInput) error {
	// Parse just the header to get column information
	tempParser := newStreamingParser(input.fileType, input.tableName, 1)
	tempTable, err := tempParser.parseFromReader(input.reader)
	if err != nil {
		// Check if this is a parsing error we should preserve (like duplicate columns)
		if strings.Contains(err.Error(), "duplicate column name") {
			return err
		}
		// Don't propagate "empty CSV data" errors in createEmptyTable
		// This function is called to handle header-only files, which is valid

		// If ParseFromReader fails for other reasons, try a simpler header-only approach
		return sp.createTableFromHeaders(ctx, db, input)
	}

	// Create table using the parsed headers
	headers := tempTable.getHeader()
	if len(headers) == 0 {
		return fmt.Errorf("%w: no headers found in file for table %s", ErrEmptyData, input.tableName)
	}

	// Infer column types from headers (all as TEXT for header-only files)
	columnInfoList := make([]columnInfo, len(headers))
	for i, colName := range headers {
		columnInfoList[i] = columnInfo{
			Name: colName,
			Type: columnTypeText,
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
		return fmt.Errorf("%w: failed to create empty table: %s", ErrDatabaseOperation, err.Error())
	}

	return nil
}

// createTableFromHeaders creates table from header information only (fallback method)
func (sp *streamProcessor) createTableFromHeaders(ctx context.Context, db *sql.DB, input readerInput) error {
	// Create a fallback table structure
	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (column1 TEXT)`,
		input.tableName,
	)

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("%w: failed to create fallback table: %s", ErrDatabaseOperation, err.Error())
	}

	return nil
}

// createDecompressedReader creates a reader that handles compression
func (sp *streamProcessor) createDecompressedReader(file *os.File, filePath string) (io.Reader, func() error, error) {
	factory := NewCompressionFactory()
	handler := factory.CreateHandlerForFile(filePath)

	reader, cleanup, err := handler.CreateReader(file)
	if err != nil {
		return nil, nil, err
	}

	// Return reader with cleanup that doesn't close the file (caller handles that)
	return reader, cleanup, nil
}

// streamXLSXFileToDatabase handles XLSX files by creating separate tables for each sheet
func (sp *streamProcessor) streamXLSXFileToDatabase(ctx context.Context, db *sql.DB, reader io.Reader, filePath string) error {
	sp.logger.Debug("reading XLSX data into memory", "path", filePath)

	// Read all data into memory (XLSX requires random access)
	data, err := io.ReadAll(reader)
	if err != nil {
		sp.logger.Error("failed to read XLSX data", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to read XLSX data: %s", ErrIOOperation, err.Error())
	}

	if len(data) == 0 {
		sp.logger.Warn("empty XLSX file", "path", filePath)
		return fmt.Errorf("%w: empty XLSX file", ErrEmptyData)
	}
	sp.logger.Debug("XLSX data loaded", "path", filePath, "size", len(data))

	// Open XLSX file from bytes
	xlsxFile, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		sp.logger.Error("failed to open XLSX file", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to open XLSX file: %s", ErrParsing, err.Error())
	}
	defer func() {
		_ = xlsxFile.Close() // Ignore close error
	}()

	// Get all sheet names
	sheetNames := xlsxFile.GetSheetList()
	if len(sheetNames) == 0 {
		sp.logger.Warn("no sheets found in XLSX file", "path", filePath)
		return fmt.Errorf("%w: no sheets found in XLSX file", ErrEmptyData)
	}
	sp.logger.Info("processing XLSX file", "path", filePath, "sheet_count", len(sheetNames))

	// Base table name from file path (sanitize to ensure a valid identifier)
	baseTableName := sanitizeTableName(tableFromFilePath(filePath))

	// Process each sheet as a separate table
	for i, sheetName := range sheetNames {
		sp.logger.Debug("processing sheet", "path", filePath, logKeySheet, sheetName, "index", i+1, "total", len(sheetNames))
		rows, err := xlsxFile.GetRows(sheetName)
		if err != nil {
			sp.logger.Error("failed to read sheet", "path", filePath, logKeySheet, sheetName, "error", err)
			return fmt.Errorf("%w: failed to read sheet %s: %s", ErrParsing, sheetName, err.Error())
		}

		// Skip empty sheets
		if len(rows) == 0 {
			sp.logger.Debug("skipping empty sheet", "path", filePath, logKeySheet, sheetName)
			continue
		}

		// Create table name: filename_sheetname
		tableName := fmt.Sprintf("%s_%s", baseTableName, sanitizeTableName(sheetName))
		sp.logger.Debug("creating table from sheet", "path", filePath, logKeySheet, sheetName, logKeyTable, tableName, "rows", len(rows))

		// Check if table already exists
		var tableExists int
		err = db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`,
			tableName,
		).Scan(&tableExists)
		if err != nil {
			return fmt.Errorf("%w: failed to check table existence: %s", ErrDatabaseOperation, err.Error())
		}

		if tableExists > 0 && !sp.replaceExisting {
			return fmt.Errorf("%w: table '%s' already exists from another file", ErrDuplicateTable, tableName)
		}
		// When replacing, createTableFromChunk drops the old table before recreating it.

		// Convert XLSX rows to table headers and records
		headers, records := convertXLSXRowsToTable(rows)

		// Create table chunk for processing
		columnInfo := inferColumnsInfo(headers, records)
		chunk := &tableChunk{
			tableName:  tableName,
			headers:    headers,
			records:    records,
			columnInfo: columnInfo,
		}

		// Create table and insert data
		if err := sp.createTableFromChunk(ctx, db, chunk); err != nil {
			return fmt.Errorf("%w: failed to create table for sheet %s: %s", ErrDatabaseOperation, sheetName, err.Error())
		}

		// Prepare and execute insert statement
		insertStmt, err := sp.prepareInsertStatement(ctx, db, chunk)
		if err != nil {
			return fmt.Errorf("%w: failed to prepare insert statement for sheet %s: %s", ErrDatabaseOperation, sheetName, err.Error())
		}
		defer func() {
			_ = insertStmt.Close() // Ignore close error
		}()

		if err := sp.insertChunkData(ctx, insertStmt, chunk); err != nil {
			return fmt.Errorf("%w: failed to insert data for sheet %s: %s", ErrDatabaseOperation, sheetName, err.Error())
		}
	}

	return nil
}
