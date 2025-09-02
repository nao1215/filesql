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
	"os"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
	"github.com/xuri/excelize/v2"
)

// streamProcessor handles streaming operations for database loading
type streamProcessor struct {
	chunkSize int
}

// newStreamProcessor creates a new stream processor instance
func newStreamProcessor(chunkSize int) *streamProcessor {
	return &streamProcessor{
		chunkSize: chunkSize,
	}
}

// streamAllFilesToDatabase streams all collected file paths to the database
func (sp *streamProcessor) streamAllFilesToDatabase(ctx context.Context, db *sql.DB, collectedPaths []string) error {
	for _, path := range collectedPaths {
		if err := sp.streamFileToDatabase(ctx, db, path); err != nil {
			return fmt.Errorf("failed to stream file %s: %w", path, err)
		}
	}
	return nil
}

// streamAllReadersToDatabase streams all reader inputs to the database
func (sp *streamProcessor) streamAllReadersToDatabase(ctx context.Context, db *sql.DB, readers []readerInput) error {
	for _, readerInput := range readers {
		if err := sp.streamReaderToDatabase(ctx, db, readerInput); err != nil {
			return fmt.Errorf("failed to stream reader input for table '%s': %w", readerInput.tableName, err)
		}
	}
	return nil
}

// streamFileToDatabase streams data from a file path directly to SQLite database using chunked processing
func (sp *streamProcessor) streamFileToDatabase(ctx context.Context, db *sql.DB, filePath string) error {
	// Check if file is supported
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

	// Create file model to determine type and table name
	fileModel := newFile(filePath)
	baseFileType := fileModel.getFileType().baseType()

	// Create decompressed reader if needed
	reader, closer, err := sp.createDecompressedReader(file, filePath)
	if err != nil {
		return fmt.Errorf("failed to create decompressed reader for %s: %w", filePath, err)
	}
	defer func() {
		if closer != nil {
			if closeErr := closer(); closeErr != nil { //nolint:revive,staticcheck
				// Log or handle close error if needed in the future
				// For now, we intentionally ignore close errors during cleanup
			}
		}
	}()

	// Handle XLSX files specially - each sheet becomes a separate table
	if baseFileType == FileTypeXLSX {
		return sp.streamXLSXFileToDatabase(ctx, db, reader, filePath)
	}

	// Create reader input for streaming
	readerInput := readerInput{
		reader:    reader, // Use decompressed reader
		tableName: tableFromFilePath(filePath),
		fileType:  baseFileType,
	}
	return sp.streamReaderToDatabase(ctx, db, readerInput)
}

// streamReaderToDatabase streams data from io.Reader directly to SQLite database
func (sp *streamProcessor) streamReaderToDatabase(ctx context.Context, db *sql.DB, input readerInput) error {
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
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if tableExists > 0 {
		return fmt.Errorf("table '%s' already exists from another file, duplicate table names are not allowed", input.tableName)
	}

	// Create streaming parser for chunked processing
	parser := newStreamingParser(input.fileType, input.tableName, sp.chunkSize)

	// Initialize the table schema (we need to peek at the first chunk to get headers)
	var tableCreated bool
	var insertStmt *sql.Stmt

	// Process data in chunks
	err = parser.ProcessInChunks(input.reader, func(chunk *tableChunk) error {
		// Create table on first chunk
		if !tableCreated {
			if err := sp.createTableFromChunk(ctx, db, chunk); err != nil {
				return fmt.Errorf("failed to create table: %w", err)
			}

			// Prepare insert statement
			var err error
			insertStmt, err = sp.prepareInsertStatement(ctx, db, chunk) //nolint:sqlclosecheck // Statement is closed after processing
			if err != nil {
				return fmt.Errorf("failed to prepare insert statement: %w", err)
			}

			tableCreated = true
		}

		// Insert chunk data
		if err := sp.insertChunkData(ctx, insertStmt, chunk); err != nil {
			return fmt.Errorf("failed to insert chunk data: %w", err)
		}

		return nil
	})

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
		}

		// For header-only files, try to create an empty table by parsing headers
		if createErr := sp.createEmptyTable(ctx, db, input); createErr != nil {
			// If createEmptyTable also fails, this indicates a truly empty file
			if err != nil {
				return err // Return the original processing error
			}
			return fmt.Errorf("failed to create empty table for header-only file: %w", createErr)
		}
		err = nil // Clear any previous error since we handled the header-only case
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
func (sp *streamProcessor) createTableFromChunk(ctx context.Context, db *sql.DB, chunk *tableChunk) error {
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
func (sp *streamProcessor) prepareInsertStatement(ctx context.Context, db *sql.DB, chunk *tableChunk) (*sql.Stmt, error) {
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
func (sp *streamProcessor) insertChunkData(ctx context.Context, stmt *sql.Stmt, chunk *tableChunk) error {
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
		return fmt.Errorf("no headers found in file for table %s", input.tableName)
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
		return fmt.Errorf("failed to create empty table: %w", err)
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
		return fmt.Errorf("failed to create fallback table: %w", err)
	}

	return nil
}

// createDecompressedReader creates a reader that handles compression
func (sp *streamProcessor) createDecompressedReader(file *os.File, filePath string) (io.Reader, func() error, error) {
	var reader io.Reader = file
	closer := func() error { return nil } // Default no-op closer

	// Check file type to determine compression
	fileModel := newFile(filePath)

	if fileModel.isGZ() {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		reader = gzReader
		closer = func() error {
			_ = gzReader.Close() // Ignore close error in cleanup
			return nil
		}
	} else if fileModel.isBZ2() {
		reader = bzip2.NewReader(file)
		closer = func() error { return nil }
	} else if fileModel.isXZ() {
		xzReader, err := xz.NewReader(file)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create xz reader: %w", err)
		}
		reader = xzReader
		closer = func() error { return nil }
	} else if fileModel.isZSTD() {
		decoder, err := zstd.NewReader(file)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		reader = decoder
		closer = func() error {
			decoder.Close()
			return nil
		}
	}

	return reader, closer, nil
}

// streamXLSXFileToDatabase handles XLSX files by creating separate tables for each sheet
func (sp *streamProcessor) streamXLSXFileToDatabase(ctx context.Context, db *sql.DB, reader io.Reader, filePath string) error {
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
			return fmt.Errorf("table '%s' already exists from another file, duplicate table names are not allowed", tableName)
		}

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
			return fmt.Errorf("failed to create table for sheet %s: %w", sheetName, err)
		}

		// Prepare and execute insert statement
		insertStmt, err := sp.prepareInsertStatement(ctx, db, chunk)
		if err != nil {
			return fmt.Errorf("failed to prepare insert statement for sheet %s: %w", sheetName, err)
		}
		defer func() {
			_ = insertStmt.Close() // Ignore close error
		}()

		if err := sp.insertChunkData(ctx, insertStmt, chunk); err != nil {
			return fmt.Errorf("failed to insert data for sheet %s: %w", sheetName, err)
		}
	}

	return nil
}
