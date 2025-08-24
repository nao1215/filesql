// Package driver provides file SQL driver implementation for database/sql.
//
// This package implements a database/sql driver that allows querying CSV, TSV, and LTSV files
// (including compressed versions) as if they were SQL tables. Files are loaded into an
// in-memory SQLite database for query execution.
//
// Key features:
//   - Support for CSV, TSV, and LTSV file formats
//   - Support for compressed files (gzip, bzip2, xz, zstd)
//   - Duplicate table name validation across multiple files
//   - Directory scanning with automatic file discovery
//   - Table export functionality
//
// Usage:
//
//	import _ "github.com/nao1215/filesql/driver"
//	db, err := sql.Open("filesql", "data.csv")
package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/nao1215/filesql/domain/model"
	"modernc.org/sqlite"
)

// Driver implements database/sql/driver.Driver interface for file-based SQL.
// It serves as the entry point for creating connections to file-based databases.
type Driver struct{}

// Connector implements database/sql/driver.Connector interface.
// It holds connection parameters and manages the creation of database connections.
// The dsn field contains file paths separated by semicolons for multiple files.
type Connector struct {
	driver *Driver
	dsn    string // Data source name - file paths separated by semicolons
}

// Connection implements database/sql/driver.Conn interface.
// It wraps an underlying SQLite connection that contains loaded file data.
type Connection struct {
	conn driver.Conn // Underlying SQLite connection with loaded file data
}

// Transaction implements database/sql/driver.Tx interface.
// It wraps an underlying SQLite transaction for atomic operations.
type Transaction struct {
	tx driver.Tx // Underlying SQLite transaction
}

// NewDriver creates a new file SQL driver
func NewDriver() *Driver {
	return &Driver{}
}

// Open implements driver.Driver interface
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector implements driver.DriverContext interface
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	return &Connector{
		driver: d,
		dsn:    dsn,
	}, nil
}

// Connect implements driver.Connector interface
func (c *Connector) Connect(_ context.Context) (driver.Conn, error) {
	// Get SQLite driver and create connection
	sqliteDriver := &sqlite.Driver{}
	conn, err := sqliteDriver.Open(":memory:")
	if err != nil {
		return nil, fmt.Errorf("failed to create in-memory database: %w", err)
	}

	// Load file data into database
	if err := c.loadFileDirectly(conn, c.dsn); err != nil {
		_ = conn.Close() // Ignore close error since we're already returning an error
		return nil, fmt.Errorf("failed to load file: %w", err)
	}

	return &Connection{conn: conn}, nil
}

// Driver implements driver.Connector interface
func (c *Connector) Driver() driver.Driver {
	return c.driver
}

// loadFileDirectly loads CSV/TSV/LTSV file(s) and/or directories into SQLite3 database using driver.Conn
func (c *Connector) loadFileDirectly(conn driver.Conn, path string) error {
	// Check if path contains multiple paths separated by semicolon
	if strings.Contains(path, ";") {
		return c.loadMultiplePaths(conn, strings.Split(path, ";"))
	}

	return c.loadSinglePath(conn, path)
}

// loadSinglePath loads a single path (file or directory) into the database
func (c *Connector) loadSinglePath(conn driver.Conn, path string) error {
	info, err := c.validatePath(path)
	if err != nil {
		return err
	}

	if info.IsDir() {
		return c.loadDirectory(conn, path)
	}
	return c.loadSingleFile(conn, path)
}

// validatePath validates that a path exists and returns its FileInfo
func (c *Connector) validatePath(path string) (os.FileInfo, error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("path does not exist: %s", path)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to stat path: %w", err)
	}
	return info, nil
}

// loadSingleFile loads a single file into SQLite3 database
func (c *Connector) loadSingleFile(conn driver.Conn, filePath string) error {
	table, err := c.parseFileToTable(filePath)
	if err != nil {
		return err
	}

	return c.loadTableIntoDatabase(conn, table)
}

// parseFileToTable converts a file to a table with proper error handling
func (c *Connector) parseFileToTable(filePath string) (*model.Table, error) {
	file := model.NewFile(filePath)

	table, err := file.ToTable()
	if err != nil {
		if errors.Is(err, model.ErrDuplicateColumnName) {
			return nil, fmt.Errorf("%w", ErrDuplicateColumnName)
		}
		return nil, fmt.Errorf("failed to parse file: %w", err)
	}

	return table, nil
}

// loadTableIntoDatabase creates table and inserts data into the database
func (c *Connector) loadTableIntoDatabase(conn driver.Conn, table *model.Table) error {
	// Create table in SQLite3
	if err := c.createTableDirectly(conn, table); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Insert records
	if err := c.insertRecordsDirectly(conn, table); err != nil {
		return fmt.Errorf("failed to insert records: %w", err)
	}

	return nil
}

// loadDirectory loads all supported files from a directory into SQLite3 database
func (c *Connector) loadDirectory(conn driver.Conn, dirPath string) error {
	tableNames := make(map[string]string)
	filesToLoad, err := c.collectDirectoryFiles(dirPath, tableNames)
	if err != nil {
		return err
	}

	return c.loadFilesWithErrorHandling(conn, filesToLoad, dirPath)
}

// loadFilesWithErrorHandling loads multiple files with appropriate error handling
func (c *Connector) loadFilesWithErrorHandling(conn driver.Conn, filesToLoad []string, context string) error {
	loadedFiles := 0
	for _, filePath := range filesToLoad {
		if err := c.loadSingleFile(conn, filePath); err != nil {
			// Log error but continue with other files (only for directory loading)
			fmt.Printf("Warning: failed to load file %s: %v\n", filepath.Base(filePath), err)
			continue
		}
		loadedFiles++
	}

	if loadedFiles == 0 {
		return fmt.Errorf("no supported files found in directory: %s", context)
	}

	return nil
}

// collectDirectoryFiles collects files from directory and validates for duplicate table names
func (c *Connector) collectDirectoryFiles(dirPath string, tableNames map[string]string) ([]string, error) {
	entries, err := c.readDirectoryEntries(dirPath)
	if err != nil {
		return nil, err
	}

	var filesToLoad []string

	// Collect files and check for duplicate table names
	for _, entry := range entries {
		if entry.IsDir() {
			continue // Skip subdirectories
		}

		fileName := entry.Name()
		filePath := filepath.Join(dirPath, fileName)

		if model.IsSupportedFile(fileName) {
			if c.shouldSkipFile(filePath, fileName) {
				continue
			}

			tableName := model.TableFromFilePath(filePath)
			if err := c.handleTableNameConflict(tableName, filePath, &filesToLoad, tableNames, dirPath); err != nil {
				return nil, err
			}
		}
	}

	return filesToLoad, nil
}

// readDirectoryEntries reads and returns directory entries
func (c *Connector) readDirectoryEntries(dirPath string) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}
	return entries, nil
}

// shouldSkipFile determines if a file should be skipped based on validation
func (c *Connector) shouldSkipFile(filePath, fileName string) bool {
	file := model.NewFile(filePath)
	_, err := file.ToTable()
	if err != nil {
		// Skip files with errors (e.g., duplicate columns) in directory loading
		fmt.Printf("Warning: skipping file %s: %v\n", fileName, err)
		return true
	}
	return false
}

// handleTableNameConflict handles table name conflicts and file selection logic
func (c *Connector) handleTableNameConflict(tableName, filePath string, filesToLoad *[]string, tableNames map[string]string, dirPath string) error {
	if existingFile, exists := tableNames[tableName]; exists {
		return c.resolveTableNameConflict(tableName, filePath, existingFile, filesToLoad, tableNames, dirPath)
	}

	// No conflict - add the file
	tableNames[tableName] = filePath
	*filesToLoad = append(*filesToLoad, filePath)
	return nil
}

// resolveTableNameConflict resolves conflicts when multiple files would create the same table name
func (c *Connector) resolveTableNameConflict(tableName, filePath, existingFile string, filesToLoad *[]string, tableNames map[string]string, dirPath string) error {
	// Check if existing file is from a different directory (normalize paths for cross-platform compatibility)
	existingDir := filepath.Clean(filepath.Dir(existingFile))
	currentDir := filepath.Clean(dirPath)
	if existingDir != currentDir {
		return fmt.Errorf("%w: table '%s' from files '%s' and '%s'",
			ErrDuplicateTableName, tableName, existingFile, filePath)
	}

	// Within same directory, check file types and compression
	existingBaseName := filepath.Base(existingFile)
	currentBaseName := filepath.Base(filePath)

	// Remove compression extensions to get base file type
	existingFileType := removeCompressionExtensions(existingBaseName)
	currentFileType := removeCompressionExtensions(currentBaseName)

	// If the base file types are different (e.g., .csv vs .tsv), it's a duplicate error
	if filepath.Ext(existingFileType) != filepath.Ext(currentFileType) {
		return fmt.Errorf("%w: table '%s' from files '%s' and '%s' (different file types with same table name)",
			ErrDuplicateTableName, tableName, existingFile, filePath)
	}

	// Same file type, different compression - prefer less compressed
	c.selectBetterFile(existingBaseName, currentBaseName, existingFile, filePath, filesToLoad, tableNames, tableName)
	return nil
}

// selectBetterFile selects the better file based on compression level
func (c *Connector) selectBetterFile(existingBaseName, currentBaseName, existingFile, filePath string, filesToLoad *[]string, tableNames map[string]string, tableName string) {
	existingCompressionCount := countCompressionExtensions(existingBaseName)
	currentCompressionCount := countCompressionExtensions(currentBaseName)

	// Prefer uncompressed files over compressed ones
	if currentCompressionCount < existingCompressionCount {
		// Replace existing file with current (less compressed) file
		for i, f := range *filesToLoad {
			if f == existingFile {
				(*filesToLoad)[i] = filePath
				break
			}
		}
		tableNames[tableName] = filePath
	}
	// Otherwise keep the existing file (skip current file)
}

// removeCompressionExtensions removes compression extensions from filename
func removeCompressionExtensions(fileName string) string {
	for _, ext := range []string{model.ExtGZ, model.ExtBZ2, model.ExtXZ, model.ExtZSTD} {
		if strings.HasSuffix(fileName, ext) {
			return strings.TrimSuffix(fileName, ext)
		}
	}
	return fileName
}

// countCompressionExtensions counts how many compression extensions a file has
func countCompressionExtensions(fileName string) int {
	count := 0
	for _, ext := range []string{model.ExtGZ, model.ExtBZ2, model.ExtXZ, model.ExtZSTD} {
		if strings.HasSuffix(fileName, ext) {
			count++
		}
	}
	return count
}

// loadMultiplePaths loads multiple specified files and/or directories into SQLite3 database
func (c *Connector) loadMultiplePaths(conn driver.Conn, paths []string) error {
	if len(paths) == 0 {
		return ErrNoPathsProvided
	}

	filesToLoad, err := c.collectAllFiles(paths)
	if err != nil {
		return err
	}

	return c.loadCollectedFiles(conn, filesToLoad)
}

// collectAllFiles collects all files from multiple paths with duplicate detection
func (c *Connector) collectAllFiles(paths []string) ([]string, error) {
	tableNames := make(map[string]string) // table name -> file path
	var filesToLoad []string

	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}

		pathFiles, err := c.collectFilesFromPath(path, tableNames)
		if err != nil {
			return nil, err
		}
		filesToLoad = append(filesToLoad, pathFiles...)
	}

	return filesToLoad, nil
}

// collectFilesFromPath collects files from a single path (file or directory)
func (c *Connector) collectFilesFromPath(path string, tableNames map[string]string) ([]string, error) {
	info, err := c.validatePath(path)
	if err != nil {
		return nil, err
	}

	if info.IsDir() {
		return c.collectDirectoryFiles(path, tableNames)
	}

	return c.collectSingleFile(path, tableNames)
}

// collectSingleFile collects a single file and checks for table name conflicts
func (c *Connector) collectSingleFile(path string, tableNames map[string]string) ([]string, error) {
	if !model.IsSupportedFile(filepath.Base(path)) {
		return nil, nil // Skip unsupported files
	}

	tableName := model.TableFromFilePath(path)
	if existingFile, exists := tableNames[tableName]; exists {
		return nil, fmt.Errorf("%w: table '%s' from files '%s' and '%s'",
			ErrDuplicateTableName, tableName, existingFile, path)
	}

	tableNames[tableName] = path
	return []string{path}, nil
}

// loadCollectedFiles loads all collected files with proper error handling
func (c *Connector) loadCollectedFiles(conn driver.Conn, filesToLoad []string) error {
	loadedFiles := 0
	for _, filePath := range filesToLoad {
		if err := c.loadSingleFile(conn, filePath); err != nil {
			return fmt.Errorf("failed to load file %s: %w", filePath, err)
		}
		loadedFiles++
	}

	if loadedFiles == 0 {
		return ErrNoFilesLoaded
	}

	return nil
}

// createTableDirectly creates table schema using driver.Conn
func (c *Connector) createTableDirectly(conn driver.Conn, table *model.Table) error {
	query := c.buildCreateTableQuery(table)
	return c.executeStatement(conn, query, nil)
}

// buildCreateTableQuery constructs a CREATE TABLE query for the given table
func (c *Connector) buildCreateTableQuery(table *model.Table) string {
	columns := make([]string, 0, len(table.Header()))
	for _, col := range table.Header() {
		columns = append(columns, fmt.Sprintf(`[%s] TEXT`, col))
	}

	return fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS [%s] (%s)`,
		table.Name(),
		strings.Join(columns, ", "),
	)
}

// insertRecordsDirectly inserts records using driver.Conn
func (c *Connector) insertRecordsDirectly(conn driver.Conn, table *model.Table) error {
	if len(table.Records()) == 0 {
		return nil
	}

	query := c.buildInsertQuery(table)
	stmt, err := conn.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	return c.insertRecords(stmt, c.convertRecordsToStringSlices(table.Records()))
}

// buildInsertQuery constructs an INSERT query for the given table
func (c *Connector) buildInsertQuery(table *model.Table) string {
	placeholders := c.buildPlaceholders(len(table.Header()))
	return fmt.Sprintf(
		`INSERT INTO [%s] VALUES (%s)`,
		table.Name(),
		placeholders,
	)
}

// buildPlaceholders creates placeholder string for prepared statements
func (c *Connector) buildPlaceholders(count int) string {
	if count == 0 {
		return ""
	}
	placeholders := "?"
	for i := 1; i < count; i++ {
		placeholders += ", ?"
	}
	return placeholders
}

// insertRecords inserts all records using the prepared statement
func (c *Connector) insertRecords(stmt driver.Stmt, records [][]string) error {
	for _, record := range records {
		args := c.convertRecordToDriverValues(record)
		if err := c.executeStatement(stmt, "", args); err != nil {
			return err
		}
	}
	return nil
}

// convertRecordsToStringSlices converts model.Record slice to [][]string
func (c *Connector) convertRecordsToStringSlices(records []model.Record) [][]string {
	result := make([][]string, len(records))
	for i, record := range records {
		result[i] = []string(record) // model.Record is type alias for []string
	}
	return result
}

// convertRecordToDriverValues converts string record to driver.Value slice
func (c *Connector) convertRecordToDriverValues(record []string) []driver.Value {
	args := make([]driver.Value, len(record))
	for i, val := range record {
		args[i] = val
	}
	return args
}

// executeStatement executes a statement with proper context support
func (c *Connector) executeStatement(conn interface{}, query string, args []driver.Value) error {
	switch stmt := conn.(type) {
	case driver.Conn:
		// For CREATE TABLE queries
		preparedStmt, err := stmt.Prepare(query)
		if err != nil {
			return err
		}
		defer preparedStmt.Close()
		return c.executeStatement(preparedStmt, "", args)

	case driver.Stmt:
		// For INSERT queries with prepared statement
		if stmtExecCtx, ok := stmt.(driver.StmtExecContext); ok {
			namedArgs := c.convertToNamedValues(args)
			_, err := stmtExecCtx.ExecContext(context.Background(), namedArgs)
			return err
		}
		return ErrStmtExecContextNotSupported

	default:
		return errors.New("unsupported statement type")
	}
}

// convertToNamedValues converts driver.Value slice to driver.NamedValue slice
func (c *Connector) convertToNamedValues(args []driver.Value) []driver.NamedValue {
	namedArgs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}
	return namedArgs
}

// Close implements driver.Conn interface
func (conn *Connection) Close() error {
	if conn.conn != nil {
		return conn.conn.Close()
	}
	return nil
}

// Begin implements driver.Conn interface (deprecated, use BeginTx instead)
func (conn *Connection) Begin() (driver.Tx, error) {
	return conn.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx implements driver.ConnBeginTx interface
func (conn *Connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if connBeginTx, ok := conn.conn.(driver.ConnBeginTx); ok {
		tx, err := connBeginTx.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &Transaction{tx: tx}, nil
	}
	// If ConnBeginTx is not implemented, return an error
	return nil, ErrBeginTxNotSupported
}

// Commit implements driver.Tx interface
func (t *Transaction) Commit() error {
	return t.tx.Commit()
}

// Rollback implements driver.Tx interface
func (t *Transaction) Rollback() error {
	return t.tx.Rollback()
}

// Prepare implements driver.Conn interface (deprecated, use PrepareContext instead)
func (conn *Connection) Prepare(query string) (driver.Stmt, error) {
	return conn.PrepareContext(context.Background(), query)
}

// PrepareContext implements driver.ConnPrepareContext interface
func (conn *Connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if connPrepareCtx, ok := conn.conn.(driver.ConnPrepareContext); ok {
		return connPrepareCtx.PrepareContext(ctx, query)
	}
	// If ConnPrepareContext is not implemented, return an error
	return nil, ErrPrepareContextNotSupported
}

// Dump exports all tables from SQLite3 database to specified directory in CSV format
func (conn *Connection) Dump(outputDir string) error {
	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0750); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Get all table names
	tableNames, err := conn.getTableNames()
	if err != nil {
		return fmt.Errorf("failed to get table names: %w", err)
	}

	// Export each table to CSV
	for _, tableName := range tableNames {
		outputPath := filepath.Join(outputDir, tableName+".csv")
		if err := conn.exportTableToCSV(tableName, outputPath); err != nil {
			return fmt.Errorf("failed to export table %s: %w", tableName, err)
		}
	}

	return nil
}

// getTableNames retrieves all user-defined table names from SQLite3 database
func (conn *Connection) getTableNames() ([]string, error) {
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
	rows, err := conn.executeQuery(query, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return conn.scanStringValues(rows, 1)
}

// exportTableToCSV exports a single table to CSV file
func (conn *Connection) exportTableToCSV(tableName, outputPath string) error {
	columns, err := conn.getTableColumns(tableName)
	if err != nil {
		return fmt.Errorf("failed to get columns for table %s: %w", tableName, err)
	}

	query := fmt.Sprintf("SELECT * FROM [%s]", tableName)
	rows, err := conn.executeQuery(query, nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	return conn.writeCSVFile(outputPath, columns, rows)
}

// getTableColumns retrieves column names for a specific table
func (conn *Connection) getTableColumns(tableName string) ([]string, error) {
	query := fmt.Sprintf("PRAGMA table_info([%s])", tableName)
	rows, err := conn.executeQuery(query, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return conn.scanStringValues(rows, 6) // PRAGMA table_info returns 6 columns, name is at index 1
}

// executeQuery executes a query and returns rows with proper context support
func (conn *Connection) executeQuery(query string, args []driver.Value) (driver.Rows, error) {
	stmt, err := conn.PrepareContext(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var namedArgs []driver.NamedValue
	if args != nil {
		namedArgs = make([]driver.NamedValue, len(args))
		for i, arg := range args {
			namedArgs[i] = driver.NamedValue{
				Ordinal: i + 1,
				Value:   arg,
			}
		}
	}

	if stmtQueryCtx, ok := stmt.(driver.StmtQueryContext); ok {
		return stmtQueryCtx.QueryContext(context.Background(), namedArgs)
	}

	// Fallback for older drivers
	driverArgs := make([]driver.Value, len(args))
	copy(driverArgs, args)
	return stmt.Query(driverArgs)
}

// scanStringValues scans string values from rows, extracting the column at the specified index
func (conn *Connection) scanStringValues(rows driver.Rows, columnCount int) ([]string, error) {
	var results []string
	dest := make([]driver.Value, columnCount)

	for {
		err := rows.Next(dest)
		if err != nil {
			if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		// For table names, extract from index 0; for column names, extract from index 1
		var value string
		if columnCount == 1 {
			// Table names query
			if name, ok := dest[0].(string); ok {
				value = name
			}
		} else if columnCount == 6 {
			// Column names query (PRAGMA table_info)
			if name, ok := dest[1].(string); ok { // Column name is at index 1
				value = name
			}
		}

		if value != "" {
			results = append(results, value)
		}
	}

	return results, nil
}

// writeCSVFile creates and writes data to a CSV file
func (conn *Connection) writeCSVFile(outputPath string, columns []string, rows driver.Rows) error {
	file, err := os.Create(outputPath) //nolint:gosec // Safe: outputPath is constructed from validated inputs
	if err != nil {
		return err
	}
	defer file.Close()

	// Write header
	header := strings.Join(columns, ",") + "\n"
	if _, err := file.WriteString(header); err != nil {
		return err
	}

	// Write data rows
	return conn.writeDataRows(file, rows, len(columns))
}

// writeDataRows writes all data rows to the CSV file
func (conn *Connection) writeDataRows(file *os.File, rows driver.Rows, columnCount int) error {
	dest := make([]driver.Value, columnCount)

	for {
		err := rows.Next(dest)
		if err != nil {
			if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		record := conn.convertRowToCSVRecord(dest)
		line := strings.Join(record, ",") + "\n"
		if _, err := file.WriteString(line); err != nil {
			return err
		}
	}

	return nil
}

// convertRowToCSVRecord converts a database row to CSV record with proper escaping
func (conn *Connection) convertRowToCSVRecord(dest []driver.Value) []string {
	record := make([]string, len(dest))
	for i, val := range dest {
		if val == nil {
			record[i] = ""
		} else {
			record[i] = conn.escapeCSVValue(fmt.Sprintf("%v", val))
		}
	}
	return record
}

// escapeCSVValue escapes a value for CSV format
func (conn *Connection) escapeCSVValue(value string) string {
	// Check if value needs to be quoted
	needsQuoting := strings.Contains(value, ",") ||
		strings.Contains(value, "\n") ||
		strings.Contains(value, "\r") ||
		strings.Contains(value, "\"")

	if needsQuoting {
		// Escape double quotes by doubling them
		escaped := strings.ReplaceAll(value, "\"", "\"\"")
		return fmt.Sprintf("\"%s\"", escaped)
	}

	return value
}
