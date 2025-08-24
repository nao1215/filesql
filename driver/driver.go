// Package driver provides file SQL driver implementation for database/sql
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

// Driver implements database/sql/driver.Driver interface for file-based SQL
type Driver struct{}

// Connector implements database/sql/driver.Connector interface
type Connector struct {
	driver *Driver
	dsn    string
}

// Connection implements database/sql/driver.Conn interface
type Connection struct {
	conn driver.Conn
}

// Transaction implements database/sql/driver.Tx interface
type Transaction struct {
	tx driver.Tx
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

	// Check if path is a directory or file
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return fmt.Errorf("path does not exist: %s", path)
	}
	if err != nil {
		return fmt.Errorf("failed to stat path: %w", err)
	}

	if info.IsDir() {
		return c.loadDirectory(conn, path)
	}
	return c.loadSingleFile(conn, path)
}

// loadSingleFile loads a single file into SQLite3 database
func (c *Connector) loadSingleFile(conn driver.Conn, filePath string) error {
	file := model.NewFile(filePath)

	// Convert file to table
	table, err := file.ToTable()
	if err != nil {
		if errors.Is(err, model.ErrDuplicateColumnName) {
			return fmt.Errorf("%w", ErrDuplicateColumnName)
		}
		return fmt.Errorf("failed to parse file: %w", err)
	}

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

	// Load the selected files
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
		return fmt.Errorf("no supported files found in directory: %s", dirPath)
	}

	return nil
}

// collectDirectoryFiles collects files from directory and validates for duplicate table names
func (c *Connector) collectDirectoryFiles(dirPath string, tableNames map[string]string) ([]string, error) {
	// Read directory contents
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	var filesToLoad []string

	// Collect files and check for duplicate table names
	for _, entry := range entries {
		if entry.IsDir() {
			continue // Skip subdirectories
		}

		fileName := entry.Name()
		filePath := filepath.Join(dirPath, fileName)

		// Check if file is supported (CSV, TSV, LTSV, or their compressed versions)
		if model.IsSupportedFile(fileName) {
			// Test if the file can be loaded (has valid structure)
			file := model.NewFile(filePath)
			table, err := file.ToTable()
			if err != nil {
				// Skip files with errors (e.g., duplicate columns) in directory loading
				fmt.Printf("Warning: skipping file %s: %v\n", fileName, err)
				continue
			}

			tableName := model.TableFromFilePath(filePath)
			if existingFile, exists := tableNames[tableName]; exists {
				// Check if existing file is from a different directory
				// If so, it's a duplicate table name error
				if filepath.Dir(existingFile) != dirPath {
					return nil, fmt.Errorf("%w: table '%s' from files '%s' and '%s'",
						ErrDuplicateTableName, tableName, existingFile, filePath)
				}

				// Within same directory, check if files have same actual file type vs compression difference
				existingBaseName := filepath.Base(existingFile)

				// Remove compression extensions to get base file type
				existingFileType := removeCompressionExtensions(existingBaseName)
				currentFileType := removeCompressionExtensions(fileName)

				// If the base file types are different (e.g., .csv vs .tsv), it's still a duplicate table name error
				if filepath.Ext(existingFileType) != filepath.Ext(currentFileType) {
					return nil, fmt.Errorf("%w: table '%s' from files '%s' and '%s' (different file types with same table name)",
						ErrDuplicateTableName, tableName, existingFile, filePath)
				}

				// Same file type, different compression - prefer less compressed
				existingCompressionCount := countCompressionExtensions(existingBaseName)
				currentCompressionCount := countCompressionExtensions(fileName)

				// Prefer uncompressed files over compressed ones
				if currentCompressionCount < existingCompressionCount {
					// Replace existing file with current (less compressed) file
					for i, f := range filesToLoad {
						if f == existingFile {
							filesToLoad[i] = filePath
							break
						}
					}
					tableNames[tableName] = filePath
				}
				// Otherwise keep the existing file (skip current file)
			} else {
				tableNames[tableName] = filePath
				filesToLoad = append(filesToLoad, filePath)
			}

			// Don't actually use the table here, just the filename for validation
			_ = table
		}
	}

	return filesToLoad, nil
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

	// Track table names to detect duplicates across all paths
	tableNames := make(map[string]string) // table name -> file path
	var filesToLoad []string

	// First pass: collect all files and detect duplicate table names
	for _, path := range paths {
		// Trim whitespace from path
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}

		// Check if path exists
		info, err := os.Stat(path)
		if os.IsNotExist(err) {
			return fmt.Errorf("path does not exist: %s", path)
		}
		if err != nil {
			return fmt.Errorf("failed to stat path %s: %w", path, err)
		}

		if info.IsDir() {
			// For directories, collect all files
			dirFiles, err := c.collectDirectoryFiles(path, tableNames)
			if err != nil {
				return fmt.Errorf("failed to collect files from directory %s: %w", path, err)
			}
			filesToLoad = append(filesToLoad, dirFiles...)
		} else {
			// For single files, check for supported format and table name conflicts
			if model.IsSupportedFile(filepath.Base(path)) {
				tableName := model.TableFromFilePath(path)
				if existingFile, exists := tableNames[tableName]; exists {
					return fmt.Errorf("%w: table '%s' from files '%s' and '%s'",
						ErrDuplicateTableName, tableName, existingFile, path)
				}
				tableNames[tableName] = path
				filesToLoad = append(filesToLoad, path)
			}
		}
	}

	// Second pass: load all files
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
	columns := make([]string, 0, len(table.Header()))
	for _, col := range table.Header() {
		columns = append(columns, fmt.Sprintf(`[%s] TEXT`, col))
	}

	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS [%s] (%s)`,
		table.Name(),
		strings.Join(columns, ", "),
	)

	stmt, err := conn.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	var result driver.Result
	if stmtExecCtx, ok := stmt.(driver.StmtExecContext); ok {
		result, err = stmtExecCtx.ExecContext(context.Background(), []driver.NamedValue{})
	} else {
		return ErrStmtExecContextNotSupported
	}
	_ = result // result is not used
	return err
}

// insertRecordsDirectly inserts records using driver.Conn
func (c *Connector) insertRecordsDirectly(conn driver.Conn, table *model.Table) error {
	if len(table.Records()) == 0 {
		return nil
	}

	// Prepare placeholders for INSERT statement
	placeholders := "?"
	for i := 1; i < len(table.Header()); i++ {
		placeholders += ", ?"
	}

	query := fmt.Sprintf(
		`INSERT INTO [%s] VALUES (%s)`,
		table.Name(),
		placeholders,
	)

	stmt, err := conn.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Insert each record
	for _, record := range table.Records() {
		args := make([]driver.Value, len(record))
		for i, val := range record {
			args[i] = val
		}

		var result driver.Result
		if stmtExecCtx, ok := stmt.(driver.StmtExecContext); ok {
			namedArgs := make([]driver.NamedValue, len(args))
			for i, arg := range args {
				namedArgs[i] = driver.NamedValue{
					Ordinal: i + 1,
					Value:   arg,
				}
			}
			result, err = stmtExecCtx.ExecContext(context.Background(), namedArgs)
		} else {
			return ErrStmtExecContextNotSupported
		}
		_ = result // result is not used
		if err != nil {
			return err
		}
	}

	return nil
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
	stmt, err := conn.PrepareContext(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var rows driver.Rows
	if stmtQueryCtx, ok := stmt.(driver.StmtQueryContext); ok {
		rows, err = stmtQueryCtx.QueryContext(context.Background(), []driver.NamedValue{})
	} else {
		rows, err = stmt.Query([]driver.Value{})
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tableNames []string
	dest := make([]driver.Value, 1)
	for {
		err := rows.Next(dest)
		if err != nil {
			if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if name, ok := dest[0].(string); ok {
			tableNames = append(tableNames, name)
		}
	}

	return tableNames, nil
}

// exportTableToCSV exports a single table to CSV file
func (conn *Connection) exportTableToCSV(tableName, outputPath string) error {
	// Get column names
	columns, err := conn.getTableColumns(tableName)
	if err != nil {
		return fmt.Errorf("failed to get columns for table %s: %w", tableName, err)
	}

	// Query all data from table
	query := fmt.Sprintf("SELECT * FROM [%s]", tableName)
	stmt, err := conn.PrepareContext(context.Background(), query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	var rows driver.Rows
	if stmtQueryCtx, ok := stmt.(driver.StmtQueryContext); ok {
		rows, err = stmtQueryCtx.QueryContext(context.Background(), []driver.NamedValue{})
	} else {
		rows, err = stmt.Query([]driver.Value{})
	}
	if err != nil {
		return err
	}
	defer rows.Close()

	// Create CSV file
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
	dest := make([]driver.Value, len(columns))
	for {
		err := rows.Next(dest)
		if err != nil {
			if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		record := make([]string, len(dest))
		for i, val := range dest {
			if val == nil {
				record[i] = ""
			} else {
				record[i] = conn.escapeCSVValue(fmt.Sprintf("%v", val))
			}
		}

		line := strings.Join(record, ",") + "\n"
		if _, err := file.WriteString(line); err != nil {
			return err
		}
	}

	return nil
}

// getTableColumns retrieves column names for a specific table
func (conn *Connection) getTableColumns(tableName string) ([]string, error) {
	query := fmt.Sprintf("PRAGMA table_info([%s])", tableName)
	stmt, err := conn.PrepareContext(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var rows driver.Rows
	if stmtQueryCtx, ok := stmt.(driver.StmtQueryContext); ok {
		rows, err = stmtQueryCtx.QueryContext(context.Background(), []driver.NamedValue{})
	} else {
		rows, err = stmt.Query([]driver.Value{})
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	dest := make([]driver.Value, 6) // PRAGMA table_info returns 6 columns
	for {
		err := rows.Next(dest)
		if err != nil {
			if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		if name, ok := dest[1].(string); ok { // Column name is at index 1
			columns = append(columns, name)
		}
	}

	return columns, nil
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
