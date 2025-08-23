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
		conn.Close()
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
	// Read directory contents
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	// Track table names to avoid duplicates
	tableNames := make(map[string]string)
	var filesToLoad []string

	// First pass: collect files and check for duplicate table names
	for _, entry := range entries {
		if entry.IsDir() {
			continue // Skip subdirectories
		}

		fileName := entry.Name()
		filePath := filepath.Join(dirPath, fileName)

		// Check if file is supported (CSV, TSV, LTSV, or their compressed versions)
		if model.IsSupportedFile(fileName) {
			tableName := model.TableFromFilePath(filePath)
			if existingFile, exists := tableNames[tableName]; exists {
				// Table name already exists, prefer the file without compression extension
				// or the one that appears first if both are compressed/uncompressed
				existingFile = filepath.Base(existingFile)

				// Count compression extensions to determine priority
				existingCompressionCount := 0
				currentCompressionCount := 0

				for _, ext := range []string{model.ExtGZ, model.ExtBZ2, model.ExtXZ, model.ExtZSTD} {
					if strings.HasSuffix(existingFile, ext) {
						existingCompressionCount++
					}
					if strings.HasSuffix(fileName, ext) {
						currentCompressionCount++
					}
				}

				// Prefer uncompressed files over compressed ones
				if currentCompressionCount < existingCompressionCount {
					// Replace existing file with current (less compressed) file
					for i, f := range filesToLoad {
						if f == tableNames[tableName] {
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
		}
	}

	// Second pass: load the selected files
	loadedFiles := 0
	for _, filePath := range filesToLoad {
		if err := c.loadSingleFile(conn, filePath); err != nil {
			// Log error but continue with other files
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

// loadMultiplePaths loads multiple specified files and/or directories into SQLite3 database
func (c *Connector) loadMultiplePaths(conn driver.Conn, paths []string) error {
	if len(paths) == 0 {
		return ErrNoPathsProvided
	}

	loadedFiles := 0
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

		// Handle based on path type
		if info.IsDir() {
			// Load directory
			if err := c.loadDirectory(conn, path); err != nil {
				return fmt.Errorf("failed to load directory %s: %w", path, err)
			}
			loadedFiles++
		} else {
			// Load single file
			if err := c.loadSingleFile(conn, path); err != nil {
				return fmt.Errorf("failed to load file %s: %w", path, err)
			}
			loadedFiles++
		}
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
		`CREATE TABLE [%s] (%s)`,
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
	if err := os.MkdirAll(outputDir, 0755); err != nil {
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
	file, err := os.Create(outputPath)
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
