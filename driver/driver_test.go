package driver

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewDriver(t *testing.T) {
	t.Parallel()

	t.Run("Create new driver", func(t *testing.T) {
		t.Parallel()

		d := NewDriver()
		if d == nil {
			t.Error("NewDriver() returned nil")
		}
	})
}

func TestDriverOpen(t *testing.T) {
	t.Parallel()

	d := NewDriver()

	tests := []struct {
		name    string
		dsn     string
		wantErr bool
	}{
		{
			name:    "Valid CSV file",
			dsn:     "../testdata/sample.csv",
			wantErr: false,
		},
		{
			name:    "Non-existent file",
			dsn:     "../testdata/nonexistent.csv",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conn, err := d.Open(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if conn == nil {
					t.Error("Open() returned nil connection")
					return
				}
				defer conn.Close()

				// Test that we can prepare a statement
				stmt, err := conn.Prepare("SELECT COUNT(*) FROM sample")
				if err != nil {
					t.Errorf("Prepare() error = %v", err)
					return
				}
				defer stmt.Close()

				// Execute query
				rows, err := stmt.Query([]driver.Value{})
				if err != nil {
					t.Errorf("Query() error = %v", err)
					return
				}
				defer rows.Close()

				// Check that we have columns
				columns := rows.Columns()
				if len(columns) == 0 {
					t.Error("Query returned no columns")
				}
			}
		})
	}
}

func TestDriverOpenConnector(t *testing.T) {
	t.Parallel()

	d := NewDriver()
	connector, err := d.OpenConnector("../testdata/sample.csv")
	if err != nil {
		t.Errorf("OpenConnector() error = %v", err)
		return
	}

	if connector == nil {
		t.Error("OpenConnector() returned nil connector")
		return
	}

	// Test that connector returns the same driver
	if connector.Driver() != d {
		t.Error("Connector.Driver() returned different driver")
	}
}

func TestConnectorConnect(t *testing.T) {
	t.Parallel()

	d := NewDriver()
	connector, err := d.OpenConnector("../testdata/sample.csv")
	if err != nil {
		t.Fatalf("OpenConnector() error = %v", err)
	}

	conn, err := connector.Connect(t.Context())
	if err != nil {
		t.Errorf("Connect() error = %v", err)
		return
	}

	if conn == nil {
		t.Error("Connect() returned nil connection")
		return
	}

	defer conn.Close()
}

func TestConnectorConnectMultiplePaths(t *testing.T) {
	t.Parallel()

	d := NewDriver()

	tests := []struct {
		name string
		dsn  string
	}{
		{
			name: "Multiple files separated by semicolon",
			dsn:  "../testdata/sample.csv;../testdata/users.csv",
		},
		{
			name: "Mixed file and directory paths",
			dsn:  "../testdata/sample.csv;../testdata",
		},
		{
			name: "Directory only",
			dsn:  "../testdata",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector, err := d.OpenConnector(tt.dsn)
			if err != nil {
				t.Fatalf("OpenConnector() error = %v", err)
			}

			conn, err := connector.Connect(t.Context())
			if err != nil {
				t.Errorf("Connect() error = %v", err)
				return
			}

			if conn == nil {
				t.Error("Connect() returned nil connection")
				return
			}

			defer conn.Close()

			// Test that we can prepare statements for sample table (should always exist)
			query := "SELECT COUNT(*) FROM sample"
			stmt, err := conn.Prepare(query)
			if err != nil {
				t.Errorf("Prepare() for table sample error = %v", err)
				return
			}
			defer stmt.Close()

			// Execute query to verify table exists
			rows, err := stmt.Query([]driver.Value{})
			if err != nil {
				t.Errorf("Query() for table sample error = %v", err)
				return
			}
			defer rows.Close()

			// Check that we have columns
			columns := rows.Columns()
			if len(columns) == 0 {
				t.Error("Query for table sample returned no columns")
			}
		})
	}
}

func TestConnectorConnectDirectory(t *testing.T) {
	t.Parallel()

	d := NewDriver()
	connector, err := d.OpenConnector("../testdata")
	if err != nil {
		t.Fatalf("OpenConnector() error = %v", err)
	}

	conn, err := connector.Connect(t.Context())
	if err != nil {
		t.Errorf("Connect() error = %v", err)
		return
	}

	if conn == nil {
		t.Error("Connect() returned nil connection")
		return
	}

	defer conn.Close()

	// Test that we can prepare statements for multiple tables
	tables := []string{"sample", "users", "products", "logs"}
	for _, table := range tables {
		query := "SELECT COUNT(*) FROM " + table
		stmt, err := conn.Prepare(query)
		if err != nil {
			t.Errorf("Prepare() for table %s error = %v", table, err)
			continue
		}
		defer stmt.Close()

		// Execute query to verify table exists
		rows, err := stmt.Query([]driver.Value{})
		if err != nil {
			t.Errorf("Query() for table %s error = %v", table, err)
			continue
		}
		defer rows.Close()

		// Check that we have columns
		columns := rows.Columns()
		if len(columns) == 0 {
			t.Errorf("Query for table %s returned no columns", table)
		}
	}
}

func TestConnectionDump(t *testing.T) {
	t.Parallel()

	// Create a temporary directory for output
	tempDir := t.TempDir()

	d := NewDriver()
	connector, err := d.OpenConnector("../testdata/sample.csv")
	if err != nil {
		t.Fatalf("OpenConnector() error = %v", err)
	}

	conn, err := connector.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer conn.Close()

	// Convert to our connection type
	filesqlConn, ok := conn.(*Connection)
	if !ok {
		t.Fatal("connection is not a filesql connection")
	}

	// Dump database
	if err := filesqlConn.Dump(tempDir); err != nil {
		t.Errorf("Dump() error = %v", err)
		return
	}

	// Check that CSV file was created
	expectedFile := filepath.Join(tempDir, "sample.csv")
	if _, err := os.Stat(expectedFile); err != nil {
		t.Errorf("expected file %s was not created: %v", expectedFile, err)
		return
	}

	// Read the dumped file and verify content
	content, err := os.ReadFile(expectedFile)
	if err != nil {
		t.Errorf("failed to read dumped file: %v", err)
		return
	}

	contentStr := string(content)
	if !strings.Contains(contentStr, "name,age") {
		t.Errorf("expected header 'name,age' in dumped file, got: %s", contentStr)
	}

	// Check that the file contains actual data (not just header)
	lines := strings.Split(strings.TrimSpace(contentStr), "\n")
	if len(lines) <= 1 {
		t.Errorf("expected more than just header in dumped file, got %d lines", len(lines))
	}
}

func TestConnectionDumpMultipleTables(t *testing.T) {
	t.Parallel()

	// Create a temporary directory for output
	tempDir := t.TempDir()

	d := NewDriver()
	// Load multiple files to create multiple tables
	connector, err := d.OpenConnector("../testdata/sample.csv;../testdata/users.csv")
	if err != nil {
		t.Fatalf("OpenConnector() error = %v", err)
	}

	conn, err := connector.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer conn.Close()

	// Convert to our connection type
	filesqlConn, ok := conn.(*Connection)
	if !ok {
		t.Fatal("connection is not a filesql connection")
	}

	// Dump database
	if err := filesqlConn.Dump(tempDir); err != nil {
		t.Errorf("Dump() error = %v", err)
		return
	}

	// Check that both CSV files were created
	expectedFiles := []string{"sample.csv", "users.csv"}
	for _, expectedFile := range expectedFiles {
		fullPath := filepath.Join(tempDir, expectedFile)
		if _, err := os.Stat(fullPath); err != nil {
			t.Errorf("expected file %s was not created: %v", fullPath, err)
			continue
		}

		// Verify file has content
		content, err := os.ReadFile(fullPath)
		if err != nil {
			t.Errorf("failed to read file %s: %v", fullPath, err)
			continue
		}

		if len(content) == 0 {
			t.Errorf("file %s is empty", fullPath)
		}
	}
}

func TestDumpDatabase(t *testing.T) {
	t.Parallel()

	// Create a temporary directory for output
	tempDir := t.TempDir()

	// Register the driver
	sql.Register("filesql_test", NewDriver())

	// Open database
	db, err := sql.Open("filesql_test", "../testdata/sample.csv")
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	// Use the connection directly to dump
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}
	defer conn.Close()

	err = conn.Raw(func(driverConn interface{}) error {
		if filesqlConn, ok := driverConn.(*Connection); ok {
			return filesqlConn.Dump(tempDir)
		}
		return ErrNotFilesqlConnection
	})
	if err != nil {
		t.Errorf("Connection.Dump() error = %v", err)
		return
	}

	// Check that CSV file was created
	expectedFile := filepath.Join(tempDir, "sample.csv")
	if _, err := os.Stat(expectedFile); err != nil {
		t.Errorf("expected file %s was not created: %v", expectedFile, err)
		return
	}

	// Read the dumped file and verify basic structure
	content, err := os.ReadFile(expectedFile)
	if err != nil {
		t.Errorf("failed to read dumped file: %v", err)
		return
	}

	contentStr := string(content)
	lines := strings.Split(strings.TrimSpace(contentStr), "\n")
	if len(lines) == 0 {
		t.Error("dumped file is empty")
	}
}

func TestConnectionGetTableNames(t *testing.T) {
	t.Parallel()

	d := NewDriver()
	connector, err := d.OpenConnector("../testdata/sample.csv")
	if err != nil {
		t.Fatalf("OpenConnector() error = %v", err)
	}

	conn, err := connector.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer conn.Close()

	filesqlConn, ok := conn.(*Connection)
	if !ok {
		t.Fatal("connection is not a filesql connection")
	}

	tableNames, err := filesqlConn.getTableNames()
	if err != nil {
		t.Errorf("getTableNames() error = %v", err)
		return
	}

	if len(tableNames) == 0 {
		t.Error("expected at least one table name")
		return
	}

	// Check that sample table exists
	found := false
	for _, name := range tableNames {
		if name == "sample" {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("expected to find 'sample' table, got tables: %v", tableNames)
	}
}

func TestConnectionGetTableColumns(t *testing.T) {
	t.Parallel()

	d := NewDriver()
	connector, err := d.OpenConnector("../testdata/sample.csv")
	if err != nil {
		t.Fatalf("OpenConnector() error = %v", err)
	}

	conn, err := connector.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer conn.Close()

	filesqlConn, ok := conn.(*Connection)
	if !ok {
		t.Fatal("connection is not a filesql connection")
	}

	columns, err := filesqlConn.getTableColumns("sample")
	if err != nil {
		t.Errorf("getTableColumns() error = %v", err)
		return
	}

	if len(columns) == 0 {
		t.Error("expected at least one column")
		return
	}

	// Check expected columns exist (based on sample.csv structure)
	expectedColumns := []string{"name", "age"}
	for _, expected := range expectedColumns {
		found := false
		for _, column := range columns {
			if column == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected column '%s' not found in columns: %v", expected, columns)
		}
	}
}

func TestEscapeCSVValue(t *testing.T) {
	t.Parallel()

	conn := &Connection{}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Simple value without special characters",
			input:    "hello",
			expected: "hello",
		},
		{
			name:     "Value with comma",
			input:    "hello,world",
			expected: "\"hello,world\"",
		},
		{
			name:     "Value with newline",
			input:    "hello\nworld",
			expected: "\"hello\nworld\"",
		},
		{
			name:     "Value with carriage return",
			input:    "hello\rworld",
			expected: "\"hello\rworld\"",
		},
		{
			name:     "Value with double quotes",
			input:    "hello\"world",
			expected: "\"hello\"\"world\"",
		},
		{
			name:     "Value with multiple quotes",
			input:    "\"hello\" \"world\"",
			expected: "\"\"\"hello\"\" \"\"world\"\"\"",
		},
		{
			name:     "Empty value",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := conn.escapeCSVValue(tt.input)
			if result != tt.expected {
				t.Errorf("escapeCSVValue(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDumpDatabaseNonFilesqlConnection(t *testing.T) {
	t.Parallel()

	// Create a temporary directory for output
	tempDir := t.TempDir()

	// Open a regular SQLite database (not filesql)
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Skip("sqlite driver not available, skipping test")
	}
	defer db.Close()

	// This should return an error since it's not a filesql connection
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}
	defer conn.Close()

	err = conn.Raw(func(driverConn interface{}) error {
		if filesqlConn, ok := driverConn.(*Connection); ok {
			return filesqlConn.Dump(tempDir)
		}
		return ErrNotFilesqlConnection
	})
	if err == nil {
		t.Error("expected error when calling Dump on non-filesql connection")
	}

	if !errors.Is(err, ErrNotFilesqlConnection) {
		t.Errorf("expected ErrNotFilesqlConnection, got: %v", err)
	}
}

func TestDumpToNonExistentDirectory(t *testing.T) {
	t.Parallel()

	d := NewDriver()
	connector, err := d.OpenConnector("../testdata/sample.csv")
	if err != nil {
		t.Fatalf("OpenConnector() error = %v", err)
	}

	conn, err := connector.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer conn.Close()

	filesqlConn, ok := conn.(*Connection)
	if !ok {
		t.Fatal("connection is not a filesql connection")
	}

	// Try to dump to a directory that doesn't exist (but can be created)
	nonExistentDir := filepath.Join(os.TempDir(), "filesql_test_nonexistent", "subdir")
	defer os.RemoveAll(filepath.Join(os.TempDir(), "filesql_test_nonexistent"))

	// This should succeed as it creates the directory
	if err := filesqlConn.Dump(nonExistentDir); err != nil {
		t.Errorf("Dump() to non-existent directory error = %v", err)
		return
	}

	// Verify the directory was created and file exists
	expectedFile := filepath.Join(nonExistentDir, "sample.csv")
	if _, err := os.Stat(expectedFile); err != nil {
		t.Errorf("expected file %s was not created: %v", expectedFile, err)
	}
}
