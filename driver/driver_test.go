package driver

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"modernc.org/sqlite"
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
	content, err := os.ReadFile(expectedFile) //nolint:gosec // Safe: expectedFile is from controlled test data
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
		content, err := os.ReadFile(fullPath) //nolint:gosec // Safe: fullPath is from controlled test data
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
	content, err := os.ReadFile(expectedFile) //nolint:gosec // Safe: expectedFile is from controlled test data
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

func TestDuplicateColumnNameValidation(t *testing.T) {
	t.Parallel()

	d := NewDriver()

	t.Run("Single file with duplicate column names", func(t *testing.T) {
		t.Parallel()

		connector, err := d.OpenConnector("../testdata/duplicate_columns.csv")
		if err != nil {
			t.Fatalf("OpenConnector() error = %v", err)
		}

		_, err = connector.Connect(t.Context())
		if err == nil {
			t.Error("expected error when loading file with duplicate column names")
			return
		}

		if !errors.Is(err, ErrDuplicateColumnName) {
			t.Errorf("expected ErrDuplicateColumnName, got: %v", err)
		}
	})

	t.Run("Multiple files, one with duplicate column names", func(t *testing.T) {
		t.Parallel()

		connector, err := d.OpenConnector("../testdata/sample.csv;../testdata/duplicate_columns.csv")
		if err != nil {
			t.Fatalf("OpenConnector() error = %v", err)
		}

		_, err = connector.Connect(t.Context())
		if err == nil {
			t.Error("expected error when loading files where one has duplicate column names")
			return
		}

		if !errors.Is(err, ErrDuplicateColumnName) {
			t.Errorf("expected ErrDuplicateColumnName, got: %v", err)
		}
	})
}

func TestDuplicateTableNameValidation(t *testing.T) {
	t.Parallel()

	d := NewDriver()

	t.Run("Multiple files with same table name", func(t *testing.T) {
		t.Parallel()

		// Both sample.csv and subdir/sample.csv would create 'sample' table
		connector, err := d.OpenConnector("../testdata/sample.csv;../testdata/subdir/sample.csv")
		if err != nil {
			t.Fatalf("OpenConnector() error = %v", err)
		}

		_, err = connector.Connect(t.Context())
		if err == nil {
			t.Error("expected error when loading files that would create duplicate table names")
			return
		}

		if !errors.Is(err, ErrDuplicateTableName) {
			t.Errorf("expected ErrDuplicateTableName, got: %v", err)
		}

		// Verify error message contains table name and file paths
		errorMessage := err.Error()
		if !strings.Contains(errorMessage, "sample") {
			t.Errorf("error message should contain table name 'sample', got: %s", errorMessage)
		}
	})

	t.Run("Multiple files with different table names", func(t *testing.T) {
		t.Parallel()

		connector, err := d.OpenConnector("../testdata/sample.csv;../testdata/users.csv")
		if err != nil {
			t.Fatalf("OpenConnector() error = %v", err)
		}

		conn, err := connector.Connect(t.Context())
		if err != nil {
			t.Errorf("expected no error when loading files with different table names, got: %v", err)
			return
		}

		if conn != nil {
			_ = conn.Close() // Ignore close error in test cleanup
		}
	})

	t.Run("Directory with files having same base name but different extensions should error", func(t *testing.T) {
		// This test checks that files with different extensions create duplicate table names within same directory
		t.Parallel()

		// Create temp directory with files having same base name
		tempDir := t.TempDir()

		// Create sample.csv
		csvContent := "id,name\n1,John\n2,Jane\n"
		if err := os.WriteFile(filepath.Join(tempDir, "sample.csv"), []byte(csvContent), 0600); err != nil {
			t.Fatalf("failed to create sample.csv: %v", err)
		}

		// Create sample.tsv (same base name "sample" -> duplicate table)
		tsvContent := "id\tname\n1\tJohn\n2\tJane\n"
		if err := os.WriteFile(filepath.Join(tempDir, "sample.tsv"), []byte(tsvContent), 0600); err != nil {
			t.Fatalf("failed to create sample.tsv: %v", err)
		}

		connector, err := d.OpenConnector(tempDir)
		if err != nil {
			t.Fatalf("OpenConnector() error = %v", err)
		}

		_, err = connector.Connect(t.Context())
		if err == nil {
			t.Error("expected error when directory contains files with same base name but different extensions")
			return
		}

		if !errors.Is(err, ErrDuplicateTableName) {
			t.Errorf("expected ErrDuplicateTableName, got: %v", err)
		}
	})

	t.Run("Directory with compressed and uncompressed versions prefers uncompressed", func(t *testing.T) {
		t.Parallel()

		// Test directory contains sample.csv and sample.csv.gz
		// Should prefer uncompressed version and not throw duplicate error within same directory
		connector, err := d.OpenConnector("../testdata")
		if err != nil {
			t.Fatalf("OpenConnector() error = %v", err)
		}

		conn, err := connector.Connect(t.Context())
		if err != nil {
			t.Errorf("expected no error when directory has compressed and uncompressed versions, got: %v", err)
			return
		}

		if conn != nil {
			_ = conn.Close() // Ignore close error in test cleanup
		}
	})
}

func TestConnectionTransactions(t *testing.T) {
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

	t.Run("BeginTx with context", func(t *testing.T) {
		tx, err := filesqlConn.BeginTx(t.Context(), driver.TxOptions{})
		if err != nil {
			t.Errorf("BeginTx() error = %v", err)
			return
		}
		if tx == nil {
			t.Error("BeginTx() returned nil transaction")
			return
		}

		// Test commit
		if err := tx.Commit(); err != nil {
			t.Errorf("Commit() error = %v", err)
		}
	})

	t.Run("BeginTx with rollback", func(t *testing.T) {
		tx, err := filesqlConn.BeginTx(t.Context(), driver.TxOptions{})
		if err != nil {
			t.Errorf("BeginTx() error = %v", err)
			return
		}
		if tx == nil {
			t.Error("BeginTx() returned nil transaction")
			return
		}

		// Test rollback
		if err := tx.Rollback(); err != nil {
			t.Errorf("Rollback() error = %v", err)
		}
	})

	t.Run("Deprecated Begin method", func(t *testing.T) {
		tx, err := filesqlConn.Begin()
		if err != nil {
			t.Errorf("Begin() error = %v", err)
			return
		}
		if tx == nil {
			t.Error("Begin() returned nil transaction")
			return
		}

		// Clean up
		if err := tx.Rollback(); err != nil {
			t.Errorf("Rollback() error = %v", err)
		}
	})
}

func TestConnectionPrepareContext(t *testing.T) {
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

	t.Run("PrepareContext with valid query", func(t *testing.T) {
		stmt, err := filesqlConn.PrepareContext(t.Context(), "SELECT COUNT(*) FROM sample")
		if err != nil {
			t.Errorf("PrepareContext() error = %v", err)
			return
		}
		if stmt == nil {
			t.Error("PrepareContext() returned nil statement")
			return
		}
		defer stmt.Close()
	})

	t.Run("Deprecated Prepare method", func(t *testing.T) {
		stmt, err := filesqlConn.Prepare("SELECT COUNT(*) FROM sample")
		if err != nil {
			t.Errorf("Prepare() error = %v", err)
			return
		}
		if stmt == nil {
			t.Error("Prepare() returned nil statement")
			return
		}
		defer stmt.Close()
	})
}

func TestConnectionClose(t *testing.T) {
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

	t.Run("Close connection", func(t *testing.T) {
		err := conn.Close()
		if err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	t.Run("Close nil connection", func(t *testing.T) {
		nilConn := &Connection{conn: nil}
		err := nilConn.Close()
		if err != nil {
			t.Errorf("Close() with nil connection error = %v", err)
		}
	})
}

func TestErrorHandling(t *testing.T) {
	t.Parallel()

	d := NewDriver()

	t.Run("loadSingleFile with invalid file", func(t *testing.T) {
		connector, err := d.OpenConnector("../testdata/sample.csv")
		if err != nil {
			t.Fatalf("OpenConnector() error = %v", err)
		}

		// Create a mock connection
		sqliteDriver := &sqlite.Driver{}
		sqliteConn, err := sqliteDriver.Open(":memory:")
		if err != nil {
			t.Fatalf("Failed to create SQLite connection: %v", err)
		}
		defer sqliteConn.Close()

		// Test with non-existent file
		filesqlConnector, ok := connector.(*Connector)
		if !ok {
			t.Fatal("connector is not a filesql Connector")
		}
		loadErr := filesqlConnector.loadSingleFile(sqliteConn, "non_existent_file.csv")
		if loadErr == nil {
			t.Error("Expected error when loading non-existent file")
		}
	})

	t.Run("loadDirectory with non-existent directory", func(t *testing.T) {
		connector, err := d.OpenConnector("../testdata/sample.csv")
		if err != nil {
			t.Fatalf("OpenConnector() error = %v", err)
		}

		// Create a mock connection
		sqliteDriver := &sqlite.Driver{}
		sqliteConn, err := sqliteDriver.Open(":memory:")
		if err != nil {
			t.Fatalf("Failed to create SQLite connection: %v", err)
		}
		defer sqliteConn.Close()

		// Test with non-existent directory
		filesqlConnector, ok := connector.(*Connector)
		if !ok {
			t.Fatal("connector is not a filesql Connector")
		}
		loadErr := filesqlConnector.loadDirectory(sqliteConn, "non_existent_directory")
		if loadErr == nil {
			t.Error("Expected error when loading non-existent directory")
		}
	})

	t.Run("loadMultiplePaths with empty paths", func(t *testing.T) {
		connector, err := d.OpenConnector("../testdata/sample.csv")
		if err != nil {
			t.Fatalf("OpenConnector() error = %v", err)
		}

		// Create a mock connection
		sqliteDriver := &sqlite.Driver{}
		sqliteConn, err := sqliteDriver.Open(":memory:")
		if err != nil {
			t.Fatalf("Failed to create SQLite connection: %v", err)
		}
		defer sqliteConn.Close()

		// Test with empty paths
		filesqlConnector, ok := connector.(*Connector)
		if !ok {
			t.Fatal("connector is not a filesql Connector")
		}
		loadErr := filesqlConnector.loadMultiplePaths(sqliteConn, []string{})
		if loadErr == nil {
			t.Error("Expected error when loading empty paths")
		}
		if !errors.Is(loadErr, ErrNoPathsProvided) {
			t.Errorf("Expected ErrNoPathsProvided, got: %v", loadErr)
		}
	})

	t.Run("loadMultiplePaths with whitespace-only paths", func(t *testing.T) {
		connector, err := d.OpenConnector("../testdata/sample.csv")
		if err != nil {
			t.Fatalf("OpenConnector() error = %v", err)
		}

		// Create a mock connection
		sqliteDriver := &sqlite.Driver{}
		sqliteConn, err := sqliteDriver.Open(":memory:")
		if err != nil {
			t.Fatalf("Failed to create SQLite connection: %v", err)
		}
		defer sqliteConn.Close()

		// Test with whitespace-only paths
		filesqlConnector, ok := connector.(*Connector)
		if !ok {
			t.Fatal("connector is not a filesql Connector")
		}
		loadErr := filesqlConnector.loadMultiplePaths(sqliteConn, []string{"   ", "\t", "\n"})
		if loadErr == nil {
			t.Error("Expected error when loading whitespace-only paths")
		}
		if !errors.Is(loadErr, ErrNoFilesLoaded) {
			t.Errorf("Expected ErrNoFilesLoaded, got: %v", loadErr)
		}
	})
}

func TestHelperFunctions(t *testing.T) {
	t.Parallel()

	t.Run("removeCompressionExtensions", func(t *testing.T) {
		tests := []struct {
			input    string
			expected string
		}{
			{"file.csv.gz", "file.csv"},
			{"file.tsv.bz2", "file.tsv"},
			{"file.ltsv.xz", "file.ltsv"},
			{"file.csv.zst", "file.csv"},
			{"file.csv", "file.csv"}, // no compression
		}

		for _, tt := range tests {
			result := removeCompressionExtensions(tt.input)
			if result != tt.expected {
				t.Errorf("removeCompressionExtensions(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		}
	})

	t.Run("countCompressionExtensions", func(t *testing.T) {
		tests := []struct {
			input    string
			expected int
		}{
			{"file.csv.gz", 1},
			{"file.tsv.bz2", 1},
			{"file.ltsv.xz", 1},
			{"file.csv.zst", 1},
			{"file.csv", 0}, // no compression
		}

		for _, tt := range tests {
			result := countCompressionExtensions(tt.input)
			if result != tt.expected {
				t.Errorf("countCompressionExtensions(%q) = %d, expected %d", tt.input, result, tt.expected)
			}
		}
	})
}

func TestExportFunctionality(t *testing.T) {
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

	t.Run("exportTableToCSV with valid table", func(t *testing.T) {
		tempDir := t.TempDir()
		outputPath := filepath.Join(tempDir, "exported_sample.csv")

		err := filesqlConn.exportTableToCSV("sample", outputPath)
		if err != nil {
			t.Errorf("exportTableToCSV() error = %v", err)
			return
		}

		// Verify file was created
		if _, err := os.Stat(outputPath); os.IsNotExist(err) {
			t.Error("Expected exported file to exist")
		}
	})

	t.Run("exportTableToCSV with non-existent table", func(t *testing.T) {
		tempDir := t.TempDir()
		outputPath := filepath.Join(tempDir, "non_existent.csv")

		err := filesqlConn.exportTableToCSV("non_existent_table", outputPath)
		if err == nil {
			t.Error("Expected error when exporting non-existent table")
		}
	})

	t.Run("getTableColumns with valid table", func(t *testing.T) {
		columns, err := filesqlConn.getTableColumns("sample")
		if err != nil {
			t.Errorf("getTableColumns() error = %v", err)
			return
		}

		if len(columns) == 0 {
			t.Error("Expected at least one column")
		}
	})

	t.Run("getTableColumns with non-existent table", func(t *testing.T) {
		columns, err := filesqlConn.getTableColumns("non_existent_table")
		if err != nil {
			t.Errorf("getTableColumns() for non-existent table error = %v", err)
		}
		if len(columns) != 0 {
			t.Errorf("Expected empty columns for non-existent table, got %v", columns)
		}
	})
}

func TestDiverseFileFormats(t *testing.T) {
	t.Parallel()

	d := NewDriver()

	t.Run("Load LTSV file", func(t *testing.T) {
		connector, err := d.OpenConnector("../testdata/logs.ltsv")
		if err != nil {
			t.Fatalf("OpenConnector() error = %v", err)
		}

		conn, err := connector.Connect(t.Context())
		if err != nil {
			t.Fatalf("Connect() error = %v", err)
		}
		defer conn.Close()

		// Verify table exists
		filesqlConn, ok := conn.(*Connection)
		if !ok {
			t.Fatal("connection is not a filesql connection")
		}

		tableNames, err := filesqlConn.getTableNames()
		if err != nil {
			t.Errorf("getTableNames() error = %v", err)
		}

		found := false
		for _, name := range tableNames {
			if name == "logs" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected to find 'logs' table")
		}
	})

	t.Run("Load compressed file", func(t *testing.T) {
		connector, err := d.OpenConnector("../testdata/sample.csv.gz")
		if err != nil {
			t.Fatalf("OpenConnector() error = %v", err)
		}

		conn, err := connector.Connect(t.Context())
		if err != nil {
			t.Fatalf("Connect() error = %v", err)
		}
		defer conn.Close()

		// Verify table exists
		filesqlConn, ok := conn.(*Connection)
		if !ok {
			t.Fatal("connection is not a filesql connection")
		}

		tableNames, err := filesqlConn.getTableNames()
		if err != nil {
			t.Errorf("getTableNames() error = %v", err)
		}

		found := false
		for _, name := range tableNames {
			if name == "sample" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected to find 'sample' table")
		}
	})
}

func TestEdgeCases(t *testing.T) {
	t.Parallel()

	d := NewDriver()

	t.Run("Empty file path in loadFileDirectly", func(t *testing.T) {
		connector, err := d.OpenConnector("../testdata/sample.csv")
		if err != nil {
			t.Fatalf("OpenConnector() error = %v", err)
		}

		sqliteDriver := &sqlite.Driver{}
		sqliteConn, err := sqliteDriver.Open(":memory:")
		if err != nil {
			t.Fatalf("Failed to create SQLite connection: %v", err)
		}
		defer sqliteConn.Close()

		filesqlConnector, ok := connector.(*Connector)
		if !ok {
			t.Fatal("connector is not a filesql Connector")
		}
		loadErr := filesqlConnector.loadFileDirectly(sqliteConn, "")
		if loadErr == nil {
			t.Error("Expected error when loading empty file path")
		}
	})

	t.Run("escapeCSVValue with various inputs", func(t *testing.T) {
		filesqlConn := &Connection{}

		tests := []struct {
			input    string
			expected string
		}{
			{"normal", "normal"},
			{"with,comma", "\"with,comma\""},
			{"with\nnewline", "\"with\nnewline\""},
			{"with\"quote", "\"with\"\"quote\""},
			{"", ""},
		}

		for _, tt := range tests {
			result := filesqlConn.escapeCSVValue(tt.input)
			if result != tt.expected {
				t.Errorf("escapeCSVValue(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		}
	})
}
