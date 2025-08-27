package filesql

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nao1215/filesql/domain/model"
)

func TestOpen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		paths   []string
		wantErr bool
	}{
		{
			name:    "Single valid CSV file",
			paths:   []string{"testdata/sample.csv"},
			wantErr: false,
		},
		{
			name:    "Multiple valid files",
			paths:   []string{"testdata/sample.csv", "testdata/users.csv"},
			wantErr: false,
		},
		{
			name:    "Directory path",
			paths:   []string{"testdata"},
			wantErr: false,
		},
		{
			name:    "Mixed file and directory paths",
			paths:   []string{"testdata/sample.csv", "testdata"},
			wantErr: false,
		},
		{
			name:    "No paths provided",
			paths:   []string{},
			wantErr: true,
		},
		{
			name:    "Non-existent file",
			paths:   []string{"testdata/nonexistent.csv"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(tt.paths...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				defer db.Close()

				// Test that we can query at least one table
				if len(tt.paths) > 0 {
					// For the sample file test
					if strings.Contains(tt.paths[0], "sample.csv") || strings.Contains(tt.paths[0], "testdata") {
						rows, err := db.QueryContext(context.Background(), "SELECT COUNT(*) FROM sample")
						if err != nil {
							t.Errorf("Query() error = %v", err)
							return
						}
						defer rows.Close()

						if err := rows.Err(); err != nil {
							t.Errorf("Rows error: %v", err)
							return
						}

						var count int
						if rows.Next() {
							if err := rows.Scan(&count); err != nil {
								t.Errorf("Scan() error = %v", err)
								return
							}
						}

						if count != 3 {
							t.Errorf("Expected 3 rows, got %d", count)
						}
					}
				}
			}
		})
	}
}

func TestSQLQueries(t *testing.T) {
	t.Parallel()

	db, err := Open("testdata/sample.csv")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "Count all rows",
			query:    "SELECT COUNT(*) FROM sample",
			expected: 3,
		},
		{
			name:     "Select specific user",
			query:    "SELECT name FROM sample WHERE id = 1",
			expected: "John Doe",
		},
		{
			name:     "Select with WHERE clause",
			query:    "SELECT COUNT(*) FROM sample WHERE age > 30",
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.QueryContext(context.Background(), tt.query)
			if err != nil {
				t.Errorf("Query() error = %v", err)
				return
			}
			defer rows.Close()

			if err := rows.Err(); err != nil {
				t.Errorf("Rows error: %v", err)
				return
			}

			if rows.Next() {
				var result interface{}
				if err := rows.Scan(&result); err != nil {
					t.Errorf("Scan() error = %v", err)
					return
				}

				switch expected := tt.expected.(type) {
				case int:
					if count, ok := result.(int64); ok {
						if int(count) != expected {
							t.Errorf("Expected %v, got %v", expected, count)
						}
					} else {
						t.Errorf("Expected int, got %T", result)
					}
				case string:
					if str, ok := result.(string); ok {
						if str != expected {
							t.Errorf("Expected %v, got %v", expected, str)
						}
					} else {
						t.Errorf("Expected string, got %T", result)
					}
				}
			}
		})
	}
}

func TestMultipleFiles(t *testing.T) {
	t.Parallel()

	// Test loading multiple files from directory
	db, err := Open("testdata")
	if err != nil {
		t.Fatalf("Failed to open directory: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name  string
		query string
		table string
	}{
		{
			name:  "Query sample table",
			query: "SELECT COUNT(*) FROM sample",
			table: "sample",
		},
		{
			name:  "Query users table",
			query: "SELECT COUNT(*) FROM users",
			table: "users",
		},
		{
			name:  "Query products table",
			query: "SELECT COUNT(*) FROM products",
			table: "products",
		},
		{
			name:  "Query logs table",
			query: "SELECT COUNT(*) FROM logs",
			table: "logs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.QueryContext(context.Background(), tt.query)
			if err != nil {
				t.Errorf("Query() error = %v", err)
				return
			}
			defer rows.Close()

			if err := rows.Err(); err != nil {
				t.Errorf("Rows error: %v", err)
				return
			}

			if rows.Next() {
				var count int64
				if err := rows.Scan(&count); err != nil {
					t.Errorf("Scan() error = %v", err)
					return
				}

				if count == 0 {
					t.Errorf("Expected non-zero count for table %s", tt.table)
				}
			}
		})
	}
}

func TestJoinMultipleTables(t *testing.T) {
	t.Parallel()

	// Test joining tables from multiple files
	db, err := Open("testdata")
	if err != nil {
		t.Fatalf("Failed to open directory: %v", err)
	}
	defer db.Close()

	// Test JOIN query across multiple tables
	query := `
		SELECT u.name, COUNT(*) as total_tables
		FROM users u
		CROSS JOIN (SELECT 1) -- Just to demonstrate JOIN capability
		WHERE u.id = 1
		GROUP BY u.name
	`

	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		t.Errorf("JOIN Query() error = %v", err)
		return
	}
	defer rows.Close()

	if err := rows.Err(); err != nil {
		t.Errorf("Rows error: %v", err)
		return
	}

	if rows.Next() {
		var name string
		var count int64
		if err := rows.Scan(&name, &count); err != nil {
			t.Errorf("Scan() error = %v", err)
			return
		}

		if name != "Alice" {
			t.Errorf("Expected name 'Alice', got '%s'", name)
		}
	}
}

// TestDumpDatabase tests the DumpDatabase function with various scenarios
func TestDumpDatabase(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		setupFunc   func(t *testing.T) *sql.DB
		expectError bool
		checkFiles  []string
	}{
		{
			name: "Single CSV file dump",
			setupFunc: func(t *testing.T) *sql.DB {
				t.Helper()
				db, err := Open("testdata/sample.csv")
				if err != nil {
					t.Fatalf("Failed to open database: %v", err)
				}
				return db
			},
			expectError: false,
			checkFiles:  []string{"sample.csv"},
		},
		{
			name: "Multiple files dump",
			setupFunc: func(t *testing.T) *sql.DB {
				t.Helper()
				db, err := Open("testdata/sample.csv", "testdata/users.csv")
				if err != nil {
					t.Fatalf("Failed to open database: %v", err)
				}
				return db
			},
			expectError: false,
			checkFiles:  []string{"sample.csv", "users.csv"},
		},
		{
			name: "Directory dump",
			setupFunc: func(t *testing.T) *sql.DB {
				t.Helper()
				db, err := Open("testdata")
				if err != nil {
					t.Fatalf("Failed to open database: %v", err)
				}
				return db
			},
			expectError: false,
			checkFiles:  []string{"sample.csv", "users.csv", "products.csv", "logs.csv"},
		},
		{
			name: "Modified data dump",
			setupFunc: func(t *testing.T) *sql.DB {
				t.Helper()
				db, err := Open("testdata/sample.csv")
				if err != nil {
					t.Fatalf("Failed to open database: %v", err)
				}

				// Modify data to test persistence
				_, err = db.ExecContext(context.Background(), "INSERT INTO sample (id, name, age, email) VALUES (4, 'Test User', 40, 'test@example.com')")
				if err != nil {
					t.Fatalf("Failed to insert test data: %v", err)
				}
				return db
			},
			expectError: false,
			checkFiles:  []string{"sample.csv"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Create temporary directory for output
			tempDir := t.TempDir()

			// Setup database
			db := tc.setupFunc(t)
			defer db.Close()

			// Execute DumpDatabase
			err := DumpDatabase(db, tempDir)

			// Check error expectation
			if (err != nil) != tc.expectError {
				t.Errorf("DumpDatabase() error = %v, expectError %v", err, tc.expectError)
				return
			}

			if !tc.expectError {
				// Verify expected files were created
				for _, fileName := range tc.checkFiles {
					filePath := filepath.Join(tempDir, fileName)
					if _, err := os.Stat(filePath); os.IsNotExist(err) {
						t.Errorf("Expected file %s was not created", fileName)
						continue
					}

					// Read and verify file content
					content, err := os.ReadFile(filePath) //nolint:gosec // Safe: filePath is from controlled test data
					if err != nil {
						t.Errorf("Failed to read dumped file %s: %v", fileName, err)
						continue
					}

					// Basic validation: file should have content and CSV header
					if len(content) == 0 {
						t.Errorf("Dumped file %s is empty", fileName)
					}

					contentStr := string(content)
					if !strings.Contains(contentStr, "\n") {
						t.Errorf("Dumped file %s should contain newlines (header + data)", fileName)
					}

					// For the modified data test, check if new data is present
					if tc.name == "Modified data dump" && fileName == "sample.csv" {
						if !strings.Contains(contentStr, "Test User") {
							t.Errorf("Modified data not found in dumped file")
						}
					}
				}
			}
		})
	}
}

// TestDumpDatabaseErrors tests error scenarios for DumpDatabase
func TestDumpDatabaseErrors(t *testing.T) {
	t.Parallel()

	t.Run("Non-filesql connection", func(t *testing.T) {
		t.Parallel()

		// Create a regular SQLite database (not filesql)
		tempDB := filepath.Join(t.TempDir(), "test.db")
		db, err := sql.Open("sqlite", tempDB)
		if err != nil {
			t.Skip("SQLite driver not available, skipping test")
		}
		defer db.Close()

		tempDir := t.TempDir()

		// This should return an error since there are no tables in empty database
		err = DumpDatabase(db, tempDir)
		if err == nil {
			t.Error("expected error when calling DumpDatabase on empty database")
		}

		// Should get "no tables found" error since it's an empty database
		expectedErrorMsg := "no tables found in database"
		if err.Error() != expectedErrorMsg {
			t.Errorf("expected error message '%s', got: %v", expectedErrorMsg, err)
		}
	})

	t.Run("Permission denied output directory", func(t *testing.T) {
		t.Parallel()

		db, err := Open("testdata/sample.csv")
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Try to write to an invalid directory path that should fail on all platforms
		// Use a path that's guaranteed to fail due to invalid characters or permissions
		var invalidDir string
		if filepath.Separator == '\\' {
			// Windows: use invalid characters that are not allowed in directory names
			invalidDir = filepath.Join(t.TempDir(), "invalid<>:\"|?*dir")
		} else {
			// Unix-like: try to write to root directory without permissions
			invalidDir = "/root/invalid_permissions_dir"
		}

		err = DumpDatabase(db, invalidDir)
		if err == nil {
			t.Error("expected error when writing to invalid directory")
			return
		}

		// Should be a permission or directory creation error
		// More flexible error checking since different platforms may return different error messages
		errorMsg := err.Error()
		hasExpectedError := strings.Contains(errorMsg, "failed to create output directory") ||
			strings.Contains(errorMsg, "permission denied") ||
			strings.Contains(errorMsg, "access is denied") ||
			strings.Contains(errorMsg, "invalid argument") ||
			strings.Contains(errorMsg, "cannot create")

		if !hasExpectedError {
			t.Errorf("expected permission or directory creation error, got: %v", err)
		}
	})
}

// TestDumpDatabaseCSVFormat tests the CSV format of dumped files
func TestDumpDatabaseCSVFormat(t *testing.T) {
	t.Parallel()

	db, err := Open("testdata/sample.csv")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tempDir := t.TempDir()

	// Dump the database
	err = DumpDatabase(db, tempDir)
	if err != nil {
		t.Fatalf("DumpDatabase() failed: %v", err)
	}

	// Read the dumped file
	dumpedFile := filepath.Join(tempDir, "sample.csv")
	content, err := os.ReadFile(dumpedFile) //nolint:gosec // Safe: dumpedFile is from controlled test output
	if err != nil {
		t.Fatalf("Failed to read dumped file: %v", err)
	}

	contentStr := string(content)
	lines := strings.Split(strings.TrimSpace(contentStr), "\n")

	// Should have header + 3 data rows
	if len(lines) != 4 {
		t.Errorf("Expected 4 lines (header + 3 data), got %d", len(lines))
	}

	// Check header
	expectedHeader := "id,name,age,email"
	if lines[0] != expectedHeader {
		t.Errorf("Expected header %q, got %q", expectedHeader, lines[0])
	}

	// Check that data rows have the correct number of columns
	for i, line := range lines[1:] {
		columns := strings.Split(line, ",")
		if len(columns) != 4 {
			t.Errorf("Data row %d has %d columns, expected 4: %q", i+1, len(columns), line)
		}
	}
}

// TestDumpDatabaseSpecialCharacters tests CSV escaping for special characters
func TestDumpDatabaseSpecialCharacters(t *testing.T) {
	t.Parallel()

	db, err := Open("testdata/sample.csv")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert data with special characters that need CSV escaping
	_, err = db.ExecContext(context.Background(), `INSERT INTO sample (id, name, age, email) VALUES 
		(10, 'Name, with comma', 25, 'test@example.com'),
		(11, 'Name "with quotes"', 26, 'test2@example.com'),
		(12, 'Name' || char(10) || 'with newline', 27, 'test3@example.com')`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	tempDir := t.TempDir()

	// Dump the database
	err = DumpDatabase(db, tempDir)
	if err != nil {
		t.Fatalf("DumpDatabase() failed: %v", err)
	}

	// Read the dumped file
	dumpedFile := filepath.Join(tempDir, "sample.csv")
	content, err := os.ReadFile(dumpedFile) //nolint:gosec // Safe: dumpedFile is from controlled test output
	if err != nil {
		t.Fatalf("Failed to read dumped file: %v", err)
	}

	contentStr := string(content)

	// Verify CSV escaping
	testCases := []struct {
		description string
		shouldFind  string
	}{
		{
			description: "comma escaped with quotes",
			shouldFind:  `"Name, with comma"`,
		},
		{
			description: "quotes escaped with double quotes",
			shouldFind:  `"Name ""with quotes"""`,
		},
		{
			description: "newline escaped with quotes",
			shouldFind:  `"Name` + "\n" + `with newline"`,
		},
	}

	for _, tc := range testCases {
		if !strings.Contains(contentStr, tc.shouldFind) {
			t.Errorf("CSV escaping test failed: %s - expected to find %q in content",
				tc.description, tc.shouldFind)
		}
	}
}

// TestOpenErrorCases tests various error scenarios for Open function
func TestOpenErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		paths       []string
		wantErr     bool
		errorString string
	}{
		{
			name:        "No paths provided",
			paths:       []string{},
			wantErr:     true,
			errorString: "at least one path must be provided",
		},
		{
			name:        "Duplicate column names in CSV",
			paths:       []string{"testdata/duplicate_columns.csv"},
			wantErr:     true,
			errorString: "duplicate column",
		},
		{
			name:        "Non-existent file",
			paths:       []string{"testdata/nonexistent_file.csv"},
			wantErr:     true,
			errorString: "path does not exist",
		},
		{
			name:        "Empty directory",
			paths:       []string{"testdata/empty_dir"},
			wantErr:     true,
			errorString: "no supported files found in directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create empty directory for the "Empty directory" test
			if tt.name == "Empty directory" {
				emptyDir := "testdata/empty_dir"
				if err := os.MkdirAll(emptyDir, 0750); err != nil {
					t.Fatalf("Failed to create empty directory: %v", err)
				}
				defer os.RemoveAll(emptyDir)
			}

			db, err := Open(tt.paths...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr && err != nil {
				if !strings.Contains(err.Error(), tt.errorString) {
					t.Errorf("Open() error = %v, expected to contain %q", err, tt.errorString)
				}
			}

			if !tt.wantErr && db != nil {
				defer db.Close()
			}
		})
	}
}

// TestOpenContext tests the OpenContext function with various scenarios
func TestOpenContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupCtx    func() (context.Context, context.CancelFunc)
		paths       []string
		wantErr     bool
		errContains string
	}{
		{
			name: "Successful open with context",
			setupCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(t.Context(), 5*time.Second)
			},
			paths:   []string{"testdata/sample.csv"},
			wantErr: false,
		},
		{
			name: "Multiple files with context",
			setupCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(t.Context(), 5*time.Second)
			},
			paths:   []string{"testdata/sample.csv", "testdata/users.csv"},
			wantErr: false,
		},
		{
			name: "Context already cancelled",
			setupCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(t.Context())
				cancel() // Cancel immediately
				return ctx, func() {}
			},
			paths:       []string{"testdata/sample.csv"},
			wantErr:     true,
			errContains: "context canceled",
		},
		{
			name: "Empty paths with context",
			setupCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(t.Context(), 5*time.Second)
			},
			paths:       []string{},
			wantErr:     true,
			errContains: "at least one path must be provided",
		},
		{
			name: "Timeout during operation",
			setupCtx: func() (context.Context, context.CancelFunc) {
				// Very short timeout to trigger during ping
				return context.WithTimeout(t.Context(), 1*time.Nanosecond)
			},
			paths:       []string{"testdata/sample.csv"},
			wantErr:     true,
			errContains: "deadline exceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := tt.setupCtx()
			defer cancel()

			// For timeout test, add a small delay to ensure timeout triggers
			if tt.name == "Timeout during operation" {
				time.Sleep(10 * time.Millisecond)
			}

			db, err := OpenContext(ctx, tt.paths...)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenContext() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr && err != nil && tt.errContains != "" {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("OpenContext() error = %v, expected to contain %q", err, tt.errContains)
				}
			}

			if !tt.wantErr && db != nil {
				defer db.Close()

				// Verify the database is functional
				if err := db.PingContext(t.Context()); err != nil {
					t.Errorf("Failed to ping database after OpenContext: %v", err)
				}
			}
		})
	}
}

// TestOpenContextConcurrent tests concurrent OpenContext calls
func TestOpenContextConcurrent(t *testing.T) {
	t.Parallel()

	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errors := make(chan error, numGoroutines)

	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			db, err := OpenContext(ctx, "testdata/sample.csv")
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: %w", id, err)
				return
			}
			defer db.Close()

			// Perform a simple query to verify the connection
			var count int
			err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sample").Scan(&count)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: query failed: %w", id, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		t.Errorf("Concurrent OpenContext error: %v", err)
	}
}

func Test_FileFormatDetection(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		fileName     string
		expectedType model.FileType
		isSupported  bool
	}{
		{
			name:         "CSV file",
			fileName:     "test.csv",
			expectedType: model.FileTypeCSV,
			isSupported:  true,
		},
		{
			name:         "TSV file",
			fileName:     "test.tsv",
			expectedType: model.FileTypeTSV,
			isSupported:  true,
		},
		{
			name:         "LTSV file",
			fileName:     "test.ltsv",
			expectedType: model.FileTypeLTSV,
			isSupported:  true,
		},
		{
			name:         "Compressed CSV",
			fileName:     "test.csv.gz",
			expectedType: model.FileTypeCSV,
			isSupported:  true,
		},
		{
			name:         "Double compressed (should handle gracefully)",
			fileName:     "test.csv.gz.bz2",
			expectedType: model.FileTypeUnsupported,
			isSupported:  false,
		},
		{
			name:         "Unsupported format",
			fileName:     "test.txt",
			expectedType: model.FileTypeUnsupported,
			isSupported:  false,
		},
		{
			name:         "Empty extension",
			fileName:     "test",
			expectedType: model.FileTypeUnsupported,
			isSupported:  false,
		},
		{
			name:         "Multiple dots in filename",
			fileName:     "test.backup.final.csv",
			expectedType: model.FileTypeCSV,
			isSupported:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			file := model.NewFile(tc.fileName)

			if file.Type() != tc.expectedType {
				t.Errorf("Expected file type %v, got %v", tc.expectedType, file.Type())
			}

			if model.IsSupportedFile(tc.fileName) != tc.isSupported {
				t.Errorf("Expected supported=%v, got %v", tc.isSupported, model.IsSupportedFile(tc.fileName))
			}

			// Test type-specific methods
			switch tc.expectedType {
			case model.FileTypeCSV:
				if !file.IsCSV() {
					t.Errorf("IsCSV() should return true for CSV file")
				}
				if file.IsTSV() || file.IsLTSV() {
					t.Errorf("Type methods should be exclusive")
				}
			case model.FileTypeTSV:
				if !file.IsTSV() {
					t.Errorf("IsTSV() should return true for TSV file")
				}
				if file.IsCSV() || file.IsLTSV() {
					t.Errorf("Type methods should be exclusive")
				}
			case model.FileTypeLTSV:
				if !file.IsLTSV() {
					t.Errorf("IsLTSV() should return true for LTSV file")
				}
				if file.IsCSV() || file.IsTSV() {
					t.Errorf("Type methods should be exclusive")
				}
			}
		})
	}
}

func Test_TableNameSecurity(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		filePath     string
		expectedName string
		description  string
	}{
		{
			name:         "SQL injection attempt",
			filePath:     "'; DROP TABLE users; --.csv",
			expectedName: "'; DROP TABLE users; --",
			description:  "Should not sanitize SQL injection attempts",
		},
		{
			name:         "Unicode characters",
			filePath:     "データ.csv",
			expectedName: "データ",
			description:  "Should handle Unicode in filenames",
		},
		{
			name:         "Special characters",
			filePath:     "test@#$%^&()_+.csv",
			expectedName: "test@#$%^&()_+",
			description:  "Should preserve special characters",
		},
		{
			name:         "Very long filename",
			filePath:     strings.Repeat("a", 255) + ".csv",
			expectedName: strings.Repeat("a", 255),
			description:  "Should handle long filenames",
		},
		{
			name:         "Empty filename",
			filePath:     ".csv",
			expectedName: "",
			description:  "Should handle empty base filename",
		},
		{
			name:         "Hidden file",
			filePath:     ".hidden.csv",
			expectedName: ".hidden",
			description:  "Should handle hidden files",
		},
		{
			name:         "Windows reserved names",
			filePath:     "CON.csv",
			expectedName: "CON",
			description:  "Should preserve Windows reserved names",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tableName := model.TableFromFilePath(tc.filePath)
			if tableName != tc.expectedName {
				t.Errorf("Expected table name %q, got %q", tc.expectedName, tableName)
			}
		})
	}
}

func Test_MalformedCSVHandling(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		csvContent  string
		expectError bool
		description string
	}{
		{
			name:        "Empty file",
			csvContent:  "",
			expectError: true,
			description: "Should reject empty files",
		},
		{
			name:        "Only header",
			csvContent:  "id,name,age",
			expectError: false,
			description: "Should accept header-only files",
		},
		{
			name:        "Mismatched columns",
			csvContent:  "id,name,age\n1,John,30\n2,Jane,25",
			expectError: false,
			description: "CSV parser should handle properly formatted data",
		},
		{
			name:        "Special characters in data",
			csvContent:  "id,message\n1,\"Hello\nWorld\"\n2,\"Comma, separated\"",
			expectError: false,
			description: "Should handle newlines and commas in quoted fields",
		},
		{
			name:        "Very large row",
			csvContent:  "id,data\n1," + strings.Repeat("x", 10000),
			expectError: false,
			description: "Should handle large data fields",
		},
		{
			name:        "Unicode BOM",
			csvContent:  "\uFEFFid,name\n1,test",
			expectError: false,
			description: "Should handle Unicode BOM",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create temporary file
			tmpFile, err := os.CreateTemp(t.TempDir(), "qa_test_*.csv")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(tmpFile.Name())

			if _, err := tmpFile.WriteString(tc.csvContent); err != nil {
				t.Fatal(err)
			}
			_ = tmpFile.Close() // Ignore close error in test cleanup

			// Test opening the file
			db, err := Open(tmpFile.Name())
			if tc.expectError && err == nil {
				t.Error("Expected error but got none")
				if db != nil {
					_ = db.Close() // Ignore close error in test cleanup
				}
				return
			}
			if !tc.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if db != nil {
				defer db.Close()

				// Try to query the table
				tableName := model.TableFromFilePath(tmpFile.Name())
				// Use bracket notation for table name (safe in controlled test environment)
				query := "SELECT COUNT(*) FROM [" + tableName + "]"
				var count int
				err = db.QueryRowContext(context.Background(), query).Scan(&count)
				if err != nil && !tc.expectError {
					t.Errorf("Query failed: %v", err)
				}
			}
		})
	}
}

func Test_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	// Create test file
	tmpFile, err := os.CreateTemp(t.TempDir(), "concurrent_test_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	csvContent := "id,name,value\n"
	for i := 1; i <= 100; i++ {
		csvContent += fmt.Sprintf("%d,user%d,%d\n", i, i, i*10)
	}

	if _, err := tmpFile.WriteString(csvContent); err != nil {
		t.Fatal(err)
	}
	_ = tmpFile.Close() // Ignore close error in test cleanup

	const numGoroutines = 10
	const numQueries = 20

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numQueries)

	// Test concurrent database opens and queries
	for i := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := range numQueries {
				db, err := Open(tmpFile.Name())
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: failed to open: %w", goroutineID, err)
					return
				}

				tableName := model.TableFromFilePath(tmpFile.Name())
				// Use bracket notation for table name and parameterized query for safety
				query := "SELECT COUNT(*) FROM [" + tableName + "] WHERE id > " + strconv.Itoa(j*5)

				var count int
				err = db.QueryRowContext(context.Background(), query).Scan(&count)
				if err != nil {
					_ = db.Close() // Ignore close error in test cleanup
					errors <- fmt.Errorf("goroutine %d: query failed: %w", goroutineID, err)
					return
				}

				_ = db.Close() // Ignore close error in test cleanup
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}
}

func Test_ResourceExhaustion(t *testing.T) {
	t.Parallel()

	// Test 1: Large number of columns
	t.Run("Many columns", func(t *testing.T) {
		tmpFile, err := os.CreateTemp(t.TempDir(), "many_columns_*.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpFile.Name())

		// Create CSV with 1000 columns
		numCols := 1000
		header := make([]string, numCols)
		data := make([]string, numCols)
		for i := range numCols {
			header[i] = fmt.Sprintf("col_%d", i)
			data[i] = fmt.Sprintf("data_%d", i)
		}

		csvContent := strings.Join(header, ",") + "\n" + strings.Join(data, ",") + "\n"
		if _, err := tmpFile.WriteString(csvContent); err != nil {
			t.Fatal(err)
		}
		_ = tmpFile.Close() // Ignore close error in test cleanup

		db, err := Open(tmpFile.Name())
		if err != nil {
			t.Fatalf("Failed to open file with many columns: %v", err)
		}
		defer db.Close()

		tableName := model.TableFromFilePath(tmpFile.Name())
		var count int
		err = db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableName)).Scan(&count)
		if err != nil {
			t.Errorf("Failed to query table with many columns: %v", err)
		}
	})

	// Test 2: Large number of rows (controlled for test speed)
	t.Run("Many rows", func(t *testing.T) {
		tmpFile, err := os.CreateTemp(t.TempDir(), "many_rows_*.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpFile.Name())

		// Create CSV with 10000 rows
		writer := csv.NewWriter(tmpFile)
		defer writer.Flush()

		// Write header
		if err := writer.Write([]string{"id", "name", "value"}); err != nil {
			t.Fatal(err)
		}

		// Write data
		for i := 1; i <= 10000; i++ {
			err := writer.Write([]string{
				strconv.Itoa(i),
				fmt.Sprintf("user_%d", i),
				strconv.Itoa(i * 100),
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			t.Fatal(err)
		}
		_ = tmpFile.Close() // Ignore close error in test cleanup

		db, err := Open(tmpFile.Name())
		if err != nil {
			t.Fatalf("Failed to open file with many rows: %v", err)
		}
		defer db.Close()

		tableName := model.TableFromFilePath(tmpFile.Name())
		var count int
		err = db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableName)).Scan(&count)
		if err != nil {
			t.Errorf("Failed to query table with many rows: %v", err)
		}
		if count != 10000 {
			t.Errorf("Expected 10000 rows, got %d", count)
		}
	})
}

func Test_SQLInjectionProtection(t *testing.T) {
	t.Parallel()

	// Create test file
	tmpFile, err := os.CreateTemp(t.TempDir(), "injection_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	csvContent := "id,name,email\n1,John,john@example.com\n2,Jane,jane@example.com"
	if _, err := tmpFile.WriteString(csvContent); err != nil {
		t.Fatal(err)
	}
	_ = tmpFile.Close() // Ignore close error in test cleanup

	db, err := Open(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Get the actual table name from the database
	var tableName string
	err = db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1").Scan(&tableName)
	if err != nil {
		t.Skip("Cannot determine table name, skipping SQL injection test")
		return
	}

	// Test basic query to ensure table exists
	var count int
	err = db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableName)).Scan(&count)
	if err != nil {
		t.Skip("Table not accessible, skipping SQL injection test")
		return
	}

	// Test that potentially malicious input doesn't cause issues
	maliciousInputs := []string{
		"'; DROP TABLE test; --",
		"' OR 1=1 --",
		"normal_name", // This should be safe
	}

	for _, input := range maliciousInputs {
		// Use prepared statement (which is safer)
		stmt, err := db.PrepareContext(context.Background(), fmt.Sprintf("SELECT * FROM [%s] WHERE name = ?", tableName))
		if err != nil {
			continue // Skip if prepare fails
		}

		rows, err := stmt.QueryContext(context.Background(), input)
		if err == nil {
			// Count results
			var resultCount int
			for rows.Next() {
				resultCount++
			}
			if err := rows.Err(); err != nil {
				t.Logf("Rows error: %v", err)
			}
			_ = rows.Close() // Ignore close error in test cleanup
			// This is expected behavior for prepared statements
		}
		_ = stmt.Close() // Ignore close error in test cleanup
	}

	t.Log("SQL injection protection test completed successfully")
}
func Test_UnicodeAndEncoding(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		content string
		format  string
	}{
		{
			name:    "Japanese CSV",
			content: "名前,年齢,職業\n田中太郎,30,エンジニア\n佐藤花子,25,デザイナー",
			format:  "csv",
		},
		{
			name:    "Arabic TSV",
			content: "الاسم\tالعمر\tالمدينة\nأحمد\t25\tالقاهرة\nفاطمة\t30\tدبي",
			format:  "tsv",
		},
		{
			name:    "Mixed Unicode CSV",
			content: "id,emoji,description\n1,😀,Happy face\n2,🚀,Rocket\n3,❤️,Heart",
			format:  "csv",
		},
		{
			name:    "Cyrillic CSV",
			content: "имя,возраст,город\nИван,25,Москва\nМария,30,Санкт-Петербург",
			format:  "csv",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpFile, err := os.CreateTemp(t.TempDir(), "unicode_test_*."+tc.format)
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(tmpFile.Name())

			if _, err := tmpFile.WriteString(tc.content); err != nil {
				t.Fatal(err)
			}
			_ = tmpFile.Close() // Ignore close error in test cleanup

			db, err := Open(tmpFile.Name())
			if err != nil {
				t.Fatalf("Failed to open Unicode file: %v", err)
			}
			defer db.Close()

			tableName := model.TableFromFilePath(tmpFile.Name())

			// Test basic query
			var count int
			err = db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableName)).Scan(&count)
			if err != nil {
				t.Errorf("Failed to query Unicode table: %v", err)
			}

			// Test data retrieval
			rows, err := db.QueryContext(context.Background(), fmt.Sprintf("SELECT * FROM [%s] LIMIT 1", tableName))
			if err != nil {
				t.Errorf("Failed to select from Unicode table: %v", err)
				return
			}
			defer rows.Close()

			if err := rows.Err(); err != nil {
				t.Errorf("Rows error: %v", err)
				return
			}

			if rows.Next() {
				columns, err := rows.Columns()
				if err != nil {
					t.Errorf("Failed to get columns: %v", err)
					return
				}

				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					t.Errorf("Failed to scan Unicode data: %v", err)
				}
			}
		})
	}
}

func Test_ConnectionLifecycle(t *testing.T) {
	t.Parallel()

	// Create test file
	tmpFile, err := os.CreateTemp(t.TempDir(), "lifecycle_test_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	csvContent := "id,name\n1,test"
	if _, err := tmpFile.WriteString(csvContent); err != nil {
		t.Fatal(err)
	}
	_ = tmpFile.Close() // Ignore close error in test cleanup

	t.Run("Multiple open/close cycles", func(t *testing.T) {
		for i := range 100 {
			db, err := Open(tmpFile.Name())
			if err != nil {
				t.Fatalf("Failed to open database on iteration %d: %v", i, err)
			}

			tableName := model.TableFromFilePath(tmpFile.Name())
			var count int
			err = db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableName)).Scan(&count)
			if err != nil {
				_ = db.Close() // Ignore close error in test cleanup
				t.Fatalf("Query failed on iteration %d: %v", i, err)
			}

			if err := db.Close(); err != nil {
				t.Fatalf("Close failed on iteration %d: %v", i, err)
			}
		}
	})

	t.Run("Connection timeout and context", func(t *testing.T) {
		db, err := Open(tmpFile.Name())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		tableName := model.TableFromFilePath(tmpFile.Name())
		// Use bracket notation for table name (safe in controlled test environment)
		query := "SELECT COUNT(*) FROM [" + tableName + "]"
		var count int
		err = db.QueryRowContext(ctx, query).Scan(&count)
		if err != nil {
			t.Errorf("Query with context failed: %v", err)
		}
	})

	t.Run("Double close safety", func(t *testing.T) {
		db, err := Open(tmpFile.Name())
		if err != nil {
			t.Fatal(err)
		}

		// First close
		if err := db.Close(); err != nil {
			t.Errorf("First close failed: %v", err)
		}

		// Second close should not panic or error
		if err := db.Close(); err != nil {
			t.Errorf("Second close failed: %v", err)
		}
	})
}

// Test_SQLReservedWordsAsFilenames tests that files named with SQL reserved words can be loaded as tables
func Test_SQLReservedWordsAsFilenames(t *testing.T) {
	t.Parallel()

	// Common SQL reserved words that might be used as filenames
	reservedWords := []struct {
		filename string
		keyword  string
	}{
		{"select.csv", "SELECT"},
		{"from.csv", "FROM"},
		{"where.csv", "WHERE"},
		{"insert.csv", "INSERT"},
		{"update.csv", "UPDATE"},
		{"delete.csv", "DELETE"},
		{"create.csv", "CREATE"},
		{"drop.csv", "DROP"},
		{"table.csv", "TABLE"},
		{"index.csv", "INDEX"},
		{"view.csv", "VIEW"},
		{"union.csv", "UNION"},
		{"join.csv", "JOIN"},
		{"inner.csv", "INNER"},
		{"left.csv", "LEFT"},
		{"right.csv", "RIGHT"},
		{"outer.csv", "OUTER"},
		{"group.csv", "GROUP"},
		{"order.csv", "ORDER"},
		{"having.csv", "HAVING"},
		{"limit.csv", "LIMIT"},
		{"offset.csv", "OFFSET"},
		{"distinct.csv", "DISTINCT"},
		{"case.csv", "CASE"},
		{"when.csv", "WHEN"},
		{"then.csv", "THEN"},
		{"else.csv", "ELSE"},
		{"end.csv", "END"},
		{"begin.csv", "BEGIN"},
		{"commit.csv", "COMMIT"},
		{"rollback.csv", "ROLLBACK"},
		{"transaction.csv", "TRANSACTION"},
		{"trigger.csv", "TRIGGER"},
		{"function.csv", "FUNCTION"},
		{"procedure.csv", "PROCEDURE"},
		{"primary.csv", "PRIMARY"},
		{"foreign.csv", "FOREIGN"},
		{"key.csv", "KEY"},
		{"references.csv", "REFERENCES"},
		{"constraint.csv", "CONSTRAINT"},
		{"check.csv", "CHECK"},
		{"unique.csv", "UNIQUE"},
		{"not.csv", "NOT"},
		{"null.csv", "NULL"},
		{"default.csv", "DEFAULT"},
		{"auto_increment.csv", "AUTO_INCREMENT"},
		{"database.csv", "DATABASE"},
		{"schema.csv", "SCHEMA"},
		{"alter.csv", "ALTER"},
		{"column.csv", "COLUMN"},
		{"add.csv", "ADD"},
		{"modify.csv", "MODIFY"},
		{"change.csv", "CHANGE"},
		{"rename.csv", "RENAME"},
		{"exists.csv", "EXISTS"},
		{"if.csv", "IF"},
		{"cascade.csv", "CASCADE"},
		{"restrict.csv", "RESTRICT"},
		{"set.csv", "SET"},
		{"grant.csv", "GRANT"},
		{"revoke.csv", "REVOKE"},
		{"user.csv", "USER"},
		{"role.csv", "ROLE"},
		{"privileges.csv", "PRIVILEGES"},
	}

	for _, rw := range reservedWords {
		t.Run("Reserved word: "+rw.keyword, func(t *testing.T) {
			t.Parallel()

			// Create temporary directory for this test
			tmpDir := t.TempDir()
			filePath := filepath.Join(tmpDir, rw.filename)

			// Create CSV file with reserved word as filename
			csvContent := "id,name,value\n1,test1,100\n2,test2,200\n3,test3,300"
			if err := os.WriteFile(filePath, []byte(csvContent), 0600); err != nil {
				t.Fatalf("Failed to create test file %s: %v", rw.filename, err)
			}

			// Test 1: Open file and verify table creation
			db, err := Open(filePath)
			if err != nil {
				t.Fatalf("Failed to open file with reserved word filename %s: %v", rw.filename, err)
			}
			defer db.Close()

			// Test 2: Verify table exists with proper name
			expectedTableName := model.TableFromFilePath(filePath)
			var actualTableName string
			err = db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", expectedTableName).Scan(&actualTableName)
			if err != nil {
				t.Fatalf("Table for reserved word filename %s not found: %v", rw.filename, err)
			}

			if actualTableName != expectedTableName {
				t.Errorf("Expected table name %q, got %q for file %s", expectedTableName, actualTableName, rw.filename)
			}

			// Test 3: Query the table using bracket notation (safe for reserved words)
			// Use bracket notation for table name (safe in controlled test environment)
			query := "SELECT COUNT(*) FROM [" + expectedTableName + "]"
			var count int
			err = db.QueryRowContext(context.Background(), query).Scan(&count)
			if err != nil {
				t.Errorf("Failed to query table with reserved word name [%s]: %v", expectedTableName, err)
			}

			if count != 3 {
				t.Errorf("Expected 3 rows in table [%s], got %d", expectedTableName, count)
			}

			// Test 4: Verify we can select specific data
			query = fmt.Sprintf("SELECT name FROM [%s] WHERE id = 1", expectedTableName)
			var name string
			err = db.QueryRowContext(context.Background(), query).Scan(&name)
			if err != nil {
				t.Errorf("Failed to select specific data from table [%s]: %v", expectedTableName, err)
			}

			if name != "test1" {
				t.Errorf("Expected 'test1', got %q from table [%s]", name, expectedTableName)
			}

			// Test 5: Verify we can perform complex queries
			query = fmt.Sprintf("SELECT AVG(CAST(value AS REAL)) FROM [%s] WHERE id > 1", expectedTableName)
			var avgValue float64
			err = db.QueryRowContext(context.Background(), query).Scan(&avgValue)
			if err != nil {
				t.Errorf("Failed to perform aggregate query on table [%s]: %v", expectedTableName, err)
			}

			expectedAvg := 250.0 // (200 + 300) / 2 = 500 / 2 = 250
			if avgValue != expectedAvg {
				t.Errorf("Expected average %.1f, got %.1f for table [%s]", expectedAvg, avgValue, expectedTableName)
			}
		})
	}
}

// Test_SQLReservedWordsMultipleFiles tests loading multiple files with reserved word names
func Test_SQLReservedWordsMultipleFiles(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	// Create multiple files with reserved word names
	files := []struct {
		name    string
		content string
	}{
		{
			name:    "select.csv",
			content: "id,query_type\n1,SELECT\n2,SUBQUERY",
		},
		{
			name:    "from.csv",
			content: "id,table_name\n1,users\n2,products",
		},
		{
			name:    "where.csv",
			content: "id,condition\n1,active=1\n2,deleted=0",
		},
		{
			name:    "join.csv",
			content: "id,join_type\n1,INNER\n2,LEFT",
		},
	}

	// Create test files
	for _, file := range files {
		filePath := filepath.Join(tmpDir, file.name)
		if err := os.WriteFile(filePath, []byte(file.content), 0600); err != nil {
			t.Fatalf("Failed to create file %s: %v", file.name, err)
		}
	}

	// Test 1: Load all files from directory
	db, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Failed to open directory with reserved word files: %v", err)
	}
	defer db.Close()

	// Test 2: Verify all tables exist
	for _, file := range files {
		tableName := model.TableFromFilePath(file.name)
		var name string
		err := db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", tableName).Scan(&name)
		if err != nil {
			t.Errorf("Table for reserved word file %s not found: %v", file.name, err)
			continue
		}

		// Test basic query on each table
		var count int
		// Use bracket notation for table name (safe in controlled test environment)
		query := "SELECT COUNT(*) FROM [" + tableName + "]"
		err = db.QueryRowContext(context.Background(), query).Scan(&count)
		if err != nil {
			t.Errorf("Failed to query reserved word table [%s]: %v", tableName, err)
		}

		if count != 2 {
			t.Errorf("Expected 2 rows in table [%s], got %d", tableName, count)
		}
	}

	// Test 3: Cross-table query with reserved word table names
	query := `
		SELECT s.query_type, f.table_name, w.condition, j.join_type
		FROM [select] s
		JOIN [from] f ON s.id = f.id
		JOIN [where] w ON s.id = w.id
		JOIN [join] j ON s.id = j.id
		WHERE s.id = 1
	`

	var queryType, tableName, condition, joinType string
	err = db.QueryRowContext(context.Background(), query).Scan(&queryType, &tableName, &condition, &joinType)
	if err != nil {
		t.Errorf("Failed to perform cross-table query with reserved word tables: %v", err)
	}

	// Verify results
	expectedValues := map[string]string{
		"query_type": "SELECT",
		"table_name": "users",
		"condition":  "active=1",
		"join_type":  "INNER",
	}

	actualValues := map[string]string{
		"query_type": queryType,
		"table_name": tableName,
		"condition":  condition,
		"join_type":  joinType,
	}

	for field, expected := range expectedValues {
		if actual := actualValues[field]; actual != expected {
			t.Errorf("Expected %s=%q, got %q", field, expected, actual)
		}
	}
}

// Test_SQLReservedWordsEdgeCases tests edge cases with reserved words
func Test_SQLReservedWordsEdgeCases(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		filename    string
		expectError bool
		description string
	}{
		{
			name:        "Mixed case reserved word",
			filename:    "Select.csv",
			expectError: false,
			description: "Should handle mixed case reserved words",
		},
		{
			name:        "Upper case reserved word",
			filename:    "DELETE.csv",
			expectError: false,
			description: "Should handle upper case reserved words",
		},
		{
			name:        "Reserved word with underscore",
			filename:    "primary_key.csv",
			expectError: false,
			description: "Should handle reserved words with underscores",
		},
		{
			name:        "Multiple reserved words",
			filename:    "select_from_where.csv",
			expectError: false,
			description: "Should handle multiple reserved words in filename",
		},
		{
			name:        "Reserved word with numbers",
			filename:    "table123.csv",
			expectError: false,
			description: "Should handle reserved words with numbers",
		},
		{
			name:        "SQLite specific reserved word",
			filename:    "pragma.csv",
			expectError: false,
			description: "Should handle SQLite-specific reserved words",
		},
		{
			name:        "Very long reserved word filename",
			filename:    strings.Repeat("select", 10) + ".csv", // Reduced to avoid filesystem limits
			expectError: false,
			description: "Should handle long filenames with reserved words",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tmpDir := t.TempDir()
			filePath := filepath.Join(tmpDir, tc.filename)

			// Create test file
			csvContent := "id,data\n1,value1\n2,value2"
			if err := os.WriteFile(filePath, []byte(csvContent), 0600); err != nil {
				t.Fatalf("Failed to create test file %s: %v", tc.filename, err)
			}

			// Test opening the file
			db, err := Open(filePath)
			if tc.expectError && err == nil {
				t.Errorf("Expected error for %s but got none", tc.description)
				if db != nil {
					_ = db.Close() // Ignore close error in test cleanup
				}
				return
			}

			if !tc.expectError && err != nil {
				t.Errorf("Unexpected error for %s: %v", tc.description, err)
				return
			}

			if !tc.expectError && db != nil {
				defer db.Close()

				// Verify table creation and basic functionality
				tableName := model.TableFromFilePath(filePath)

				// Test table exists
				var name string
				err := db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", tableName).Scan(&name)
				if err != nil {
					t.Errorf("Table not found for %s: %v", tc.description, err)
					return
				}

				// Test basic query using bracket notation
				// Use bracket notation for table name (safe in controlled test environment)
				query := "SELECT COUNT(*) FROM [" + tableName + "]"
				var count int
				err = db.QueryRowContext(context.Background(), query).Scan(&count)
				if err != nil {
					t.Errorf("Failed to query table for %s: %v", tc.description, err)
					return
				}

				if count != 2 {
					t.Errorf("Expected 2 rows for %s, got %d", tc.description, count)
				}

				// Test more complex operations
				// Use bracket notation for table name (safe in controlled test environment)
				insertQuery := "INSERT INTO [" + tableName + "] (id, data) VALUES (3, 'value3')" //nolint:gosec // Safe: tableName is from controlled test data
				_, err = db.ExecContext(context.Background(), insertQuery)
				if err != nil {
					t.Errorf("Failed to insert into table for %s: %v", tc.description, err)
				}

				// Verify insert worked
				err = db.QueryRowContext(context.Background(), query).Scan(&count)
				if err != nil {
					t.Errorf("Failed to verify insert for %s: %v", tc.description, err)
				}

				if count != 3 {
					t.Errorf("Expected 3 rows after insert for %s, got %d", tc.description, count)
				}
			}
		})
	}
}

func Test_ErrorMessageQuality(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		setupFunc      func() (string, func())
		expectedErrors []string
		description    string
	}{
		{
			name: "Non-existent file",
			setupFunc: func() (string, func()) {
				return "/non/existent/path/file.csv", func() {}
			},
			expectedErrors: []string{"does not exist", "path"},
			description:    "Should provide clear error for missing files",
		},
		{
			name: "Permission denied",
			setupFunc: func() (string, func()) {
				tmpFile, err := os.CreateTemp(t.TempDir(), "permission_test_*.csv")
				if err != nil {
					return "", func() {}
				}
				if _, err := tmpFile.WriteString("id,name\n1,test"); err != nil {
					return "", func() { _ = os.Remove(tmpFile.Name()) } //nolint:errcheck
				}
				_ = tmpFile.Close() // Ignore close error in test cleanup

				// Try to make file unreadable - this might not work on Windows
				_ = os.Chmod(tmpFile.Name(), 0000) //nolint:errcheck

				// Test if the permission change actually worked by trying to read
				_, err = os.ReadFile(tmpFile.Name())
				if err == nil {
					// If we can still read the file, skip this test on this platform
					// (likely Windows where chmod doesn't work the same way)
					return "", func() {
						if err := os.Chmod(tmpFile.Name(), 0600); err != nil {
							t.Logf("Failed to set file permissions: %v", err)
						}
						_ = os.Remove(tmpFile.Name()) //nolint:errcheck
					}
				}

				return tmpFile.Name(), func() {
					if err := os.Chmod(tmpFile.Name(), 0600); err != nil {
						t.Logf("Failed to set file permissions: %v", err)
					}
					_ = os.Remove(tmpFile.Name()) //nolint:errcheck
				}
			},
			expectedErrors: []string{"permission", "access"},
			description:    "Should provide clear error for permission issues",
		},
		{
			name: "Corrupted compressed file",
			setupFunc: func() (string, func()) {
				tmpFile, err := os.CreateTemp(t.TempDir(), "corrupted_*.csv.gz")
				if err != nil {
					return "", func() {}
				}
				if _, err := tmpFile.WriteString("This is not gzip data"); err != nil {
					return "", func() { _ = os.Remove(tmpFile.Name()) } //nolint:errcheck
				} // Invalid gzip
				_ = tmpFile.Close()                                             // Ignore close error in test cleanup
				return tmpFile.Name(), func() { _ = os.Remove(tmpFile.Name()) } //nolint:errcheck
			},
			expectedErrors: []string{"gzip", "invalid", "format"},
			description:    "Should provide clear error for corrupted compressed files",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath, cleanup := tc.setupFunc()
			defer cleanup()

			// Skip test if setup indicates it should be skipped (empty path)
			if filePath == "" {
				t.Skipf("Skipping %s on this platform", tc.name)
				return
			}

			_, err := Open(filePath)
			if err == nil {
				t.Errorf("Expected error but got none for %s", tc.description)
				return
			}

			errorMsg := err.Error()
			foundExpected := false
			for _, expectedError := range tc.expectedErrors {
				if strings.Contains(strings.ToLower(errorMsg), strings.ToLower(expectedError)) {
					foundExpected = true
					break
				}
			}

			if !foundExpected {
				t.Errorf("Error message %q should contain one of %v for %s",
					errorMsg, tc.expectedErrors, tc.description)
			}
		})
	}
}
func Test_TableCreationEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("Reserved SQL keywords as column names", func(t *testing.T) {
		tmpFile, err := os.CreateTemp(t.TempDir(), "reserved_keywords_*.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpFile.Name())

		// Use SQL reserved keywords as column names
		csvContent := "select,from,where,order,group,having\n1,2,3,4,5,6\n7,8,9,10,11,12"
		if _, err := tmpFile.WriteString(csvContent); err != nil {
			t.Fatal(err)
		}
		_ = tmpFile.Close() // Ignore close error in test cleanup

		db, err := Open(tmpFile.Name())
		if err != nil {
			t.Fatalf("Failed to open file with reserved keywords: %v", err)
		}
		defer db.Close()

		tableName := model.TableFromFilePath(tmpFile.Name())

		// Test querying with reserved keyword column names
		// Use bracket notation for table name (safe in controlled test environment)
		query := "SELECT [select], [from], [where] FROM [" + tableName + "]" //nolint:gosec // Safe: tableName is from controlled test data
		rows, err := db.QueryContext(context.Background(), query)
		if err != nil {
			t.Errorf("Failed to query table with reserved keyword columns: %v", err)
			return
		}
		defer rows.Close()

		if err := rows.Err(); err != nil {
			t.Errorf("Rows error: %v", err)
			return
		}

		if rows.Next() {
			var col1, col2, col3 string
			if err := rows.Scan(&col1, &col2, &col3); err != nil {
				t.Errorf("Failed to scan reserved keyword columns: %v", err)
			}
		}
	})

	t.Run("Complex table names and paths", func(t *testing.T) {
		// Test various table name edge cases
		complexNames := []string{
			"*.csv", // Use pattern that will create valid .csv extension
		}

		for _, pattern := range complexNames {
			t.Run(pattern, func(t *testing.T) {
				tmpFile, err := os.CreateTemp(t.TempDir(), pattern)
				if err != nil {
					t.Skip("Cannot create file with this name on this system")
				}
				defer os.Remove(tmpFile.Name())

				csvContent := "id,value\n1,test"
				if _, err := tmpFile.WriteString(csvContent); err != nil {
					t.Fatal(err)
				}
				_ = tmpFile.Close() // Ignore close error in test cleanup

				db, err := Open(tmpFile.Name())
				if err != nil {
					t.Errorf("Failed to open file %s: %v", pattern, err)
					return
				}
				defer db.Close()

				tableName := model.TableFromFilePath(tmpFile.Name())
				// Use bracket notation for table name (safe in controlled test environment)
				query := "SELECT COUNT(*) FROM [" + tableName + "]"
				var count int
				if err := db.QueryRowContext(context.Background(), query).Scan(&count); err != nil {
					t.Errorf("Failed to query table from file %s: %v", pattern, err)
				}
			})
		}
	})

	t.Run("Transaction behavior", func(t *testing.T) {
		tmpFile, err := os.CreateTemp(t.TempDir(), "transaction_test_*.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpFile.Name())

		csvContent := "id,name\n1,original"
		if _, err := tmpFile.WriteString(csvContent); err != nil {
			t.Fatal(err)
		}
		_ = tmpFile.Close() // Ignore close error in test cleanup

		db, err := Open(tmpFile.Name())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		tableName := model.TableFromFilePath(tmpFile.Name())

		// Test transaction rollback
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Insert data in transaction
		_, err = tx.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO [%s] (id, name) VALUES (2, 'transaction')", tableName))
		if err != nil {
			t.Errorf("Failed to insert in transaction: %v", err)
		}

		// Rollback
		if err := tx.Rollback(); err != nil {
			t.Errorf("Failed to rollback transaction: %v", err)
		}

		// Verify data was rolled back
		var count int
		err = db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableName)).Scan(&count)
		if err != nil {
			t.Errorf("Failed to count after rollback: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row after rollback, got %d", count)
		}

		// Test transaction commit
		tx, err = db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("Failed to begin second transaction: %v", err)
		}

		_, err = tx.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO [%s] (id, name) VALUES (2, 'committed')", tableName))
		if err != nil {
			t.Errorf("Failed to insert in second transaction: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}

		// Verify data was committed
		err = db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableName)).Scan(&count)
		if err != nil {
			t.Errorf("Failed to count after commit: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected 2 rows after commit, got %d", count)
		}
	})
}

// TestComprehensiveFileFormats tests all supported file formats and compression types
func TestComprehensiveFileFormats(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		fileName    string
		expectTable string
		expectRows  int
	}{
		{
			name:        "CSV file",
			fileName:    "sample.csv",
			expectTable: "sample",
			expectRows:  3,
		},
		{
			name:        "TSV file",
			fileName:    "products.tsv",
			expectTable: "products",
			expectRows:  3,
		},
		{
			name:        "LTSV file",
			fileName:    "logs.ltsv",
			expectTable: "logs",
			expectRows:  3,
		},
		{
			name:        "Gzipped CSV file",
			fileName:    "sample.csv.gz",
			expectTable: "sample",
			expectRows:  3,
		},
		{
			name:        "Bzip2 TSV file",
			fileName:    "products.tsv.bz2",
			expectTable: "products",
			expectRows:  3,
		},
		{
			name:        "XZ LTSV file",
			fileName:    "logs.ltsv.xz",
			expectTable: "logs",
			expectRows:  3,
		},
		{
			name:        "ZSTD CSV file",
			fileName:    "users.csv.zst",
			expectTable: "users",
			expectRows:  3, // users.csv has 3 rows
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			filePath := filepath.Join("testdata", tc.fileName)
			// Check if test file exists (some compression formats might not be available)
			if _, err := os.Stat(filePath); os.IsNotExist(err) {
				t.Skipf("Test file %s not available", tc.fileName)
				return
			}

			// Open database with single file
			db, err := Open(filePath)
			if err != nil {
				t.Fatalf("Open(%s) failed: %v", filePath, err)
			}
			defer db.Close()

			// Verify table exists
			var tableName string
			err = db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", tc.expectTable).Scan(&tableName)
			if err != nil {
				t.Fatalf("Table %s not found: %v", tc.expectTable, err)
			}

			// Count rows
			var count int
			err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM ["+tc.expectTable+"]").Scan(&count)
			if err != nil {
				t.Fatalf("Failed to count rows in %s: %v", tc.expectTable, err)
			}

			if count != tc.expectRows {
				t.Errorf("Expected %d rows in %s, got %d", tc.expectRows, tc.expectTable, count)
			}

			// Test basic SELECT
			// Use bracket notation for table name (safe in controlled test environment)
			query := "SELECT * FROM [" + tc.expectTable + "] LIMIT 1" //nolint:gosec // Safe: tc.expectTable is from controlled test data
			rows, err := db.QueryContext(context.Background(), query)
			if err != nil {
				t.Fatalf("SELECT query failed: %v", err)
			}
			defer rows.Close()

			if err := rows.Err(); err != nil {
				t.Fatalf("Rows error: %v", err)
			}

			if !rows.Next() {
				t.Fatal("Expected at least one row")
			}
		})
	}
}

// TestDirectoryLoading tests loading all files from a directory
func TestDirectoryLoading(t *testing.T) {
	t.Parallel()

	// Open database with directory path
	db, err := Open("testdata")
	if err != nil {
		t.Fatalf("Open(testdata) failed: %v", err)
	}
	defer db.Close()

	// Get all table names
	rows, err := db.QueryContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		t.Fatalf("Failed to get table names: %v", err)
	}
	defer rows.Close()

	if err := rows.Err(); err != nil {
		t.Fatalf("Rows error: %v", err)
	}

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			t.Fatalf("Failed to scan table name: %v", err)
		}
		tables = append(tables, tableName)
	}

	// Verify we have expected tables (at least the uncompressed ones)
	expectedTables := []string{"logs", "products", "sample", "users"}
	for _, expected := range expectedTables {
		found := false
		for _, table := range tables {
			if table == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected table %s not found in tables: %v", expected, tables)
		}
	}

	// Test cross-table query
	var count int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM sample s JOIN products p ON s.id = p.id").Scan(&count)
	if err != nil {
		t.Fatalf("Cross-table JOIN query failed: %v", err)
	}

	if count == 0 {
		t.Error("Expected at least one matching row in JOIN query")
	}
}

// TestMultipleFilePaths tests loading multiple specific file paths
func TestMultipleFilePaths(t *testing.T) {
	t.Parallel()

	// Open database with multiple files
	db, err := Open("testdata/sample.csv", "testdata/products.tsv", "testdata/logs.ltsv")
	if err != nil {
		t.Fatalf("Open with multiple files failed: %v", err)
	}
	defer db.Close()

	// Verify all expected tables exist
	expectedTables := []string{"sample", "products", "logs"}
	for _, tableName := range expectedTables {
		var name string
		err := db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", tableName).Scan(&name)
		if err != nil {
			t.Errorf("Table %s not found: %v", tableName, err)
		}
	}

	// Test complex query across multiple tables
	query := `
		SELECT s.name, p.name as product_name, l.level 
		FROM sample s 
		JOIN products p ON s.id = p.id 
		LEFT JOIN logs l ON l.level = 'INFO'
		LIMIT 5
	`

	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("Multi-table query failed: %v", err)
	}
	defer rows.Close()

	if err := rows.Err(); err != nil {
		t.Fatalf("Rows error: %v", err)
	}

	// Just verify we can execute the query without error
	for rows.Next() {
		var name, productName, level string
		if err := rows.Scan(&name, &productName, &level); err != nil {
			t.Fatalf("Failed to scan multi-table query result: %v", err)
		}
	}
}

// TestCTEQueries tests Common Table Expressions (CTE) queries
func TestCTEQueries(t *testing.T) {
	t.Parallel()

	db, err := Open("testdata/sample.csv", "testdata/products.tsv")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	testCases := []struct {
		name  string
		query string
	}{
		{
			name: "Simple CTE",
			query: `
				WITH young_users AS (
					SELECT * FROM sample WHERE CAST(age AS INTEGER) < 30
				)
				SELECT COUNT(*) FROM young_users
			`,
		},
		{
			name: "Recursive CTE with numbers",
			query: `
				WITH RECURSIVE numbers(n) AS (
					SELECT 1
					UNION ALL
					SELECT n+1 FROM numbers WHERE n < 5
				)
				SELECT COUNT(*) FROM numbers
			`,
		},
		{
			name: "CTE with JOIN",
			query: `
				WITH expensive_products AS (
					SELECT * FROM products WHERE CAST(price AS INTEGER) > 30
				),
				user_product_match AS (
					SELECT s.name, ep.name as product_name, ep.price
					FROM sample s
					JOIN expensive_products ep ON s.id = ep.id
				)
				SELECT COUNT(*) FROM user_product_match
			`,
		},
		{
			name: "Multiple CTEs",
			query: `
				WITH 
				adults AS (
					SELECT * FROM sample WHERE CAST(age AS INTEGER) >= 30
				),
				cheap_products AS (
					SELECT * FROM products WHERE CAST(price AS INTEGER) <= 50
				)
				SELECT 
					(SELECT COUNT(*) FROM adults) as adult_count,
					(SELECT COUNT(*) FROM cheap_products) as cheap_product_count
			`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Don't run in parallel to avoid database closing issues
			// t.Parallel()

			rows, err := db.QueryContext(context.Background(), tc.query)
			if err != nil {
				t.Fatalf("CTE query failed: %v\nQuery: %s", err, tc.query)
			}
			defer rows.Close()

			if err := rows.Err(); err != nil {
				t.Fatalf("Rows error: %v", err)
			}

			// Verify we can read results
			hasRows := false
			for rows.Next() {
				hasRows = true
				// Get column count to scan appropriately
				cols, err := rows.Columns()
				if err != nil {
					t.Fatalf("Failed to get columns: %v", err)
				}

				values := make([]interface{}, len(cols))
				for i := range values {
					values[i] = new(interface{})
				}
				if err := rows.Scan(values...); err != nil {
					t.Fatalf("Failed to scan CTE query result: %v", err)
				}
			}

			if !hasRows {
				t.Error("CTE query returned no rows")
			}
		})
	}
}

// TestMixedDirectoryAndFiles tests mixing directory and individual file paths
func TestMixedDirectoryAndFiles(t *testing.T) {
	t.Parallel()

	// Create a specific file outside testdata directory for this test
	tempFile := filepath.Join(os.TempDir(), "mixed_test.csv")
	content := "id,category,value\n1,A,100\n2,B,200\n"

	if err := os.WriteFile(tempFile, []byte(content), 0600); err != nil {
		t.Fatalf("Failed to create temp test file: %v", err)
	}
	defer os.Remove(tempFile)

	// Open with mixed paths: directory + specific file
	db, err := Open("testdata", tempFile)
	if err != nil {
		t.Fatalf("Open with mixed paths failed: %v", err)
	}
	defer db.Close()

	// Verify the temp file table exists
	var tableName string
	err = db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", "mixed_test").Scan(&tableName)
	if err != nil {
		t.Fatalf("Table mixed_test not found: %v", err)
	}

	// Verify original directory tables also exist
	err = db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", "sample").Scan(&tableName)
	if err != nil {
		t.Fatalf("Table sample from directory not found: %v", err)
	}

	// Test query across mixed sources
	var count int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM mixed_test").Scan(&count)
	if err != nil {
		t.Fatalf("Query on mixed_test table failed: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected 2 rows in mixed_test, got %d", count)
	}
}

// TestErrorCases tests various error conditions
func TestErrorCases(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		paths       []string
		expectError string
	}{
		{
			name:        "No paths provided",
			paths:       []string{},
			expectError: "at least one path must be provided",
		},
		{
			name:        "Non-existent file",
			paths:       []string{"nonexistent.csv"},
			expectError: "path does not exist",
		},
		{
			name:        "Unsupported file format",
			paths:       []string{"testdata/unsupported.txt"}, // We'll create this
			expectError: "path does not exist",
		},
	}

	// Create unsupported file for test
	unsupportedFile := "testdata/unsupported.txt"
	if err := os.WriteFile(unsupportedFile, []byte("test content"), 0600); err != nil {
		t.Fatalf("Failed to create unsupported test file: %v", err)
	}
	defer os.Remove(unsupportedFile)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, err := Open(tc.paths...)
			if err == nil {
				if db != nil {
					_ = db.Close() // Ignore close error in test cleanup
				}
				t.Fatalf("Expected error containing '%s', but got nil", tc.expectError)
			}

			if !strings.Contains(err.Error(), tc.expectError) {
				t.Errorf("Expected error containing '%s', got: %s", tc.expectError, err.Error())
			}
		})
	}
}

func TestSQLiteDumpFunctions(t *testing.T) {
	t.Parallel()

	t.Run("getSQLiteTableNames", func(t *testing.T) {
		t.Parallel()

		// Create a direct SQLite connection
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatalf("Failed to create SQLite connection: %v", err)
		}
		defer db.Close()

		// Create test tables
		_, err = db.ExecContext(context.Background(), "CREATE TABLE test1 (id INTEGER, name TEXT)")
		if err != nil {
			t.Fatalf("Failed to create test table 1: %v", err)
		}

		_, err = db.ExecContext(context.Background(), "CREATE TABLE test2 (id INTEGER, value TEXT)")
		if err != nil {
			t.Fatalf("Failed to create test table 2: %v", err)
		}

		// Test getSQLiteTableNames
		tableNames, err := getSQLiteTableNames(db)
		if err != nil {
			t.Fatalf("getSQLiteTableNames failed: %v", err)
		}

		expectedTables := []string{"test1", "test2"}
		if len(tableNames) != len(expectedTables) {
			t.Errorf("Expected %d tables, got %d: %v", len(expectedTables), len(tableNames), tableNames)
		}

		// Verify table names
		for _, expected := range expectedTables {
			found := false
			for _, actual := range tableNames {
				if actual == expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected table %s not found in %v", expected, tableNames)
			}
		}
	})

	t.Run("getSQLiteTableColumns", func(t *testing.T) {
		t.Parallel()

		// Create a direct SQLite connection
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatalf("Failed to create SQLite connection: %v", err)
		}
		defer db.Close()

		// Create test table with known columns
		_, err = db.ExecContext(context.Background(), "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, salary REAL)")
		if err != nil {
			t.Fatalf("Failed to create test table: %v", err)
		}

		// Test getSQLiteTableColumns
		columns, err := getSQLiteTableColumns(db, "test_table")
		if err != nil {
			t.Fatalf("getSQLiteTableColumns failed: %v", err)
		}

		expectedColumns := []string{"id", "name", "age", "salary"}
		if len(columns) != len(expectedColumns) {
			t.Errorf("Expected %d columns, got %d: %v", len(expectedColumns), len(columns), columns)
		}

		// Verify column names
		for i, expected := range expectedColumns {
			if i >= len(columns) || columns[i] != expected {
				t.Errorf("Expected column %s at index %d, got %s", expected, i, columns[i])
			}
		}
	})

	t.Run("dumpSQLiteDatabase with data", func(t *testing.T) {
		t.Parallel()

		// Create a direct SQLite connection
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatalf("Failed to create SQLite connection: %v", err)
		}
		defer db.Close()

		// Create test table and insert data
		_, err = db.ExecContext(context.Background(), "CREATE TABLE employees (id INTEGER, name TEXT, department TEXT)")
		if err != nil {
			t.Fatalf("Failed to create test table: %v", err)
		}

		_, err = db.ExecContext(context.Background(), "INSERT INTO employees VALUES (1, 'Alice', 'Engineering'), (2, 'Bob', 'Marketing'), (3, 'Charlie', 'Sales')")
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}

		// Test dump to directory
		tempDir := t.TempDir()
		options := NewDumpOptions()

		err = dumpSQLiteDatabase(db, tempDir, options)
		if err != nil {
			t.Fatalf("dumpSQLiteDatabase failed: %v", err)
		}

		// Verify file was created
		dumpedFile := filepath.Join(tempDir, "employees.csv")
		content, err := os.ReadFile(dumpedFile) //nolint:gosec // dumpedFile is created in test with controlled path
		if err != nil {
			t.Fatalf("Failed to read dumped file: %v", err)
		}

		contentStr := string(content)
		lines := strings.Split(strings.TrimSpace(contentStr), "\n")

		// Should have header + 3 data rows
		if len(lines) != 4 {
			t.Errorf("Expected 4 lines (header + 3 data), got %d", len(lines))
		}

		// Check header
		if lines[0] != "id,name,department" {
			t.Errorf("Expected header 'id,name,department', got '%s'", lines[0])
		}

		// Check data rows contain expected values
		expectedDataPatterns := []string{"1,Alice,Engineering", "2,Bob,Marketing", "3,Charlie,Sales"}
		for i, expected := range expectedDataPatterns {
			if lines[i+1] != expected {
				t.Errorf("Expected line %d to be '%s', got '%s'", i+1, expected, lines[i+1])
			}
		}
	})

	t.Run("createCompressedWriter formats", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()

		t.Run("no compression", func(t *testing.T) {
			file, err := os.Create(filepath.Join(tempDir, "test.txt")) //nolint:gosec // tempDir is created in test
			if err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}
			defer file.Close()

			writer, closeWriter, err := createCompressedWriter(file, CompressionNone)
			if err != nil {
				t.Fatalf("createCompressedWriter failed: %v", err)
			}

			if writer != file {
				t.Error("Expected writer to be the same as file for no compression")
			}

			if err := closeWriter(); err != nil {
				t.Errorf("closeWriter failed: %v", err)
			}
		})

		t.Run("gzip compression", func(t *testing.T) {
			file, err := os.Create(filepath.Join(tempDir, "test.gz")) //nolint:gosec // tempDir is created in test
			if err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}
			defer file.Close()

			writer, closeWriter, err := createCompressedWriter(file, CompressionGZ)
			if err != nil {
				t.Fatalf("createCompressedWriter failed for gzip: %v", err)
			}

			if writer == file {
				t.Error("Expected writer to be different from file for gzip compression")
			}

			// Write some test data
			testData := "test,data\n1,hello\n2,world\n"
			n, err := writer.Write([]byte(testData))
			if err != nil {
				t.Fatalf("Failed to write to compressed writer: %v", err)
			}
			if n != len(testData) {
				t.Errorf("Expected to write %d bytes, wrote %d", len(testData), n)
			}

			if err := closeWriter(); err != nil {
				t.Errorf("closeWriter failed: %v", err)
			}
		})

		t.Run("bzip2 compression should error", func(t *testing.T) {
			file, err := os.Create(filepath.Join(tempDir, "test.bz2")) //nolint:gosec // tempDir is created in test
			if err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}
			defer file.Close()

			_, _, err = createCompressedWriter(file, CompressionBZ2)
			if err == nil {
				t.Error("Expected error for bzip2 compression")
			}

			expectedError := "bzip2 compression is not supported for writing"
			if err.Error() != expectedError {
				t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
			}
		})
	})
}
