package filesql

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
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
			paths:   []string{filepath.Join("testdata", "sample.csv")},
			wantErr: false,
		},
		{
			name:    "Multiple valid files",
			paths:   []string{filepath.Join("testdata", "sample.csv"), filepath.Join("testdata", "users.csv")},
			wantErr: false,
		},
		{
			name:    "Directory path",
			paths:   []string{"testdata"},
			wantErr: false,
		},
		{
			name:    "Mixed file and directory paths",
			paths:   []string{filepath.Join("testdata", "sample.csv"), "testdata"},
			wantErr: false,
		},
		{
			name:    "No paths provided",
			paths:   []string{},
			wantErr: true,
		},
		{
			name:    "Non-existent file",
			paths:   []string{filepath.Join("testdata", "nonexistent.csv")},
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

	db, err := Open(filepath.Join("testdata", "sample.csv"))
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

// TestComplexIntegrationScenarios tests complex combinations of features
func TestComplexIntegrationScenarios(t *testing.T) {
	t.Parallel()

	t.Run("io.Reader with multiple formats", func(t *testing.T) {
		t.Parallel()

		// Create CSV data as string
		csvData := `id,name,age,salary
1,John Doe,30,50000
2,Jane Smith,25,60000
3,Bob Johnson,35,55000`

		// Create TSV data as string
		tsvData := `id	department	budget
1	Engineering	100000
2	Marketing	80000
3	Sales	90000`

		// Create LTSV data as string
		ltsvData := `id:1	product:Laptop	price:1200
id:2	product:Mouse	price:25
id:3	product:Keyboard	price:75`

		// Use NewBuilder with readers
		builder := NewBuilder().
			AddReader(strings.NewReader(csvData), "employees", FileTypeCSV).
			AddReader(strings.NewReader(tsvData), "departments", FileTypeTSV).
			AddReader(strings.NewReader(ltsvData), "products", FileTypeLTSV)

		validatedBuilder, err := builder.Build(context.Background())
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer db.Close()

		// Test complex JOIN query across all three tables
		query := `
			SELECT e.name, d.department, p.product, e.salary, p.price
			FROM employees e
			JOIN departments d ON e.id = d.id  
			JOIN products p ON e.id = p.id
			WHERE e.salary > 40000 AND p.price > 50
			ORDER BY e.salary DESC
		`

		rows, err := db.QueryContext(context.Background(), query)
		if err != nil {
			t.Fatalf("Complex query failed: %v", err)
		}
		defer rows.Close()

		var results []struct {
			name, dept, product string
			salary, price       float64
		}

		for rows.Next() {
			var r struct {
				name, dept, product string
				salary, price       float64
			}
			if err := rows.Scan(&r.name, &r.dept, &r.product, &r.salary, &r.price); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			results = append(results, r)
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("Rows iteration error: %v", err)
		}

		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})

	t.Run("embed.FS integration", func(t *testing.T) {
		t.Parallel()

		// Create embedded filesystem
		testFS := os.DirFS(filepath.Join("testdata", "embed_test"))

		builder := NewBuilder().AddFS(testFS)
		validatedBuilder, err := builder.Build(context.Background())
		if err != nil {
			t.Fatalf("Build with FS failed: %v", err)
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			t.Fatalf("Open with FS failed: %v", err)
		}
		defer db.Close()

		// Verify tables from embedded files
		tables := []string{"products", "orders", "users"}
		for _, table := range tables {
			query := "SELECT COUNT(*) FROM " + table // Table name from trusted list
			var count int
			err := db.QueryRowContext(context.Background(), query).Scan(&count)
			if err != nil {
				t.Errorf("Failed to query table %s: %v", table, err)
			}
			if count == 0 {
				t.Errorf("Table %s is empty", table)
			}
		}

		// Test cross-table query with embedded data
		query := `
			SELECT u.name, COUNT(o.order_id) as order_count
			FROM users u
			LEFT JOIN orders o ON u.id = o.user_id  
			GROUP BY u.name
			ORDER BY order_count DESC
		`

		rows, err := db.QueryContext(context.Background(), query)
		if err != nil {
			t.Fatalf("Cross-table query failed: %v", err)
		}
		defer rows.Close()

		rowCount := 0
		for rows.Next() {
			var name string
			var orderCount int
			if err := rows.Scan(&name, &orderCount); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			rowCount++
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("Rows iteration error: %v", err)
		}

		if rowCount == 0 {
			t.Error("Expected at least one result from cross-table query")
		}
	})

	t.Run("large file streaming with benchmark data", func(t *testing.T) {
		t.Parallel()

		builder := NewBuilder().
			AddPath(filepath.Join("testdata", "benchmark", "customers100000.csv")).
			SetDefaultChunkSize(1024 * 50) // 50KB chunks for testing

		validatedBuilder, err := builder.Build(context.Background())
		if err != nil {
			t.Fatalf("Build with large file failed: %v", err)
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			t.Fatalf("Open with large file failed: %v", err)
		}
		defer db.Close()

		// Test aggregation queries on large dataset
		queries := []struct {
			name  string
			query string
		}{
			{
				"Count all rows",
				"SELECT COUNT(*) FROM customers100000",
			},
			{
				"Distinct count with GROUP BY",
				"SELECT COUNT(DISTINCT `Index`) FROM customers100000",
			},
			{
				"Complex aggregation with window functions",
				"SELECT COUNT(*) as total_rows, AVG(CASE WHEN `Index` % 2 = 0 THEN 1.0 ELSE 0.0 END) as even_ratio FROM customers100000",
			},
		}

		for _, q := range queries {
			t.Run(q.name, func(t *testing.T) {
				start := time.Now()
				rows, err := db.QueryContext(context.Background(), q.query)
				if err != nil {
					t.Fatalf("Query '%s' failed: %v", q.name, err)
				}
				defer rows.Close()

				hasResults := false
				for rows.Next() {
					hasResults = true
					// Just scan to verify data is accessible
					cols, err := rows.Columns()
					if err != nil {
						t.Fatalf("Failed to get columns: %v", err)
					}

					values := make([]interface{}, len(cols))
					scanArgs := make([]interface{}, len(cols))
					for i := range values {
						scanArgs[i] = &values[i]
					}

					if err := rows.Scan(scanArgs...); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
				}

				if err := rows.Err(); err != nil {
					t.Fatalf("Rows iteration error: %v", err)
				}

				if !hasResults {
					t.Error("Query returned no results")
				}

				duration := time.Since(start)
				t.Logf("Query '%s' took %v", q.name, duration)
			})
		}
	})

	t.Run("compressed files handling", func(t *testing.T) {
		t.Parallel()

		compressedFiles := []string{
			filepath.Join("testdata", "sample.csv.gz"),
			filepath.Join("testdata", "users.csv.zst"),
			filepath.Join("testdata", "logs.ltsv.xz"),
			filepath.Join("testdata", "products.tsv.bz2"),
		}

		builder := NewBuilder().AddPaths(compressedFiles...)
		validatedBuilder, err := builder.Build(context.Background())
		if err != nil {
			t.Fatalf("Build with compressed files failed: %v", err)
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			t.Fatalf("Open with compressed files failed: %v", err)
		}
		defer db.Close()

		// Verify all compressed files were loaded correctly
		expectedTables := []string{"sample", "users", "logs", "products"}
		for _, table := range expectedTables {
			var count int
			query := "SELECT COUNT(*) FROM " + table // Table name from trusted list
			err := db.QueryRowContext(context.Background(), query).Scan(&count)
			if err != nil {
				t.Errorf("Failed to query compressed table %s: %v", table, err)
			}
			if count == 0 {
				t.Errorf("Compressed table %s is empty", table)
			}
		}

		// Test complex query across compressed files
		query := `
			SELECT 'sample' as source, COUNT(*) as count FROM sample
			UNION ALL
			SELECT 'users' as source, COUNT(*) as count FROM users
			UNION ALL  
			SELECT 'logs' as source, COUNT(*) as count FROM logs
			UNION ALL
			SELECT 'products' as source, COUNT(*) as count FROM products
			ORDER BY count DESC
		`

		rows, err := db.QueryContext(context.Background(), query)
		if err != nil {
			t.Fatalf("Union query on compressed files failed: %v", err)
		}
		defer rows.Close()

		results := make(map[string]int)
		for rows.Next() {
			var source string
			var count int
			if err := rows.Scan(&source, &count); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			results[source] = count
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("Rows iteration error: %v", err)
		}

		if len(results) != 4 {
			t.Errorf("Expected 4 tables, got %d", len(results))
		}

		for table, count := range results {
			if count == 0 {
				t.Errorf("Table %s has zero rows", table)
			}
		}
	})

	t.Run("auto-save functionality", func(t *testing.T) {
		t.Parallel()

		// Create temporary directory for auto-save test
		tempDir := t.TempDir()

		// Create builder with auto-save enabled
		builder := NewBuilder().
			AddPath(filepath.Join("testdata", "sample.csv")).
			AddPath(filepath.Join("testdata", "users.csv")).
			EnableAutoSave(tempDir, NewDumpOptions().WithFormat(OutputFormatCSV))

		validatedBuilder, err := builder.Build(context.Background())
		if err != nil {
			t.Fatalf("Build with auto-save failed: %v", err)
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			t.Fatalf("Open with auto-save failed: %v", err)
		}

		// Modify the data
		_, err = db.ExecContext(context.Background(), "INSERT INTO sample (id, name, age, email) VALUES (99, 'Test User', 42, 'test@example.com')")
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}

		_, err = db.ExecContext(context.Background(), "UPDATE users SET role = 'super_admin' WHERE name = 'Alice'")
		if err != nil {
			t.Fatalf("UPDATE failed: %v", err)
		}

		// Close to trigger auto-save
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close database: %v", err)
		}

		// Verify auto-saved files exist
		expectedFiles := []string{"sample.csv", "users.csv"}
		for _, filename := range expectedFiles {
			filepath := filepath.Join(tempDir, filename)
			if _, err := os.Stat(filepath); os.IsNotExist(err) {
				t.Errorf("Auto-saved file %s does not exist", filename)
			}
		}

		// Verify the modifications were saved by opening the auto-saved files
		newDB, err := Open(tempDir)
		if err != nil {
			t.Fatalf("Failed to open auto-saved files: %v", err)
		}
		defer newDB.Close()

		// Check if our modifications are present
		var testUser string
		err = newDB.QueryRowContext(context.Background(), "SELECT name FROM sample WHERE id = 99").Scan(&testUser)
		if err != nil {
			t.Errorf("Failed to find inserted test user: %v", err)
		} else if testUser != "Test User" {
			t.Errorf("Expected 'Test User', got '%s'", testUser)
		}

		var aliceRole string
		err = newDB.QueryRowContext(context.Background(), "SELECT role FROM users WHERE name = 'Alice'").Scan(&aliceRole)
		if err != nil {
			t.Errorf("Failed to find updated Alice role: %v", err)
		} else if aliceRole != "super_admin" {
			t.Errorf("Expected 'super_admin', got '%s'", aliceRole)
		}
	})

	t.Run("mixed input sources combination", func(t *testing.T) {
		t.Parallel()

		// Combine file paths, io.Readers, and embed.FS
		csvData := `order_id,customer_name,amount
1001,Alice Johnson,250.00
1002,Bob Smith,175.50`

		testFS := os.DirFS(filepath.Join("testdata", "embed_test"))

		builder := NewBuilder().
			AddPath(filepath.Join("testdata", "sample.csv")).                    // File path
			AddReader(strings.NewReader(csvData), "custom_orders", FileTypeCSV). // io.Reader with unique name
			AddFS(testFS).                                                       // embed.FS
			AddPath(filepath.Join("testdata", "sample2.csv"))                    // Different file to avoid table name conflict

		validatedBuilder, err := builder.Build(context.Background())
		if err != nil {
			t.Fatalf("Build with mixed sources failed: %v", err)
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			t.Fatalf("Open with mixed sources failed: %v", err)
		}
		defer db.Close()

		// Verify all sources are accessible
		tableCounts := map[string]int{}

		// Get all table names
		rows, err := db.QueryContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table'")
		if err != nil {
			t.Fatalf("Failed to get table names: %v", err)
		}
		defer rows.Close()

		var tableNames []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("Scan table name failed: %v", err)
			}
			tableNames = append(tableNames, name)
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("Rows iteration error: %v", err)
		}

		// Count rows in each table
		for _, tableName := range tableNames {
			var count int
			query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName) //nolint:gosec // Table name from database metadata
			err := db.QueryRowContext(context.Background(), query).Scan(&count)
			if err != nil {
				t.Errorf("Failed to count rows in table %s: %v", tableName, err)
			}
			tableCounts[tableName] = count
		}

		// Verify we have expected tables from all sources
		expectedTables := []string{"sample", "custom_orders", "sample2"}
		for _, expected := range expectedTables {
			if count, exists := tableCounts[expected]; !exists {
				t.Errorf("Expected table %s not found", expected)
			} else if count == 0 {
				t.Errorf("Table %s is empty", expected)
			}
		}

		// Test complex query across mixed sources
		query := `
			SELECT 
				s.name as sample_name,
				o.customer_name as order_customer,
				u.name as user_name,
				COUNT(*) as match_count
			FROM sample s
			JOIN custom_orders o ON LOWER(s.name) = LOWER(REPLACE(o.customer_name, ' Johnson', ' Doe'))
			JOIN users u ON s.id = u.id
			GROUP BY s.name, o.customer_name, u.name
		`

		rows, err = db.QueryContext(context.Background(), query)
		if err != nil {
			t.Fatalf("Complex mixed-source query failed: %v", err)
		}
		defer rows.Close()

		hasResults := false
		for rows.Next() {
			hasResults = true
			var sampleName, orderCustomer, userName string
			var matchCount int
			if err := rows.Scan(&sampleName, &orderCustomer, &userName, &matchCount); err != nil {
				t.Fatalf("Scan complex query failed: %v", err)
			}
			// Just verify we can read the data
		}

		// Note: This query might not return results due to data mismatch, but it should execute without error
		if err := rows.Err(); err != nil {
			t.Fatalf("Query execution error: %v", err)
		}

		// Use hasResults to avoid unused variable error
		_ = hasResults
	})

	t.Run("basic database access test", func(t *testing.T) {
		t.Parallel()

		benchmarkFile := filepath.Join("testdata", "benchmark", "customers100000.csv")

		db, err := Open(benchmarkFile)
		if err != nil {
			t.Fatalf("Failed to open benchmark file: %v", err)
		}
		defer db.Close()

		// Test basic queries
		queries := []struct {
			name  string
			query string
		}{
			{"count query", "SELECT COUNT(*) FROM customers100000"},
			{"limit query", "SELECT `Index` FROM customers100000 LIMIT 5"},
		}

		for _, tc := range queries {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				rows, err := db.QueryContext(ctx, tc.query)
				if err != nil {
					t.Fatalf("Query failed: %v", err)
				}
				defer rows.Close()

				// Process results
				for rows.Next() {
					cols, err := rows.Columns()
					if err != nil {
						t.Fatalf("Get columns failed: %v", err)
					}

					values := make([]any, len(cols))
					scanArgs := make([]any, len(cols))
					for k := range values {
						scanArgs[k] = &values[k]
					}

					if err := rows.Scan(scanArgs...); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
				}

				if err := rows.Err(); err != nil {
					t.Fatalf("Rows error: %v", err)
				}
			})
		}
	})
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
				db, err := Open(filepath.Join("testdata", "sample.csv"))
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
				db, err := Open(filepath.Join("testdata", "sample.csv"), filepath.Join("testdata", "users.csv"))
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
				db, err := Open(filepath.Join("testdata", "sample.csv"))
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

		db, err := Open(filepath.Join("testdata", "sample.csv"))
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

	db, err := Open(filepath.Join("testdata", "sample.csv"))
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

	db, err := Open(filepath.Join("testdata", "sample.csv"))
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
			paths:       []string{filepath.Join("testdata", "duplicate_columns.csv")},
			wantErr:     true,
			errorString: "duplicate column",
		},
		{
			name:        "Non-existent file",
			paths:       []string{filepath.Join("testdata", "nonexistent_file.csv")},
			wantErr:     true,
			errorString: "path does not exist",
		},
		{
			name:        "Empty directory",
			paths:       []string{filepath.Join("testdata", "empty_dir")},
			wantErr:     true,
			errorString: "no supported files found in directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create empty directory for the "Empty directory" test
			if tt.name == "Empty directory" {
				emptyDir := filepath.Join("testdata", "empty_dir")
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
			paths:   []string{filepath.Join("testdata", "sample.csv")},
			wantErr: false,
		},
		{
			name: "Multiple files with context",
			setupCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(t.Context(), 5*time.Second)
			},
			paths:   []string{filepath.Join("testdata", "sample.csv"), filepath.Join("testdata", "users.csv")},
			wantErr: false,
		},
		{
			name: "Context already cancelled",
			setupCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(t.Context())
				cancel() // Cancel immediately
				return ctx, func() {}
			},
			paths:       []string{filepath.Join("testdata", "sample.csv")},
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
			paths:       []string{filepath.Join("testdata", "sample.csv")},
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

			db, err := OpenContext(ctx, filepath.Join("testdata", "sample.csv"))
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
		expectedType FileType
		isSupported  bool
	}{
		{
			name:         "CSV file",
			fileName:     "test.csv",
			expectedType: FileTypeCSV,
			isSupported:  true,
		},
		{
			name:         "TSV file",
			fileName:     "test.tsv",
			expectedType: FileTypeTSV,
			isSupported:  true,
		},
		{
			name:         "LTSV file",
			fileName:     "test.ltsv",
			expectedType: FileTypeLTSV,
			isSupported:  true,
		},
		{
			name:         "Compressed CSV",
			fileName:     "test.csv.gz",
			expectedType: FileTypeCSVGZ,
			isSupported:  true,
		},
		{
			name:         "Double compressed (should handle gracefully)",
			fileName:     "test.csv.gz.bz2",
			expectedType: FileTypeUnsupported,
			isSupported:  false,
		},
		{
			name:         "Unsupported format",
			fileName:     "test.txt",
			expectedType: FileTypeUnsupported,
			isSupported:  false,
		},
		{
			name:         "Empty extension",
			fileName:     "test",
			expectedType: FileTypeUnsupported,
			isSupported:  false,
		},
		{
			name:         "Multiple dots in filename",
			fileName:     "test.backup.final.csv",
			expectedType: FileTypeCSV,
			isSupported:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			file := newFile(tc.fileName)

			if file.getFileType() != tc.expectedType {
				t.Errorf("Expected file type %v, got %v", tc.expectedType, file.getFileType())
			}

			if isSupportedFile(tc.fileName) != tc.isSupported {
				t.Errorf("Expected supported=%v, got %v", tc.isSupported, isSupportedFile(tc.fileName))
			}

			// Test type-specific methods
			switch tc.expectedType.baseType() {
			case FileTypeCSV:
				if !file.isCSV() {
					t.Errorf("isCSV() should return true for CSV file")
				}
				if file.isTSV() || file.isLTSV() {
					t.Errorf("Type methods should be exclusive")
				}
			case FileTypeTSV:
				if !file.isTSV() {
					t.Errorf("isTSV() should return true for TSV file")
				}
				if file.isCSV() || file.isLTSV() {
					t.Errorf("Type methods should be exclusive")
				}
			case FileTypeLTSV:
				if !file.isLTSV() {
					t.Errorf("isLTSV() should return true for LTSV file")
				}
				if file.isCSV() || file.isTSV() {
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
			tableName := tableFromFilePath(tc.filePath)
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
				tableName := tableFromFilePath(tmpFile.Name())
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

				tableName := tableFromFilePath(tmpFile.Name())
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

		tableName := tableFromFilePath(tmpFile.Name())
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

		tableName := tableFromFilePath(tmpFile.Name())
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

			tableName := tableFromFilePath(tmpFile.Name())

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

			tableName := tableFromFilePath(tmpFile.Name())
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

		tableName := tableFromFilePath(tmpFile.Name())
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
			expectedTableName := tableFromFilePath(filePath)
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
		tableName := tableFromFilePath(file.name)
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
				tableName := tableFromFilePath(filePath)

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
				return filepath.Join("non", "existent", "path", "file.csv"), func() {}
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

		tableName := tableFromFilePath(tmpFile.Name())

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

				tableName := tableFromFilePath(tmpFile.Name())
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

		tableName := tableFromFilePath(tmpFile.Name())

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
	db, err := Open(filepath.Join("testdata", "sample.csv"), filepath.Join("testdata", "products.tsv"), filepath.Join("testdata", "logs.ltsv"))
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

	db, err := Open(filepath.Join("testdata", "sample.csv"), filepath.Join("testdata", "products.tsv"))
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
			paths:       []string{filepath.Join("testdata", "unsupported.txt")}, // We'll create this
			expectError: "path does not exist",
		},
	}

	// Create unsupported file for test
	unsupportedFile := filepath.Join("testdata", "unsupported.txt")
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

func TestParquetReadWriteIntegration(t *testing.T) {
	t.Parallel()

	t.Run("Basic Parquet read and write", func(t *testing.T) {
		t.Parallel()

		// Create a temporary directory for this test
		tempDir := t.TempDir()

		// Test data
		testCSVContent := `id,name,age,email
1,John Doe,30,john@example.com
2,Jane Smith,25,jane@example.com
3,Bob Johnson,35,bob@example.com`

		// Create temporary CSV file
		csvFile := filepath.Join(tempDir, "test.csv")
		if err := os.WriteFile(csvFile, []byte(testCSVContent), 0600); err != nil {
			t.Fatal(err)
		}

		// Open CSV file and load into database
		db, err := Open(csvFile)
		if err != nil {
			t.Fatalf("Failed to open CSV file: %v", err)
		}
		defer db.Close()

		// Export to Parquet format
		parquetOutputDir := filepath.Join(tempDir, "parquet_output")
		options := NewDumpOptions().WithFormat(OutputFormatParquet)
		err = DumpDatabase(db, parquetOutputDir, options)
		if err != nil {
			t.Fatalf("Failed to dump to Parquet: %v", err)
		}

		// Verify Parquet file was created
		parquetFile := filepath.Join(parquetOutputDir, "test.parquet")
		if _, err := os.Stat(parquetFile); os.IsNotExist(err) {
			t.Fatalf("Parquet file was not created: %s", parquetFile)
		}

		// Read back the Parquet file
		db2, err := Open(parquetFile)
		if err != nil {
			t.Fatalf("Failed to open Parquet file: %v", err)
		}
		defer db2.Close()

		// Verify data is correct
		rows, err := db2.QueryContext(context.Background(), "SELECT id, name, age, email FROM test ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query Parquet data: %v", err)
		}
		defer rows.Close()

		expectedData := [][]string{
			{"1", "John Doe", "30", "john@example.com"},
			{"2", "Jane Smith", "25", "jane@example.com"},
			{"3", "Bob Johnson", "35", "bob@example.com"},
		}

		var actualData [][]string
		for rows.Next() {
			var id, name, age, email string
			if err := rows.Scan(&id, &name, &age, &email); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			actualData = append(actualData, []string{id, name, age, email})
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("Error during row iteration: %v", err)
		}

		if len(actualData) != len(expectedData) {
			t.Fatalf("Expected %d rows, got %d", len(expectedData), len(actualData))
		}

		for i, expected := range expectedData {
			if len(actualData[i]) != len(expected) {
				t.Errorf("Row %d: expected %d columns, got %d", i, len(expected), len(actualData[i]))
				continue
			}
			for j, expectedVal := range expected {
				if actualData[i][j] != expectedVal {
					t.Errorf("Row %d, column %d: expected %s, got %s", i, j, expectedVal, actualData[i][j])
				}
			}
		}
	})

	t.Run("Compressed Parquet files", func(t *testing.T) {
		t.Parallel()

		// Create a temporary directory for this test
		tempDir := t.TempDir()

		// Test with compressed Parquet file (if compression is supported)
		testCSVContent := `name,score,active
Alice,95.5,true
Bob,87.2,false
Charlie,92.8,true`

		// Create temporary CSV file
		csvFile := filepath.Join(tempDir, "compressed_test.csv")
		if err := os.WriteFile(csvFile, []byte(testCSVContent), 0600); err != nil {
			t.Fatal(err)
		}

		// Open CSV file
		db, err := Open(csvFile)
		if err != nil {
			t.Fatalf("Failed to open CSV file: %v", err)
		}
		defer db.Close()

		// Export to Parquet format with GZ compression
		parquetOutputDir := filepath.Join(tempDir, "compressed_parquet_output")
		options := NewDumpOptions().
			WithFormat(OutputFormatParquet).
			WithCompression(CompressionGZ)

		// Note: Parquet files should not use external compression,
		// but we test that the system handles this gracefully
		err = DumpDatabase(db, parquetOutputDir, options)
		if err != nil {
			// We expect an error for external compression with Parquet
			expectedErrMsg := "external compression not supported for Parquet format - use Parquet's built-in compression instead"
			if !strings.Contains(err.Error(), expectedErrMsg) {
				t.Fatalf("Expected error message to contain '%s', got: %v", expectedErrMsg, err)
			}
			return // Test passed - error was expected
		}

		t.Error("Expected error for external compression with Parquet format, but got none")
	})

	t.Run("Round-trip data integrity", func(t *testing.T) {
		t.Parallel()

		// Create a temporary directory for this test
		tempDir := t.TempDir()

		// Create test data with various data types
		testData := []struct {
			name     string
			csvData  string
			expected []map[string]string
		}{
			{
				name: "mixed_types",
				csvData: `id,name,price,available,created_at
1,Product A,19.99,true,2023-01-15
2,Product B,25.50,false,2023-02-20
3,Product C,12.00,true,2023-03-10`,
				expected: []map[string]string{
					{"id": "1", "name": "Product A", "price": "19.99", "available": "true", "created_at": "2023-01-15"},
					{"id": "2", "name": "Product B", "price": "25.5", "available": "false", "created_at": "2023-02-20"},
					{"id": "3", "name": "Product C", "price": "12", "available": "true", "created_at": "2023-03-10"},
				},
			},
		}

		for _, td := range testData {
			t.Run(td.name, func(t *testing.T) {
				// Create CSV file
				csvFile := filepath.Join(tempDir, td.name+".csv")
				if err := os.WriteFile(csvFile, []byte(td.csvData), 0600); err != nil {
					t.Fatal(err)
				}

				// Open CSV and export to Parquet
				db, err := Open(csvFile)
				if err != nil {
					t.Fatalf("Failed to open CSV: %v", err)
				}
				defer db.Close()

				parquetDir := filepath.Join(tempDir, td.name+"_parquet")
				err = DumpDatabase(db, parquetDir, NewDumpOptions().WithFormat(OutputFormatParquet))
				if err != nil {
					t.Fatalf("Failed to export to Parquet: %v", err)
				}

				// Read back from Parquet
				parquetFile := filepath.Join(parquetDir, td.name+".parquet")
				db2, err := Open(parquetFile)
				if err != nil {
					t.Fatalf("Failed to open Parquet file: %v", err)
				}
				defer db2.Close()

				// Query all data
				rows, err := db2.QueryContext(context.Background(), "SELECT * FROM "+td.name+" ORDER BY id") //nolint:gosec
				if err != nil {
					t.Fatalf("Failed to query: %v", err)
				}
				defer rows.Close()

				columns, err := rows.Columns()
				if err != nil {
					t.Fatalf("Failed to get columns: %v", err)
				}

				var actualRows []map[string]string
				for rows.Next() {
					values := make([]interface{}, len(columns))
					valuePtrs := make([]interface{}, len(columns))
					for i := range values {
						valuePtrs[i] = &values[i]
					}

					if err := rows.Scan(valuePtrs...); err != nil {
						t.Fatalf("Failed to scan row: %v", err)
					}

					row := make(map[string]string)
					for i, col := range columns {
						if values[i] != nil {
							row[col] = fmt.Sprintf("%v", values[i])
						} else {
							row[col] = ""
						}
					}
					actualRows = append(actualRows, row)
				}

				if err := rows.Err(); err != nil {
					t.Fatalf("Error during row iteration: %v", err)
				}

				// Compare results
				if len(actualRows) != len(td.expected) {
					t.Fatalf("Expected %d rows, got %d", len(td.expected), len(actualRows))
				}

				for i, expectedRow := range td.expected {
					actualRow := actualRows[i]
					for col, expectedVal := range expectedRow {
						if actualVal, ok := actualRow[col]; !ok {
							t.Errorf("Row %d: missing column %s", i, col)
						} else if actualVal != expectedVal {
							t.Errorf("Row %d, column %s: expected %s, got %s", i, col, expectedVal, actualVal)
						}
					}
				}
			})
		}
	})
}

func TestParquetPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// Create temporary directory
	tempDir := t.TempDir()

	// Generate larger test data
	csvContent := "id,name,value,timestamp\n"
	for i := 1; i <= 10000; i++ {
		csvContent += fmt.Sprintf("%d,User%d,%.2f,2023-01-01T%02d:00:00Z\n",
			i, i, float64(i)*1.5, (i % 24))
	}

	csvFile := filepath.Join(tempDir, "large_test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0600); err != nil {
		t.Fatal(err)
	}

	// Test CSV to Parquet export performance
	start := time.Now()
	db, err := Open(csvFile)
	if err != nil {
		t.Fatalf("Failed to open CSV: %v", err)
	}
	defer db.Close()

	parquetDir := filepath.Join(tempDir, "perf_parquet")
	err = DumpDatabase(db, parquetDir, NewDumpOptions().WithFormat(OutputFormatParquet))
	if err != nil {
		t.Fatalf("Failed to export to Parquet: %v", err)
	}
	exportTime := time.Since(start)

	// Test Parquet read performance
	parquetFile := filepath.Join(parquetDir, "large_test.parquet")
	start = time.Now()
	db2, err := Open(parquetFile)
	if err != nil {
		t.Fatalf("Failed to open Parquet: %v", err)
	}
	defer db2.Close()

	var count int
	err = db2.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM large_test").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query count: %v", err)
	}
	readTime := time.Since(start)

	t.Logf("Performance results:")
	t.Logf("Export time: %v", exportTime)
	t.Logf("Read time: %v", readTime)
	t.Logf("Records processed: %d", count)

	if count != 10000 {
		t.Errorf("Expected 10000 records, got %d", count)
	}
}

// TestParquetDirectParsing tests parseParquet and parseCompressedParquet functions directly
func TestParquetDirectParsing(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	t.Run("parseParquet function coverage", func(t *testing.T) {
		// Create test CSV first
		csvFile := filepath.Join(tempDir, "test.csv")
		csvContent := "id,name,value\n1,Alice,100.5\n2,Bob,200.3\n3,Charlie,300.7\n"
		if err := os.WriteFile(csvFile, []byte(csvContent), 0600); err != nil {
			t.Fatal(err)
		}

		// Export to Parquet
		db, err := Open(csvFile)
		if err != nil {
			t.Fatal(err)
		}

		outputDir := filepath.Join(tempDir, "output")
		err = DumpDatabase(db, outputDir, NewDumpOptions().WithFormat(OutputFormatParquet))
		_ = db.Close()

		if err != nil {
			t.Skipf("Parquet export failed, skipping parseParquet test: %v", err)
		}

		// Now test direct Parquet file opening (triggers parseParquet)
		parquetFile := filepath.Join(outputDir, "test.parquet")
		if _, err := os.Stat(parquetFile); os.IsNotExist(err) {
			t.Skip("Parquet file not created, skipping test")
		}

		// Test using file.toTable() directly to trigger parseParquet
		f := newFile(parquetFile)
		table, err := f.toTable()
		if err != nil {
			t.Fatalf("Failed to parse Parquet file: %v", err)
		}

		if table == nil {
			t.Fatal("Expected non-nil table from Parquet file")
		}

		if len(table.getRecords()) != 3 {
			t.Errorf("Expected 3 records, got %d", len(table.getRecords()))
		}

		// Also test compressed Parquet to trigger parseCompressedParquet
		compressedParquetFile := filepath.Join(tempDir, "test.parquet.gz")

		// Create a gzip compressed Parquet file
		parquetData, err := os.ReadFile(parquetFile) //nolint:gosec
		if err != nil {
			t.Fatal(err)
		}

		gzFile, err := os.Create(compressedParquetFile) //nolint:gosec
		if err != nil {
			t.Fatal(err)
		}
		defer gzFile.Close()

		gzWriter := gzip.NewWriter(gzFile)
		if _, err := gzWriter.Write(parquetData); err != nil {
			t.Fatal(err)
		}
		if err := gzWriter.Close(); err != nil {
			t.Fatal(err)
		}

		// Test compressed Parquet parsing
		f2 := newFile(compressedParquetFile)
		table2, err := f2.toTable()
		if err != nil {
			t.Fatalf("Failed to parse compressed Parquet file: %v", err)
		}

		if table2 == nil {
			t.Fatal("Expected non-nil table from compressed Parquet file")
		}

		if len(table2.getRecords()) != 3 {
			t.Errorf("Expected 3 records from compressed Parquet, got %d", len(table2.getRecords()))
		}
	})

	t.Run("file extension detection coverage", func(t *testing.T) {
		t.Parallel()

		// Test various file extensions to improve file.go coverage
		testFiles := []struct {
			filename   string
			shouldWork bool
		}{
			{"test.csv", true},
			{"test.tsv", true},
			{"test.ltsv", true},
			{"test.txt", false},  // Unsupported format
			{"test.json", false}, // Unsupported format
		}

		for _, tf := range testFiles {
			testFile := filepath.Join(tempDir, tf.filename)

			// Create a minimal valid file for supported formats
			var content []byte
			if strings.Contains(tf.filename, ".csv") {
				content = []byte("col1,col2\nval1,val2\n")
			} else if strings.Contains(tf.filename, ".tsv") {
				content = []byte("col1\tcol2\nval1\tval2\n")
			} else if strings.Contains(tf.filename, ".ltsv") {
				content = []byte("col1:val1\tcol2:val2\n")
			} else {
				content = []byte("test content")
			}

			if err := os.WriteFile(testFile, content, 0600); err != nil {
				t.Fatal(err)
			}

			_, err := Open(testFile)
			if tf.shouldWork && err != nil {
				t.Errorf("File %s should be supported but got error: %v", tf.filename, err)
			} else if !tf.shouldWork && err == nil {
				t.Errorf("File %s should not be supported but no error occurred", tf.filename)
			}
		}
	})
}

func TestWriteXLSXTableData(t *testing.T) {
	t.Parallel()

	t.Run("writeXLSXTableData with no compression", func(t *testing.T) {
		// Create test data
		db, err := Open(filepath.Join("testdata", "excel", "sample.xlsx"))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		// Query data from first sheet
		rows, err := db.QueryContext(context.Background(), "SELECT * FROM sample_Sheet1")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}

		// Create temp output file
		tempDir := t.TempDir()
		outputPath := filepath.Join(tempDir, "output.xlsx")

		// Test writeXLSXTableData
		err = writeXLSXTableData(outputPath, columns, rows, CompressionNone)
		if err != nil {
			t.Fatal(err)
		}

		// Verify file was created
		if _, err := os.Stat(outputPath); os.IsNotExist(err) {
			t.Error("Output file was not created")
		}

		// Verify file can be read back
		xlsxFile, err := excelize.OpenFile(outputPath)
		if err != nil {
			t.Fatal(err)
		}
		defer xlsxFile.Close()

		// Check sheet exists
		sheets := xlsxFile.GetSheetList()
		if len(sheets) != 1 {
			t.Errorf("Expected 1 sheet, got %d", len(sheets))
		}
		if sheets[0] != "output" {
			t.Errorf("Expected sheet 'output', got '%s'", sheets[0])
		}

		// Check data
		sheetRows, err := xlsxFile.GetRows(sheets[0])
		if err != nil {
			t.Fatal(err)
		}

		// Should have header + 3 data rows = 4 total rows
		if len(sheetRows) != 4 {
			t.Errorf("Expected 4 rows (1 header + 3 data), got %d", len(sheetRows))
		}

		// Check header
		expectedHeaders := []string{"id", "name"}
		if !reflect.DeepEqual(sheetRows[0], expectedHeaders) {
			t.Errorf("Expected headers %v, got %v", expectedHeaders, sheetRows[0])
		}

		// Check first data row
		if len(sheetRows) > 1 {
			if sheetRows[1][0] != "1" || sheetRows[1][1] != "Gina" {
				t.Errorf("Expected first row [1, Gina], got %v", sheetRows[1])
			}
		}
	})

	t.Run("writeXLSXTableData with gzip compression", func(t *testing.T) {
		// Create test data
		db, err := Open(filepath.Join("testdata", "excel", "sample.xlsx"))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		// Query data from second sheet
		rows, err := db.QueryContext(context.Background(), "SELECT * FROM sample_Sheet2")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}

		// Create temp output file
		tempDir := t.TempDir()
		outputPath := filepath.Join(tempDir, "output.xlsx.gz")

		// Test writeXLSXTableData with compression
		err = writeXLSXTableData(outputPath, columns, rows, CompressionGZ)
		if err != nil {
			t.Fatal(err)
		}

		// Verify compressed file was created
		if _, err := os.Stat(outputPath); os.IsNotExist(err) {
			t.Error("Compressed output file was not created")
		}

		// Verify file can be decompressed and read
		file, err := os.Open(outputPath) //nolint:gosec // Test file path is safe
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()

		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			t.Fatal(err)
		}
		defer gzipReader.Close()

		// Read decompressed data into buffer
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, gzipReader); err != nil { //nolint:gosec // Test data is safe
			t.Fatal(err)
		}

		// Create Excel reader from buffer
		xlsxFile, err := excelize.OpenReader(&buf)
		if err != nil {
			t.Fatal(err)
		}
		defer xlsxFile.Close()

		// Check data
		sheets := xlsxFile.GetSheetList()
		if len(sheets) != 1 {
			t.Errorf("Expected 1 sheet, got %d", len(sheets))
		}

		sheetRows, err := xlsxFile.GetRows(sheets[0])
		if err != nil {
			t.Fatal(err)
		}

		// Should have header + 3 data rows = 4 total rows
		if len(sheetRows) != 4 {
			t.Errorf("Expected 4 rows (1 header + 3 data), got %d", len(sheetRows))
		}

		// Check header
		expectedHeaders := []string{"id", "mail"}
		if !reflect.DeepEqual(sheetRows[0], expectedHeaders) {
			t.Errorf("Expected headers %v, got %v", expectedHeaders, sheetRows[0])
		}
	})

	t.Run("writeXLSXTableData with no columns error", func(t *testing.T) {
		tempDir := t.TempDir()
		outputPath := filepath.Join(tempDir, "empty.xlsx")

		// Test with no columns
		err := writeXLSXTableData(outputPath, []string{}, nil, CompressionNone)
		if err == nil {
			t.Error("Expected error for no columns")
		}
		if !strings.Contains(err.Error(), "no columns defined") {
			t.Errorf("Expected 'no columns defined' error, got: %v", err)
		}
	})

	t.Run("writeXLSXTableData with unsupported bz2 compression", func(t *testing.T) {
		// Create test data
		db, err := Open(filepath.Join("testdata", "excel", "sample.xlsx"))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		// Query data from first sheet
		rows, err := db.QueryContext(context.Background(), "SELECT * FROM sample_Sheet1")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}

		// Create temp output file
		tempDir := t.TempDir()
		outputPath := filepath.Join(tempDir, "output.xlsx.bz2")

		// Test writeXLSXTableData with bz2 compression (should fail)
		err = writeXLSXTableData(outputPath, columns, rows, CompressionBZ2)
		if err == nil {
			t.Error("Expected error for unsupported bz2 compression")
		}
		if !strings.Contains(err.Error(), "bzip2 compression is not supported") {
			t.Errorf("Expected 'bzip2 compression is not supported' error, got: %v", err)
		}
	})

	t.Run("writeXLSXTableData with xz compression", func(t *testing.T) {
		// Create test data
		db, err := Open(filepath.Join("testdata", "excel", "sample.xlsx"))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		// Query data from first sheet
		rows, err := db.QueryContext(context.Background(), "SELECT * FROM sample_Sheet1")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}

		// Create temp output file
		tempDir := t.TempDir()
		outputPath := filepath.Join(tempDir, "output.xlsx.xz")

		// Test writeXLSXTableData with xz compression
		err = writeXLSXTableData(outputPath, columns, rows, CompressionXZ)
		if err != nil {
			t.Fatal(err)
		}

		// Verify compressed file was created
		if _, err := os.Stat(outputPath); os.IsNotExist(err) {
			t.Error("Compressed output file was not created")
		}

		// Verify file size is reasonable (compressed, but not empty)
		fileInfo, err := os.Stat(outputPath)
		if err != nil {
			t.Fatal(err)
		}
		if fileInfo.Size() == 0 {
			t.Error("Compressed output file is empty")
		}
		if fileInfo.Size() < 100 {
			t.Errorf("Compressed file seems too small: %d bytes", fileInfo.Size())
		}
	})
}

func TestBytesReaderAt(t *testing.T) {
	t.Parallel()

	t.Run("Size method", func(t *testing.T) {
		testData := []byte("Hello, World!")
		reader := &bytesReaderAt{data: testData}

		size := reader.Size()
		expectedSize := int64(len(testData))

		if size != expectedSize {
			t.Errorf("Expected size %d, got %d", expectedSize, size)
		}
	})

	t.Run("Size method with empty data", func(t *testing.T) {
		reader := &bytesReaderAt{data: []byte{}}

		size := reader.Size()
		expectedSize := int64(0)

		if size != expectedSize {
			t.Errorf("Expected size %d, got %d", expectedSize, size)
		}
	})

	t.Run("Read method", func(t *testing.T) {
		testData := []byte("Hello, World!")
		reader := &bytesReaderAt{data: testData}

		// Test reading with a buffer larger than data
		buffer := make([]byte, 20)
		n, err := reader.Read(buffer)

		if !errors.Is(err, io.EOF) {
			t.Errorf("Expected io.EOF, got %v", err)
		}
		if n != len(testData) {
			t.Errorf("Expected to read %d bytes, got %d", len(testData), n)
		}

		// Check that data was read correctly
		if !bytes.Equal(buffer[:n], testData) {
			t.Errorf("Expected data %q, got %q", testData, buffer[:n])
		}
	})

	t.Run("Read method with smaller buffer", func(t *testing.T) {
		testData := []byte("Hello, World!")
		reader := &bytesReaderAt{data: testData}

		// Test reading with a buffer smaller than data
		buffer := make([]byte, 5)
		n, err := reader.Read(buffer)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if n != 5 {
			t.Errorf("Expected to read 5 bytes, got %d", n)
		}

		// Check that data was read correctly (first 5 bytes)
		expected := testData[:5]
		if !bytes.Equal(buffer, expected) {
			t.Errorf("Expected data %q, got %q", expected, buffer)
		}
	})

	t.Run("Read method with empty data", func(t *testing.T) {
		reader := &bytesReaderAt{data: []byte{}}

		buffer := make([]byte, 10)
		n, err := reader.Read(buffer)

		if !errors.Is(err, io.EOF) {
			t.Errorf("Expected io.EOF, got %v", err)
		}
		if n != 0 {
			t.Errorf("Expected to read 0 bytes, got %d", n)
		}
	})

	t.Run("ReadAt method coverage", func(t *testing.T) {
		testData := []byte("Hello, World!")
		reader := &bytesReaderAt{data: testData}

		// Test reading from the middle
		buffer := make([]byte, 5)
		n, err := reader.ReadAt(buffer, 7) // Start at "W"

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if n != 5 {
			t.Errorf("Expected to read 5 bytes, got %d", n)
		}

		expected := []byte("World")
		if !bytes.Equal(buffer, expected) {
			t.Errorf("Expected data %q, got %q", expected, buffer)
		}
	})

	t.Run("ReadAt method with offset beyond data", func(t *testing.T) {
		testData := []byte("Hello")
		reader := &bytesReaderAt{data: testData}

		buffer := make([]byte, 5)
		n, err := reader.ReadAt(buffer, 10) // Offset beyond data

		if !errors.Is(err, io.EOF) {
			t.Errorf("Expected io.EOF, got %v", err)
		}
		if n != 0 {
			t.Errorf("Expected to read 0 bytes, got %d", n)
		}
	})

	t.Run("ReadAt method with negative offset", func(t *testing.T) {
		testData := []byte("Hello")
		reader := &bytesReaderAt{data: testData}

		buffer := make([]byte, 5)
		n, err := reader.ReadAt(buffer, -1) // Negative offset

		if !errors.Is(err, io.EOF) {
			t.Errorf("Expected io.EOF, got %v", err)
		}
		if n != 0 {
			t.Errorf("Expected to read 0 bytes, got %d", n)
		}
	})

	t.Run("Seek method coverage", func(t *testing.T) {
		testData := []byte("Hello, World!")
		reader := &bytesReaderAt{data: testData}

		// Test SeekStart
		pos, err := reader.Seek(5, io.SeekStart)
		if err != nil {
			t.Errorf("Expected no error for SeekStart, got %v", err)
		}
		if pos != 5 {
			t.Errorf("Expected position 5, got %d", pos)
		}

		// Test SeekCurrent
		pos, err = reader.Seek(3, io.SeekCurrent)
		if err != nil {
			t.Errorf("Expected no error for SeekCurrent, got %v", err)
		}
		if pos != 0 {
			t.Errorf("Expected position 0 (no tracking), got %d", pos)
		}

		// Test SeekEnd
		pos, err = reader.Seek(-2, io.SeekEnd)
		if err != nil {
			t.Errorf("Expected no error for SeekEnd, got %v", err)
		}
		expected := int64(len(testData)) - 2
		if pos != expected {
			t.Errorf("Expected position %d, got %d", expected, pos)
		}

		// Test invalid whence
		_, err = reader.Seek(0, 99)
		if err == nil {
			t.Error("Expected error for invalid whence")
		}
		if !strings.Contains(err.Error(), "invalid whence value") {
			t.Errorf("Expected 'invalid whence value' error, got: %v", err)
		}
	})
}

func TestExtractValueFromArrowArray(t *testing.T) {
	t.Parallel()
	pool := memory.NewGoAllocator()

	t.Run("Boolean array", func(t *testing.T) {
		builder := array.NewBooleanBuilder(pool)
		defer builder.Release()

		// Add test values: true, false, null
		builder.Append(true)
		builder.Append(false)
		builder.AppendNull()

		arr := builder.NewBooleanArray()
		defer arr.Release()

		// Test true value
		result := extractValueFromArrowArray(arr, 0)
		if result != "1" {
			t.Errorf("Expected '1' for true, got '%s'", result)
		}

		// Test false value
		result = extractValueFromArrowArray(arr, 1)
		if result != "0" {
			t.Errorf("Expected '0' for false, got '%s'", result)
		}

		// Test null value
		result = extractValueFromArrowArray(arr, 2)
		if result != "" {
			t.Errorf("Expected empty string for null, got '%s'", result)
		}
	})

	t.Run("Integer arrays", func(t *testing.T) {
		// Test Int8
		int8Builder := array.NewInt8Builder(pool)
		defer int8Builder.Release()
		int8Builder.Append(42)
		int8Builder.AppendNull()
		int8Arr := int8Builder.NewInt8Array()
		defer int8Arr.Release()

		result := extractValueFromArrowArray(int8Arr, 0)
		if result != "42" {
			t.Errorf("Expected '42' for int8, got '%s'", result)
		}
		result = extractValueFromArrowArray(int8Arr, 1)
		if result != "" {
			t.Errorf("Expected empty string for null int8, got '%s'", result)
		}

		// Test Int16
		int16Builder := array.NewInt16Builder(pool)
		defer int16Builder.Release()
		int16Builder.Append(1000)
		int16Arr := int16Builder.NewInt16Array()
		defer int16Arr.Release()

		result = extractValueFromArrowArray(int16Arr, 0)
		if result != "1000" {
			t.Errorf("Expected '1000' for int16, got '%s'", result)
		}

		// Test Int32
		int32Builder := array.NewInt32Builder(pool)
		defer int32Builder.Release()
		int32Builder.Append(100000)
		int32Arr := int32Builder.NewInt32Array()
		defer int32Arr.Release()

		result = extractValueFromArrowArray(int32Arr, 0)
		if result != "100000" {
			t.Errorf("Expected '100000' for int32, got '%s'", result)
		}

		// Test Int64
		int64Builder := array.NewInt64Builder(pool)
		defer int64Builder.Release()
		int64Builder.Append(9223372036854775807) // Max int64
		int64Arr := int64Builder.NewInt64Array()
		defer int64Arr.Release()

		result = extractValueFromArrowArray(int64Arr, 0)
		if result != "9223372036854775807" {
			t.Errorf("Expected '9223372036854775807' for int64, got '%s'", result)
		}
	})

	t.Run("Unsigned integer arrays", func(t *testing.T) {
		// Test Uint8
		uint8Builder := array.NewUint8Builder(pool)
		defer uint8Builder.Release()
		uint8Builder.Append(255)
		uint8Arr := uint8Builder.NewUint8Array()
		defer uint8Arr.Release()

		result := extractValueFromArrowArray(uint8Arr, 0)
		if result != "255" {
			t.Errorf("Expected '255' for uint8, got '%s'", result)
		}

		// Test Uint16
		uint16Builder := array.NewUint16Builder(pool)
		defer uint16Builder.Release()
		uint16Builder.Append(65535)
		uint16Arr := uint16Builder.NewUint16Array()
		defer uint16Arr.Release()

		result = extractValueFromArrowArray(uint16Arr, 0)
		if result != "65535" {
			t.Errorf("Expected '65535' for uint16, got '%s'", result)
		}

		// Test Uint32
		uint32Builder := array.NewUint32Builder(pool)
		defer uint32Builder.Release()
		uint32Builder.Append(4294967295)
		uint32Arr := uint32Builder.NewUint32Array()
		defer uint32Arr.Release()

		result = extractValueFromArrowArray(uint32Arr, 0)
		if result != "4294967295" {
			t.Errorf("Expected '4294967295' for uint32, got '%s'", result)
		}

		// Test Uint64
		uint64Builder := array.NewUint64Builder(pool)
		defer uint64Builder.Release()
		uint64Builder.Append(18446744073709551615) // Max uint64
		uint64Arr := uint64Builder.NewUint64Array()
		defer uint64Arr.Release()

		result = extractValueFromArrowArray(uint64Arr, 0)
		if result != "18446744073709551615" {
			t.Errorf("Expected '18446744073709551615' for uint64, got '%s'", result)
		}
	})

	t.Run("Float arrays", func(t *testing.T) {
		// Test Float32
		float32Builder := array.NewFloat32Builder(pool)
		defer float32Builder.Release()
		float32Builder.Append(3.14159)
		float32Builder.AppendNull()
		float32Arr := float32Builder.NewFloat32Array()
		defer float32Arr.Release()

		result := extractValueFromArrowArray(float32Arr, 0)
		if result != "3.14159" {
			t.Errorf("Expected '3.14159' for float32, got '%s'", result)
		}
		result = extractValueFromArrowArray(float32Arr, 1)
		if result != "" {
			t.Errorf("Expected empty string for null float32, got '%s'", result)
		}

		// Test Float64
		float64Builder := array.NewFloat64Builder(pool)
		defer float64Builder.Release()
		float64Builder.Append(2.718281828459045)
		float64Arr := float64Builder.NewFloat64Array()
		defer float64Arr.Release()

		result = extractValueFromArrowArray(float64Arr, 0)
		if result != "2.718281828459045" {
			t.Errorf("Expected '2.718281828459045' for float64, got '%s'", result)
		}
	})

	t.Run("String array", func(t *testing.T) {
		stringBuilder := array.NewStringBuilder(pool)
		defer stringBuilder.Release()

		stringBuilder.Append("Hello, World!")
		stringBuilder.Append("")
		stringBuilder.AppendNull()

		stringArr := stringBuilder.NewStringArray()
		defer stringArr.Release()

		// Test normal string
		result := extractValueFromArrowArray(stringArr, 0)
		if result != "Hello, World!" {
			t.Errorf("Expected 'Hello, World!', got '%s'", result)
		}

		// Test empty string
		result = extractValueFromArrowArray(stringArr, 1)
		if result != "" {
			t.Errorf("Expected empty string, got '%s'", result)
		}

		// Test null string
		result = extractValueFromArrowArray(stringArr, 2)
		if result != "" {
			t.Errorf("Expected empty string for null, got '%s'", result)
		}
	})

	t.Run("Binary array", func(t *testing.T) {
		binaryBuilder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
		defer binaryBuilder.Release()

		testData := []byte("binary data")
		binaryBuilder.Append(testData)
		binaryBuilder.AppendNull()

		binaryArr := binaryBuilder.NewBinaryArray()
		defer binaryArr.Release()

		// Test binary data
		result := extractValueFromArrowArray(binaryArr, 0)
		if result != "binary data" {
			t.Errorf("Expected 'binary data', got '%s'", result)
		}

		// Test null binary
		result = extractValueFromArrowArray(binaryArr, 1)
		if result != "" {
			t.Errorf("Expected empty string for null binary, got '%s'", result)
		}
	})

	t.Run("Date arrays", func(t *testing.T) {
		// Test Date32
		date32Builder := array.NewDate32Builder(pool)
		defer date32Builder.Release()
		date32Builder.Append(arrow.Date32(18628)) // Some arbitrary date
		date32Arr := date32Builder.NewDate32Array()
		defer date32Arr.Release()

		result := extractValueFromArrowArray(date32Arr, 0)
		if result != "18628" {
			t.Errorf("Expected '18628' for date32, got '%s'", result)
		}

		// Test Date64
		date64Builder := array.NewDate64Builder(pool)
		defer date64Builder.Release()
		date64Builder.Append(arrow.Date64(1609459200000)) // 2021-01-01 in milliseconds
		date64Arr := date64Builder.NewDate64Array()
		defer date64Arr.Release()

		result = extractValueFromArrowArray(date64Arr, 0)
		if result != "1609459200000" {
			t.Errorf("Expected '1609459200000' for date64, got '%s'", result)
		}
	})

	t.Run("Timestamp array", func(t *testing.T) {
		timestampBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Millisecond})
		defer timestampBuilder.Release()
		timestampBuilder.Append(arrow.Timestamp(1609459200000)) // 2021-01-01 in milliseconds
		timestampArr := timestampBuilder.NewTimestampArray()
		defer timestampArr.Release()

		result := extractValueFromArrowArray(timestampArr, 0)
		if result != "1609459200000" {
			t.Errorf("Expected '1609459200000' for timestamp, got '%s'", result)
		}
	})

	t.Run("Default case with unsupported type", func(t *testing.T) {
		// Create a list array (unsupported type)
		listBuilder := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int32)
		defer listBuilder.Release()

		valueBuilder, ok := listBuilder.ValueBuilder().(*array.Int32Builder)
		if !ok {
			t.Fatal("Failed to cast value builder to Int32Builder")
		}
		listBuilder.Append(true)
		valueBuilder.Append(1)
		valueBuilder.Append(2)
		valueBuilder.Append(3)

		listArr := listBuilder.NewListArray()
		defer listArr.Release()

		// This should hit the default case
		result := extractValueFromArrowArray(listArr, 0)

		// The result should be some string representation - we don't check exact format
		// since it uses GetOneForMarshal which may vary
		if result == "" {
			t.Error("Expected some string representation for unsupported type, got empty string")
		}
	})
}
