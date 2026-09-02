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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xuri/excelize/v2"
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
			// testdata/tree, not testdata: see
			// TestOpenDirectoryWithCollidingBasenamesFails for why loading the
			// whole tree is an error.
			name:    "Directory path",
			paths:   []string{filepath.Join("testdata", "tree")},
			wantErr: false,
		},
		{
			// sample2.csv, not sample.csv: the tree holds its own sample.csv, and
			// two different files asking for one table is a collision rather than
			// something to resolve silently.
			name:    "Mixed file and directory paths",
			paths:   []string{filepath.Join("testdata", "sample2.csv"), filepath.Join("testdata", "tree")},
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
			// Skip slow directory tests in local development
			if (tt.name == "Directory path" || tt.name == "Mixed file and directory paths") && os.Getenv("GITHUB_ACTIONS") == "" {
				t.Skip("Skipping slow directory test in local development")
			}

			db, err := Open(context.Background(), tt.paths...)
			if tt.wantErr {
				assert.Error(t, err, "Open(context.Background(), ) should have failed")
				return
			}
			assert.NoError(t, err, "Open(context.Background(), ) should have succeeded")

			if !tt.wantErr {
				defer db.Close()

				// Test that we can query at least one table
				if len(tt.paths) > 0 {
					// For the sample file test
					if strings.Contains(tt.paths[0], "sample.csv") || strings.Contains(tt.paths[0], "testdata") {
						rows, err := db.QueryContext(context.Background(), "SELECT COUNT(*) FROM sample")
						if err != nil {
							assert.Fail(t, "Query() error = %v", err)
							return
						}
						defer rows.Close()

						if err := rows.Err(); err != nil {
							assert.NoError(t, err, "Rows error")
							return
						}

						var count int
						if rows.Next() {
							if err := rows.Scan(&count); err != nil {
								assert.Fail(t, "Scan() error = %v", err)
								return
							}
						}

						if count != 3 {
							assert.Fail(t, "Expected 3 rows, got %d", count)
						}
					}
				}
			}
		})
	}
}

func TestSQLQueries(t *testing.T) {
	t.Parallel()

	db, err := Open(context.Background(), filepath.Join("testdata", "sample.csv"))
	require.NoError(t, err, "Failed to open database")
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
			assert.NoError(t, err, "Query() error")
			if err != nil {
				return
			}
			defer rows.Close()

			assert.NoError(t, rows.Err(), "Rows error")

			if rows.Next() {
				var result interface{}
				if err := rows.Scan(&result); err != nil {
					assert.NoError(t, err, "Scan() error")
					return
				}

				switch expected := tt.expected.(type) {
				case int:
					if count, ok := result.(int64); ok {
						assert.Equal(t, expected, int(count), "Expected count to match")
					} else {
						assert.Failf(t, "Type assertion failed", "Expected int, got %T", result)
					}
				case string:
					if str, ok := result.(string); ok {
						assert.Equal(t, expected, str, "Expected string to match")
					} else {
						assert.Failf(t, "Type assertion failed", "Expected string, got %T", result)
					}
				}
			}
		})
	}
}

func TestMultipleFiles(t *testing.T) {
	t.Parallel()

	if os.Getenv("GITHUB_ACTIONS") == "" {
		t.Skip("Skipping slow multiple files test in local development")
	}

	// testdata/tree, not testdata: the wider tree holds several files that map
	// to one table from different directories, which is now a reported
	// collision rather than a silent drop. See
	// TestOpenDirectoryWithCollidingBasenamesFails.
	db, err := Open(context.Background(), filepath.Join("testdata", "tree"))
	require.NoError(t, err, "Failed to open directory")
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
			assert.NoError(t, err, "Query() error")
			if err != nil {
				return
			}
			defer rows.Close()

			assert.NoError(t, rows.Err(), "Rows error")

			if rows.Next() {
				var count int64
				if err := rows.Scan(&count); err != nil {
					assert.NoError(t, err, "Scan() error")
					return
				}

				assert.NotEqual(t, int64(0), count, "Expected non-zero count for table %s", tt.table)
			}
		})
	}
}

func TestJoinMultipleTables(t *testing.T) {
	t.Parallel()

	if os.Getenv("GITHUB_ACTIONS") == "" {
		t.Skip("Skipping slow join multiple tables test in local development")
	}

	// See TestMultipleFiles for why this is testdata/tree.
	db, err := Open(context.Background(), filepath.Join("testdata", "tree"))
	require.NoError(t, err, "Failed to open directory")
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
		assert.Fail(t, "JOIN Query() error = %v", err)
		return
	}
	defer rows.Close()

	if err := rows.Err(); err != nil {
		assert.NoError(t, err, "Rows error")
		return
	}

	if rows.Next() {
		var name string
		var count int64
		if err := rows.Scan(&name, &count); err != nil {
			assert.Fail(t, "Scan() error = %v", err)
			return
		}

		if name != "Alice" {
			assert.Fail(t, "Expected name 'Alice', got '%s'", name)
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

		validatedBuilder, err := buildForTest(context.Background(), builder)
		require.NoError(t, err, "Build failed")

		db, err := validatedBuilder.Open(context.Background())
		require.NoError(t, err, "Open failed")
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
		require.NoError(t, err, "Complex query failed")
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
				require.NoError(t, err, "Scan failed")
			}
			results = append(results, r)
		}

		require.NoError(t, rows.Err(), "Rows iteration error")

		if len(results) != 2 {
			assert.Fail(t, "Expected 2 results, got %d", len(results))
		}
	})

	t.Run("embed.FS integration", func(t *testing.T) {
		t.Parallel()

		// Create embedded filesystem
		testFS := os.DirFS(filepath.Join("testdata", "embed_test"))

		builder := NewBuilder().AddFS(testFS)
		validatedBuilder, err := buildForTest(context.Background(), builder)
		require.NoError(t, err, "Build with FS failed")

		db, err := validatedBuilder.Open(context.Background())
		require.NoError(t, err, "Open with FS failed")
		defer db.Close()

		// Verify tables from embedded files
		tables := []string{"products", "orders", "users"}
		for _, table := range tables {
			query := "SELECT COUNT(*) FROM " + table // Table name from trusted list
			var count int
			err := db.QueryRowContext(context.Background(), query).Scan(&count)
			if err != nil {
				assert.Fail(t, "Failed to query table %s: %v", table, err)
			}
			if count == 0 {
				assert.Fail(t, "Table %s is empty", table)
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
		require.NoError(t, err, "Cross-table query failed")
		defer rows.Close()

		rowCount := 0
		for rows.Next() {
			var name string
			var orderCount int
			if err := rows.Scan(&name, &orderCount); err != nil {
				require.NoError(t, err, "Scan failed")
			}
			rowCount++
		}

		require.NoError(t, rows.Err(), "Rows iteration error")

		if rowCount == 0 {
			t.Error("Expected at least one result from cross-table query")
		}
	})

	t.Run("large file streaming with benchmark data", func(t *testing.T) {
		t.Parallel()

		// Skip this test in local development, only run on GitHub Actions
		if os.Getenv("GITHUB_ACTIONS") == "" {
			t.Skip("Skipping large file test in local development")
		}

		builder := NewBuilder().
			AddPath(filepath.Join("testdata", "benchmark", "customers100000.csv")).
			SetDefaultChunkSize(500) // 500 rows per chunk for testing

		validatedBuilder, err := buildForTest(context.Background(), builder)
		require.NoError(t, err, "Build with large file failed")

		db, err := validatedBuilder.Open(context.Background())
		require.NoError(t, err, "Open with large file failed")
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
					require.NoError(t, err, "Query '%s' failed", q.name)
				}
				defer rows.Close()

				hasResults := false
				for rows.Next() {
					hasResults = true
					// Just scan to verify data is accessible
					cols, err := rows.Columns()
					if err != nil {
						require.NoError(t, err, "Failed to get columns")
					}

					values := make([]interface{}, len(cols))
					scanArgs := make([]interface{}, len(cols))
					for i := range values {
						scanArgs[i] = &values[i]
					}

					if err := rows.Scan(scanArgs...); err != nil {
						require.NoError(t, err, "Scan failed")
					}
				}

				require.NoError(t, rows.Err(), "Rows iteration error")

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
		validatedBuilder, err := buildForTest(context.Background(), builder)
		require.NoError(t, err, "Build with compressed files failed")

		db, err := validatedBuilder.Open(context.Background())
		require.NoError(t, err, "Open with compressed files failed")
		defer db.Close()

		// Verify all compressed files were loaded correctly
		expectedTables := []string{"sample", "users", "logs", "products"}
		for _, table := range expectedTables {
			var count int
			query := "SELECT COUNT(*) FROM " + table // Table name from trusted list
			err := db.QueryRowContext(context.Background(), query).Scan(&count)
			if err != nil {
				assert.Fail(t, "Failed to query compressed table %s: %v", table, err)
			}
			if count == 0 {
				assert.Fail(t, "Compressed table %s is empty", table)
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
		require.NoError(t, err, "Union query on compressed files failed")
		defer rows.Close()

		results := make(map[string]int)
		for rows.Next() {
			var source string
			var count int
			if err := rows.Scan(&source, &count); err != nil {
				require.NoError(t, err, "Scan failed")
			}
			results[source] = count
		}

		require.NoError(t, rows.Err(), "Rows iteration error")

		if len(results) != 4 {
			assert.Fail(t, "Expected 4 tables, got %d", len(results))
		}

		for table, count := range results {
			if count == 0 {
				assert.Fail(t, "Table %s has zero rows", table)
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

		validatedBuilder, err := buildForTest(context.Background(), builder)
		require.NoError(t, err, "Build with auto-save failed")

		db, err := validatedBuilder.Open(context.Background())
		require.NoError(t, err, "Open with auto-save failed")

		// Modify the data
		_, err = db.ExecContext(context.Background(), "INSERT INTO sample (id, name, age, email) VALUES (99, 'Test User', 42, 'test@example.com')")
		require.NoError(t, err, "INSERT failed")

		_, err = db.ExecContext(context.Background(), "UPDATE users SET role = 'super_admin' WHERE name = 'Alice'")
		require.NoError(t, err, "UPDATE failed")

		// Close to trigger auto-save
		if err := db.Close(); err != nil {
			assert.NoError(t, err, "Failed to close database")
		}

		// Verify auto-saved files exist
		expectedFiles := []string{"sample.csv", "users.csv"}
		for _, filename := range expectedFiles {
			filepath := filepath.Join(tempDir, filename)
			if _, err := os.Stat(filepath); os.IsNotExist(err) {
				assert.Fail(t, "Auto-saved file %s does not exist", filename)
			}
		}

		// Verify the modifications were saved by opening the auto-saved files
		newDB, err := Open(context.Background(), tempDir)
		require.NoError(t, err, "Failed to open auto-saved files")
		defer newDB.Close()

		// Check if our modifications are present
		var testUser string
		err = newDB.QueryRowContext(context.Background(), "SELECT name FROM sample WHERE id = 99").Scan(&testUser)
		require.NoError(t, err, "Failed to find inserted test user")
		assert.Equal(t, "Test User", testUser, "Expected 'Test User', got '%s'", testUser)

		var aliceRole string
		err = newDB.QueryRowContext(context.Background(), "SELECT role FROM users WHERE name = 'Alice'").Scan(&aliceRole)
		require.NoError(t, err, "Failed to find updated Alice role")
		assert.Equal(t, "super_admin", aliceRole, "Expected 'super_admin', got '%s'", aliceRole)
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

		validatedBuilder, err := buildForTest(context.Background(), builder)
		require.NoError(t, err, "Build with mixed sources failed")

		db, err := validatedBuilder.Open(context.Background())
		require.NoError(t, err, "Open with mixed sources failed")
		defer db.Close()

		// Verify all sources are accessible
		tableCounts := map[string]int{}

		// Get all table names
		rows, err := db.QueryContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table'")
		require.NoError(t, err, "Failed to get table names")
		defer rows.Close()

		var tableNames []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				require.NoError(t, err, "Scan table name failed")
			}
			tableNames = append(tableNames, name)
		}

		require.NoError(t, rows.Err(), "Rows iteration error")

		// Count rows in each table
		for _, tableName := range tableNames {
			var count int
			query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName) //nolint:gosec // Table name from database metadata
			err := db.QueryRowContext(context.Background(), query).Scan(&count)
			if err != nil {
				assert.Fail(t, "Failed to count rows in table %s: %v", tableName, err)
			}
			tableCounts[tableName] = count
		}

		// Verify we have expected tables from all sources
		expectedTables := []string{"sample", "custom_orders", "sample2"}
		for _, expected := range expectedTables {
			if count, exists := tableCounts[expected]; !exists {
				assert.Fail(t, "Expected table %s not found", expected)
			} else if count == 0 {
				assert.Fail(t, "Table %s is empty", expected)
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
		require.NoError(t, err, "Complex mixed-source query failed")
		defer rows.Close()

		hasResults := false
		for rows.Next() {
			hasResults = true
			var sampleName, orderCustomer, userName string
			var matchCount int
			if err := rows.Scan(&sampleName, &orderCustomer, &userName, &matchCount); err != nil {
				require.NoError(t, err, "Scan complex query failed")
			}
			// Just verify we can read the data
		}

		// Note: This query might not return results due to data mismatch, but it should execute without error
		require.NoError(t, rows.Err(), "Query execution error")

		// Use hasResults to avoid unused variable error
		_ = hasResults
	})

	t.Run("basic database access test", func(t *testing.T) {
		t.Parallel()

		benchmarkFile := filepath.Join("testdata", "benchmark", "customers100000.csv")

		db, err := Open(context.Background(), benchmarkFile)
		require.NoError(t, err, "Failed to open benchmark file")
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
					require.NoError(t, err, "Query failed")
				}
				defer rows.Close()

				// Process results
				for rows.Next() {
					cols, err := rows.Columns()
					if err != nil {
						require.NoError(t, err, "Get columns failed")
					}

					values := make([]any, len(cols))
					scanArgs := make([]any, len(cols))
					for k := range values {
						scanArgs[k] = &values[k]
					}

					if err := rows.Scan(scanArgs...); err != nil {
						require.NoError(t, err, "Scan failed")
					}
				}

				require.NoError(t, rows.Err(), "Rows error")
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
				db, err := Open(context.Background(), filepath.Join("testdata", "sample.csv"))
				require.NoError(t, err, "Failed to open database")
				return db
			},
			expectError: false,
			checkFiles:  []string{"sample.csv"},
		},
		{
			name: "Multiple files dump",
			setupFunc: func(t *testing.T) *sql.DB {
				t.Helper()
				db, err := Open(context.Background(), filepath.Join("testdata", "sample.csv"), filepath.Join("testdata", "users.csv"))
				require.NoError(t, err, "Failed to open database")
				return db
			},
			expectError: false,
			checkFiles:  []string{"sample.csv", "users.csv"},
		},
		{
			name: "Directory dump",
			setupFunc: func(t *testing.T) *sql.DB {
				t.Helper()
				// See TestMultipleFiles for why this is testdata/tree.
				db, err := Open(context.Background(), filepath.Join("testdata", "tree"))
				require.NoError(t, err, "Failed to open database")
				return db
			},
			expectError: false,
			checkFiles:  []string{"sample.csv", "users.csv", "products.csv", "logs.csv"},
		},
		{
			name: "Modified data dump",
			setupFunc: func(t *testing.T) *sql.DB {
				t.Helper()
				db, err := Open(context.Background(), filepath.Join("testdata", "sample.csv"))
				require.NoError(t, err, "Failed to open database")

				// Modify data to test persistence
				_, err = db.ExecContext(context.Background(), "INSERT INTO sample (id, name, age, email) VALUES (4, 'Test User', 40, 'test@example.com')")
				if err != nil {
					require.NoError(t, err, "Failed to insert test data")
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
			err := DumpDatabase(context.Background(), db, tempDir)

			// Check error expectation
			if (err != nil) != tc.expectError {
				assert.Fail(t, "DumpDatabase(context.Background(), ) error = %v, expectError %v", err, tc.expectError)
				return
			}

			if !tc.expectError {
				// Verify expected files were created
				for _, fileName := range tc.checkFiles {
					filePath := filepath.Join(tempDir, fileName)
					if _, err := os.Stat(filePath); os.IsNotExist(err) {
						assert.Fail(t, "Expected file %s was not created", fileName)
						continue
					}

					// Read and verify file content
					content, err := os.ReadFile(filePath) //nolint:gosec // Safe: filePath is from controlled test data
					if err != nil {
						assert.Fail(t, "Failed to read dumped file %s: %v", fileName, err)
						continue
					}

					// Basic validation: file should have content and CSV header
					if len(content) == 0 {
						assert.Fail(t, "Dumped file %s is empty", fileName)
					}

					contentStr := string(content)
					if !strings.Contains(contentStr, "\n") {
						assert.Fail(t, "Dumped file %s should contain newlines (header + data)", fileName)
					}

					// For the modified data test, check if new data is present
					if tc.name == "Modified data dump" && fileName == "sample.csv" {
						if !strings.Contains(contentStr, "Test User") {
							assert.Fail(t, "Modified data not found in dumped file")
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
		err = DumpDatabase(context.Background(), db, tempDir)
		if err == nil {
			t.Error("expected error when calling DumpDatabase on empty database")
		}

		// Should get "no tables found" error since it's an empty database
		assert.True(t, errors.Is(err, ErrNoTables), "expected ErrNoTables, got: %v", err)
	})

	t.Run("Permission denied output directory", func(t *testing.T) {
		t.Parallel()

		db, err := Open(context.Background(), filepath.Join("testdata", "sample.csv"))
		if err != nil {
			require.NoError(t, err, "Failed to open database")
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

		err = DumpDatabase(context.Background(), db, invalidDir)
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
			assert.NoError(t, err, "expected permission or directory creation error, got")
		}
	})
}

// TestDumpDatabaseCSVFormat tests the CSV format of dumped files
func TestDumpDatabaseCSVFormat(t *testing.T) {
	t.Parallel()

	db, err := Open(context.Background(), filepath.Join("testdata", "sample.csv"))
	if err != nil {
		require.NoError(t, err, "Failed to open database")
	}
	defer db.Close()

	tempDir := t.TempDir()

	// Dump the database
	err = DumpDatabase(context.Background(), db, tempDir)
	if err != nil {
		require.NoError(t, err, "DumpDatabase(context.Background(), ) failed")
	}

	// Read the dumped file
	dumpedFile := filepath.Join(tempDir, "sample.csv")
	content, err := os.ReadFile(dumpedFile) //nolint:gosec // Safe: dumpedFile is from controlled test output
	if err != nil {
		require.NoError(t, err, "Failed to read dumped file")
	}

	contentStr := string(content)
	lines := strings.Split(strings.TrimSpace(contentStr), "\n")

	// Should have header + 3 data rows
	if len(lines) != 4 {
		assert.Fail(t, "Expected 4 lines (header + 3 data), got %d", len(lines))
	}

	// Check header
	expectedHeader := "id,name,age,email"
	if lines[0] != expectedHeader {
		assert.Fail(t, "Expected header %q, got %q", expectedHeader, lines[0])
	}

	// Check that data rows have the correct number of columns
	for i, line := range lines[1:] {
		columns := strings.Split(line, ",")
		if len(columns) != 4 {
			assert.Fail(t, "Data row %d has %d columns, expected 4: %q", i+1, len(columns), line)
		}
	}
}

// TestDumpDatabaseSpecialCharacters tests CSV escaping for special characters
func TestDumpDatabaseSpecialCharacters(t *testing.T) {
	t.Parallel()

	db, err := Open(context.Background(), filepath.Join("testdata", "sample.csv"))
	if err != nil {
		require.NoError(t, err, "Failed to open database")
	}
	defer db.Close()

	// Insert data with special characters that need CSV escaping
	_, err = db.ExecContext(context.Background(), `INSERT INTO sample (id, name, age, email) VALUES 
		(10, 'Name, with comma', 25, 'test@example.com'),
		(11, 'Name "with quotes"', 26, 'test2@example.com'),
		(12, 'Name' || char(10) || 'with newline', 27, 'test3@example.com')`)
	if err != nil {
		require.NoError(t, err, "Failed to insert test data")
	}

	tempDir := t.TempDir()

	// Dump the database
	err = DumpDatabase(context.Background(), db, tempDir)
	if err != nil {
		require.NoError(t, err, "DumpDatabase(context.Background(), ) failed")
	}

	// Read the dumped file
	dumpedFile := filepath.Join(tempDir, "sample.csv")
	content, err := os.ReadFile(dumpedFile) //nolint:gosec // Safe: dumpedFile is from controlled test output
	if err != nil {
		require.NoError(t, err, "Failed to read dumped file")
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
			assert.Contains(t, contentStr, tc.shouldFind, "CSV escaping test failed: %s - expected to find %q in content", tc.description, tc.shouldFind)
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
					require.NoError(t, err, "Failed to create")
				}
				defer os.RemoveAll(emptyDir)
			}

			db, err := Open(context.Background(), tt.paths...)
			if tt.wantErr {
				assert.Error(t, err, "Open(context.Background(), ) should have failed")
				return
			}
			assert.NoError(t, err, "Open(context.Background(), ) should have succeeded")

			if tt.wantErr && err != nil {
				if !strings.Contains(err.Error(), tt.errorString) {
					assert.Fail(t, "Open(context.Background(), ) error = %v, expected to contain %q", err, tt.errorString)
				}
			}

			if !tt.wantErr && db != nil {
				defer db.Close()
			}
		})
	}
}

// TestOpenUnderAContext covers what the context Open takes decides: a deadline
// it finishes inside, one that is already spent, and the paths it is given.
func TestOpenUnderAContext(t *testing.T) {
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
			name: "Context already canceled",
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
				// A deadline already in the past, so the context carries its
				// error the moment it is built. A very short timeout does not:
				// its error arrives when the runtime timer fires, which on a
				// loaded Windows runner is late enough to lose the race.
				return context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
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

			db, err := Open(ctx, tt.paths...)
			if tt.wantErr {
				assert.Error(t, err, "Open() should have failed")
			} else {
				assert.NoError(t, err, "Open() should have succeeded")
			}

			if tt.wantErr && err != nil && tt.errContains != "" {
				assert.Contains(t, err.Error(), tt.errContains, "Open() error should contain expected string")
			}

			if !tt.wantErr && db != nil {
				defer db.Close()

				// Verify the database is functional
				if err := db.PingContext(t.Context()); err != nil {
					assert.NoError(t, err, "Failed to ping database after Open")
				}
			}
		})
	}
}

// TestOpenConcurrent tests concurrent Open calls
func TestOpenConcurrent(t *testing.T) {
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

			db, err := Open(ctx, filepath.Join("testdata", "sample.csv"))
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
		assert.NoError(t, err, "Concurrent Open error")
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
			expectedName: "_DROP_TABLE_users___",
			description:  "Should sanitize SQL injection attempts",
		},
		{
			name:         "Unicode characters",
			filePath:     "データ.csv",
			expectedName: "データ",
			description:  "Should keep a name written in a non-Latin script; the table name is quoted",
		},
		{
			name:         "Special characters",
			filePath:     "test@#$%^&()_+.csv",
			expectedName: "test_",
			description:  "Should sanitize special characters",
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
			expectedName: "sheet",
			description:  "Should handle empty base filename with fallback",
		},
		{
			name:         "Hidden file",
			filePath:     ".hidden.csv",
			expectedName: "_hidden",
			description:  "Should sanitize hidden files",
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
			tableName := sanitizeTableName(tableFromFilePath(tc.filePath))
			if tableName != tc.expectedName {
				assert.Fail(t, "Expected table name %q, got %q", tc.expectedName, tableName)
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
			db, err := Open(context.Background(), tmpFile.Name())
			if tc.expectError && err == nil {
				t.Error("Expected error but got none")
				if db != nil {
					_ = db.Close() // Ignore close error in test cleanup
				}
				return
			}
			if !tc.expectError && err != nil {
				assert.NoError(t, err, "Unexpected error")
				return
			}

			if db != nil {
				defer db.Close()

				// Try to query the table
				tableName := sanitizeTableName(tableFromFilePath(tmpFile.Name()))
				// Use bracket notation for table name (safe in controlled test environment)
				query := "SELECT COUNT(*) FROM [" + tableName + "]"
				var count int
				err = db.QueryRowContext(context.Background(), query).Scan(&count)
				if err != nil && !tc.expectError {
					assert.NoError(t, err, "Query failed")
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

	var sb strings.Builder
	sb.WriteString("id,name,value\n")
	for i := 1; i <= 100; i++ {
		fmt.Fprintf(&sb, "%d,user%d,%d\n", i, i, i*10)
	}
	csvContent := sb.String()

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
				db, err := Open(context.Background(), tmpFile.Name())
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: failed to open: %w", goroutineID, err)
					return
				}

				tableName := sanitizeTableName(tableFromFilePath(tmpFile.Name()))
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

	if os.Getenv("GITHUB_ACTIONS") == "" {
		t.Skip("Skipping slow resource exhaustion test in local development")
	}

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

		db, err := Open(context.Background(), tmpFile.Name())
		require.NoError(t, err, "Failed to open file with many columns")
		defer db.Close()

		tableName := sanitizeTableName(tableFromFilePath(tmpFile.Name()))
		var count int
		err = db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableName)).Scan(&count)
		assert.NoError(t, err, "Failed to query table with many columns")
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

		db, err := Open(context.Background(), tmpFile.Name())
		require.NoError(t, err, "Failed to open file with many rows")
		defer db.Close()

		tableName := sanitizeTableName(tableFromFilePath(tmpFile.Name()))
		var count int
		err = db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableName)).Scan(&count)
		assert.NoError(t, err, "Failed to query table with many rows")
		if count != 10000 {
			assert.Fail(t, "Expected 10000 rows, got %d", count)
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

	db, err := Open(context.Background(), tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Get the actual table name from the database
	var tableName string
	err = db.QueryRowContext(context.Background(), `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite\_%' ESCAPE '\' LIMIT 1`).Scan(&tableName)
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

			db, err := Open(context.Background(), tmpFile.Name())
			require.NoError(t, err, "Failed to open Unicode file")
			defer db.Close()

			tableName := sanitizeTableName(tableFromFilePath(tmpFile.Name()))

			// Test basic query
			var count int
			err = db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableName)).Scan(&count)
			assert.NoError(t, err, "Failed to query Unicode table")

			// Test data retrieval
			rows, err := db.QueryContext(context.Background(), fmt.Sprintf("SELECT * FROM [%s] LIMIT 1", tableName))
			assert.NoError(t, err, "Failed to select from Unicode table")
			defer rows.Close()

			if err := rows.Err(); err != nil {
				assert.NoError(t, err, "Rows error")
				return
			}

			if rows.Next() {
				columns, err := rows.Columns()
				assert.NoError(t, err, "Failed to get columns")

				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					assert.NoError(t, err, "Failed to scan Unicode data")
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
			db, err := Open(context.Background(), tmpFile.Name())
			if err != nil {
				require.NoError(t, err, "Failed to open database on iteration %d", i)
			}

			tableName := sanitizeTableName(tableFromFilePath(tmpFile.Name()))
			var count int
			err = db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableName)).Scan(&count)
			if err != nil {
				_ = db.Close() // Ignore close error in test cleanup
				require.NoError(t, err, "Query failed on iteration %d", i)
			}

			if err := db.Close(); err != nil {
				require.NoError(t, err, "Close failed on iteration %d", i)
			}
		}
	})

	t.Run("Connection timeout and context", func(t *testing.T) {
		db, err := Open(context.Background(), tmpFile.Name())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		tableName := sanitizeTableName(tableFromFilePath(tmpFile.Name()))
		// Use bracket notation for table name (safe in controlled test environment)
		query := "SELECT COUNT(*) FROM [" + tableName + "]"
		var count int
		err = db.QueryRowContext(ctx, query).Scan(&count)
		assert.NoError(t, err, "Query with context failed")
	})

	t.Run("Double close safety", func(t *testing.T) {
		db, err := Open(context.Background(), tmpFile.Name())
		if err != nil {
			t.Fatal(err)
		}

		// First close
		if err := db.Close(); err != nil {
			assert.NoError(t, err, "First close failed")
		}

		// Second close should not panic or error
		if err := db.Close(); err != nil {
			assert.NoError(t, err, "Second close failed")
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
				require.NoError(t, err, "Failed to create test file %s", rw.filename)
			}

			// Test 1: Open file and verify table creation
			db, err := Open(context.Background(), filePath)
			if err != nil {
				require.NoError(t, err, "Failed to open file with reserved word filename %s", rw.filename)
			}
			defer db.Close()

			// Test 2: Verify table exists with proper name
			expectedTableName := sanitizeTableName(tableFromFilePath(filePath))
			var actualTableName string
			err = db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", expectedTableName).Scan(&actualTableName)
			if err != nil {
				require.NoError(t, err, "Table for reserved word filename %s not found", rw.filename)
			}

			if actualTableName != expectedTableName {
				assert.Fail(t, "Expected table name %q, got %q for file %s", expectedTableName, actualTableName, rw.filename)
			}

			// Test 3: Query the table using bracket notation (safe for reserved words)
			// Use bracket notation for table name (safe in controlled test environment)
			query := "SELECT COUNT(*) FROM [" + expectedTableName + "]"
			var count int
			err = db.QueryRowContext(context.Background(), query).Scan(&count)
			if err != nil {
				assert.Fail(t, "Failed to query table with reserved word name [%s]: %v", expectedTableName, err)
			}

			if count != 3 {
				assert.Fail(t, "Expected 3 rows in table [%s], got %d", expectedTableName, count)
			}

			// Test 4: Verify we can select specific data
			query = fmt.Sprintf("SELECT name FROM [%s] WHERE id = 1", expectedTableName)
			var name string
			err = db.QueryRowContext(context.Background(), query).Scan(&name)
			if err != nil {
				assert.Fail(t, "Failed to select specific data from table [%s]: %v", expectedTableName, err)
			}

			if name != "test1" {
				assert.Fail(t, "Expected 'test1', got %q from table [%s]", name, expectedTableName)
			}

			// Test 5: Verify we can perform complex queries
			query = fmt.Sprintf("SELECT AVG(CAST(value AS REAL)) FROM [%s] WHERE id > 1", expectedTableName)
			var avgValue float64
			err = db.QueryRowContext(context.Background(), query).Scan(&avgValue)
			if err != nil {
				assert.Fail(t, "Failed to perform aggregate query on table [%s]: %v", expectedTableName, err)
			}

			expectedAvg := 250.0 // (200 + 300) / 2 = 500 / 2 = 250
			if avgValue != expectedAvg {
				assert.Fail(t, "Expected average %.1f, got %.1f for table [%s]", expectedAvg, avgValue, expectedTableName)
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
			require.NoError(t, err, "Failed to create file %s", file.name)
		}
	}

	// Test 1: Load all files from directory
	db, err := Open(context.Background(), tmpDir)
	require.NoError(t, err, "Failed to open directory with reserved word files")
	defer db.Close()

	// Test 2: Verify all tables exist
	for _, file := range files {
		tableName := sanitizeTableName(tableFromFilePath(file.name))
		var name string
		err := db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", tableName).Scan(&name)
		if err != nil {
			assert.Fail(t, "Table for reserved word file %s not found: %v", file.name, err)
			continue
		}

		// Test basic query on each table
		var count int
		// Use bracket notation for table name (safe in controlled test environment)
		query := "SELECT COUNT(*) FROM [" + tableName + "]"
		err = db.QueryRowContext(context.Background(), query).Scan(&count)
		if err != nil {
			assert.Fail(t, "Failed to query reserved word table [%s]: %v", tableName, err)
		}

		if count != 2 {
			assert.Fail(t, "Expected 2 rows in table [%s], got %d", tableName, count)
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
	assert.NoError(t, err, "Failed to perform cross-table query with reserved word tables")

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
			assert.Fail(t, "Expected %s=%q, got %q", field, expected, actual)
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
				require.NoError(t, err, "Failed to create test file %s", tc.filename)
			}

			// Test opening the file
			db, err := Open(context.Background(), filePath)
			if tc.expectError && err == nil {
				assert.Fail(t, "Expected error for %s but got none", tc.description)
				if db != nil {
					_ = db.Close() // Ignore close error in test cleanup
				}
				return
			}

			if !tc.expectError && err != nil {
				assert.NoError(t, err, "Unexpected error for %s", tc.description)
				return
			}

			if !tc.expectError && db != nil {
				defer db.Close()

				// Verify table creation and basic functionality
				tableName := sanitizeTableName(tableFromFilePath(filePath))

				// Test table exists
				var name string
				err := db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", tableName).Scan(&name)
				if err != nil {
					assert.Fail(t, "Table not found for %s: %v", tc.description, err)
					return
				}

				// Test basic query using bracket notation
				// Use bracket notation for table name (safe in controlled test environment)
				query := "SELECT COUNT(*) FROM [" + tableName + "]"
				var count int
				err = db.QueryRowContext(context.Background(), query).Scan(&count)
				if err != nil {
					assert.Fail(t, "Failed to query table for %s: %v", tc.description, err)
					return
				}

				if count != 2 {
					assert.Fail(t, "Expected 2 rows for %s, got %d", tc.description, count)
				}

				// Test more complex operations
				// Use bracket notation for table name (safe in controlled test environment)
				insertQuery := "INSERT INTO [" + tableName + "] (id, data) VALUES (3, 'value3')" //nolint:gosec // Safe: tableName is from controlled test data
				_, err = db.ExecContext(context.Background(), insertQuery)
				if err != nil {
					assert.Fail(t, "Failed to insert into table for %s: %v", tc.description, err)
				}

				// Verify insert worked
				err = db.QueryRowContext(context.Background(), query).Scan(&count)
				if err != nil {
					assert.Fail(t, "Failed to verify insert for %s: %v", tc.description, err)
				}

				if count != 3 {
					assert.Fail(t, "Expected 3 rows after insert for %s, got %d", tc.description, count)
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

			_, err := Open(context.Background(), filePath)
			if err == nil {
				assert.Fail(t, "Expected error but got none for %s", tc.description)
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
				assert.Fail(t, "Error message %q should contain one of %v for %s", errorMsg, tc.expectedErrors, tc.description)
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

		db, err := Open(context.Background(), tmpFile.Name())
		require.NoError(t, err, "Failed to open file with reserved keywords")
		defer db.Close()

		tableName := sanitizeTableName(tableFromFilePath(tmpFile.Name()))

		// Test querying with reserved keyword column names
		// Use bracket notation for table name (safe in controlled test environment)
		query := "SELECT [select], [from], [where] FROM [" + tableName + "]" //nolint:gosec // Safe: tableName is from controlled test data
		rows, err := db.QueryContext(context.Background(), query)
		assert.NoError(t, err, "Failed to query table with reserved keyword columns")
		defer rows.Close()

		if err := rows.Err(); err != nil {
			assert.NoError(t, err, "Rows error")
			return
		}

		if rows.Next() {
			var col1, col2, col3 string
			if err := rows.Scan(&col1, &col2, &col3); err != nil {
				assert.NoError(t, err, "Failed to scan reserved keyword columns")
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

				db, err := Open(context.Background(), tmpFile.Name())
				if err != nil {
					assert.Fail(t, "Failed to open file %s: %v", pattern, err)
					return
				}
				defer db.Close()

				tableName := sanitizeTableName(tableFromFilePath(tmpFile.Name()))
				// Use bracket notation for table name (safe in controlled test environment)
				query := "SELECT COUNT(*) FROM [" + tableName + "]"
				var count int
				if err := db.QueryRowContext(context.Background(), query).Scan(&count); err != nil {
					assert.Fail(t, "Failed to query table from file %s: %v", pattern, err)
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

		db, err := Open(context.Background(), tmpFile.Name())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		tableName := sanitizeTableName(tableFromFilePath(tmpFile.Name()))

		// Test transaction rollback
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(t, err, "Failed to begin transaction")

		// Insert data in transaction
		_, err = tx.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO [%s] (id, name) VALUES (2, 'transaction')", tableName))
		assert.NoError(t, err, "Failed to insert in transaction")

		// Rollback
		if err := tx.Rollback(); err != nil {
			assert.NoError(t, err, "Failed to rollback transaction")
		}

		// Verify data was rolled back
		var count int
		err = db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableName)).Scan(&count)
		assert.NoError(t, err, "Failed to count after rollback")
		if count != 1 {
			assert.Fail(t, "Expected 1 row after rollback, got %d", count)
		}

		// Test transaction commit
		tx, err = db.BeginTx(context.Background(), nil)
		require.NoError(t, err, "Failed to begin second transaction")

		_, err = tx.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO [%s] (id, name) VALUES (2, 'committed')", tableName))
		assert.NoError(t, err, "Failed to insert in second transaction")

		if err := tx.Commit(); err != nil {
			assert.NoError(t, err, "Failed to commit transaction")
		}

		// Verify data was committed
		err = db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableName)).Scan(&count)
		assert.NoError(t, err, "Failed to count after commit")
		if count != 2 {
			assert.Fail(t, "Expected 2 rows after commit, got %d", count)
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
			db, err := Open(context.Background(), filePath)
			if err != nil {
				require.NoError(t, err, "Open(context.Background(), %s) failed", filePath)
			}
			defer db.Close()

			// Verify table exists
			var tableName string
			err = db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", tc.expectTable).Scan(&tableName)
			if err != nil {
				require.NoError(t, err, "Table %s not found", tc.expectTable)
			}

			// Count rows
			var count int
			err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM ["+tc.expectTable+"]").Scan(&count)
			if err != nil {
				require.NoError(t, err, "Failed to count rows in %s", tc.expectTable)
			}

			if count != tc.expectRows {
				assert.Fail(t, "Expected %d rows in %s, got %d", tc.expectRows, tc.expectTable, count)
			}

			// Test basic SELECT
			// Use bracket notation for table name (safe in controlled test environment)
			query := "SELECT * FROM [" + tc.expectTable + "] LIMIT 1" //nolint:gosec // Safe: tc.expectTable is from controlled test data
			rows, err := db.QueryContext(context.Background(), query)
			require.NoError(t, err, "SELECT query failed")
			defer rows.Close()

			if err := rows.Err(); err != nil {
				require.NoError(t, err, "Rows error")
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

	if os.Getenv("GITHUB_ACTIONS") == "" {
		t.Skip("Skipping slow directory loading test in local development")
	}

	// See TestMultipleFiles for why this is testdata/tree.
	db, err := Open(context.Background(), filepath.Join("testdata", "tree"))
	require.NoError(t, err, "Open(context.Background(), testdata/tree) failed")
	defer db.Close()

	// Get all table names
	rows, err := db.QueryContext(context.Background(), `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite\_%' ESCAPE '\' ORDER BY name`)
	require.NoError(t, err, "Failed to get table names")
	defer rows.Close()

	if err := rows.Err(); err != nil {
		require.NoError(t, err, "Rows error")
	}

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			require.NoError(t, err, "Failed to scan table name")
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
			assert.Fail(t, "Expected table %s not found in tables: %v", expected, tables)
		}
	}

	// Test cross-table query
	var count int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM sample s JOIN products p ON s.id = p.id").Scan(&count)
	require.NoError(t, err, "Cross-table JOIN query failed")

	if count == 0 {
		t.Error("Expected at least one matching row in JOIN query")
	}
}

// TestMultipleFilePaths tests loading multiple specific file paths
func TestMultipleFilePaths(t *testing.T) {
	t.Parallel()

	// Open database with multiple files
	db, err := Open(context.Background(), filepath.Join("testdata", "sample.csv"), filepath.Join("testdata", "products.tsv"), filepath.Join("testdata", "logs.ltsv"))
	require.NoError(t, err, "Open with multiple files failed")
	defer db.Close()

	// Verify all expected tables exist
	expectedTables := []string{"sample", "products", "logs"}
	for _, tableName := range expectedTables {
		var name string
		err := db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", tableName).Scan(&name)
		if err != nil {
			assert.Fail(t, "Table %s not found: %v", tableName, err)
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
	require.NoError(t, err, "Multi-table query failed")
	defer rows.Close()

	if err := rows.Err(); err != nil {
		require.NoError(t, err, "Rows error")
	}

	// Just verify we can execute the query without error
	for rows.Next() {
		var name, productName, level string
		if err := rows.Scan(&name, &productName, &level); err != nil {
			require.NoError(t, err, "Failed to scan multi-table query result")
		}
	}
}

// TestCTEQueries tests Common Table Expressions (CTE) queries
func TestCTEQueries(t *testing.T) {
	t.Parallel()

	db, err := Open(context.Background(), filepath.Join("testdata", "sample.csv"), filepath.Join("testdata", "products.tsv"))
	require.NoError(t, err, "Open failed")
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
				require.Fail(t, "CTE query failed: %v\nQuery: %s", err, tc.query)
			}
			defer rows.Close()

			if err := rows.Err(); err != nil {
				require.NoError(t, err, "Rows error")
			}

			// Verify we can read results
			hasRows := false
			for rows.Next() {
				hasRows = true
				// Get column count to scan appropriately
				cols, err := rows.Columns()
				require.NoError(t, err, "Failed to get columns")

				values := make([]interface{}, len(cols))
				for i := range values {
					values[i] = new(interface{})
				}
				if err := rows.Scan(values...); err != nil {
					require.NoError(t, err, "Failed to scan CTE query result")
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

	if os.Getenv("GITHUB_ACTIONS") == "" {
		t.Skip("Skipping slow mixed directory and files test in local development")
	}

	// Create a specific file outside testdata directory for this test
	tempFile := filepath.Join(os.TempDir(), "mixed_test.csv")
	content := "id,category,value\n1,A,100\n2,B,200\n"

	if err := os.WriteFile(tempFile, []byte(content), 0600); err != nil {
		require.NoError(t, err, "Failed to create")
	}
	defer os.Remove(tempFile)

	// Open with mixed paths: directory + specific file. See
	// TestOpenDirectoryWithCollidingBasenamesFails for why this is testdata/tree.
	db, err := Open(context.Background(), filepath.Join("testdata", "tree"), tempFile)
	require.NoError(t, err, "Open with mixed paths failed")
	defer db.Close()

	// Verify the temp file table exists
	var tableName string
	err = db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", "mixed_test").Scan(&tableName)
	require.NoError(t, err, "Table mixed_test not found")

	// Verify original directory tables also exist
	err = db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", "sample").Scan(&tableName)
	require.NoError(t, err, "Table sample from directory not found")

	// Test query across mixed sources
	var count int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM mixed_test").Scan(&count)
	require.NoError(t, err, "Query on mixed_test table failed")

	if count != 2 {
		assert.Fail(t, "Expected 2 rows in mixed_test, got %d", count)
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
			expectError: "file not found",
		},
		{
			name:        "Unsupported file format",
			paths:       []string{filepath.Join("testdata", "unsupported.txt")}, // We'll create this
			expectError: "file not found",
		},
	}

	// Create unsupported file for test
	unsupportedFile := filepath.Join("testdata", "unsupported.txt")
	if err := os.WriteFile(unsupportedFile, []byte("test content"), 0600); err != nil {
		require.NoError(t, err, "Failed to create")
	}
	defer os.Remove(unsupportedFile)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, err := Open(context.Background(), tc.paths...)
			if err == nil {
				if db != nil {
					_ = db.Close() // Ignore close error in test cleanup
				}
				require.Fail(t, "Expected error containing '%s', but got nil", tc.expectError)
			}

			if !strings.Contains(err.Error(), tc.expectError) {
				assert.Fail(t, "Expected error containing '%s', got: %s", tc.expectError, err.Error())
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
			require.NoError(t, err, "Failed to create")
		}
		defer db.Close()

		// Create test tables
		_, err = db.ExecContext(context.Background(), "CREATE TABLE test1 (id INTEGER, name TEXT)")
		if err != nil {
			require.NoError(t, err, "Failed to create")
		}

		_, err = db.ExecContext(context.Background(), "CREATE TABLE test2 (id INTEGER, value TEXT)")
		if err != nil {
			require.NoError(t, err, "Failed to create")
		}

		// Test getSQLiteTableNames
		tableNames, err := getSQLiteTableNames(context.Background(), db)
		require.NoError(t, err, "getSQLiteTableNames failed")

		expectedTables := []string{"test1", "test2"}
		if len(tableNames) != len(expectedTables) {
			assert.Fail(t, "Expected %d tables, got %d: %v", len(expectedTables), len(tableNames), tableNames)
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
				assert.Fail(t, "Expected table %s not found in %v", expected, tableNames)
			}
		}
	})

	t.Run("getSQLiteTableColumns", func(t *testing.T) {
		t.Parallel()

		// Create a direct SQLite connection
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			require.NoError(t, err, "Failed to create")
		}
		defer db.Close()

		// Create test table with known columns
		_, err = db.ExecContext(context.Background(), "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, salary REAL)")
		if err != nil {
			require.NoError(t, err, "Failed to create")
		}

		// Test getSQLiteTableColumns
		columns, _, err := getSQLiteTableColumns(context.Background(), db, "test_table")
		require.NoError(t, err, "getSQLiteTableColumns failed")

		expectedColumns := []string{"id", "name", "age", "salary"}
		if len(columns) != len(expectedColumns) {
			assert.Fail(t, "Expected %d columns, got %d: %v", len(expectedColumns), len(columns), columns)
		}

		// Verify column names
		for i, expected := range expectedColumns {
			if i >= len(columns) || columns[i] != expected {
				assert.Fail(t, "Expected column %s at index %d, got %s", expected, i, columns[i])
			}
		}
	})

	t.Run("dumpSQLiteDatabase with data", func(t *testing.T) {
		t.Parallel()

		// Create a direct SQLite connection
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			require.NoError(t, err, "Failed to create")
		}
		defer db.Close()

		// Create test table and insert data
		_, err = db.ExecContext(context.Background(), "CREATE TABLE employees (id INTEGER, name TEXT, department TEXT)")
		if err != nil {
			require.NoError(t, err, "Failed to create")
		}

		_, err = db.ExecContext(context.Background(), "INSERT INTO employees VALUES (1, 'Alice', 'Engineering'), (2, 'Bob', 'Marketing'), (3, 'Charlie', 'Sales')")
		if err != nil {
			require.NoError(t, err, "Failed to insert test data")
		}

		// Test dump to directory
		tempDir := t.TempDir()
		options := NewDumpOptions()

		err = dumpSQLiteDatabase(context.Background(), db, tempDir, options)
		require.NoError(t, err, "dumpSQLiteDatabase failed")

		// Verify file was created
		dumpedFile := filepath.Join(tempDir, "employees.csv")
		content, err := os.ReadFile(dumpedFile) //nolint:gosec // dumpedFile is created in test with controlled path
		if err != nil {
			require.NoError(t, err, "Failed to read dumped file")
		}

		contentStr := string(content)
		lines := strings.Split(strings.TrimSpace(contentStr), "\n")

		// Should have header + 3 data rows
		if len(lines) != 4 {
			assert.Fail(t, "Expected 4 lines (header + 3 data), got %d", len(lines))
		}

		// Check header
		if lines[0] != "id,name,department" {
			assert.Fail(t, "Expected header 'id,name,department', got '%s'", lines[0])
		}

		// Check data rows contain expected values
		expectedDataPatterns := []string{"1,Alice,Engineering", "2,Bob,Marketing", "3,Charlie,Sales"}
		for i, expected := range expectedDataPatterns {
			if lines[i+1] != expected {
				assert.Fail(t, "Expected line %d to be '%s', got '%s'", i+1, expected, lines[i+1])
			}
		}
	})

	t.Run("createCompressedWriter formats", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()

		t.Run("no compression", func(t *testing.T) {
			file, err := os.Create(filepath.Join(tempDir, "test.txt")) //nolint:gosec // tempDir is created in test
			if err != nil {
				require.NoError(t, err, "Failed to create")
			}
			defer file.Close()

			writer, closeWriter, err := createCompressedWriter(file, CompressionNone)
			require.NoError(t, err, "createCompressedWriter failed")

			if writer != file {
				t.Error("Expected writer to be the same as file for no compression")
			}

			if err := closeWriter(); err != nil {
				assert.NoError(t, err, "closeWriter failed")
			}
		})

		t.Run("gzip compression", func(t *testing.T) {
			file, err := os.Create(filepath.Join(tempDir, "test.gz")) //nolint:gosec // tempDir is created in test
			if err != nil {
				require.NoError(t, err, "Failed to create")
			}
			defer file.Close()

			writer, closeWriter, err := createCompressedWriter(file, CompressionGZ)
			require.NoError(t, err, "createCompressedWriter failed for gzip")

			if writer == file {
				t.Error("Expected writer to be different from file for gzip compression")
			}

			// Write some test data
			testData := "test,data\n1,hello\n2,world\n"
			n, err := writer.Write([]byte(testData))
			require.NoError(t, err, "Failed to write to compressed writer")
			if n != len(testData) {
				assert.Fail(t, "Expected to write %d bytes, wrote %d", len(testData), n)
			}

			if err := closeWriter(); err != nil {
				assert.NoError(t, err, "closeWriter failed")
			}
		})

		t.Run("bzip2 compression should error", func(t *testing.T) {
			file, err := os.Create(filepath.Join(tempDir, "test.bz2")) //nolint:gosec // tempDir is created in test
			if err != nil {
				require.NoError(t, err, "Failed to create")
			}
			defer file.Close()

			_, _, err = createCompressedWriter(file, CompressionBZ2)
			if err == nil {
				t.Error("Expected error for bzip2 compression")
			}

			assert.True(t, errors.Is(err, ErrUnsupportedFormat), "expected ErrUnsupportedFormat, got: %v", err)
			assert.Contains(t, err.Error(), "bzip2 compression is not supported for writing")
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
		db, err := Open(context.Background(), csvFile)
		require.NoError(t, err, "Failed to open CSV file")
		defer db.Close()

		// Export to Parquet format
		parquetOutputDir := filepath.Join(tempDir, "parquet_output")
		options := NewDumpOptions().WithFormat(OutputFormatParquet)
		err = DumpDatabase(context.Background(), db, parquetOutputDir, options)
		require.NoError(t, err, "Failed to dump to Parquet")

		// Verify Parquet file was created
		parquetFile := filepath.Join(parquetOutputDir, "test.parquet")
		if _, err := os.Stat(parquetFile); os.IsNotExist(err) {
			require.Fail(t, "Parquet file was not created: %s", parquetFile)
		}

		// Read back the Parquet file
		db2, err := Open(context.Background(), parquetFile)
		require.NoError(t, err, "Failed to open Parquet file")
		defer db2.Close()

		// Verify data is correct
		rows, err := db2.QueryContext(context.Background(), "SELECT id, name, age, email FROM test ORDER BY id")
		require.NoError(t, err, "Failed to query Parquet data")
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
				require.NoError(t, err, "Failed to scan row")
			}
			actualData = append(actualData, []string{id, name, age, email})
		}

		if err := rows.Err(); err != nil {
			require.NoError(t, err, "Error during row iteration")
		}

		require.Equal(t, len(expectedData), len(actualData), "Expected %d rows, got %d", len(expectedData), len(actualData))

		for i, expected := range expectedData {
			if len(actualData[i]) != len(expected) {
				assert.Fail(t, "Row %d: expected %d columns, got %d", i, len(expected), len(actualData[i]))
				continue
			}
			for j, expectedVal := range expected {
				if actualData[i][j] != expectedVal {
					assert.Fail(t, "Row %d, column %d: expected %s, got %s", i, j, expectedVal, actualData[i][j])
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
		db, err := Open(context.Background(), csvFile)
		require.NoError(t, err, "Failed to open CSV file")
		defer db.Close()

		// Export to Parquet format with GZ compression
		parquetOutputDir := filepath.Join(tempDir, "compressed_parquet_output")
		options := NewDumpOptions().
			WithFormat(OutputFormatParquet).
			WithCompression(CompressionGZ)

		// Note: Parquet files should not use external compression,
		// but we test that the system handles this gracefully
		err = DumpDatabase(context.Background(), db, parquetOutputDir, options)
		if err != nil {
			// We expect an error for external compression with Parquet
			expectedErrMsg := "external compression not supported for Parquet format - use Parquet's built-in compression instead"
			if !strings.Contains(err.Error(), expectedErrMsg) {
				require.Contains(t, err.Error(), expectedErrMsg, "Expected error message to contain '%s', got: %v", expectedErrMsg, err)
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
				db, err := Open(context.Background(), csvFile)
				require.NoError(t, err, "Failed to open CSV")
				defer db.Close()

				parquetDir := filepath.Join(tempDir, td.name+"_parquet")
				err = DumpDatabase(context.Background(), db, parquetDir, NewDumpOptions().WithFormat(OutputFormatParquet))
				require.NoError(t, err, "Failed to export to Parquet")

				// Read back from Parquet
				parquetFile := filepath.Join(parquetDir, td.name+".parquet")
				db2, err := Open(context.Background(), parquetFile)
				require.NoError(t, err, "Failed to open Parquet file")
				defer db2.Close()

				// Query all data
				rows, err := db2.QueryContext(context.Background(), "SELECT * FROM "+td.name+" ORDER BY id") //nolint:gosec
				require.NoError(t, err, "Failed to query")
				defer rows.Close()

				columns, err := rows.Columns()
				require.NoError(t, err, "Failed to get columns")

				var actualRows []map[string]string
				for rows.Next() {
					values := make([]interface{}, len(columns))
					valuePtrs := make([]interface{}, len(columns))
					for i := range values {
						valuePtrs[i] = &values[i]
					}

					if err := rows.Scan(valuePtrs...); err != nil {
						require.NoError(t, err, "Failed to scan row")
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
					require.NoError(t, err, "Error during row iteration")
				}

				// Compare results
				require.Equal(t, len(td.expected), len(actualRows), "Expected %d rows, got %d", len(td.expected), len(actualRows))

				for i, expectedRow := range td.expected {
					actualRow := actualRows[i]
					for col, expectedVal := range expectedRow {
						if actualVal, ok := actualRow[col]; !ok {
							assert.Fail(t, "Row %d: missing column %s", i, col)
						} else if actualVal != expectedVal {
							assert.Fail(t, "Row %d, column %s: expected %s, got %s", i, col, expectedVal, actualVal)
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

	if os.Getenv("GITHUB_ACTIONS") == "" {
		t.Skip("Skipping slow Parquet performance test in local development")
	}

	// Create temporary directory
	tempDir := t.TempDir()

	// Generate larger test data
	var sb strings.Builder
	sb.WriteString("id,name,value,timestamp\n")
	for i := 1; i <= 10000; i++ {
		fmt.Fprintf(&sb, "%d,User%d,%.2f,2023-01-01T%02d:00:00Z\n",
			i, i, float64(i)*1.5, (i % 24))
	}
	csvContent := sb.String()

	csvFile := filepath.Join(tempDir, "large_test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0600); err != nil {
		t.Fatal(err)
	}

	// Test CSV to Parquet export performance
	start := time.Now()
	db, err := Open(context.Background(), csvFile)
	require.NoError(t, err, "Failed to open CSV")
	defer db.Close()

	parquetDir := filepath.Join(tempDir, "perf_parquet")
	err = DumpDatabase(context.Background(), db, parquetDir, NewDumpOptions().WithFormat(OutputFormatParquet))
	require.NoError(t, err, "Failed to export to Parquet")
	exportTime := time.Since(start)

	// Test Parquet read performance
	parquetFile := filepath.Join(parquetDir, "large_test.parquet")
	start = time.Now()
	db2, err := Open(context.Background(), parquetFile)
	require.NoError(t, err, "Failed to open Parquet")
	defer db2.Close()

	var count int
	err = db2.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM large_test").Scan(&count)
	require.NoError(t, err, "Failed to query count")
	readTime := time.Since(start)

	t.Logf("Performance results:")
	t.Logf("Export time: %v", exportTime)
	t.Logf("Read time: %v", readTime)
	t.Logf("Records processed: %d", count)

	if count != 10000 {
		assert.Fail(t, "Expected 10000 records, got %d", count)
	}
}

// TestParquetDirectParsing tests parseParquet and parseCompressedParquet functions directly
func TestWriteXLSXTableData(t *testing.T) {
	t.Parallel()

	// The write goes through writeSQLiteTableData rather than the XLSX writer
	// directly: that is the path a dump takes, and it stages the file under a
	// temporary name. Calling the writer with a final path hid the fact that a
	// staged XLSX write could not succeed at all.
	dumpSheet := func(t *testing.T, table, query string, opts DumpOptions) string {
		t.Helper()

		db, err := Open(context.Background(), filepath.Join("testdata", "excel", "sample.xlsx"))
		require.NoError(t, err)
		defer db.Close()

		rows, err := db.QueryContext(context.Background(), query)
		require.NoError(t, err)
		defer rows.Close()

		columns, err := rows.Columns()
		require.NoError(t, err)

		outputPath := filepath.Join(t.TempDir(), table+opts.FileExtension())
		require.NoError(t, writeSQLiteTableData(nil, outputPath, table, columns, rows, opts, nil))
		return outputPath
	}

	t.Run("uncompressed output is a readable workbook named after the table", func(t *testing.T) {
		t.Parallel()

		opts := NewDumpOptions().WithFormat(OutputFormatXLSX)
		outputPath := dumpSheet(t, "people", "SELECT * FROM sample_Sheet1", opts)

		xlsxFile, err := excelize.OpenFile(outputPath)
		require.NoError(t, err)
		defer xlsxFile.Close()

		sheets := xlsxFile.GetSheetList()
		require.Len(t, sheets, 1)
		assert.Equal(t, "people", sheets[0], "the sheet is named after the table, not after the staged file")

		sheetRows, err := xlsxFile.GetRows(sheets[0])
		require.NoError(t, err)
		require.Len(t, sheetRows, 4, "1 header + 3 data rows")
		assert.Equal(t, []string{"id", "name"}, sheetRows[0])
		assert.Equal(t, []string{"1", "Gina"}, sheetRows[1])
	})

	t.Run("gzip output decompresses to the same workbook", func(t *testing.T) {
		t.Parallel()

		opts := NewDumpOptions().WithFormat(OutputFormatXLSX).WithCompression(CompressionGZ)
		outputPath := dumpSheet(t, "mails", "SELECT * FROM sample_Sheet2", opts)

		file, err := os.Open(outputPath) //nolint:gosec // Test file path is safe
		require.NoError(t, err)
		defer file.Close()

		gzipReader, err := gzip.NewReader(file)
		require.NoError(t, err)
		defer gzipReader.Close()

		var buf bytes.Buffer
		_, err = io.Copy(&buf, gzipReader) //nolint:gosec // Test data is safe
		require.NoError(t, err)

		xlsxFile, err := excelize.OpenReader(&buf)
		require.NoError(t, err)
		defer xlsxFile.Close()

		sheets := xlsxFile.GetSheetList()
		require.Len(t, sheets, 1)
		assert.Equal(t, "mails", sheets[0])

		sheetRows, err := xlsxFile.GetRows(sheets[0])
		require.NoError(t, err)
		require.Len(t, sheetRows, 4, "1 header + 3 data rows")
		assert.Equal(t, []string{"id", "mail"}, sheetRows[0])
	})

	t.Run("xz output is written and is not empty", func(t *testing.T) {
		t.Parallel()

		opts := NewDumpOptions().WithFormat(OutputFormatXLSX).WithCompression(CompressionXZ)
		outputPath := dumpSheet(t, "people", "SELECT * FROM sample_Sheet1", opts)

		info, err := os.Stat(outputPath)
		require.NoError(t, err)
		assert.Greater(t, info.Size(), int64(100), "a compressed workbook is larger than this")
	})

	t.Run("no columns is rejected", func(t *testing.T) {
		t.Parallel()

		err := writeXLSXTableData(io.Discard, "empty", []string{}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no columns defined")
	})

	t.Run("bz2 is rejected because it has no writer", func(t *testing.T) {
		t.Parallel()

		db, err := Open(context.Background(), filepath.Join("testdata", "excel", "sample.xlsx"))
		require.NoError(t, err)
		defer db.Close()

		rows, err := db.QueryContext(context.Background(), "SELECT * FROM sample_Sheet1")
		require.NoError(t, err)
		defer rows.Close()

		columns, err := rows.Columns()
		require.NoError(t, err)

		opts := NewDumpOptions().WithFormat(OutputFormatXLSX).WithCompression(CompressionBZ2)
		outputPath := filepath.Join(t.TempDir(), "people"+opts.FileExtension())
		err = writeSQLiteTableData(nil, outputPath, "people", columns, rows, opts, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bzip2 compression is not supported")
		assert.NoFileExists(t, outputPath, "a rejected dump leaves nothing behind")
	})
}

// TestAnEmptySourceIsRefusedExceptWhereADocumentCanSayNothing pins which
// formats refuse a source with nothing in it and which load one as a table with
// no rows. The split is deliberate and was undocumented: JSON and JSONL are the
// two formats whose empty input a caller reads as "the pipeline produced no
// rows", and sqly relies on opening such a file rather than failing. Every other
// format refuses it, because nothing in those formats can say "no rows" without
// saying anything at all.
func TestAnEmptySourceIsRefusedExceptWhereADocumentCanSayNothing(t *testing.T) {
	t.Parallel()

	refusing := []struct {
		ext      string
		fileType FileType
	}{
		{".csv", FileTypeCSV},
		{".tsv", FileTypeTSV},
		{".ltsv", FileTypeLTSV},
		{".parquet", FileTypeParquet},
		{".xlsx", FileTypeXLSX},
	}
	loading := []struct {
		ext      string
		fileType FileType
	}{
		{".json", FileTypeJSON},
		{".jsonl", FileTypeJSONL},
	}

	t.Run("an empty file of a format that cannot say nothing", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		for _, f := range refusing {
			path := filepath.Join(dir, "e"+f.ext)
			require.NoError(t, os.WriteFile(path, nil, 0o600))

			_, err := Open(t.Context(), path)
			require.Error(t, err, "an empty %s file must be refused", f.ext)
			assert.ErrorIs(t, err, ErrEmptyData, "an empty %s file must be refused", f.ext)
		}
	})

	t.Run("an empty JSON or JSONL file is a table with no rows", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		for _, f := range loading {
			path := filepath.Join(dir, "e"+f.ext)
			require.NoError(t, os.WriteFile(path, nil, 0o600))

			db, err := Open(t.Context(), path)
			require.NoError(t, err, "an empty %s file loads as a table with no rows", f.ext)

			var rows int
			require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM e").Scan(&rows))
			assert.Equal(t, 0, rows, f.ext)
			require.NoError(t, db.Close())
		}
	})

	t.Run("a reader answers the way a file of the same format does", func(t *testing.T) {
		t.Parallel()

		// And the concrete type of the reader does not enter into it. The
		// builder used to peek when the reader happened to be a
		// *strings.Reader, so the same empty bytes were refused through one
		// reader type and loaded through another.
		readers := []struct {
			name string
			make func() io.Reader
		}{
			{"strings.Reader", func() io.Reader { return strings.NewReader("") }},
			{"bytes.Reader", func() io.Reader { return bytes.NewReader(nil) }},
			{"bytes.Buffer", func() io.Reader { return &bytes.Buffer{} }},
		}
		for _, r := range readers {
			for _, f := range refusing {
				_, err := NewBuilder().AddReader(r.make(), "e", f.fileType).Open(t.Context())
				require.Error(t, err, "%s: an empty %s reader must be refused", r.name, f.ext)
				assert.ErrorIs(t, err, ErrEmptyData, "%s: %s", r.name, f.ext)
			}
			for _, f := range loading {
				db, err := NewBuilder().AddReader(r.make(), "e", f.fileType).Open(t.Context())
				require.NoError(t, err, "%s: an empty %s reader is a table with no rows", r.name, f.ext)

				var rows int
				require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM e").Scan(&rows))
				assert.Equal(t, 0, rows, "%s: %s", r.name, f.ext)
				require.NoError(t, db.Close())
			}
		}
	})

	t.Run("a document saying there is nothing is a table", func(t *testing.T) {
		t.Parallel()

		// The distinction the exemption rests on: "[]" is a document that says
		// there are no rows, which a CSV holding only its header also is.
		dir := t.TempDir()
		path := filepath.Join(dir, "e.json")
		require.NoError(t, os.WriteFile(path, []byte("[]"), 0o600))

		db, err := Open(t.Context(), path)
		require.NoError(t, err)
		defer db.Close()

		var rows int
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM e").Scan(&rows))
		assert.Equal(t, 0, rows)
	})
}

func TestEdgeCasesEmptyAndMalformedData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		fileContent string
		fileName    string
		expectedErr bool
		description string
	}{
		{
			name:        "Completely empty file",
			fileContent: "",
			fileName:    "empty.csv",
			expectedErr: true,
			description: "Should fail gracefully on completely empty files",
		},
		{
			name:        "Header only file",
			fileContent: "col1,col2,col3\n",
			fileName:    "header_only.csv",
			expectedErr: false,
			description: "Should handle files with header but no data rows",
		},
		{
			name:        "Only newlines",
			fileContent: "\n\n\n",
			fileName:    "only_newlines.csv",
			expectedErr: true,
			description: "Should handle files with only newlines",
		},
		{
			name:        "Unmatched quotes in CSV",
			fileContent: "col1,col2\n\"unclosed quote,value2",
			fileName:    "unmatched_quotes.csv",
			expectedErr: true,
			description: "Should handle malformed CSV with unmatched quotes",
		},
		{
			name:        "BOM in UTF-8 file",
			fileContent: "\uFEFFcol1,col2\nvalue1,value2",
			fileName:    "bom_file.csv",
			expectedErr: false,
			description: "Should handle BOM correctly",
		},
		{
			name:        "Mixed line endings",
			fileContent: "col1,col2\r\nvalue1,value2\nvalue3,value4\r",
			fileName:    "mixed_endings.csv",
			expectedErr: false,
			description: "Should handle mixed line endings",
		},
		{
			name:        "Very long column name",
			fileContent: strings.Repeat("a", 1000) + ",col2\nvalue1,value2",
			fileName:    "long_column.csv",
			expectedErr: false,
			description: "Should handle very long column names",
		},
		{
			name:        "Non-ASCII column names",
			fileContent: "名前,年齢,メール\n田中,30,tanaka@example.com",
			fileName:    "non_ascii.csv",
			expectedErr: false,
			description: "Should handle non-ASCII column names",
		},
		{
			name:        "Empty column name",
			fileContent: "col1,,col3\nvalue1,value2,value3",
			fileName:    "empty_column_name.csv",
			expectedErr: false,
			description: "Should handle empty column names",
		},
		{
			name:        "Inconsistent row lengths",
			fileContent: "col1,col2,col3\nvalue1,value2\nvalue3,value4,value5,value6",
			fileName:    "inconsistent_rows.csv",
			expectedErr: true,
			description: "Should reject rows with different numbers of columns under the default stop policy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create temporary test file with proper extension
			ext := filepath.Ext(tt.fileName)
			prefix := strings.TrimSuffix(tt.fileName, ext)
			tmpFile, err := os.CreateTemp(t.TempDir(), prefix+"*"+ext)
			if err != nil {
				require.NoError(t, err, "Failed to create")
			}
			defer tmpFile.Close()

			// Write test content
			if _, err := tmpFile.WriteString(tt.fileContent); err != nil {
				require.NoError(t, err, "Failed to write test content")
			}

			// Test with timeout context
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Attempt to open the file
			db, err := Open(ctx, tmpFile.Name())

			if tt.expectedErr {
				if err == nil {
					if db != nil {
						_ = db.Close() // Ignore error in test cleanup
					}
					assert.Error(t, err, "Expected error for %s, but got none", tt.description)
				}
				return
			}

			if err != nil {
				assert.NoError(t, err, "Unexpected error for %s", tt.description)
				return
			}

			if db == nil {
				assert.Fail(t, "Expected valid db for %s, but got nil", tt.description)
				return
			}
			defer db.Close()

			// Try a basic query to ensure the database is functional
			actualFileName := filepath.Base(tmpFile.Name())
			tableName := strings.TrimSuffix(actualFileName, filepath.Ext(actualFileName))
			query := fmt.Sprintf("SELECT COUNT(*) FROM \"%s\"", tableName) //nolint:gosec // Table name is from test data //nolint:gosec // Table name is from test data
			rows, err := db.QueryContext(ctx, query)
			if err != nil {
				assert.Fail(t, "Query failed for %s: %v", tt.description, err)
				return
			}
			defer rows.Close()
			if err := rows.Err(); err != nil {
				assert.Fail(t, "Rows error for %s: %v", tt.description, err)
				return
			}

			var count int
			if rows.Next() {
				if err := rows.Scan(&count); err != nil {
					assert.Fail(t, "Scan failed for %s: %v", tt.description, err)
				}
			}
		})
	}
}

func TestEdgeCasesReaderInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		content     string
		fileType    FileType
		expectedErr bool
		errOnOpen   bool
	}{
		{
			// The refusal comes from the read rather than from the build: the
			// build cannot answer it without consuming the stream.
			name:        "Empty reader",
			content:     "",
			fileType:    FileTypeCSV,
			expectedErr: false,
			errOnOpen:   true,
		},
		{
			name:        "Null bytes in content",
			content:     "col1,col2\nvalue1\x00,value2",
			fileType:    FileTypeCSV,
			expectedErr: false,
		},
		{
			name:        "Very large single cell",
			content:     "col1,col2\n" + strings.Repeat("x", 100000) + ",value2",
			fileType:    FileTypeCSV,
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reader := strings.NewReader(tt.content)

			builder := NewBuilder().AddReader(reader, "test_table", tt.fileType)

			ctx := context.Background()
			validatedBuilder, err := buildForTest(ctx, builder)

			if tt.expectedErr {
				if err == nil {
					assert.Error(t, err, "Expected error for %s, but got none", tt.name)
				}
				return
			}

			if err != nil {
				assert.NoError(t, err, "Unexpected error for %s", tt.name)
				return
			}

			db, err := validatedBuilder.Open(ctx)
			if tt.errOnOpen {
				require.Error(t, err, "Expected the read to refuse %s", tt.name)
				assert.ErrorIs(t, err, ErrEmptyData, tt.name)
				return
			}
			if err != nil {
				assert.Fail(t, "Failed to open database for %s: %v", tt.name, err)
				return
			}
			defer db.Close()
		})
	}
}

func TestEdgeCasesCompression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		content     []byte
		expectedErr bool
		description string
	}{
		{
			name:        "Corrupted gzip header",
			content:     []byte{0x1f, 0x8b, 0x08, 0x00}, // Incomplete gzip header
			expectedErr: true,
			description: "Should handle corrupted gzip files gracefully",
		},
		{
			name:        "Non-gzip data with .gz extension",
			content:     []byte("col1,col2\nvalue1,value2"),
			expectedErr: true,
			description: "Should detect when .gz file is not actually gzipped",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tmpFile, err := os.CreateTemp(t.TempDir(), "test_*.csv.gz")
			if err != nil {
				require.NoError(t, err, "Failed to create")
			}
			defer tmpFile.Close()

			if _, err := tmpFile.Write(tt.content); err != nil {
				require.NoError(t, err, "Failed to write test content")
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			db, err := Open(ctx, tmpFile.Name())

			if tt.expectedErr {
				if err == nil {
					if db != nil {
						_ = db.Close() // Ignore error in test cleanup
					}
					assert.Error(t, err, "Expected error for %s, but got none", tt.description)
				}
				return
			}

			if err != nil {
				assert.NoError(t, err, "Unexpected error for %s", tt.description)
				return
			}

			if db != nil {
				_ = db.Close() // Ignore error in test cleanup
			}
		})
	}
}

func TestEdgeCasesMemoryLimits(t *testing.T) {
	t.Parallel()

	// Test extremely wide file (many columns)
	t.Run("Many columns file", func(t *testing.T) {
		t.Parallel()

		const numCols = 1000
		var header strings.Builder
		var dataRow strings.Builder

		for i := range numCols {
			if i > 0 {
				header.WriteString(",")
				dataRow.WriteString(",")
			}
			header.WriteString("col")
			header.WriteString(strconv.Itoa(i))
			dataRow.WriteString("value")
			dataRow.WriteString(strconv.Itoa(i))
		}

		content := header.String() + "\n" + dataRow.String()

		tmpFile, err := os.CreateTemp(t.TempDir(), "wide_file_*.csv")
		if err != nil {
			require.NoError(t, err, "Failed to create")
		}
		defer tmpFile.Close()

		if _, err := tmpFile.WriteString(content); err != nil {
			require.NoError(t, err, "Failed to write test content")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		db, err := Open(ctx, tmpFile.Name())
		assert.NoError(t, err, "Failed to handle wide file")
		defer db.Close()

		// Verify we can query the wide table
		tableName := filepath.Base(tmpFile.Name())
		tableName = strings.TrimSuffix(tableName, filepath.Ext(tableName))
		tableName = strings.TrimSuffix(tableName, filepath.Ext(tableName)) // Remove .csv

		query := fmt.Sprintf("SELECT COUNT(*) FROM \"%s\"", tableName) //nolint:gosec // Table name is from test data
		rows, err := db.QueryContext(ctx, query)
		assert.NoError(t, err, "Query failed on wide file")
		defer rows.Close()
		if err := rows.Err(); err != nil {
			assert.NoError(t, err, "Rows error on wide file")
			return
		}
	})
}

func TestOpenJSON(t *testing.T) {
	t.Parallel()

	t.Run("opens JSON file and queries with json_extract", func(t *testing.T) {
		t.Parallel()

		db, err := Open(context.Background(), filepath.Join("testdata", "sample.json"))
		require.NoError(t, err)
		defer db.Close()

		// Query using json_extract to access fields
		rows, err := db.QueryContext(context.Background(),
			"SELECT json_extract(data, '$.name') AS name FROM sample ORDER BY json_extract(data, '$.id')")
		require.NoError(t, err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			names = append(names, name)
		}
		require.NoError(t, rows.Err())

		assert.Equal(t, []string{"Alice", "Bob", "Charlie"}, names)
	})

	t.Run("opens JSON file and queries numeric field", func(t *testing.T) {
		t.Parallel()

		db, err := Open(context.Background(), filepath.Join("testdata", "sample.json"))
		require.NoError(t, err)
		defer db.Close()

		var count int
		err = db.QueryRowContext(context.Background(),
			"SELECT COUNT(*) FROM sample WHERE json_extract(data, '$.age') > 28").Scan(&count)
		require.NoError(t, err)

		assert.Equal(t, 2, count) // Alice(30) and Charlie(35)
	})

	t.Run("opens nested JSON file and queries nested fields", func(t *testing.T) {
		t.Parallel()

		db, err := Open(context.Background(), filepath.Join("testdata", "nested.json"))
		require.NoError(t, err)
		defer db.Close()

		var city string
		err = db.QueryRowContext(context.Background(),
			"SELECT json_extract(data, '$.address.city') FROM nested WHERE json_extract(data, '$.name') = 'Alice'").Scan(&city)
		require.NoError(t, err)

		assert.Equal(t, "Tokyo", city)
	})

	t.Run("opens JSON file and counts records", func(t *testing.T) {
		t.Parallel()

		db, err := Open(context.Background(), filepath.Join("testdata", "sample.json"))
		require.NoError(t, err)
		defer db.Close()

		var count int
		err = db.QueryRowContext(context.Background(),
			"SELECT COUNT(*) FROM sample").Scan(&count)
		require.NoError(t, err)

		assert.Equal(t, 3, count)
	})
}

func TestOpenJSONL(t *testing.T) {
	t.Parallel()

	t.Run("opens JSONL file and queries with json_extract", func(t *testing.T) {
		t.Parallel()

		db, err := Open(context.Background(), filepath.Join("testdata", "sample.jsonl"))
		require.NoError(t, err)
		defer db.Close()

		rows, err := db.QueryContext(context.Background(),
			"SELECT json_extract(data, '$.name') AS name FROM sample ORDER BY json_extract(data, '$.id')")
		require.NoError(t, err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			names = append(names, name)
		}
		require.NoError(t, rows.Err())

		assert.Equal(t, []string{"Alice", "Bob", "Charlie"}, names)
	})

	t.Run("opens JSONL file and queries with WHERE clause", func(t *testing.T) {
		t.Parallel()

		db, err := Open(context.Background(), filepath.Join("testdata", "sample.jsonl"))
		require.NoError(t, err)
		defer db.Close()

		var email string
		err = db.QueryRowContext(context.Background(),
			"SELECT json_extract(data, '$.email') FROM sample WHERE json_extract(data, '$.name') = 'Bob'").Scan(&email)
		require.NoError(t, err)

		assert.Equal(t, "bob@example.com", email)
	})
}

func TestOpenCompressedJSON(t *testing.T) {
	t.Parallel()

	jsonContent := `[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"},{"id":3,"name":"Charlie"}]`
	jsonlContent := "{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}\n{\"id\":3,\"name\":\"Charlie\"}\n"

	t.Run("opens gzip compressed JSON and queries with json_extract", func(t *testing.T) {
		t.Parallel()

		fp := filepath.Join(t.TempDir(), "people.json.gz")
		f, err := os.Create(filepath.Clean(fp))
		require.NoError(t, err)

		gw := gzip.NewWriter(f)
		_, err = gw.Write([]byte(jsonContent))
		require.NoError(t, err)
		require.NoError(t, gw.Close())
		require.NoError(t, f.Close())

		db, err := Open(context.Background(), fp)
		require.NoError(t, err)
		defer db.Close()

		var count int
		err = db.QueryRowContext(context.Background(),
			"SELECT COUNT(*) FROM people").Scan(&count)
		require.NoError(t, err)

		assert.Equal(t, 3, count)
	})

	t.Run("opens gzip compressed JSONL and queries with json_extract", func(t *testing.T) {
		t.Parallel()

		fp := filepath.Join(t.TempDir(), "people.jsonl.gz")
		f, err := os.Create(filepath.Clean(fp))
		require.NoError(t, err)

		gw := gzip.NewWriter(f)
		_, err = gw.Write([]byte(jsonlContent))
		require.NoError(t, err)
		require.NoError(t, gw.Close())
		require.NoError(t, f.Close())

		db, err := Open(context.Background(), fp)
		require.NoError(t, err)
		defer db.Close()

		rows, err := db.QueryContext(context.Background(),
			"SELECT json_extract(data, '$.name') AS name FROM people ORDER BY json_extract(data, '$.id')")
		require.NoError(t, err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			names = append(names, name)
		}
		require.NoError(t, rows.Err())

		assert.Equal(t, []string{"Alice", "Bob", "Charlie"}, names)
	})
}

// Test_NonLatinFileNameBecomesQueryableTable pins the whole path from a file
// name written in a non-Latin script to a query: the table keeps the name, and
// two such files in one database stay distinct instead of both collapsing onto
// the "sheet" fallback.
func Test_NonLatinFileNameBecomesQueryableTable(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	dir := t.TempDir()
	files := map[string]string{
		"売上.csv":     "id,amount\n1,100\n2,200\n",
		"顧客.csv":     "id,name\n1,alice\n",
		"Данные.csv": "id\n7\n",
		"café.csv":   "id\n9\n",
	}
	paths := make([]string, 0, len(files))
	for name, body := range files {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte(body), 0600))
		paths = append(paths, path)
	}

	db, err := Open(ctx, paths...)
	require.NoError(t, err)
	defer db.Close()

	// Each file keeps its own table, so a query naming the file finds it.
	var amount int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT amount FROM "売上" WHERE id = '2'`).Scan(&amount))
	assert.Equal(t, 200, amount)

	var name string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT name FROM "顧客" WHERE id = '1'`).Scan(&name))
	assert.Equal(t, "alice", name)

	var id int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT id FROM "Данные"`).Scan(&id))
	assert.Equal(t, 7, id)

	require.NoError(t, db.QueryRowContext(ctx, `SELECT id FROM "café"`).Scan(&id))
	assert.Equal(t, 9, id)

	// None of them fell back to the shared "sheet" name.
	rows, err := db.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'sheet'`)
	require.NoError(t, err)
	defer rows.Close()
	assert.False(t, rows.Next(), `no table should have collapsed onto the "sheet" fallback`)
	require.NoError(t, rows.Err())
}

// TestDumpEntryPointsRefuseANilDatabase pins the answer this package gives to a
// nil argument everywhere else: an error naming what was nil, not a panic. A
// nil database is what a caller holds after an error they did not check, and a
// library that crashes on it takes their process down over it.
func TestDumpEntryPointsRefuseANilDatabase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "DumpDatabase",
			call: func() error { return DumpDatabase(context.Background(), nil, dir) },
		},
		{
			name: "DumpACH",
			call: func() error { return DumpACH(ctx, nil, "t", filepath.Join(dir, "a.ach")) },
		},
		{
			name: "DumpACHWithSource",
			call: func() error {
				return DumpACHWithSource(ctx, nil, "t", filepath.Join(dir, "b.ach"), strings.NewReader(""))
			},
		},
		{
			name: "DumpFedWire",
			call: func() error { return DumpFedWire(ctx, nil, "t", filepath.Join(dir, "a.fed")) },
		},
		{
			name: "DumpFedWireWithSource",
			call: func() error {
				return DumpFedWireWithSource(ctx, nil, "t", filepath.Join(dir, "b.fed"), strings.NewReader(""))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var err error
			require.NotPanics(t, func() { err = tt.call() })
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrNilInput)
		})
	}
}

// loadedColumnNames reports the columns of a table in declaration order.
func loadedColumnNames(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT * FROM "`+table+`" LIMIT 0`) //nolint:gosec // table name is a test literal
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	require.NoError(t, err)
	require.NoError(t, rows.Err())
	return cols
}

// TestLoadColumnNameContainingDoubleQuote covers a column whose name holds a
// double quote, which the load path once dropped into a CREATE TABLE identifier
// without doubling, breaking the statement. Every format that takes its column
// names from the data reaches the same createTable, so the CSV, XLSX, and
// Parquet paths are all exercised, and the name a"b must survive the round trip.
func TestLoadColumnNameContainingDoubleQuote(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("csv", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		// A quoted CSV field "a""b" is the single column name a"b.
		path := filepath.Join(dir, "quoted.csv")
		require.NoError(t, os.WriteFile(path, []byte("\"a\"\"b\"\nvalue\n"), 0o600))

		db, err := Open(ctx, path)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		assert.Equal(t, []string{`a"b`}, loadedColumnNames(t, db, "quoted"))
		var got string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT "a""b" FROM quoted`).Scan(&got))
		assert.Equal(t, "value", got)
	})

	t.Run("csv name that is only a quote", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "onlyquote.csv")
		require.NoError(t, os.WriteFile(path, []byte("\"\"\"\"\nvalue\n"), 0o600))

		db, err := Open(ctx, path)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		assert.Equal(t, []string{`"`}, loadedColumnNames(t, db, "onlyquote"))
	})

	t.Run("xlsx", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "quoted.xlsx")
		// The sheet is named after the file so the one table is "quoted".
		f := excelize.NewFile()
		require.NoError(t, f.SetSheetName("Sheet1", "quoted"))
		require.NoError(t, f.SetCellValue("quoted", "A1", `a"b`))
		require.NoError(t, f.SetCellValue("quoted", "A2", "value"))
		require.NoError(t, f.SaveAs(path))
		require.NoError(t, f.Close())

		db, err := Open(ctx, path)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		assert.Equal(t, []string{`a"b`}, loadedColumnNames(t, db, "quoted"))
		var got string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT "a""b" FROM quoted`).Scan(&got))
		assert.Equal(t, "value", got)
	})

	t.Run("parquet dump round-trip", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		src := filepath.Join(dir, "quoted.csv")
		require.NoError(t, os.WriteFile(src, []byte("\"a\"\"b\"\n42\n"), 0o600))

		db, err := Open(ctx, src)
		require.NoError(t, err)
		out := filepath.Join(dir, "out")
		require.NoError(t, DumpDatabase(context.Background(), db, out, NewDumpOptions().WithFormat(OutputFormatParquet)))
		require.NoError(t, db.Close())

		db2, err := Open(ctx, filepath.Join(out, "quoted.parquet"))
		require.NoError(t, err)
		defer func() { _ = db2.Close() }()

		assert.Equal(t, []string{`a"b`}, loadedColumnNames(t, db2, "quoted"))
		var got string
		require.NoError(t, db2.QueryRowContext(ctx, `SELECT "a""b" FROM quoted`).Scan(&got))
		assert.Equal(t, "42", got)
	})
}

// TestTableNameHoldingAQuotingCharacter pins that a name SQLite accepts is a
// name this package accepts.
//
// A table name reached SQL by concatenation almost everywhere, so a name
// carrying the character that ends the quoting broke the statement and the
// caller was handed SQLite's tokenizer error about a token they never wrote. A
// double quote failed the load and a backtick loaded and then failed the dump,
// because the load quoted with one and the dump with the other. Both are legal
// identifiers, and AddReader keeps the name the caller gives it, so a name that
// came from user input reaches these statements unaltered.
func TestTableNameHoldingAQuotingCharacter(t *testing.T) {
	t.Parallel()

	names := []struct {
		name  string
		table string
	}{
		{name: "a double quote, which ends the load's own quoting", table: `a"b`},
		{name: "two double quotes, which the escaping must not collapse", table: `a""b`},
		{name: "a backtick, which ends the dump's", table: "a`b"},
		{name: "a single quote", table: "a'b"},
		{name: "brackets", table: "a[b]"},
		{name: "a newline", table: "a\nb"},
		{name: "a name that is not ASCII", table: "テーブル"},
	}

	for _, tt := range names {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			validated, err := buildForTest(

				ctx, NewBuilder().
					AddReader(strings.NewReader("v\n1\n2\n"), tt.table, FileTypeCSV))

			require.NoError(t, err)
			db, err := validated.Open(ctx)
			require.NoError(t, err, "a name SQLite accepts must load")
			defer db.Close()

			var rows int
			require.NoError(t, db.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM `+quoteIdentifier(tt.table)).Scan(&rows))
			assert.Equal(t, 2, rows)

			// The dump reads the table through statements of its own, which is
			// where the backtick failed after the load had succeeded.
			out := filepath.Join(t.TempDir(), "out")
			err = DumpDatabase(context.Background(), db, out)
			fileName := tt.table + ".csv"
			if usableAsFileName(fileName) && sanitizeTableName(tableFromFilePath(fileName)) == tt.table {
				require.NoError(t, err, "a name a load would give back must be dumped")
				return
			}
			// A name the file system cannot hold, or one a load would spell
			// differently, is refused by this package, with its own error
			// rather than the driver's.
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidData)
		})
	}
}

// TestAFileThatBeginsWithABlankLineReadsAlike pins one answer for a file whose
// first line holds nothing. A blank line in the middle of a file is skipped by
// every loader, and the first line was not: a CSV skipped it and took the next
// row as its header, a TSV read it as a header of one empty column and refused
// the rows that followed, and a sheet whose first row was blank loaded as no
// table at all with no error, so a workbook holding rows opened as an empty
// database.
func TestAFileThatBeginsWithABlankLineReadsAlike(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		file string
		body string
	}{
		{"csv", "t.csv", "\n1,2\n3,4\n"},
		{"tsv", "t.tsv", "\n1\t2\n3\t4\n"},
		{"csv with two blank lines", "t.csv", "\n\n1,2\n3,4\n"},
		{"tsv with two blank lines", "t.tsv", "\n\n1\t2\n3\t4\n"},
		// A line of whitespace is what a hand-edited export leaves as readily
		// as an empty one, and it carries nothing a column can be named after.
		{"csv with a line of spaces", "t.csv", "   \n1,2\n3,4\n"},
		{"tsv with a line of spaces", "t.tsv", "   \n1\t2\n3\t4\n"},
		{"csv with a line of one ideographic space", "t.csv", "\u3000\n1,2\n3,4\n"},
		{"csv with a blank line and a line of spaces", "t.csv", "\n   \n1,2\n3,4\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tt.file)
			if err := os.WriteFile(path, []byte(tt.body), 0o600); err != nil {
				t.Fatalf("write: %v", err)
			}
			db, err := Open(t.Context(), path)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()

			rows, err := db.QueryContext(t.Context(), "SELECT * FROM t")
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			defer rows.Close()
			columns, err := rows.Columns()
			if err != nil {
				t.Fatalf("columns: %v", err)
			}
			if want := []string{"1", "2"}; !reflect.DeepEqual(columns, want) {
				t.Errorf("columns = %q, want %q: the first row that holds something is the header", columns, want)
			}
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows: %v", err)
			}
			if count != 1 {
				t.Errorf("rows = %d, want 1", count)
			}
		})
	}
}

// TestAHeaderOfEmptyCellsIsStillAHeader pins the sibling the skip must not take
// with it: a line that holds empty cells is a header of that many columns,
// which is what names them after their positions.
func TestAHeaderOfEmptyCellsIsStillAHeader(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		file string
		body string
	}{
		{"t.csv", ",\n1,2\n"},
		{"t.tsv", "\t\n1\t2\n"},
	} {
		path := filepath.Join(t.TempDir(), tt.file)
		if err := os.WriteFile(path, []byte(tt.body), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		db, err := Open(t.Context(), path)
		if err != nil {
			t.Fatalf("open %q: %v", tt.body, err)
		}
		rows, err := db.QueryContext(t.Context(), "SELECT * FROM t")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatalf("columns: %v", err)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows: %v", err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close rows: %v", err)
		}
		if want := []string{"column_1", "column_2"}; !reflect.DeepEqual(columns, want) {
			t.Errorf("%q loads with columns %q, want %q", tt.body, columns, want)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}
}

// TestOpen_EndedContext is Open's share of the contract the
// godoc states for every entry point that takes a context: the load stops and
// the error says which way the context ended. Open is the same call with a
// background context, so it has nothing to stop for.
func TestOpen_EndedContext(t *testing.T) {
	t.Parallel()

	path := csvFixture(t)

	for _, tc := range endedContexts() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, err := Open(tc.make(t), path)
			if db != nil {
				_ = db.Close()
			}
			assert.ErrorIs(t, err, tc.want)
		})
	}
}

// TestAFileThatBeginsWithALineOfWhitespace pins the cases the class above
// cannot reach: a file of one column, where taking the whitespace line as the
// header neither fails nor reports anything, and the rows that follow a file
// whose first line was skipped.
//
// LTSV is not here because it has no header: a line of whitespace there is a
// record holding a field that names no label, which the malformed-row policy
// decides on and whose refusal quotes the field, so a caller can see what it
// was. That answer is pinned in the LTSV tests.
func TestAFileThatBeginsWithALineOfWhitespace(t *testing.T) {
	t.Parallel()

	t.Run("a one-column file keeps its header", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "t.csv")
		require.NoError(t, os.WriteFile(path, []byte("   \nid\n1\n2\n"), 0o600))

		db, err := Open(t.Context(), path)
		require.NoError(t, err)
		defer db.Close()

		assert.Equal(t, []string{"id"}, tableColumns(t, db, "t"), "the header is the first line that holds something")
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t").Scan(&count))
		assert.Equal(t, 2, count, "the header did not become a row")
	})

	t.Run("a later blank line is still skipped", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "t.tsv")
		require.NoError(t, os.WriteFile(path, []byte("   \nid\tname\n1\talice\n\n2\tbob\n"), 0o600))

		db, err := Open(t.Context(), path)
		require.NoError(t, err)
		defer db.Close()

		var count int
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t").Scan(&count))
		assert.Equal(t, 2, count, "the width comes from the header, not from the line before it")
	})

	t.Run("nothing is skipped, so the header was found", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "t.csv")
		require.NoError(t, os.WriteFile(path, []byte("   \nid,name\n1,alice\n2,bob\n"), 0o600))

		builder := NewBuilder().AddPath(path).WithMalformedRowPolicy(MalformedRowSkip)
		db, err := builder.Open(t.Context())
		require.NoError(t, err)
		defer db.Close()

		assert.Empty(t, builder.SkippedRows(), "a file with a stray line of whitespace loses no rows")
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t").Scan(&count))
		assert.Equal(t, 2, count)
	})

	t.Run("a line of two whitespace fields is still a header", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "t.csv")
		require.NoError(t, os.WriteFile(path, []byte(" , \n1,2\n"), 0o600))

		_, err := Open(t.Context(), path)
		require.Error(t, err, "a comma makes the line a header of two columns, which name one column twice")
		assert.ErrorIs(t, err, ErrDuplicateColumn)
	})

	t.Run("a line of whitespace further down is a row of one field", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "t.csv")
		require.NoError(t, os.WriteFile(path, []byte("id,name\n1,alice\n   \n2,bob\n"), 0o600))

		_, err := Open(t.Context(), path)
		require.Error(t, err, "only the header search passes over a line of whitespace")
		assert.ErrorIs(t, err, ErrColumnMismatch)
	})
}

// tableColumns is the column names of a table, in order.
func tableColumns(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()

	query := "SELECT * FROM " + quoteIdentifier(table) + " LIMIT 0" //nolint:gosec // Table name is quoted and comes from the test
	rows, err := db.QueryContext(t.Context(), query)
	require.NoError(t, err)
	defer rows.Close()
	columns, err := rows.Columns()
	require.NoError(t, err)
	require.NoError(t, rows.Err())
	return columns
}
