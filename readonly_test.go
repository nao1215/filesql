package filesql

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyDB_Query(t *testing.T) {
	ctx := context.Background()

	// Create a normal database
	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	// Wrap in read-only
	rodb := NewReadOnlyDB(db)

	// SELECT should work
	rows, err := rodb.Query("SELECT * FROM test")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	assert.Greater(t, count, 0, "should have rows")
}

func TestReadOnlyDB_QueryContext(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	rows, err := rodb.QueryContext(ctx, "SELECT * FROM test")
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		// Just iterate to consume all rows
	}
	require.NoError(t, rows.Err())
}

func TestReadOnlyDB_QueryRow(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	var count int
	err = rodb.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0)
}

func TestReadOnlyDB_QueryRowContext(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	var count int
	err = rodb.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0)
}

func TestReadOnlyDB_ExecRejectsWrite(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	tests := []struct {
		name  string
		query string
	}{
		{"INSERT", "INSERT INTO test (name, age, city) VALUES ('Test', 99, 'Test City')"},
		{"UPDATE", "UPDATE test SET name = 'Changed' WHERE name = 'Alice'"},
		{"DELETE", "DELETE FROM test WHERE name = 'Alice'"},
		{"DROP", "DROP TABLE test"},
		{"ALTER", "ALTER TABLE test ADD COLUMN new_col TEXT"},
		{"CREATE", "CREATE TABLE new_table (id INTEGER)"},
		{"TRUNCATE", "TRUNCATE TABLE test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := rodb.Exec(tt.query)
			assert.True(t, errors.Is(err, ErrReadOnly), "expected ErrReadOnly for %s", tt.name)
		})
	}
}

func TestReadOnlyDB_ExecContextRejectsWrite(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	_, err = rodb.ExecContext(ctx, "UPDATE test SET name = 'Changed'")
	assert.True(t, errors.Is(err, ErrReadOnly))
}

// TestReadOnlyDB_ExecAllowsNonWrite verifies that non-write Exec statements work
func TestReadOnlyDB_ExecAllowsNonWrite(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	// PRAGMA and other non-write statements should work via Exec
	_, err = rodb.Exec("PRAGMA table_info(test)")
	require.NoError(t, err)

	_, err = rodb.ExecContext(ctx, "PRAGMA table_info(test)")
	require.NoError(t, err)
}

func TestReadOnlyDB_PrepareRejectsWrite(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	// Prepare for SELECT should work
	stmt, err := rodb.Prepare("SELECT * FROM test WHERE name = ?")
	require.NoError(t, err)
	require.NoError(t, stmt.Close())

	// Prepare for write should fail
	_, err = rodb.Prepare("INSERT INTO test VALUES (?, ?, ?)")
	assert.True(t, errors.Is(err, ErrReadOnly))
}

func TestReadOnlyDB_PrepareContextRejectsWrite(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	// PrepareContext for SELECT should work
	stmt, err := rodb.PrepareContext(ctx, "SELECT * FROM test WHERE name = ?")
	require.NoError(t, err)
	require.NoError(t, stmt.Close())

	// PrepareContext for write should fail
	_, err = rodb.PrepareContext(ctx, "DELETE FROM test WHERE name = ?")
	assert.True(t, errors.Is(err, ErrReadOnly))
}

func TestReadOnlyDB_Transaction(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	// Begin transaction
	tx, err := rodb.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	// SELECT in transaction should work
	rows, err := tx.Query("SELECT * FROM test")
	require.NoError(t, err)
	for rows.Next() {
		// Just iterate to consume all rows
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	// Write in transaction should fail
	_, err = tx.Exec("UPDATE test SET name = 'Changed'")
	assert.True(t, errors.Is(err, ErrReadOnly))
}

func TestReadOnlyDB_BeginTx(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	tx, err := rodb.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	// Query should work
	var count int
	err = tx.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
}

func TestReadOnlyDB_Ping(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	err = rodb.Ping()
	require.NoError(t, err)

	err = rodb.PingContext(ctx)
	require.NoError(t, err)
}

func TestReadOnlyDB_UnderlyingDB(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	// DB() should return the underlying *sql.DB
	underlying := rodb.DB()
	assert.Equal(t, db, underlying)
}

func TestBuilderOpenReadOnly(t *testing.T) {
	ctx := context.Background()

	builder := NewBuilder().
		AddPath(filepath.Join("testdata", "test.csv"))

	validatedBuilder, err := builder.Build(ctx)
	require.NoError(t, err)

	rodb, err := validatedBuilder.OpenReadOnly(ctx)
	require.NoError(t, err)
	defer rodb.Close()

	// SELECT should work
	var count int
	err = rodb.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0)

	// UPDATE should fail
	_, err = rodb.Exec("UPDATE test SET name = 'Changed'")
	assert.True(t, errors.Is(err, ErrReadOnly))
}

func TestIsWriteStatement(t *testing.T) {
	tests := []struct {
		query    string
		expected bool
	}{
		// Write statements
		{"INSERT INTO users VALUES (1)", true},
		{"  INSERT INTO users VALUES (1)", true}, // with leading space
		{"insert into users values (1)", true},   // lowercase
		{"INSERT\nINTO users VALUES (1)", true},  // with newline after INSERT
		{"UPDATE users SET name = 'a'", true},
		{"DELETE FROM users", true},
		{"DROP TABLE users", true},
		{"ALTER TABLE users ADD col TEXT", true},
		{"CREATE TABLE users (id INT)", true},
		{"TRUNCATE TABLE users", true},
		{"REPLACE INTO users VALUES (1)", true},
		{"UPSERT INTO users VALUES (1)", true},

		// Write statements hidden behind comments
		{"/*x*/ DELETE FROM users", true},       // leading block comment
		{"-- comment\nDELETE FROM users", true}, // leading line comment
		{"/* multi\nline */ UPDATE users SET a=1", true},
		{"  /*a*/ /*b*/ INSERT INTO users VALUES (1)", true}, // multiple comments

		// Write statements behind a CTE (WITH ... DELETE/UPDATE/INSERT)
		{"WITH cte AS (SELECT 1) DELETE FROM users", true},
		{"WITH cte AS (SELECT 1) UPDATE users SET a = 1", true},
		{"WITH cte AS (SELECT 1) INSERT INTO users SELECT * FROM cte", true},
		{"WITH RECURSIVE cte(x) AS (SELECT 1) DELETE FROM users", true},

		// DML with RETURNING (returns rows but still mutates data)
		{"DELETE FROM users WHERE id = 1 RETURNING id", true},
		{"UPDATE users SET a = 1 RETURNING a", true},

		// Read statements
		{"SELECT * FROM users", false},
		{"select * from users", false},
		{"  SELECT * FROM users", false},
		{"WITH cte AS (SELECT 1) SELECT * FROM cte", false},
		{"WITH cte AS (SELECT 1), d AS (SELECT 2) SELECT * FROM cte, d", false},
		{"EXPLAIN SELECT * FROM users", false},
		{"PRAGMA table_info(users)", false},
		{"-- delete is mentioned in a comment\nSELECT * FROM users", false},
		{"SELECT note FROM users WHERE note = 'please delete this'", false}, // keyword inside a string literal
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result := isWriteStatement(tt.query)
			assert.Equal(t, tt.expected, result, "query: %s", tt.query)
		})
	}
}

// TestReadOnlyStmt tests prepared statement operations
func TestReadOnlyStmt_Query(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	stmt, err := rodb.Prepare("SELECT * FROM test WHERE name = ?")
	require.NoError(t, err)
	defer stmt.Close()

	// Query should work
	rows, err := stmt.Query("Alice")
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		// consume rows
	}
	require.NoError(t, rows.Err())
}

func TestReadOnlyStmt_QueryContext(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	stmt, err := rodb.PrepareContext(ctx, "SELECT * FROM test WHERE name = ?")
	require.NoError(t, err)
	defer stmt.Close()

	// QueryContext should work
	rows, err := stmt.QueryContext(ctx, "Alice")
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		// consume rows
	}
	require.NoError(t, rows.Err())
}

func TestReadOnlyStmt_QueryRow(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	stmt, err := rodb.Prepare("SELECT COUNT(*) FROM test WHERE name = ?")
	require.NoError(t, err)
	defer stmt.Close()

	var count int
	err = stmt.QueryRow("Alice").Scan(&count)
	require.NoError(t, err)
}

func TestReadOnlyStmt_QueryRowContext(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	stmt, err := rodb.PrepareContext(ctx, "SELECT COUNT(*) FROM test WHERE name = ?")
	require.NoError(t, err)
	defer stmt.Close()

	var count int
	err = stmt.QueryRowContext(ctx, "Alice").Scan(&count)
	require.NoError(t, err)
}

func TestReadOnlyStmt_ExecNonWrite(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	// Prepare a non-write statement
	stmt, err := rodb.Prepare("SELECT * FROM test")
	require.NoError(t, err)
	defer stmt.Close()

	// Exec on non-write statement should work (though unusual for SELECT)
	_, err = stmt.Exec()
	require.NoError(t, err)

	// ExecContext on non-write statement should work
	_, err = stmt.ExecContext(ctx)
	require.NoError(t, err)
}

// TestReadOnlyTx tests transaction operations
func TestReadOnlyTx_QueryContext(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	tx, err := rodb.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	// QueryContext should work
	rows, err := tx.QueryContext(ctx, "SELECT * FROM test")
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		// consume rows
	}
	require.NoError(t, rows.Err())
}

func TestReadOnlyTx_QueryRowContext(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	tx, err := rodb.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	var count int
	err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0)
}

func TestReadOnlyTx_ExecContextRejectsWrite(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	tx, err := rodb.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	// ExecContext with write should fail
	_, err = tx.ExecContext(ctx, "INSERT INTO test (name, age, city) VALUES ('Test', 99, 'City')")
	assert.True(t, errors.Is(err, ErrReadOnly))
}

func TestReadOnlyTx_ExecContextAllowsNonWrite(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	tx, err := rodb.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	// ExecContext with non-write should work
	_, err = tx.ExecContext(ctx, "PRAGMA table_info(test)")
	require.NoError(t, err)
}

func TestReadOnlyTx_Commit(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	tx, err := rodb.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Query something
	var count int
	err = tx.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)

	// Commit should work
	err = tx.Commit()
	require.NoError(t, err)
}

func TestReadOnlyTx_Prepare(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	tx, err := rodb.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	// Prepare SELECT should work
	stmt, err := tx.Prepare("SELECT * FROM test WHERE name = ?")
	require.NoError(t, err)
	require.NoError(t, stmt.Close())

	// Prepare write should fail
	_, err = tx.Prepare("UPDATE test SET name = ? WHERE name = ?")
	assert.True(t, errors.Is(err, ErrReadOnly))
}

func TestReadOnlyTx_PrepareContext(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	tx, err := rodb.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	// PrepareContext SELECT should work
	stmt, err := tx.PrepareContext(ctx, "SELECT * FROM test WHERE name = ?")
	require.NoError(t, err)
	require.NoError(t, stmt.Close())

	// PrepareContext write should fail
	_, err = tx.PrepareContext(ctx, "DELETE FROM test WHERE name = ?")
	assert.True(t, errors.Is(err, ErrReadOnly))
}

// TestReadOnlyActuallyPreventsWrites verifies that data is not modified
func TestReadOnlyActuallyPreventsWrites(t *testing.T) {
	ctx := context.Background()

	// Open the database normally to get initial state
	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)

	var initialCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&initialCount)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Now open as read-only and try to modify
	db2, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db2.Close()

	rodb := NewReadOnlyDB(db2)

	// Try INSERT - should fail
	_, err = rodb.Exec("INSERT INTO test (name, age, city) VALUES ('NewPerson', 99, 'NewCity')")
	assert.True(t, errors.Is(err, ErrReadOnly))

	// Try UPDATE - should fail
	_, err = rodb.Exec("UPDATE test SET name = 'Modified' WHERE name = 'Alice'")
	assert.True(t, errors.Is(err, ErrReadOnly))

	// Try DELETE - should fail
	_, err = rodb.Exec("DELETE FROM test WHERE name = 'Alice'")
	assert.True(t, errors.Is(err, ErrReadOnly))

	// Verify count hasn't changed
	var finalCount int
	err = rodb.QueryRow("SELECT COUNT(*) FROM test").Scan(&finalCount)
	require.NoError(t, err)
	assert.Equal(t, initialCount, finalCount, "count should not have changed")

	// Verify data hasn't changed (Alice should still exist with original name)
	var name string
	err = rodb.QueryRow("SELECT name FROM test WHERE name = 'Alice'").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "Alice", name)
}

// TestReadOnlyTxActuallyPreventsWrites verifies transaction writes are blocked
func TestReadOnlyTxActuallyPreventsWrites(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	var initialCount int
	err = rodb.QueryRow("SELECT COUNT(*) FROM test").Scan(&initialCount)
	require.NoError(t, err)

	// Begin transaction
	tx, err := rodb.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Try write operations in transaction
	_, err = tx.Exec("INSERT INTO test (name, age, city) VALUES ('TxPerson', 88, 'TxCity')")
	assert.True(t, errors.Is(err, ErrReadOnly))

	_, err = tx.ExecContext(ctx, "UPDATE test SET age = 100 WHERE name = 'Alice'")
	assert.True(t, errors.Is(err, ErrReadOnly))

	require.NoError(t, tx.Rollback())

	// Verify count hasn't changed
	var finalCount int
	err = rodb.QueryRow("SELECT COUNT(*) FROM test").Scan(&finalCount)
	require.NoError(t, err)
	assert.Equal(t, initialCount, finalCount)
}

// TestReadOnlyStmtExecWithWriteFlag tests that prepared statements with isWrite=true block exec
func TestReadOnlyStmtExecBlocked(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	// Manually create a ReadOnlyStmt with isWrite=true to test the blocking path
	// This simulates what would happen if somehow a write statement got prepared
	stmt, err := db.PrepareContext(ctx, "SELECT * FROM test")
	require.NoError(t, err)
	defer stmt.Close()

	roStmt := &ReadOnlyStmt{stmt: stmt, isWrite: true}

	// Exec should be blocked because isWrite is true
	_, err = roStmt.Exec()
	assert.True(t, errors.Is(err, ErrReadOnly))

	_, err = roStmt.ExecContext(ctx)
	assert.True(t, errors.Is(err, ErrReadOnly))
}

// TestReadOnlyTx_ExecAllowsNonWriteExec tests Exec (not ExecContext) with non-write
func TestReadOnlyTx_ExecAllowsNonWriteExec(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	tx, err := rodb.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	// Exec (not ExecContext) with non-write should work
	_, err = tx.Exec("PRAGMA table_info(test)")
	require.NoError(t, err)
}

// TestReadOnlyClose verifies close works properly
func TestReadOnlyClose(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)

	rodb := NewReadOnlyDB(db)

	// Close should work
	err = rodb.Close()
	require.NoError(t, err)

	// After close, queries should fail
	rows, err := rodb.Query("SELECT * FROM test")
	if rows != nil {
		for rows.Next() {
			// consume
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	assert.Error(t, err)
}

// TestReadOnlyDB_PrepareContextError tests PrepareContext when db returns error
func TestReadOnlyDB_PrepareContextError(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)

	rodb := NewReadOnlyDB(db)

	// Close the underlying db to cause PrepareContext to fail
	require.NoError(t, db.Close())

	_, err = rodb.PrepareContext(ctx, "SELECT * FROM test")
	assert.Error(t, err)
}

// TestReadOnlyDB_BeginTxError tests BeginTx when db returns error
func TestReadOnlyDB_BeginTxError(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)

	rodb := NewReadOnlyDB(db)

	// Close the underlying db to cause BeginTx to fail
	require.NoError(t, db.Close())

	_, err = rodb.BeginTx(ctx, nil)
	assert.Error(t, err)
}

// TestReadOnlyTx_PrepareContextError tests PrepareContext in transaction when error occurs
func TestReadOnlyTx_PrepareContextError(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)

	rodb := NewReadOnlyDB(db)

	tx, err := rodb.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Rollback to invalidate the transaction, then try to prepare
	require.NoError(t, tx.Rollback())

	// Now PrepareContext should fail because tx is no longer valid
	_, err = tx.PrepareContext(ctx, "SELECT * FROM test")
	assert.Error(t, err)
}

// countTestRows returns the current number of rows in the "test" table using
// the underlying *sql.DB so the count itself bypasses read-only protection.
func countTestRows(t *testing.T, rodb *ReadOnlyDB) int {
	t.Helper()
	var count int
	require.NoError(t, rodb.DB().QueryRowContext(context.Background(), "SELECT COUNT(*) FROM test").Scan(&count))
	return count
}

// TestReadOnlyDB_QueryRejectsWrites verifies that write statements routed
// through the Query* methods (e.g. DELETE ... RETURNING) are rejected and do
// not mutate data.
func TestReadOnlyDB_QueryRejectsWrites(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)
	before := countTestRows(t, rodb)

	t.Run("Query with DELETE ... RETURNING", func(t *testing.T) {
		_, err := rodb.Query("DELETE FROM test WHERE name = 'Alice' RETURNING name") //nolint:rowserrcheck,sqlclosecheck // write is rejected, so no rows are returned
		assert.True(t, errors.Is(err, ErrReadOnly), "expected ErrReadOnly, got %v", err)
	})

	t.Run("QueryContext with DELETE ... RETURNING", func(t *testing.T) {
		_, err := rodb.QueryContext(ctx, "DELETE FROM test WHERE name = 'Bob' RETURNING name") //nolint:rowserrcheck,sqlclosecheck // write is rejected, so no rows are returned
		assert.True(t, errors.Is(err, ErrReadOnly), "expected ErrReadOnly, got %v", err)
	})

	t.Run("QueryRow with DELETE ... RETURNING", func(t *testing.T) {
		var name string
		err := rodb.QueryRow("DELETE FROM test WHERE name = 'Alice' RETURNING name").Scan(&name)
		assert.Error(t, err, "QueryRow must surface an error for write statements")
	})

	// No write may have slipped through.
	assert.Equal(t, before, countTestRows(t, rodb), "read-only DB must not mutate data")
}

// TestReadOnlyDB_ExecRejectsObfuscatedWrites verifies that writes hidden behind
// comments or CTEs are still rejected.
func TestReadOnlyDB_ExecRejectsObfuscatedWrites(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)
	before := countTestRows(t, rodb)

	cases := []string{
		`/*x*/ DELETE FROM test WHERE name = 'Alice'`,
		"-- comment\nDELETE FROM test WHERE name = 'Alice'",
		`WITH cte AS (SELECT 1) DELETE FROM test`,
	}
	for _, query := range cases {
		t.Run(query, func(t *testing.T) {
			_, err := rodb.Exec(query)
			assert.True(t, errors.Is(err, ErrReadOnly), "expected ErrReadOnly, got %v", err)
		})
	}

	assert.Equal(t, before, countTestRows(t, rodb), "read-only DB must not mutate data")
}

// TestReadOnlyTx_QueryRejectsWrites verifies the transaction Query* paths reject
// write statements too.
func TestReadOnlyTx_QueryRejectsWrites(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)
	before := countTestRows(t, rodb)

	tx, err := rodb.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	_, err = tx.Query("DELETE FROM test WHERE name = 'Alice' RETURNING name") //nolint:rowserrcheck,sqlclosecheck // write is rejected, so no rows are returned
	assert.True(t, errors.Is(err, ErrReadOnly), "expected ErrReadOnly, got %v", err)

	_, err = tx.QueryContext(ctx, "WITH cte AS (SELECT 1) DELETE FROM test") //nolint:rowserrcheck,sqlclosecheck // write is rejected, so no rows are returned
	assert.True(t, errors.Is(err, ErrReadOnly), "expected ErrReadOnly, got %v", err)

	require.NoError(t, tx.Rollback())
	assert.Equal(t, before, countTestRows(t, rodb), "read-only Tx must not mutate data")
}

// TestReadOnlyDB_PrepareRejectsObfuscatedAndSQLiteWrites verifies that prepared
// statements cannot be created for write statements, including DML with
// RETURNING and SQLite-specific mutators, so the ReadOnlyStmt query paths can
// never run a write.
func TestReadOnlyDB_PrepareRejectsObfuscatedAndSQLiteWrites(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	cases := []string{
		"DELETE FROM test WHERE name = 'Alice' RETURNING name",
		"WITH cte AS (SELECT 1) DELETE FROM test",
		"VACUUM",
		"PRAGMA foreign_keys = ON",
		"ATTACH DATABASE 'other.db' AS other",
	}
	for _, query := range cases {
		t.Run(query, func(t *testing.T) {
			_, err := rodb.PrepareContext(ctx, query)
			assert.True(t, errors.Is(err, ErrReadOnly), "expected ErrReadOnly, got %v", err)
		})
	}
}

// TestReadOnlyDB_ExecRejectsSQLiteMutators verifies SQLite statements that
// mutate state without a DML verb are rejected by Exec.
func TestReadOnlyDB_ExecRejectsSQLiteMutators(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)

	for _, query := range []string{"VACUUM", "REINDEX", "ANALYZE", "PRAGMA journal_mode = WAL"} {
		t.Run(query, func(t *testing.T) {
			_, err := rodb.Exec(query)
			assert.True(t, errors.Is(err, ErrReadOnly), "expected ErrReadOnly, got %v", err)
		})
	}

	// A reading PRAGMA is still allowed.
	_, err = rodb.Exec("PRAGMA table_info(test)")
	require.NoError(t, err)
}

// TestReadOnlyStmt_QueryRowGuard verifies the defensive guard on the prepared
// statement QueryRow path: a (synthetic) write statement surfaces an error
// through Scan rather than executing.
func TestReadOnlyStmt_QueryRowGuard(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	rodb := NewReadOnlyDB(db)
	before := countTestRows(t, rodb)

	// PrepareContext blocks writes, so build a read statement and flip the
	// internal flag to exercise the defense-in-depth guard directly.
	stmt, err := rodb.PrepareContext(ctx, "SELECT name FROM test")
	require.NoError(t, err)
	defer stmt.Close()
	stmt.isWrite = true

	var name string
	err = stmt.QueryRowContext(ctx).Scan(&name)
	assert.Error(t, err, "QueryRow must surface an error when the statement is a write")

	_, err = stmt.QueryContext(ctx) //nolint:rowserrcheck,sqlclosecheck // rejected before any rows are produced
	assert.True(t, errors.Is(err, ErrReadOnly), "expected ErrReadOnly, got %v", err)

	assert.Equal(t, before, countTestRows(t, rodb), "guarded statement must not mutate data")
}
