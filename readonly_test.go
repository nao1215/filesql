package filesql

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openReadOnlyUsers is a two-row users table opened read-only, which is what
// every case below starts from.
func openReadOnlyUsers(t *testing.T) *sql.DB {
	t.Helper()

	validated, err := NewBuilder().
		AddReader(strings.NewReader("id,name\n1,Alice\n2,Bob\n"), "users", FileTypeCSV).
		Build(context.Background())
	require.NoError(t, err)

	db, err := validated.OpenReadOnly(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// countUsers is the read every case performs to show the data is still there.
func countUsers(t *testing.T, db *sql.DB) int {
	t.Helper()

	var rows int
	require.NoError(t, db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&rows))
	return rows
}

// TestOpenReadOnly_ReadsWork covers the half of read-only mode that has to keep
// working: a query, through each of the entry points a caller reaches for.
func TestOpenReadOnly_ReadsWork(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openReadOnlyUsers(t)

	assert.Equal(t, 2, countUsers(t, db))

	rows, err := db.QueryContext(ctx, `SELECT name FROM users ORDER BY id`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	names := make([]string, 0, 2)
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"Alice", "Bob"}, names)

	stmt, err := db.PrepareContext(ctx, `SELECT name FROM users WHERE id = ?`)
	require.NoError(t, err)
	defer func() { _ = stmt.Close() }()

	var name string
	require.NoError(t, stmt.QueryRowContext(ctx, 2).Scan(&name))
	assert.Equal(t, "Bob", name)
}

// TestOpenReadOnly_WritesAreRefused covers the other half. The refusal comes
// from SQLite rather than from this package, so the cases assert that the
// statement failed and changed nothing, not the wording of the error.
func TestOpenReadOnly_WritesAreRefused(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	for _, query := range []string{
		`INSERT INTO users VALUES (3, 'Cora')`,
		`UPDATE users SET name = 'nobody'`,
		`DELETE FROM users`,
		`DROP TABLE users`,
		`CREATE TABLE extra (id INTEGER)`,
		`ALTER TABLE users ADD COLUMN age INTEGER`,
	} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()

			db := openReadOnlyUsers(t)

			_, err := db.ExecContext(ctx, query)
			require.Error(t, err, "a write must not succeed on a read-only database")
			assert.Equal(t, 2, countUsers(t, db), "the table has to be as it was")
		})
	}
}

// TestOpenReadOnly_WritesThroughEveryEntryPoint covers the paths that inspecting
// the SQL text used to have to cover one at a time. The pragma is on the
// connection, so a write is refused whichever way it is issued -- including the
// ones that carry no error of their own until the row is scanned.
func TestOpenReadOnly_WritesThroughEveryEntryPoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("through Query rather than Exec", func(t *testing.T) {
		t.Parallel()

		db := openReadOnlyUsers(t)

		rows, err := db.QueryContext(ctx, `DELETE FROM users RETURNING id`)
		if err == nil {
			err = rows.Err()
			_ = rows.Close()
		}
		require.Error(t, err)
		assert.Equal(t, 2, countUsers(t, db))
	})

	t.Run("through QueryRow", func(t *testing.T) {
		t.Parallel()

		db := openReadOnlyUsers(t)

		var id int
		err := db.QueryRowContext(ctx, `DELETE FROM users RETURNING id`).Scan(&id)
		require.Error(t, err)
		assert.Equal(t, 2, countUsers(t, db))
	})

	t.Run("through a prepared statement", func(t *testing.T) {
		t.Parallel()

		db := openReadOnlyUsers(t)

		stmt, err := db.PrepareContext(ctx, `INSERT INTO users VALUES (?, ?)`)
		if err == nil {
			defer func() { _ = stmt.Close() }()
			_, err = stmt.ExecContext(ctx, 3, "Cora")
		}
		require.Error(t, err)
		assert.Equal(t, 2, countUsers(t, db))
	})

	t.Run("inside a transaction", func(t *testing.T) {
		t.Parallel()

		db := openReadOnlyUsers(t)

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		_, err = tx.ExecContext(ctx, `DELETE FROM users`)
		require.Error(t, err)
		assert.Equal(t, 2, countUsers(t, db))
	})
}

// TestOpenReadOnly_LeavesOpenWritable pins that read-only is a property of the
// handle rather than of the data: the same builder opened with Open still
// writes.
func TestOpenReadOnly_LeavesOpenWritable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	validated, err := NewBuilder().
		AddReader(strings.NewReader("id,name\n1,Alice\n"), "users", FileTypeCSV).
		Build(ctx)
	require.NoError(t, err)

	db, err := validated.Open(ctx)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.ExecContext(ctx, `INSERT INTO users VALUES (2, 'Bob')`)
	require.NoError(t, err)

	var rows int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&rows))
	assert.Equal(t, 2, rows)
}

// TestOpenReadOnly_ClosingOrder covers the lifetime of the shared-cache
// in-memory database the read-only handle is opened against. Its data lives
// only as long as some connection to it is open, so a read-only database has to
// keep working after the loader database it was swapped in for is gone, and
// closing it twice has to be harmless.
func TestOpenReadOnly_ClosingOrder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	validated, err := NewBuilder().
		AddReader(strings.NewReader("id,name\n1,Alice\n2,Bob\n"), "users", FileTypeCSV).
		Build(ctx)
	require.NoError(t, err)

	db, err := validated.OpenReadOnly(ctx)
	require.NoError(t, err)

	assert.Equal(t, 2, countUsers(t, db))
	require.NoError(t, db.Close())
	assert.Error(t, db.PingContext(ctx), "a closed database is closed")
	assert.NoError(t, db.Close(), "closing twice is harmless")
}

// TestOpenReadOnly_WithAutoSave covers the combination that has a close of its
// own to perform. Auto-save writes what was loaded back out, which for a
// read-only handle is the data unchanged, and the write to the file happens even
// though writes to the database do not.
func TestOpenReadOnly_WithAutoSave(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	outDir := t.TempDir()

	validated, err := NewBuilder().
		AddPath("testdata/sample.csv").
		EnableAutoSave(outDir).
		Build(ctx)
	require.NoError(t, err)

	db, err := validated.OpenReadOnly(ctx)
	require.NoError(t, err)

	var rows int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sample`).Scan(&rows))
	assert.Equal(t, 3, rows)

	_, err = db.ExecContext(ctx, `DELETE FROM sample`)
	require.Error(t, err, "auto-save must not make the handle writable")

	require.NoError(t, db.Close())
	assert.FileExists(t, filepath.Join(outDir, "sample.csv"))
}

// TestSetupReadOnlyIfNeeded_Failures covers the swap's error paths. Each one
// has to close the loader database it was handed rather than leaving the
// shared-cache database alive with nobody holding it.
func TestSetupReadOnlyIfNeeded_Failures(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("a writable open is left alone", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		got, err := (&DBBuilder{}).setupReadOnlyIfNeeded(ctx, db, false)

		require.NoError(t, err)
		assert.Same(t, db, got)
	})

	t.Run("no in-memory DSN to reopen", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		_, err := (&DBBuilder{}).setupReadOnlyIfNeeded(ctx, db, true)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("a DSN the driver cannot connect to", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		b := &DBBuilder{memDSN: "file:/nonexistent-directory/db.sqlite?mode=rw"}
		_, err := b.setupReadOnlyIfNeeded(ctx, db, true)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})
}

// TestHandleDSN pins the one string the read-only handle is built from. The
// pragma has to be appended to a DSN that already carries a query string, and a
// writable handle has to be handed the loader's DSN unchanged.
func TestHandleDSN(t *testing.T) {
	t.Parallel()

	b := &DBBuilder{memDSN: "file:filesql_mem_x?mode=memory&cache=shared"}

	assert.Equal(t, "file:filesql_mem_x?mode=memory&cache=shared", b.handleDSN(false))
	assert.Equal(t, "file:filesql_mem_x?mode=memory&cache=shared&_pragma=query_only(1)", b.handleDSN(true))
}
