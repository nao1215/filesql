package filesql

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// csvFixture writes a small CSV for a test and returns its path.
func csvFixture(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "users.csv")
	require.NoError(t, os.WriteFile(path, []byte("id,name\n1,Alice\n"), 0o600))
	return path
}

// canceledContext returns a context that is already done.
func canceledContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// builtBuilder returns a builder that has collected path, which is the state a
// load starts from.
func builtBuilder(t *testing.T, path string) *DBBuilder {
	t.Helper()

	builder, err := NewBuilder().AddPath(path).Build(context.Background())
	require.NoError(t, err)
	return builder
}

// TestBuilderEntryPoints_CanceledContext checks that each way of loading stops
// on a context that is already done, before it opens files or writes tables. A
// load that ignored cancellation would leave half the tables of an abandoned
// request behind.
func TestBuilderEntryPoints_CanceledContext(t *testing.T) {
	t.Parallel()

	path := csvFixture(t)

	t.Run("Open", func(t *testing.T) {
		t.Parallel()

		_, err := builtBuilder(t, path).Open(canceledContext(t))
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("LoadInto", func(t *testing.T) {
		t.Parallel()

		err := builtBuilder(t, path).LoadInto(canceledContext(t), openTestDB(t))
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("LoadIntoTx", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		assert.ErrorIs(t, builtBuilder(t, path).LoadIntoTx(canceledContext(t), tx), context.Canceled)
	})
}

// TestLoadIntoTx_Refusals covers what LoadIntoTx cannot do. The caller owns the
// transaction, so there is nothing for auto-save to attach its close to, and a
// nil transaction has to be named rather than panicking.
func TestLoadIntoTx_Refusals(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := csvFixture(t)

	t.Run("a nil transaction", func(t *testing.T) {
		t.Parallel()

		err := builtBuilder(t, path).LoadIntoTx(ctx, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("auto-save", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		err = builtBuilder(t, path).EnableAutoSave(t.TempDir()).LoadIntoTx(ctx, tx)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("no input at all", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		assert.Error(t, NewBuilder().LoadIntoTx(ctx, tx), "a builder with no input has nothing to load")
	})
}

// TestOpenReadOnly_PassesTheOpenFailureThrough checks that a read-only open
// does not swallow the failure of the load it performs.
func TestOpenReadOnly_PassesTheOpenFailureThrough(t *testing.T) {
	t.Parallel()

	rodb, err := NewBuilder().OpenReadOnly(context.Background())
	require.Error(t, err, "a builder with no input has nothing to open")
	assert.Nil(t, rodb)
}

// TestValidateDatabaseConnection covers the health check a load runs before it
// hands the database back.
func TestValidateDatabaseConnection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	builder := NewBuilder()

	t.Run("a working database passes", func(t *testing.T) {
		t.Parallel()
		assert.NoError(t, builder.validateDatabaseConnection(ctx, openTestDB(t)))
	})

	t.Run("a closed database is reported", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, db.Close())

		assert.Error(t, builder.validateDatabaseConnection(ctx, db))
	})
}
