package filesql

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// achFixture returns the bytes of a small ACH file that parses.
func achFixture(t *testing.T) []byte {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "ppd-debit.ach"))
	require.NoError(t, err)
	return data
}

// wireFixture returns the bytes of a small Fedwire file that parses.
func wireFixture(t *testing.T) []byte {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "customer-transfer.fed"))
	require.NoError(t, err)
	return data
}

// TestStreamWriteBackFormatsToDatabase covers the two loaders that build tables
// from a whole file at once. They share a shape — validate the name, parse,
// then create and fill one table per section — so the refusals are checked for
// both.
func TestStreamWriteBackFormatsToDatabase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name string
		ext  string
		load func(ctx context.Context, db dbtx, content []byte, filePath string, replaceExisting bool) error
		data func(t *testing.T) []byte
	}{
		{
			name: "ACH file",
			ext:  extACH,
			data: achFixture,
			load: func(ctx context.Context, db dbtx, content []byte, filePath string, replaceExisting bool) error {
				return streamACHFileToDatabase(ctx, db, strings.NewReader(string(content)), filePath, "", replaceExisting)
			},
		},
		{
			name: "Fedwire",
			ext:  extFED,
			data: wireFixture,
			load: func(ctx context.Context, db dbtx, content []byte, filePath string, replaceExisting bool) error {
				return streamWireFileToDatabase(ctx, db, strings.NewReader(string(content)), filePath, "", replaceExisting)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" refuses a reserved table name", func(t *testing.T) {
			t.Parallel()

			err := tt.load(ctx, openTestDB(t), tt.data(t), sourceTablePrefix+"payment"+tt.ext, false)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrReservedTableName)
		})

		t.Run(tt.name+" reports content it cannot parse", func(t *testing.T) {
			t.Parallel()

			err := tt.load(ctx, openTestDB(t), []byte("this is not a payment file"), "payment"+tt.ext, false)
			assert.Error(t, err)
		})

		t.Run(tt.name+" reports a database it cannot query", func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			require.NoError(t, db.Close())

			err := tt.load(ctx, db, tt.data(t), "payment"+tt.ext, false)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrDatabaseOperation)
		})

		t.Run(tt.name+" refuses to load twice over its own tables", func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			content := tt.data(t)
			require.NoError(t, tt.load(ctx, db, content, "payment"+tt.ext, false))

			err := tt.load(ctx, db, content, "payment"+tt.ext, false)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrDuplicateTable)
		})

		t.Run(tt.name+" replaces its own tables when asked", func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			content := tt.data(t)
			require.NoError(t, tt.load(ctx, db, content, "payment"+tt.ext, false))

			assert.NoError(t, tt.load(ctx, db, content, "payment"+tt.ext, true),
				"a reload in replace mode drops the tables it is about to rebuild")
		})
	}
}

// TestParseACHFile covers the parse step on its own, which is what turns file
// bytes into tables and into the structure a later dump rebuilds the file from.
func TestParseACHFile(t *testing.T) {
	t.Parallel()

	t.Run("returns the tables and the structure behind them", func(t *testing.T) {
		t.Parallel()

		tables, tableSet, err := parseACHFile(strings.NewReader(string(achFixture(t))), "payment")
		require.NoError(t, err)
		require.NotNil(t, tableSet, "a dump needs the structure the tables came from")
		assert.NotEmpty(t, tables)
		assert.NotNil(t, tableSet.GetFileHeaderTable(), "the file header is what an ACH file starts with")
	})

	t.Run("reports content that is not an ACH file", func(t *testing.T) {
		t.Parallel()

		tables, tableSet, err := parseACHFile(strings.NewReader("this is not an ACH file"), "payment")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrACH)
		assert.Nil(t, tables)
		assert.Nil(t, tableSet)
	})
}

// TestParseFedWireFile is the same for Fedwire, which is one message table.
func TestParseFedWireFile(t *testing.T) {
	t.Parallel()

	t.Run("returns the message table and the structure behind it", func(t *testing.T) {
		t.Parallel()

		tables, tableSet, err := parseFedWireFile(strings.NewReader(string(wireFixture(t))), "payment")
		require.NoError(t, err)
		require.NotNil(t, tableSet)
		require.Len(t, tables, 1, "a Fedwire file holds one message")
		assert.Equal(t, "payment_message", tables[0].getName())
		assert.NotNil(t, tableSet.GetMessageTable())
	})

	t.Run("reports content that is not a Fedwire file", func(t *testing.T) {
		t.Parallel()

		tables, tableSet, err := parseFedWireFile(strings.NewReader("this is not a Fedwire file"), "payment")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrWire)
		assert.Nil(t, tables)
		assert.Nil(t, tableSet)
	})
}

// TestDumpWithTableSet_NilTableSet covers the argument neither dump can work
// without: the file is rebuilt from the structure, so there is nothing to write
// without one.
func TestDumpWithTableSet_NilTableSet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	out := filepath.Join(t.TempDir(), "payment")

	assert.ErrorIs(t, DumpACHWithTableSet(ctx, db, "payment", out+extACH, nil), ErrNilInput)
	assert.ErrorIs(t, DumpFedWireWithTableSet(ctx, db, "payment", out+extFED, nil), ErrNilInput)
}

// TestInsertRecordsIntoTable_Failures covers the insert step used by the
// write-back loaders.
func TestInsertRecordsIntoTable_Failures(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("the statement cannot be prepared", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, db.Close())

		err := insertRecordsIntoTable(ctx, db, "users", newHeader([]string{"id"}), []record{newRecord([]string{"1"})})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("a row the table refuses", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		_, err := db.ExecContext(ctx, `CREATE TABLE users (id TEXT CHECK (id <> 'refused'))`)
		require.NoError(t, err)

		err = insertRecordsIntoTable(ctx, db, "users", newHeader([]string{"id"}), []record{newRecord([]string{"refused"})})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})
}

// TestReadTableToTableData covers the read-back a dump starts from.
func TestReadTableToTableData(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("reads rows and turns a NULL into an empty value", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		_, err := db.ExecContext(ctx, `CREATE TABLE users (id TEXT, name TEXT)`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO users VALUES ('1', NULL)`)
		require.NoError(t, err)

		data, err := readTableToTableData(ctx, db, "users")
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name"}, data.Headers)
		require.Len(t, data.Records, 1)
		assert.Equal(t, []string{"1", ""}, data.Records[0], "a NULL has no text of its own to write back")
	})

	t.Run("reports a table that is not there", func(t *testing.T) {
		t.Parallel()

		_, err := readTableToTableData(ctx, openTestDB(t), "missing")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTableNotFound)
	})
}
