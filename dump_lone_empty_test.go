package filesql

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDumpLoneEmptyField pins that a row whose only column is empty survives a
// dump and a reload.
//
// In CSV a record of one empty field, written plainly, is a blank line, and a
// blank line is not a record — a reader skips it. A one-column table of five
// rows, two of them empty, came back with three: the rows were gone and the dump
// reported success. Quoting the field says "one field, empty" and cannot be read
// as anything else. A record of several columns is unaffected, because the
// delimiters already say how many fields there are.
//
// TSV has no quoting to say that with, so the two halves agree the other way:
// the blank line is the value, and the reader takes it back as that column's
// empty field.
func TestDumpLoneEmptyField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format OutputFormat
		want   string
	}{
		{name: "csv", format: OutputFormatCSV, want: "v\nalice\n\"\"\nbob\n"},
		{name: "tsv", format: OutputFormatTSV, want: "v\nalice\n\nbob\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openWithTable(t,
				"CREATE TABLE t (v TEXT)",
				"INSERT INTO t VALUES ('alice')",
				"INSERT INTO t VALUES ('')",
				"INSERT INTO t VALUES ('bob')")

			assert.Equal(t, tt.want, dumpToString(t, db, NewDumpOptions().WithFormat(tt.format)))
		})
	}

	t.Run("every row comes back, empty ones included", func(t *testing.T) {
		t.Parallel()

		stored := []string{"alice", "", "bob", "", "carol"}

		ctx := t.Context()
		src := filepath.Join(t.TempDir(), "seed.csv")
		require.NoError(t, os.WriteFile(src, []byte("a\n1\n"), 0o600))

		db, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.ExecContext(ctx, "DROP TABLE seed")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "CREATE TABLE names (v TEXT)")
		require.NoError(t, err)
		for _, v := range stored {
			_, err = db.ExecContext(ctx, "INSERT INTO names VALUES (?)", v)
			require.NoError(t, err)
		}

		outDir := t.TempDir()
		require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions()))

		reloaded, err := OpenContext(ctx, filepath.Join(outDir, "names.csv"))
		require.NoError(t, err)
		defer reloaded.Close()

		rows, err := reloaded.QueryContext(ctx, "SELECT v FROM names")
		require.NoError(t, err)
		defer rows.Close()
		got := make([]string, 0, len(stored))
		for rows.Next() {
			var v string
			require.NoError(t, rows.Scan(&v))
			got = append(got, v)
		}
		require.NoError(t, rows.Err())

		assert.Equal(t, stored, got)
	})

	t.Run("a multi-column row with every field empty is unaffected", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t,
			"CREATE TABLE t (a TEXT, b TEXT)",
			"INSERT INTO t VALUES ('', '')")

		assert.Equal(t, "a,b\n,\n", dumpToString(t, db, NewDumpOptions()))
	})

	t.Run("a lone NULL is written the same way, since neither format has a NULL", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t,
			"CREATE TABLE t (v TEXT)",
			"INSERT INTO t VALUES (NULL)")

		assert.Equal(t, "v\n\"\"\n", dumpToString(t, db, NewDumpOptions()))
	})
}
