package filesql

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLTSVValueKeepsSurroundingWhitespace pins that an LTSV value is read as the
// bytes between the colon and the field's end.
//
// The reader trimmed the value, so a cell with meaningful spaces around it came
// back without them, while CSV and TSV kept theirs and the LTSV writer wrote
// them: a dump and reload through LTSV lost them, and the same data loaded from
// two formats disagreed. LTSV defines a value as everything up to the next tab or
// newline and says nothing about trimming.
//
// The label is still trimmed. LTSV restricts a label to letters, digits,
// underscore, dot, and hyphen, so a space around one is already malformed, and
// tolerating it costs nothing.
func TestLTSVValueKeepsSurroundingWhitespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "a space after the colon belongs to the value", content: "v: padded\n", want: " padded"},
		{name: "a trailing space belongs to the value", content: "v:padded \n", want: "padded "},
		{name: "spaces on both sides belong to the value", content: "v:  padded  \n", want: "  padded  "},
		{name: "a value of only spaces is not an empty value", content: "v:   \n", want: "   "},
		{name: "a value with no whitespace is unchanged", content: "v:padded\n", want: "padded"},
		{name: "an inner space was never at risk", content: "v:two words\n", want: "two words"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			src := filepath.Join(t.TempDir(), "t.ltsv")
			require.NoError(t, os.WriteFile(src, []byte(tt.content), 0o600))

			db, err := OpenContext(ctx, src)
			require.NoError(t, err)
			defer db.Close()

			var got string
			require.NoError(t, db.QueryRowContext(ctx, "SELECT v FROM t").Scan(&got))
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("a label keeps being trimmed, since a space in one is malformed", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		src := filepath.Join(t.TempDir(), "t.ltsv")
		require.NoError(t, os.WriteFile(src, []byte(" v :x\n"), 0o600))

		db, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer db.Close()

		rows, err := db.QueryContext(ctx, "SELECT * FROM t")
		require.NoError(t, err)
		defer rows.Close()
		cols, err := rows.Columns()
		require.NoError(t, err)
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"v"}, cols)
	})

	t.Run("a dump and reload through LTSV keeps the whitespace", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, "CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('  padded  ')")

		outDir := t.TempDir()
		require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions().WithFormat(OutputFormatLTSV)))

		reloaded, err := OpenContext(t.Context(), filepath.Join(outDir, "t.ltsv"))
		require.NoError(t, err)
		defer reloaded.Close()

		var got string
		require.NoError(t, reloaded.QueryRowContext(t.Context(), "SELECT v FROM t").Scan(&got))
		assert.Equal(t, "  padded  ", got)
	})

	t.Run("LTSV and CSV agree on the same value", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()

		ltsvPath := filepath.Join(dir, "a.ltsv")
		require.NoError(t, os.WriteFile(ltsvPath, []byte("v:  padded  \n"), 0o600))
		csvPath := filepath.Join(dir, "b.csv")
		require.NoError(t, os.WriteFile(csvPath, []byte("v\n\"  padded  \"\n"), 0o600))

		db, err := OpenContext(ctx, ltsvPath, csvPath)
		require.NoError(t, err)
		defer db.Close()

		var fromLTSV, fromCSV string
		require.NoError(t, db.QueryRowContext(ctx, "SELECT v FROM a").Scan(&fromLTSV))
		require.NoError(t, db.QueryRowContext(ctx, "SELECT v FROM b").Scan(&fromCSV))
		assert.Equal(t, fromCSV, fromLTSV)
	})
}
