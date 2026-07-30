package filesql

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLTSVColumnOrderIsTheOrderTheLabelsAppear pins that an LTSV file's columns
// come out in the order its labels are written.
//
// They were collected into a map and then read back out of it, and Go randomizes
// map iteration, so the column order was drawn afresh on every load: the same
// file gave "id,name" one run and "name,id" the next. SELECT * answered in a
// different order each time, and a dump of an LTSV table wrote its columns in a
// different order each time, which made the file's own round-trip unstable.
//
// The load is repeated because one pass cannot tell a stable order from a lucky
// draw.
func TestLTSVColumnOrderIsTheOrderTheLabelsAppear(t *testing.T) {
	t.Parallel()

	const attempts = 20

	tests := []struct {
		name    string
		content string
		want    []string
	}{
		{
			name:    "one record",
			content: "id:1\tname:alice\tage:20\n",
			want:    []string{"id", "name", "age"},
		},
		{
			name:    "a label that only appears in a later record goes last",
			content: "id:1\tname:alice\nid:2\tname:bob\tnickname:bo\n",
			want:    []string{"id", "name", "nickname"},
		},
		{
			name:    "a record that lists its labels in another order does not reorder the columns",
			content: "id:1\tname:alice\nname:bob\tid:2\n",
			want:    []string{"id", "name"},
		},
		{
			name:    "labels that sort differently than they appear",
			content: "zebra:1\tapple:2\tmango:3\n",
			want:    []string{"zebra", "apple", "mango"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for range attempts {
				ctx := t.Context()
				src := filepath.Join(t.TempDir(), "logs.ltsv")
				require.NoError(t, os.WriteFile(src, []byte(tt.content), 0o600))

				db, err := OpenContext(ctx, src)
				require.NoError(t, err)

				rows, err := db.QueryContext(ctx, "SELECT * FROM logs")
				require.NoError(t, err)
				got, err := rows.Columns()
				require.NoError(t, err)
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				require.NoError(t, db.Close())

				require.Equal(t, tt.want, got)
			}
		})
	}
}

// TestLTSVColumnOrderSurvivesChunking pins the same order for a file large enough
// to be loaded in chunks, which is a second code path with its own copy of the
// header collection.
func TestLTSVColumnOrderSurvivesChunking(t *testing.T) {
	t.Parallel()

	const attempts = 10

	var sb strings.Builder
	for i := range 500 {
		sb.WriteString("id:")
		sb.WriteString(strings.Repeat("0", 1) + string(rune('0'+i%10)))
		sb.WriteString("\tname:alice\tage:20\n")
	}
	content := sb.String()

	for range attempts {
		ctx := t.Context()
		src := filepath.Join(t.TempDir(), "logs.ltsv")
		require.NoError(t, os.WriteFile(src, []byte(content), 0o600))

		validated, err := NewBuilder().AddPath(src).SetDefaultChunkSize(10).Build(ctx)
		require.NoError(t, err)
		db, err := validated.Open(ctx)
		require.NoError(t, err)

		rows, err := db.QueryContext(ctx, "SELECT * FROM logs")
		require.NoError(t, err)
		got, err := rows.Columns()
		require.NoError(t, err)
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		var count int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM logs").Scan(&count))
		require.NoError(t, db.Close())

		require.Equal(t, []string{"id", "name", "age"}, got)
		assert.Equal(t, 500, count)
	}
}
