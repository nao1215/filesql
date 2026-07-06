package filesql

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/unicode"
)

// TestOpenHonorsUnicodeBOM verifies that a leading Unicode byte-order mark on a
// text input is honored rather than leaking into the data: a UTF-8 BOM is
// stripped so the first column keeps its plain name, and UTF-16 input is
// transcoded to UTF-8. Tools such as Excel, Notepad, and PowerShell emit these
// encodings, so a user must be able to query the first column by its real name.
func TestOpenHonorsUnicodeBOM(t *testing.T) {
	t.Parallel()

	utf8BOM := []byte{0xEF, 0xBB, 0xBF}

	utf16LE := func(s string) []byte {
		enc := unicode.UTF16(unicode.LittleEndian, unicode.UseBOM)
		out, err := enc.NewEncoder().Bytes([]byte(s))
		require.NoError(t, err)
		return out
	}

	tests := []struct {
		name    string
		file    string
		content []byte
		query   string
		want    string
	}{
		{
			name:    "utf-8 BOM on CSV keeps first column name queryable",
			file:    "bom.csv",
			content: append(append([]byte{}, utf8BOM...), []byte("name,age\nalice,30\n")...),
			query:   "SELECT name FROM bom WHERE age = 30",
			want:    "alice",
		},
		{
			name:    "utf-8 BOM on TSV keeps first column name queryable",
			file:    "bom.tsv",
			content: append(append([]byte{}, utf8BOM...), []byte("name\tage\nalice\t30\n")...),
			query:   "SELECT name FROM bom WHERE age = 30",
			want:    "alice",
		},
		{
			name:    "utf-8 BOM on LTSV keeps first label queryable",
			file:    "bom.ltsv",
			content: append(append([]byte{}, utf8BOM...), []byte("name:alice\tage:30\n")...),
			query:   "SELECT name FROM bom WHERE age = 30",
			want:    "alice",
		},
		{
			// The streaming JSON reader stores each document in a "data" column, so
			// the contract this guards is that a BOM no longer aborts the parse.
			name:    "utf-8 BOM on JSON parses instead of failing",
			file:    "bom.json",
			content: append(append([]byte{}, utf8BOM...), []byte(`[{"name":"alice","age":30}]`)...),
			query:   "SELECT json_extract(data, '$.name') FROM bom",
			want:    "alice",
		},
		{
			name:    "utf-8 BOM on JSONL parses instead of failing",
			file:    "bom.jsonl",
			content: append(append([]byte{}, utf8BOM...), []byte("{\"name\":\"alice\",\"age\":30}\n")...),
			query:   "SELECT json_extract(data, '$.name') FROM bom",
			want:    "alice",
		},
		{
			name:    "utf-16 LE CSV is transcoded to UTF-8",
			file:    "utf16.csv",
			content: utf16LE("name,age\nalice,30\n"),
			query:   "SELECT name FROM utf16 WHERE age = 30",
			want:    "alice",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tt.file)
			require.NoError(t, os.WriteFile(path, tt.content, 0o600))

			db, err := OpenContext(context.Background(), path)
			require.NoError(t, err)
			defer db.Close()

			var got string
			require.NoError(t, db.QueryRowContext(context.Background(), tt.query).Scan(&got))
			assert.Equal(t, tt.want, got)
		})
	}
}
