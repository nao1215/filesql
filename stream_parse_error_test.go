package filesql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseFromReader_EmptyInput covers what each format's parser answers for an
// input with nothing in it. JSON and JSONL are excluded on purpose: an empty one
// is a valid zero-row table, which the loader turns into an empty table rather
// than a failure. XLSX is excluded because no bytes at all is not an empty
// workbook but an unreadable one, which the case below covers.
func TestParseFromReader_EmptyInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fileType FileType
	}{
		{"CSV", FileTypeCSV},
		{"TSV", FileTypeTSV},
		{"Parquet", FileTypeParquet},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parser := newStreamingParser(tt.fileType, CompressionNone, "empty", 100)
			_, err := parser.parseFromReader(strings.NewReader(""))
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrEmptyData)
		})
	}
}

// TestParseFromReader_UnparsableInput covers the binary formats given bytes that
// are not the format at all, which is what a mislabelled file looks like.
func TestParseFromReader_UnparsableInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fileType FileType
	}{
		{"Parquet", FileTypeParquet},
		{"XLSX", FileTypeXLSX},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parser := newStreamingParser(tt.fileType, CompressionNone, "wrong", 100)
			_, err := parser.parseFromReader(strings.NewReader("id,name\n1,Alice\n"))
			assert.Error(t, err, "bytes that are not the format must not load as a table")
		})
	}
}

// TestParseDelimitedStream_MalformedRowPolicies covers what a ragged row does
// under each policy. The counts matter as much as the outcome: a load that
// dropped rows reports how many, so a caller can tell a clean load from a lossy
// one.
func TestParseDelimitedStream_MalformedRowPolicies(t *testing.T) {
	t.Parallel()

	// The second row has one field too few and the third one too many.
	const content = "id,name,email\n1,Alice\n3,Carol,c@example.com,extra\n4,Dave,d@example.com\n"

	t.Run("stop refuses the file", func(t *testing.T) {
		t.Parallel()

		parser := newStreamingParser(FileTypeCSV, CompressionNone, "users", 100)
		parser.malformedRowPolicy = MalformedRowStop

		_, err := parser.parseFromReader(strings.NewReader(content))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrColumnMismatch)
	})

	t.Run("skip drops the ragged rows and counts them", func(t *testing.T) {
		t.Parallel()

		parser := newStreamingParser(FileTypeCSV, CompressionNone, "users", 100)
		parser.malformedRowPolicy = MalformedRowSkip

		table, err := parser.parseFromReader(strings.NewReader(content))
		require.NoError(t, err)
		assert.Len(t, table.getRecords(), 1, "only the well-formed row is kept")
		assert.Equal(t, 2, parser.skippedRows)
		assert.Equal(t, 3, parser.totalRows)
	})

	t.Run("fill pads a short row and still refuses a long one", func(t *testing.T) {
		t.Parallel()

		parser := newStreamingParser(FileTypeCSV, CompressionNone, "users", 100)
		parser.malformedRowPolicy = MalformedRowFill

		short := newStreamingParser(FileTypeCSV, CompressionNone, "users", 100)
		short.malformedRowPolicy = MalformedRowFill
		table, err := short.parseFromReader(strings.NewReader("id,name,email\n1,Alice\n"))
		require.NoError(t, err)
		require.Len(t, table.getRecords(), 1)
		assert.Equal(t, []string{"1", "Alice", ""}, []string(table.getRecords()[0]), "a missing field becomes an empty one")

		_, err = parser.parseFromReader(strings.NewReader(content))
		require.Error(t, err, "a row with more fields than the header would lose data if it were reshaped")
		assert.ErrorIs(t, err, ErrColumnMismatch)
	})
}

// TestMalformedRowPolicy_String pins the names the policies are configured by.
func TestMalformedRowPolicy_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "stop", MalformedRowStop.String())
	assert.Equal(t, "skip", MalformedRowSkip.String())
	assert.Equal(t, "fill", MalformedRowFill.String())
	assert.Equal(t, "MalformedRowPolicy(9)", MalformedRowPolicy(9).String())
}
