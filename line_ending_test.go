package filesql

import (
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLineEnding_String pins the names of the terminators.
func TestLineEnding_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "lf", LineEndingLF.String())
	assert.Equal(t, "crlf", LineEndingCRLF.String())
	assert.Equal(t, "unknown", LineEnding(9).String())
}

// TestLineEnding_Terminator covers the bytes each value writes, including a
// value from outside the set: a save must still terminate its records, so an
// unknown one writes the default rather than nothing.
func TestLineEnding_Terminator(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "\n", LineEndingLF.terminator())
	assert.Equal(t, "\r\n", LineEndingCRLF.terminator())
	assert.Equal(t, "\n", LineEnding(9).terminator())
}

// TestCountLineEndings covers the rule that decides a file's terminator. The
// majority wins so that a file with one stray ending keeps the one the rest of
// its lines use — rewriting those lines is the loss this whole feature exists to
// prevent.
func TestCountLineEndings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		sample string
		want   LineEnding
	}{
		{"all CRLF", "id,v\r\n1,a\r\n2,b\r\n", LineEndingCRLF},
		{"all LF", "id,v\n1,a\n2,b\n", LineEndingLF},
		{"mostly CRLF", "id,v\r\n1,a\r\n2,b\n", LineEndingCRLF},
		{"mostly LF", "id,v\n1,a\n2,b\r\n", LineEndingLF},
		{"a tie keeps LF", "id,v\r\n1,a\n", LineEndingLF},
		{"no line ending at all", "id,v", LineEndingLF},
		{"nothing at all", "", LineEndingLF},
		{"a lone carriage return is not a terminator", "id,v\r1,a\n", LineEndingLF},
		{
			// The shape a spreadsheet export has: CRLF between records, and a value
			// carrying its own line breaks inside quotes.
			name:   "a quoted line break is not a terminator",
			sample: "id,note\r\n1,\"line\nline\nline\nline\"\r\n2,b\r\n",
			want:   LineEndingCRLF,
		},
		{
			name:   "a quoted CRLF does not make an LF file CRLF",
			sample: "id,note\n1,\"line\r\nline\r\nline\"\n2,b\n",
			want:   LineEndingLF,
		},
		{
			name:   "a doubled quote inside a field leaves the count alone",
			sample: "id,note\r\n1,\"say \"\"hi\"\"\"\r\n2,b\r\n",
			want:   LineEndingCRLF,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := countLineEndings(strings.NewReader(tt.sample), OutputFormatCSV)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestDetectLineEnding covers reading the terminator off a file, which is what
// an in-place save does before it rewrites one.
func TestDetectLineEnding(t *testing.T) {
	t.Parallel()

	t.Run("a plain file", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		crlf := filepath.Join(dir, "crlf.csv")
		lf := filepath.Join(dir, "lf.csv")
		require.NoError(t, os.WriteFile(crlf, []byte("id,v\r\n1,a\r\n"), 0o600))
		require.NoError(t, os.WriteFile(lf, []byte("id,v\n1,a\n"), 0o600))

		assert.Equal(t, LineEndingCRLF, detectLineEnding(crlf, OutputFormatCSV))
		assert.Equal(t, LineEndingLF, detectLineEnding(lf, OutputFormatCSV))
	})

	t.Run("a compressed file is read through its codec", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "crlf.csv.gz")
		file, err := os.Create(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		gz := gzip.NewWriter(file)
		_, err = gz.Write([]byte("id,v\r\n1,a\r\n"))
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		require.NoError(t, file.Close())

		assert.Equal(t, LineEndingCRLF, detectLineEnding(path, OutputFormatCSV),
			"the terminator is in the compressed bytes, so the codec has to be undone to see it")
	})

	t.Run("a file that cannot be read answers with the default", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, LineEndingLF, detectLineEnding(filepath.Join(t.TempDir(), "missing.csv"), OutputFormatCSV),
			"detection is on the destination's behalf and must not fail the save")
	})
}

// TestCountLineEndings_QuotesAreOnlyCSV pins that a quote is honored for CSV
// alone. TSV and LTSV define no quoting, so a quote there is data: honoring it
// would let one unmatched quote swallow every terminator after it.
func TestCountLineEndings_QuotesAreOnlyCSV(t *testing.T) {
	t.Parallel()

	// A single quote, then CRLF records. Read as CSV the quote opens a field that
	// never closes; read as TSV it is just a character in a value.
	const sample = "id\tnote\r\n1\ta \" quote\r\n2\tb\r\n"

	got, err := countLineEndings(strings.NewReader(sample), OutputFormatTSV)
	require.NoError(t, err)
	assert.Equal(t, LineEndingCRLF, got, "a quote is data in TSV")

	got, err = countLineEndings(strings.NewReader(sample), OutputFormatLTSV)
	require.NoError(t, err)
	assert.Equal(t, LineEndingCRLF, got, "a quote is data in LTSV")
}

// TestCountLineEndings_CountsPastTheFirstBuffer checks that the whole file
// decides, not its beginning. A file whose first part is CRLF and whose bulk is
// LF is an LF file, and stopping at a fixed prefix would answer the opposite and
// rewrite every one of those LF lines.
func TestCountLineEndings_CountsPastTheFirstBuffer(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	for range lineEndingReadSize {
		b.WriteString("a\r\n")
	}
	for range lineEndingReadSize * 2 {
		b.WriteString("a\n")
	}

	got, err := countLineEndings(strings.NewReader(b.String()), OutputFormatCSV)
	require.NoError(t, err)
	assert.Equal(t, LineEndingLF, got, "the majority over the file wins, not over its first buffer")
}

// TestCountLineEndings_PartialReadIsNotEvidence covers a stream that fails
// partway. Half a file does not say what the whole one uses, so the failure is
// reported and the caller falls back to the default rather than acting on the
// part it managed to read.
func TestCountLineEndings_PartialReadIsNotEvidence(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("disk went away")
	reader := io.MultiReader(
		strings.NewReader("id,v\r\n1,a\r\n"),
		iotest.ErrReader(wantErr),
	)

	_, err := countLineEndings(reader, OutputFormatCSV)
	assert.ErrorIs(t, err, wantErr)
}

// TestDetectLineEnding_UnreadableFileKeepsTheDefault is the same rule at the
// level a save uses: detection is an improvement on the destination's behalf and
// must never fail the save.
func TestDetectLineEnding_UnreadableFileKeepsTheDefault(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	truncated := filepath.Join(dir, "broken.csv.gz")
	// Bytes that are not a gzip member: the codec fails before any line is seen.
	require.NoError(t, os.WriteFile(truncated, []byte("id,v\r\n1,a\r\n"), 0o600))

	assert.Equal(t, LineEndingLF, detectLineEnding(truncated, OutputFormatCSV))
}

// TestQuotedLineBreakSurvivesSave pins that a line break inside a quoted field
// is data rather than a terminator, in both directions.
//
// It was rewritten to whatever the file's terminator is, so editing one row
// changed the contents of a multi-line cell in a row nobody touched: a CRLF file
// whose cell held "x\ny" came back holding "x\r\ny", and an LF file whose cell
// held "x\r\ny" came back holding "x\ny". The third case passed for the wrong
// reason — the read dropped the carriage return and the write put one back, so
// the two errors cancelled out whenever the cell's break matched the file's
// terminator.
func TestQuotedLineBreakSurvivesSave(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "CRLF file, cell holds LF",
			in:   "id,v\r\n1,\"x\ny\"\r\n2,b\r\n",
			want: "id,v\r\n1,\"x\ny\"\r\n2,edited\r\n",
		},
		{
			name: "LF file, cell holds CRLF",
			in:   "id,v\n1,\"x\r\ny\"\n2,b\n",
			want: "id,v\n1,\"x\r\ny\"\n2,edited\n",
		},
		{
			name: "CRLF file, cell holds CRLF",
			in:   "id,v\r\n1,\"x\r\ny\"\r\n2,b\r\n",
			want: "id,v\r\n1,\"x\r\ny\"\r\n2,edited\r\n",
		},
		{
			name: "LF file, cell holds LF",
			in:   "id,v\n1,\"x\ny\"\n2,b\n",
			want: "id,v\n1,\"x\ny\"\n2,edited\n",
		},
		{
			name: "cell holds a lone carriage return",
			in:   "id,v\n1,\"x\ry\"\n2,b\n",
			want: "id,v\n1,\"x\ry\"\n2,edited\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "a.csv")
			require.NoError(t, os.WriteFile(path, []byte(tt.in), 0o600))
			require.NoError(t, autoSaveOverwrite(t, []string{path}, "UPDATE a SET v='edited' WHERE id=2"))

			got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got), "only the edited row may differ from what was there")
		})
	}
}

// TestQuotedLineBreakSurvivesLoad is the read half on its own, so a fix to the
// writer alone cannot look like progress.
func TestQuotedLineBreakSurvivesLoad(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "a.csv")
	require.NoError(t, os.WriteFile(path, []byte("id,address\n1,\"line1\r\nline2\"\n"), 0o600))

	db, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	var got string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT address FROM a WHERE id=1`).Scan(&got))
	assert.Equal(t, "line1\r\nline2", got, "the bytes between the quotes are what the file holds")
}
