package filesql

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
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
	assert.Equal(t, "\r", lineEndingCR.terminator())
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
		{"a lone carriage return ties with the one LF", "id,v\r1,a\n", LineEndingLF},
		{"all lone carriage returns", "id,v\r1,a\r2,b\r", lineEndingCR},
		{"mostly lone carriage returns", "id,v\r1,a\r2,b\n", lineEndingCR},
		{"CRLF outnumbers lone carriage returns", "id,v\r\n1,a\r\n2,b\r", LineEndingCRLF},
		{"a quoted lone carriage return is not a terminator", "id,v\n1,\"a\rb\"\n", LineEndingLF},
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
// the two errors canceled out whenever the cell's break matched the file's
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

	db, err := Open(context.Background(), path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	var got string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT address FROM a WHERE id=1`).Scan(&got))
	assert.Equal(t, "line1\r\nline2", got, "the bytes between the quotes are what the file holds")
}

// TestCarriageReturnTerminatedFileIgnoresQuotesWhereTheFormatHasNone pins that
// only CSV has a quote that changes where a record ends.
//
// A file whose lines end with a lone carriage return is read as lines, and
// which carriage returns are terminators was decided with CSV's quoting for
// every format. TSV and LTSV have none -- this module says so in the TSV
// reader, in the LTSV reader and in countLineEndings below -- so a single `"`
// in a value made every carriage return after it data: a three-row one-column
// file loaded as one row holding the other two, with no error, and a wider one
// failed about a column count that named neither the quote nor the line ending.
func TestCarriageReturnTerminatedFileIgnoresQuotesWhereTheFormatHasNone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		file string
		body string
		// column is the one to read the rows out of.
		column string
		want   []string
	}{
		{
			name: "tsv without a quote", file: "t.tsv",
			body: "v\r1\r2\r3\r", column: "v", want: []string{"1", "2", "3"},
		},
		{
			name: "tsv with one quote in a value", file: "t.tsv",
			body: "v\r1\"\r2\r3\r", column: "v", want: []string{`1"`, "2", "3"},
		},
		{
			name: "tsv with two quotes in two values", file: "t.tsv",
			body: "v\r1\"\r2\"\r3\r", column: "v", want: []string{`1"`, `2"`, "3"},
		},
		{
			name: "tsv with three quotes in one value", file: "t.tsv",
			body: "v\r1\r\"\"\"\r3\r", column: "v", want: []string{"1", `"""`, "3"},
		},
		{
			name: "ltsv with one quote in a value", file: "t.ltsv",
			body: "a:1\ra:\"\ra:3\r", column: "a", want: []string{"1", `"`, "3"},
		},
		{
			// CSV is the format the quote belongs to: a carriage return inside a
			// quoted field is data and stays one.
			name: "csv keeps a carriage return inside a quoted field", file: "t.csv",
			body: "v\r1\r\"a\rb\"\r3\r", column: "v", want: []string{"1", "a\rb", "3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tt.file)
			require.NoError(t, os.WriteFile(path, []byte(tt.body), 0o600))

			db, err := Open(t.Context(), path)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			query := fmt.Sprintf(`SELECT %q FROM t`, tt.column) //nolint:gosec // The column name is this test's own literal.
			rows, err := db.QueryContext(t.Context(), query)
			require.NoError(t, err)
			t.Cleanup(func() { _ = rows.Close() })

			var got []string
			for rows.Next() {
				var value string
				require.NoError(t, rows.Scan(&value))
				got = append(got, value)
			}
			require.NoError(t, rows.Err())
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestTheTwoLineEndingReadingsAgree holds the read side against the save side.
// One decides how to split a file into records and the other decides what
// terminator to write back over it, so a file the loader read as
// carriage-return terminated has to be one the save writes carriage returns to.
func TestTheTwoLineEndingReadingsAgree(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		file   string
		format OutputFormat
		body   string
	}{
		{name: "csv", file: "t.csv", format: OutputFormatCSV, body: "v\r1\r2\r"},
		{name: "csv with a quote", file: "t.csv", format: OutputFormatCSV, body: "v\r1\r\"a\rb\"\r"},
		{name: "tsv", file: "t.tsv", format: OutputFormatTSV, body: "v\r1\r2\r"},
		{name: "tsv with a quote", file: "t.tsv", format: OutputFormatTSV, body: "v\r1\"\r2\r"},
		{name: "ltsv with a quote", file: "t.ltsv", format: OutputFormatLTSV, body: "a:1\ra:\"\r"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tt.file)
			require.NoError(t, os.WriteFile(path, []byte(tt.body), 0o600))

			assert.Equal(t, lineEndingCR, detectLineEnding(path, tt.format),
				"the save side reads this file as carriage-return terminated")

			db, err := Open(t.Context(), path)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			var rows int
			require.NoError(t, db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM t`).Scan(&rows))
			assert.Equal(t, 2, rows, "and the read side split it into the records that terminator names")
		})
	}
}
