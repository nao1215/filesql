package filesql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIsIntegerRejectsZeroPadded guards zero-padded codes such as ZIP codes and
// product IDs: an integer literal with a redundant leading zero must not be
// classified as an integer, because SQLite INTEGER would drop the leading zero
// (for example "02134" -> 2134). A lone "0" is a normal integer.
func TestIsIntegerRejectsZeroPadded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{"plain integer", "123", true},
		{"lone zero stays integer", "0", true},
		{"negative integer", "-42", true},
		{"zero-padded code is not an integer", "007", false},
		{"zip code is not an integer", "02134", false},
		{"double zero is not an integer", "00", false},
		{"signed zero-padded is not an integer", "-01", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, isInteger(tt.value))
		})
	}
}

// TestIsFloatRejectsZeroPadded ensures a zero-padded integer literal does not
// slip through to REAL either (float64 would render "007" as 7 too). A genuine
// decimal keeps its float classification.
func TestIsFloatRejectsZeroPadded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{"zero-padded code is not a float", "007", false},
		{"zip code is not a float", "02134", false},
		{"decimal stays float", "0.5", true},
		{"plain integer stays float-parseable", "42", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, isFloat(tt.value))
		})
	}
}

// TestClassifyValueZeroPadded verifies a zero-padded code is classified as TEXT
// while ordinary numbers keep their numeric types.
func TestClassifyValueZeroPadded(t *testing.T) {
	t.Parallel()

	require.Equal(t, columnTypeText, classifyValue("02134"))
	require.Equal(t, columnTypeText, classifyValue("007"))
	require.Equal(t, columnTypeInteger, classifyValue("0"))
	require.Equal(t, columnTypeInteger, classifyValue("42"))
	require.Equal(t, columnTypeReal, classifyValue("0.5"))
}

// TestInferColumnTypeZeroPadded verifies a column entirely made of zero-padded
// codes is inferred as TEXT, so the leading zeros survive.
func TestInferColumnTypeZeroPadded(t *testing.T) {
	t.Parallel()

	got := inferColumnType([]string{"02134", "00501", "10001"})
	require.Equal(t, columnTypeText, got)
}

// TestInferColumnTypePreservesLateZeroPadded covers a value the sampler would
// skip. Inference samples at most maxSampleSize values per column, and the
// guards that keep a zero-padded code out of an INTEGER column only run on what
// is sampled — so whether a code survived depended on where in the file it sat.
func TestInferColumnTypePreservesLateZeroPadded(t *testing.T) {
	t.Parallel()

	values := make([]string, 0, maxSampleSize*2)
	for i := range maxSampleSize * 2 {
		values = append(values, strconv.Itoa(i+1))
	}
	values[len(values)-1] = "007"

	require.Equal(t, columnTypeText, inferColumnType(values))
}

// TestOpenContextPreservesZeroPaddedCodesPastTheFirstChunk is the end-to-end
// half of the same rule, across the boundary that decides the schema. Types are
// inferred from the first chunk alone, so a code arriving in a later one met a
// column that was already INTEGER and was rewritten by SQLite's affinity: 007
// came back as 7, at no error and no warning.
func TestOpenContextPreservesZeroPaddedCodesPastTheFirstChunk(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("code\n")
	for i := range DefaultRowsPerChunk * 2 {
		fmt.Fprintf(&b, "%d\n", i+1)
	}
	b.WriteString("007\n")

	path := filepath.Join(t.TempDir(), "codes.csv")
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0600))

	ctx := context.Background()
	db, err := OpenContext(ctx, path)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var got string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT code FROM codes WHERE code LIKE '0%'`).Scan(&got))
	require.Equal(t, "007", got)

	// The rows that arrived before the promotion keep their own values: a plain
	// integer's text form is the digits it was read from.
	var first string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT code FROM codes LIMIT 1`).Scan(&first))
	require.Equal(t, "1", first)

	var rows int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM codes`).Scan(&rows))
	require.Equal(t, DefaultRowsPerChunk*2+1, rows)
}

// TestOpenContextPreservesZeroPaddedCodes is the end-to-end regression test: a
// column of zero-padded codes must round-trip through the loaded database as its
// exact textual value, not an integer with the leading zeros stripped.
func TestOpenContextPreservesZeroPaddedCodes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "zips.csv")
	content := "zip\n02134\n00501\n"
	require.NoError(t, os.WriteFile(csvPath, []byte(content), 0600))

	ctx := context.Background()
	db, err := OpenContext(ctx, csvPath)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var got string
	err = db.QueryRowContext(ctx, `SELECT zip FROM zips ORDER BY zip LIMIT 1`).Scan(&got)
	require.NoError(t, err)
	require.Equal(t, "00501", got)
}

// TestSurroundingWhitespaceKeepsAValueText covers the other way a numeric column
// rewrites what it was given. SQLite's affinity converts " 5 " to 5, so the
// spaces the file quoted were gone, while the text column beside it kept its
// own: the same input was preserved or altered depending on what it looked like.
//
// A value with no surrounding whitespace is unaffected, and whitespace around
// something that is not a number was already text.
func TestSurroundingWhitespaceKeepsAValueText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "a padded integer", value: " 5 ", want: true},
		{name: "a leading-space integer", value: "  42", want: true},
		{name: "a trailing-space integer", value: "42 ", want: true},
		{name: "a padded real", value: " 1.5 ", want: true},
		{name: "a padded negative", value: " -7 ", want: true},
		{name: "a plain integer", value: "5", want: false},
		{name: "a plain real", value: "1.5", want: false},
		{name: "padded text", value: " ab ", want: false},
		{name: "whitespace only", value: "   ", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := mustStayText(tt.value); got != tt.want {
				t.Errorf("mustStayText(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

// TestQuotedWhitespaceSurvivesTheLoad is the same rule seen through a load: both
// columns keep the bytes the file quoted, which is what the quotes said.
func TestQuotedWhitespaceSurvivesTheLoad(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "padded.csv")
	if err := os.WriteFile(path, []byte("num,txt\n\" 5 \",\" ab \"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	db, err := OpenContext(ctx, path)
	if err != nil {
		t.Fatalf("OpenContext: %v", err)
	}
	defer func() { _ = db.Close() }()

	var num, txt string
	if err := db.QueryRowContext(ctx, "SELECT num, txt FROM padded").Scan(&num, &txt); err != nil {
		t.Fatalf("query: %v", err)
	}
	if num != " 5 " {
		t.Errorf("num = %q, want %q: the quotes made the spaces part of the value", num, " 5 ")
	}
	if txt != " ab " {
		t.Errorf("txt = %q, want %q", txt, " ab ")
	}
}
