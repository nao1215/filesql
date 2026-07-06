package filesql

import (
	"context"
	"os"
	"path/filepath"
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
