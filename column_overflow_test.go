package filesql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nao1215/filesql/internal/infer"
	"github.com/stretchr/testify/require"
)

// TestIsFloatRejectsInt64Overflow guards the core of nao1215/sqly#218: an
// integer literal whose magnitude exceeds int64 must not be classified as a
// float, because converting it to float64 loses precision and renders it in
// scientific notation. Such values fall through to TEXT instead.
func TestIsFloatRejectsInt64Overflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{"in-range integer is float-parseable", "123", true},
		{"max int64 is float-parseable", "9223372036854775807", true},
		{"overflow integer is not treated as float", "11040320260000000000", false},
		{"huge integer is not treated as float", "99999999999999999999999999", false},
		{"negative overflow integer is not treated as float", "-11040320260000000000", false},
		{"decimal stays float", "3.14", true},
		{"scientific notation literal stays float", "1.104032026e+19", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, infer.IsFloat(tt.value))
		})
	}
}

// TestClassifyValueInt64Overflow verifies that int64-overflowing integers are
// classified as TEXT, while in-range integers and genuine floats keep their
// numeric types.
func TestClassifyValueInt64Overflow(t *testing.T) {
	t.Parallel()

	require.Equal(t, columnTypeText, columnTypeOf(infer.Classify("11040320260000000000")))
	require.Equal(t, columnTypeText, columnTypeOf(infer.Classify("-11040320260000000000")))
	require.Equal(t, columnTypeInteger, columnTypeOf(infer.Classify("9223372036854775807")))
	require.Equal(t, columnTypeReal, columnTypeOf(infer.Classify("1.104032026e+19")))
}

// TestInferColumnTypeInt64Overflow verifies that a column entirely made of
// int64-overflowing integers is inferred as TEXT.
func TestInferColumnTypeInt64Overflow(t *testing.T) {
	t.Parallel()

	got := columnTypeOf(infer.Column([]string{
		"11040320260000000000",
		"11040320260000000001",
		"11040320260000000002",
	}))
	require.Equal(t, columnTypeText, got)
}

// TestOpenContextPreservesLargeIntegerPastTheFirstChunk is the same rule across
// the boundary that decides the schema. Column types come from the first chunk,
// so an account number past int64 arriving later met a column that was already
// INTEGER, and came back as 1.104032026e+19 — the loss the classifier refuses
// to allow in the first chunk, reached by arriving after it.
func TestOpenContextPreservesLargeIntegerPastTheFirstChunk(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("account\n")
	for i := range DefaultChunkSize * 2 {
		fmt.Fprintf(&b, "%d\n", i+1)
	}
	b.WriteString("11040320260000000000\n")

	path := filepath.Join(t.TempDir(), "accounts.csv")
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0600))

	ctx := context.Background()
	db, err := OpenContext(ctx, path)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var got string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT account FROM accounts WHERE length(account) = 20`).Scan(&got))
	require.Equal(t, "11040320260000000000", got)
}

// TestOpenContextKeepsAnIntegerPast2p53BesideAFloat pins the column-level form
// of the same loss. An integer between 2^53 and int64 max is exact in an
// INTEGER column, but a float beside it used to make the column REAL, and
// SQLite's REAL affinity then stored the nearest double: 9007199254740993 came
// back as 9007199254740992.0. Such a column has to be TEXT, and a dump of it
// has to read back byte-identical.
func TestOpenContextKeepsAnIntegerPast2p53BesideAFloat(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "mixed.csv")
	require.NoError(t, os.WriteFile(csvPath, []byte("v\n9007199254740993\n0.5\n"), 0600))

	ctx := context.Background()
	db, err := OpenContext(ctx, csvPath)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var kinds, values string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT group_concat(typeof(v)), group_concat(quote(v)) FROM mixed`).Scan(&kinds, &values))
	require.Equal(t, "text,text", kinds)
	require.Equal(t, "'9007199254740993','0.5'", values)

	// The dump writes the exact digits, so loading it lands in the same place.
	out := filepath.Join(dir, "out")
	require.NoError(t, DumpDatabase(db, out))
	dumped, err := os.ReadFile(filepath.Join(out, "mixed.csv")) //nolint:gosec // test-owned path
	require.NoError(t, err)
	require.Equal(t, "v\n9007199254740993\n0.5\n", string(dumped))
}

// TestOpenContextPreservesLargeIntegerExactly is the end-to-end regression test
// for nao1215/sqly#218. A CSV value larger than math.MaxInt64 must round-trip
// through the loaded database as its exact textual value, not a lossy
// scientific-notation float.
func TestOpenContextPreservesLargeIntegerExactly(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "bigint.csv")
	content := "ctsn,pocode\n" +
		"11040320260000000000,100031464478\n" +
		"11040320260000000001,100031464478\n"
	require.NoError(t, os.WriteFile(csvPath, []byte(content), 0600))

	ctx := context.Background()
	db, err := OpenContext(ctx, csvPath)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var got string
	err = db.QueryRowContext(ctx, `SELECT ctsn FROM bigint ORDER BY ctsn LIMIT 1`).Scan(&got)
	require.NoError(t, err)
	require.Equal(t, "11040320260000000000", got)
}
