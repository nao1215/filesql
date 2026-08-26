package filesql

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

// queryColumn returns the values of a single column across all rows, ordered by
// the given ORDER BY expression, so a test can assert on the imported content
// regardless of physical row order.
func queryColumn(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("query %q failed: %v", query, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v sql.NullString
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		out = append(out, v.String)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration failed: %v", err)
	}
	return out
}

func openWithPolicy(t *testing.T, content string, ft FileType, policy MalformedRowPolicy) (*sql.DB, error) {
	t.Helper()
	b, err := NewBuilder().
		AddReader(strings.NewReader(content), "t", ft).
		WithMalformedRowPolicy(policy).
		Build(context.Background())
	if err != nil {
		return nil, err
	}
	return b.Open(context.Background())
}

// TestSkippedRowsReportsWhatWasDropped covers what a caller has to be able to
// say after MalformedRowSkip has done its work. Skipping is an instruction, not
// a silence: an import that dropped one row and one that dropped most of the
// file looked identical, and there was no number a caller could put in front of
// a user before a write-back made the loss permanent.
func TestSkippedRowsReportsWhatWasDropped(t *testing.T) {
	t.Parallel()

	t.Run("a skipping import counts the rows it dropped", func(t *testing.T) {
		t.Parallel()

		b, err := NewBuilder().
			AddReader(strings.NewReader("a,b\n1,2\n3\n5,6\n7\n"), "t", FileTypeCSV).
			WithMalformedRowPolicy(MalformedRowSkip).
			Build(context.Background())
		if err != nil {
			t.Fatalf("build failed: %v", err)
		}
		db, err := b.Open(context.Background())
		if err != nil {
			t.Fatalf("open failed: %v", err)
		}
		defer db.Close()

		got := b.SkippedRows()
		if len(got) != 1 {
			t.Fatalf("SkippedRows() = %v, want one entry", got)
		}
		if got[0].Table != "t" {
			t.Errorf("Table = %q, want %q", got[0].Table, "t")
		}
		if got[0].Count != 2 {
			t.Errorf("Count = %d, want 2 (rows 2 and 4 are short)", got[0].Count)
		}
		if got[0].Total != 4 {
			t.Errorf("Total = %d, want 4 data rows seen", got[0].Total)
		}
	})

	t.Run("an import that dropped nothing reports nothing", func(t *testing.T) {
		t.Parallel()

		b, err := NewBuilder().
			AddReader(strings.NewReader("a,b\n1,2\n3,4\n"), "t", FileTypeCSV).
			WithMalformedRowPolicy(MalformedRowSkip).
			Build(context.Background())
		if err != nil {
			t.Fatalf("build failed: %v", err)
		}
		db, err := b.Open(context.Background())
		if err != nil {
			t.Fatalf("open failed: %v", err)
		}
		defer db.Close()

		if got := b.SkippedRows(); len(got) != 0 {
			t.Errorf("SkippedRows() = %v, want none: a clean import has nothing to report", got)
		}
	})
}

func TestMalformedRowPolicy_Stop(t *testing.T) {
	t.Parallel()

	t.Run("short row aborts import with ErrColumnMismatch instead of dropping data", func(t *testing.T) {
		t.Parallel()
		// Third row is missing the "zip" field. The default (stop) policy must
		// surface an error rather than silently importing an empty table.
		const csv = "id,name,zip\n1,alice,01234\n2,bob,123\n3,caro\n"
		db, err := openWithPolicy(t, csv, FileTypeCSV, MalformedRowStop)
		if db != nil {
			defer db.Close()
		}
		if err == nil {
			t.Fatalf("expected an error for a ragged row under the stop policy, got nil")
		}
		if !errors.Is(err, ErrColumnMismatch) {
			t.Fatalf("expected ErrColumnMismatch, got %v", err)
		}
	})

	t.Run("long row aborts import", func(t *testing.T) {
		t.Parallel()
		const csv = "id,name\n1,alice\n2,bob,extra\n"
		db, err := openWithPolicy(t, csv, FileTypeCSV, MalformedRowStop)
		if db != nil {
			defer db.Close()
		}
		if !errors.Is(err, ErrColumnMismatch) {
			t.Fatalf("expected ErrColumnMismatch, got %v", err)
		}
	})

	t.Run("default policy is stop", func(t *testing.T) {
		t.Parallel()
		const csv = "id,name,zip\n1,alice,01234\n3,caro\n"
		// No WithMalformedRowPolicy call: the zero value must behave as stop.
		b, err := NewBuilder().AddReader(strings.NewReader(csv), "t", FileTypeCSV).Build(context.Background())
		if err != nil {
			t.Fatalf("build failed: %v", err)
		}
		db, err := b.Open(context.Background())
		if db != nil {
			defer db.Close()
		}
		if !errors.Is(err, ErrColumnMismatch) {
			t.Fatalf("expected ErrColumnMismatch for default policy, got %v", err)
		}
	})
}

func TestMalformedRowPolicy_Skip(t *testing.T) {
	t.Parallel()

	t.Run("ragged rows are dropped and well-formed rows are imported", func(t *testing.T) {
		t.Parallel()
		const csv = "id,name,zip\n1,alice,01234\n2,bob,123\n3,caro\n4,dave,99999\n"
		db, err := openWithPolicy(t, csv, FileTypeCSV, MalformedRowSkip)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer db.Close()

		names := queryColumn(t, db, `SELECT name FROM t ORDER BY id`)
		want := []string{"alice", "bob", "dave"}
		if strings.Join(names, ",") != strings.Join(want, ",") {
			t.Fatalf("names = %v, want %v", names, want)
		}
	})

	t.Run("long rows are skipped too", func(t *testing.T) {
		t.Parallel()
		const csv = "id,name\n1,alice\n2,bob,extra\n3,carol\n"
		db, err := openWithPolicy(t, csv, FileTypeCSV, MalformedRowSkip)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer db.Close()
		count := queryColumn(t, db, `SELECT COUNT(*) FROM t`)
		if count[0] != "2" {
			t.Fatalf("row count = %s, want 2", count[0])
		}
	})
}

func TestMalformedRowPolicy_Fill(t *testing.T) {
	t.Parallel()

	t.Run("short rows are padded with empty strings", func(t *testing.T) {
		t.Parallel()
		const csv = "id,name,zip\n1,alice,01234\n3,caro\n"
		db, err := openWithPolicy(t, csv, FileTypeCSV, MalformedRowFill)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer db.Close()

		// The missing zip of row 3 becomes an empty string, and every input row
		// is retained.
		zip := queryColumn(t, db, `SELECT COALESCE(zip, '') FROM t ORDER BY id`)
		if len(zip) != 2 {
			t.Fatalf("expected 2 rows, got %d (%v)", len(zip), zip)
		}
		if zip[1] != "" {
			t.Fatalf("row 3 zip = %q, want empty string", zip[1])
		}
	})

	t.Run("long rows are rejected instead of truncated", func(t *testing.T) {
		t.Parallel()
		const csv = "id,name\n1,alice\n2,bob,extra\n"
		db, err := openWithPolicy(t, csv, FileTypeCSV, MalformedRowFill)
		if db != nil {
			defer db.Close()
		}
		if !errors.Is(err, ErrColumnMismatch) {
			t.Fatalf("expected ErrColumnMismatch, got %v", err)
		}
	})
}

func TestMalformedRowPolicy_TSV(t *testing.T) {
	t.Parallel()

	const tsv = "id\tname\tzip\n1\talice\t01234\n3\tcaro\n"

	t.Run("stop aborts on a ragged TSV row", func(t *testing.T) {
		t.Parallel()
		db, err := openWithPolicy(t, tsv, FileTypeTSV, MalformedRowStop)
		if db != nil {
			defer db.Close()
		}
		if !errors.Is(err, ErrColumnMismatch) {
			t.Fatalf("expected ErrColumnMismatch, got %v", err)
		}
	})

	t.Run("skip drops the ragged TSV row", func(t *testing.T) {
		t.Parallel()
		db, err := openWithPolicy(t, tsv, FileTypeTSV, MalformedRowSkip)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer db.Close()
		count := queryColumn(t, db, `SELECT COUNT(*) FROM t`)
		if count[0] != "1" {
			t.Fatalf("row count = %s, want 1", count[0])
		}
	})
}

// TestMalformedRowPolicy_WellFormedUnaffected is a metamorphic check: a file
// with no ragged rows imports identically under every policy.
func TestMalformedRowPolicy_WellFormedUnaffected(t *testing.T) {
	t.Parallel()
	const csv = "id,name\n1,alice\n2,bob\n3,carol\n"
	for _, policy := range []MalformedRowPolicy{MalformedRowStop, MalformedRowSkip, MalformedRowFill} {
		db, err := openWithPolicy(t, csv, FileTypeCSV, policy)
		if err != nil {
			t.Fatalf("policy %v: unexpected error: %v", policy, err)
		}
		names := queryColumn(t, db, `SELECT name FROM t ORDER BY id`)
		_ = db.Close()
		if strings.Join(names, ",") != "alice,bob,carol" {
			t.Fatalf("policy %v: names = %v", policy, names)
		}
	}
}

// TestXLSXIgnoresTheMalformedRowPolicy pins the independence the policy's
// documentation claims. A workbook's rows are checked by the XLSX reader, which
// refuses one wider than its header whatever the policy says, so a caller who
// reaches for MalformedRowSkip to get past a ragged workbook has to know it does
// not reach there.
func TestXLSXIgnoresTheMalformedRowPolicy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "report.xlsx")
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	// A title above the header, which is what a person's spreadsheet looks like:
	// the first row is one cell, and the rows under it are three.
	require.NoError(t, f.SetCellValue("Sheet1", "A1", "Quarterly report"))
	require.NoError(t, f.SetSheetRow("Sheet1", "A2", &[]any{"Name", "Amount", "Joined"}))
	require.NoError(t, f.SetSheetRow("Sheet1", "A3", &[]any{"Alice", 100, 45000}))
	require.NoError(t, f.SaveAs(path))

	for _, policy := range []MalformedRowPolicy{MalformedRowStop, MalformedRowSkip, MalformedRowFill} {
		t.Run(policy.String(), func(t *testing.T) {
			t.Parallel()

			built, err := NewBuilder().AddPath(path).WithMalformedRowPolicy(policy).Build(ctx)
			require.NoError(t, err)
			db, err := built.Open(ctx)
			if db != nil {
				defer db.Close()
			}
			require.Error(t, err, "a workbook row wider than its header is refused whatever the policy says")
			assert.Contains(t, err.Error(), "row 2 has 3 cells where the header has 1")
		})
	}
}

// TestMalformedRowFillRefusesALongRecord is the behavior the builder's godoc
// describes: a short record is padded and a long one is refused, because
// truncating it would discard a cell the file holds without saying so.
func TestMalformedRowFillRefusesALongRecord(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	built, err := NewBuilder().
		AddReader(strings.NewReader("a,b,c\n1,2\n"), "short", FileTypeCSV).
		WithMalformedRowPolicy(MalformedRowFill).Build(ctx)
	require.NoError(t, err)
	db, err := built.Open(ctx)
	require.NoError(t, err, "a short record is padded")
	var c string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT c FROM short`).Scan(&c))
	assert.Equal(t, "", c, "the missing cell is the empty string")
	require.NoError(t, db.Close())

	built, err = NewBuilder().
		AddReader(strings.NewReader("a\n1,2\n"), "long", FileTypeCSV).
		WithMalformedRowPolicy(MalformedRowFill).Build(ctx)
	require.NoError(t, err)
	db, err = built.Open(ctx)
	if db != nil {
		defer db.Close()
	}
	require.Error(t, err, "a long record is refused rather than truncated")
	assert.ErrorIs(t, err, ErrColumnMismatch)
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
