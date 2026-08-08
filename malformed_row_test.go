package filesql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
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
