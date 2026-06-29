package filesql

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// TestParquetRoundTripPreservesNull checks that a SQL NULL survives a Parquet
// dump and reload instead of collapsing into an empty string, so NULL and ""
// stay distinguishable.
func TestParquetRoundTripPreservesNull(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "src.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE t (id TEXT, name TEXT);`); err != nil {
		t.Fatal(err)
	}
	// Row 0 id is NULL, row 1 id is an empty string, row 2 id is a value.
	if _, err := db.ExecContext(ctx, `INSERT INTO t VALUES (NULL,'A'),('','B'),('1','C');`); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "out")
	opts := NewDumpOptions().WithFormat(OutputFormatParquet)
	if err := DumpDatabase(db, out, opts); err != nil {
		t.Fatalf("DumpDatabase: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(out, "*.parquet"))
	if err != nil || len(files) != 1 {
		t.Fatalf("expected one parquet file, got %v (err %v)", files, err)
	}

	rdb, err := OpenContext(ctx, files[0])
	if err != nil {
		t.Fatalf("OpenContext: %v", err)
	}
	defer func() { _ = rdb.Close() }()

	rows, err := rdb.QueryContext(ctx, "SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	var got []sql.NullString
	for rows.Next() {
		var v sql.NullString
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if len(got) != 3 {
		t.Fatalf("reimported %d rows, want 3", len(got))
	}
	if got[0].Valid {
		t.Errorf("row 0 id = %q, want SQL NULL", got[0].String)
	}
	if !got[1].Valid || got[1].String != "" {
		t.Errorf("row 1 id = %#v, want an empty string (not NULL)", got[1])
	}
	if !got[2].Valid || got[2].String != "1" {
		t.Errorf("row 2 id = %#v, want \"1\"", got[2])
	}
}

// TestParquetRoundTripPreservesNullInLaterColumn checks that a NULL in a column
// other than the first survives the round-trip and does not shift the values of
// the surrounding columns.
func TestParquetRoundTripPreservesNullInLaterColumn(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "src.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE t (a TEXT, b TEXT, c TEXT);`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO t VALUES ('x', NULL, 'z');`); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "out")
	if err := DumpDatabase(db, out, NewDumpOptions().WithFormat(OutputFormatParquet)); err != nil {
		t.Fatalf("DumpDatabase: %v", err)
	}
	files, err := filepath.Glob(filepath.Join(out, "*.parquet"))
	if err != nil || len(files) != 1 {
		t.Fatalf("expected one parquet file, got %v (err %v)", files, err)
	}

	rdb, err := OpenContext(ctx, files[0])
	if err != nil {
		t.Fatalf("OpenContext: %v", err)
	}
	defer func() { _ = rdb.Close() }()

	var a, c string
	var b sql.NullString
	if err := rdb.QueryRowContext(ctx, "SELECT a, b, c FROM t").Scan(&a, &b, &c); err != nil {
		t.Fatal(err)
	}
	if a != "x" || c != "z" {
		t.Errorf("a,c = %q,%q, want x,z (neighbors unaffected)", a, c)
	}
	if b.Valid {
		t.Errorf("b = %q, want SQL NULL", b.String)
	}
}
