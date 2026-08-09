package filesql

import (
	"context"
	"database/sql"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	pqfile "github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	_ "modernc.org/sqlite"
)

// Parquet declares the type of every column in its own schema, so an import has
// no reason to guess and no reason to fall back to TEXT. A TEXT column carries
// TEXT affinity into SQLite, which decides comparison and ordering: with it,
// MAX picks the lexicographically largest value and a numeric predicate compares
// digit strings. These tests pin the schema to what the file says.

// TestParquetImportUsesSchemaTypes checks that the declared Parquet types reach
// SQLite, so numeric columns compare and order as numbers.
func TestParquetImportUsesSchemaTypes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	// products.parquet is written by testdata/generate_parquet.go with an
	// explicit Arrow schema: id INT64, name STRING, price DOUBLE.
	db, err := OpenContext(ctx, filepath.Join("testdata", "products.parquet"))
	if err != nil {
		t.Fatalf("OpenContext: %v", err)
	}
	defer func() { _ = db.Close() }()

	t.Run("typeof reports the declared type, not text", func(t *testing.T) {
		var idType, nameType, priceType string
		row := db.QueryRowContext(ctx,
			"SELECT typeof(id), typeof(name), typeof(price) FROM products LIMIT 1")
		if err := row.Scan(&idType, &nameType, &priceType); err != nil {
			t.Fatal(err)
		}
		if idType != "integer" {
			t.Errorf("typeof(id) = %q, want \"integer\"", idType)
		}
		if nameType != "text" {
			t.Errorf("typeof(name) = %q, want \"text\"", nameType)
		}
		if priceType != "real" {
			t.Errorf("typeof(price) = %q, want \"real\"", priceType)
		}
	})

	t.Run("MAX compares numerically", func(t *testing.T) {
		var maxPrice float64
		if err := db.QueryRowContext(ctx, "SELECT MAX(price) FROM products").Scan(&maxPrice); err != nil {
			t.Fatal(err)
		}
		// Lexicographically "999.99" > "79.99" > "29.99" agrees with the numeric
		// answer here, so the assertion below is what separates the two.
		if maxPrice != 999.99 {
			t.Errorf("MAX(price) = %v, want 999.99", maxPrice)
		}
	})

	t.Run("a numeric predicate filters numerically", func(t *testing.T) {
		var got int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM products WHERE price > 100").Scan(&got); err != nil {
			t.Fatal(err)
		}
		// With TEXT affinity every row matched, because SQLite compared the
		// integer 100 as the string "100" against "999.99", "29.99", "79.99".
		if got != 1 {
			t.Errorf("COUNT(*) WHERE price > 100 = %d, want 1 (only 999.99)", got)
		}
	})
}

// TestParquetImportPrefersSchemaOverValueShape pins the reason the schema is
// read at all: the values alone cannot tell these columns apart. A STRING column
// of digits has to stay TEXT so its leading zeros survive, and a DOUBLE column
// holding whole numbers has to stay REAL.
func TestParquetImportPrefersSchemaOverValueShape(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()

	// Build the file through a SQLite table whose declared types say what the
	// values do not.
	src, err := sql.Open("sqlite", filepath.Join(dir, "src.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	if _, err := src.ExecContext(ctx, `CREATE TABLE codes (zip TEXT, ratio REAL);`); err != nil {
		t.Fatal(err)
	}
	if _, err := src.ExecContext(ctx, `INSERT INTO codes VALUES ('01234', 2.0),('00001', 3.0);`); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "out")
	if err := DumpDatabase(src, out, NewDumpOptions().WithFormat(OutputFormatParquet)); err != nil {
		t.Fatalf("DumpDatabase: %v", err)
	}

	db, err := OpenContext(ctx, filepath.Join(out, "codes.parquet"))
	if err != nil {
		t.Fatalf("OpenContext: %v", err)
	}
	defer func() { _ = db.Close() }()

	var zip, zipType, ratioType string
	row := db.QueryRowContext(ctx, "SELECT zip, typeof(zip), typeof(ratio) FROM codes ORDER BY zip LIMIT 1")
	if err := row.Scan(&zip, &zipType, &ratioType); err != nil {
		t.Fatal(err)
	}
	if zipType != "text" {
		t.Errorf("typeof(zip) = %q, want \"text\": a STRING column of digits is still text", zipType)
	}
	if zip != "00001" {
		t.Errorf("zip = %q, want \"00001\": the leading zeros must survive", zip)
	}
	if ratioType != "real" {
		t.Errorf("typeof(ratio) = %q, want \"real\": a DOUBLE holding whole numbers is still real", ratioType)
	}
}

// TestParquetAndCSVAgreeOnNumericQueries is the differential form of the same
// property: the same rows loaded from Parquet and from CSV must answer the same
// query the same way. CSV infers its types from the values, Parquet reads them
// from its schema, and the two paths have to arrive at the same place.
func TestParquetAndCSVAgreeOnNumericQueries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()

	// Values chosen so lexicographic and numeric order disagree on every query
	// below: "9.99" sorts above "1000.5", and "100" sorts below "9".
	csvPath := filepath.Join(dir, "goods.csv")
	csvBody := "name,price,qty\ncheap,9.99,100\nmid,100,9\npricey,1000.5,20\n"
	if err := os.WriteFile(csvPath, []byte(csvBody), 0o600); err != nil {
		t.Fatal(err)
	}

	// Round-trip the CSV through a Parquet dump so both inputs hold the same
	// rows, then query each and compare.
	csvDB, err := OpenContext(ctx, csvPath)
	if err != nil {
		t.Fatalf("OpenContext(csv): %v", err)
	}
	defer func() { _ = csvDB.Close() }()

	out := filepath.Join(dir, "out")
	if err := DumpDatabase(csvDB, out, NewDumpOptions().WithFormat(OutputFormatParquet)); err != nil {
		t.Fatalf("DumpDatabase: %v", err)
	}
	parquetDB, err := OpenContext(ctx, filepath.Join(out, "goods.parquet"))
	if err != nil {
		t.Fatalf("OpenContext(parquet): %v", err)
	}
	defer func() { _ = parquetDB.Close() }()

	queries := []struct {
		name  string
		query string
	}{
		{"max of a real column", "SELECT MAX(price) FROM goods"},
		{"min of a real column", "SELECT MIN(price) FROM goods"},
		{"max of an integer column", "SELECT MAX(qty) FROM goods"},
		{"order by a real column", "SELECT group_concat(name) FROM (SELECT name FROM goods ORDER BY price)"},
		{"filter on a real column", "SELECT COUNT(*) FROM goods WHERE price > 50"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			var fromCSV, fromParquet string
			if err := csvDB.QueryRowContext(ctx, tt.query).Scan(&fromCSV); err != nil {
				t.Fatalf("csv: %v", err)
			}
			if err := parquetDB.QueryRowContext(ctx, tt.query).Scan(&fromParquet); err != nil {
				t.Fatalf("parquet: %v", err)
			}
			if fromCSV != fromParquet {
				t.Errorf("%s\n csv     = %q\n parquet = %q", tt.query, fromCSV, fromParquet)
			}
		})
	}
}

// TestParquetDumpWritesTypedColumns checks the export side. Parquet is a typed
// format and a consumer reads its schema, so writing every column as STRING
// hands the next tool a table of digit strings.
func TestParquetDumpWritesTypedColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "src.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx,
		`CREATE TABLE goods (name TEXT, price REAL, qty INTEGER);`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO goods VALUES ('cheap',9.99,100),('pricey',1000.5,20);`); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "out")
	if err := DumpDatabase(db, out, NewDumpOptions().WithFormat(OutputFormatParquet)); err != nil {
		t.Fatalf("DumpDatabase: %v", err)
	}

	assertParquetSchema(t, filepath.Join(out, "goods.parquet"),
		map[string]string{"name": "utf8", "price": "float64", "qty": "int64"})
}

// TestParquetDumpOfEmptyTableKeepsDeclaredTypes checks the one case the values
// cannot decide. A table the session emptied still knows its declared types, and
// an auto-save that rewrote them all as STRING would change the schema of a
// table whose rows were merely deleted.
func TestParquetDumpOfEmptyTableKeepsDeclaredTypes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "src.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx,
		`CREATE TABLE goods (name TEXT, price REAL, qty INTEGER);`); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "out")
	if err := DumpDatabase(db, out, NewDumpOptions().WithFormat(OutputFormatParquet)); err != nil {
		t.Fatalf("DumpDatabase: %v", err)
	}

	assertParquetSchema(t, filepath.Join(out, "goods.parquet"),
		map[string]string{"name": "utf8", "price": "float64", "qty": "int64"})
}

// assertParquetSchema checks the Arrow type of each named column in a Parquet
// file.
func assertParquetSchema(t *testing.T, path string, want map[string]string) {
	t.Helper()

	f, err := os.Open(path) //nolint:gosec // test-owned path
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	reader, err := pqfile.NewParquetReader(f)
	if err != nil {
		t.Fatalf("NewParquetReader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		t.Fatalf("NewFileReader: %v", err)
	}
	schema, err := arrowReader.Schema()
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}

	for name, wantType := range want {
		fields := schema.FieldIndices(name)
		if len(fields) != 1 {
			t.Fatalf("column %q appears %d times in the parquet schema", name, len(fields))
		}
		if got := schema.Field(fields[0]).Type.Name(); got != wantType {
			t.Errorf("column %q has parquet type %q, want %q", name, got, wantType)
		}
	}
}

// TestParquetNonFiniteRealStaysReal pins the invariant arrowColumnInfoList
// states: the column type taken from the Parquet schema and the text
// extractValueFromArrowArray renders have to agree, because SQLite applies the
// column's affinity to that text.
//
// "%g" spells an infinity "+Inf", which SQLite's REAL affinity cannot convert,
// so the cell was stored as TEXT inside a column declared REAL: a double in the
// file came back as a string, and typeof() said so.
func TestParquetNonFiniteRealStaysReal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "src.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, `CREATE TABLE m (v REAL);`); err != nil {
		t.Fatal(err)
	}
	for _, v := range []float64{math.Inf(1), math.Inf(-1), 1.5} {
		if _, err := db.ExecContext(ctx, `INSERT INTO m VALUES (?);`, v); err != nil {
			t.Fatal(err)
		}
	}

	out := filepath.Join(dir, "out")
	if err := DumpDatabase(db, out, NewDumpOptions().WithFormat(OutputFormatParquet)); err != nil {
		t.Fatalf("DumpDatabase: %v", err)
	}
	assertParquetSchema(t, filepath.Join(out, "m.parquet"), map[string]string{"v": "float64"})

	reloaded, err := OpenContext(ctx, filepath.Join(out, "m.parquet"))
	if err != nil {
		t.Fatalf("OpenContext: %v", err)
	}
	defer func() { _ = reloaded.Close() }()

	rows, err := reloaded.QueryContext(ctx, `SELECT v, typeof(v) FROM m;`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var (
		values []float64
		types  []string
	)
	for rows.Next() {
		var (
			v  float64
			ty string
		)
		if err := rows.Scan(&v, &ty); err != nil {
			t.Fatalf("scan: %v", err)
		}
		values = append(values, v)
		types = append(types, ty)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if want := []string{"real", "real", "real"}; !reflect.DeepEqual(types, want) {
		t.Errorf("typeof = %v, want %v: a double column stays REAL whatever it holds", types, want)
	}
	if len(values) != 3 || !math.IsInf(values[0], 1) || !math.IsInf(values[1], -1) || values[2] != 1.5 {
		t.Errorf("values = %v, want [+Inf -Inf 1.5]", values)
	}
}

// TestParquetNaNBecomesNull pins the one value SQLite cannot hold. It has no NaN
// at all — a computed one is stored as NULL — so a NaN carried by a Parquet
// double becomes NULL rather than the word "NaN" sitting in a REAL column as
// text.
func TestParquetNaNBecomesNull(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "src.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, `CREATE TABLE m (v REAL);`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO m VALUES (?);`, math.NaN()); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "out")
	if err := DumpDatabase(db, out, NewDumpOptions().WithFormat(OutputFormatParquet)); err != nil {
		t.Fatalf("DumpDatabase: %v", err)
	}

	reloaded, err := OpenContext(ctx, filepath.Join(out, "m.parquet"))
	if err != nil {
		t.Fatalf("OpenContext: %v", err)
	}
	defer func() { _ = reloaded.Close() }()

	var ty string
	if err := reloaded.QueryRowContext(ctx, `SELECT typeof(v) FROM m;`).Scan(&ty); err != nil {
		t.Fatalf("query: %v", err)
	}
	if ty != "null" {
		t.Errorf("typeof = %q, want \"null\": SQLite has no NaN to store", ty)
	}
}
