package filesql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	pqfile "github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	rows, err := reloaded.QueryContext(ctx, `SELECT v, typeof(v) FROM m ORDER BY rowid;`)
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
//
// The file is written directly rather than dumped from SQLite, because SQLite
// turns a NaN into NULL on the way in: a dumped file holds a null, and the
// renderer this covers would never see a NaN at all.
func TestParquetNaNBecomesNull(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nan.parquet")
	writeFloat64Parquet(t, path, "v", []float64{math.NaN(), 1.5})

	db, err := OpenContext(ctx, path)
	if err != nil {
		t.Fatalf("OpenContext: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, `SELECT typeof(v) FROM nan ORDER BY rowid;`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var types []string
	for rows.Next() {
		var ty string
		if err := rows.Scan(&ty); err != nil {
			t.Fatalf("scan: %v", err)
		}
		types = append(types, ty)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if want := []string{"null", "real"}; !reflect.DeepEqual(types, want) {
		t.Errorf("typeof = %v, want %v: SQLite has no NaN to store, and the finite value beside it is untouched", types, want)
	}
}

// TestParquetInfinityFromFileStaysReal reads an infinity written straight into a
// Parquet double, which is what a file another tool produced looks like.
func TestParquetInfinityFromFileStaysReal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "inf.parquet")
	writeFloat64Parquet(t, path, "v", []float64{math.Inf(1), math.Inf(-1)})

	db, err := OpenContext(ctx, path)
	if err != nil {
		t.Fatalf("OpenContext: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, `SELECT v, typeof(v) FROM inf ORDER BY rowid;`)
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

	if want := []string{"real", "real"}; !reflect.DeepEqual(types, want) {
		t.Errorf("typeof = %v, want %v", types, want)
	}
	if len(values) != 2 || !math.IsInf(values[0], 1) || !math.IsInf(values[1], -1) {
		t.Errorf("values = %v, want [+Inf -Inf]", values)
	}
}

// TestSQLiteFloatText covers the renderer directly, including the float32 width
// a Parquet FLOAT column is read at: rendering it as a float64 would hand SQLite
// the expansion (3.141590118408203) instead of the number the file holds.
func TestSQLiteFloatText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   float64
		bitSize int
		want    string
	}{
		{name: "positive infinity is a literal SQLite overflows to one", value: math.Inf(1), bitSize: 64, want: "9e999"},
		{name: "negative infinity likewise", value: math.Inf(-1), bitSize: 64, want: "-9e999"},
		{name: "NaN is empty, which is how a null is rendered", value: math.NaN(), bitSize: 64, want: ""},
		{name: "a finite double is unchanged", value: 1.5, bitSize: 64, want: "1.5"},
		{name: "a float32 renders at its own width", value: float64(float32(3.14159)), bitSize: 32, want: "3.14159"},
		{name: "a float32 infinity is the same literal", value: float64(float32(math.Inf(1))), bitSize: 32, want: "9e999"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := sqliteFloatText(tt.value, tt.bitSize); got != tt.want {
				t.Errorf("sqliteFloatText(%v, %d) = %q, want %q", tt.value, tt.bitSize, got, tt.want)
			}
		})
	}
}

// writeFloat64Parquet writes a one-column Parquet file of doubles, so a test can
// hand the reader a value SQLite would not have let through on the way in.
func writeFloat64Parquet(t *testing.T, path, column string, values []float64) {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{{Name: column, Type: arrow.PrimitiveTypes.Float64, Nullable: true}}, nil)
	builder := array.NewFloat64Builder(memory.NewGoAllocator())
	defer builder.Release()
	builder.AppendValues(values, nil)

	arr := builder.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{arr}, int64(len(values)))
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	f, err := os.Create(path) //nolint:gosec // test-owned path
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	if err := pqarrow.WriteTable(tbl, f, 1024, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
}

// damagedParquet is products.parquet with a data page corrupted, found by
// fuzzing the binary readers. The Arrow library this package reads Parquet with
// panics on it -- its error path releases a column it never built -- and a
// caller reading a file chosen by someone else cannot defend against that, so
// the boundary is held here and the file is an error like any other bad input.
const damagedParquet = "BhwYCAMAAAAAAAAAGAgBAAAAAAAAABYAFgAYCAMAAAAAAAAAGAgBAAAAAAAAAAAAAAIDJAAV" +
	"BBU+FT5MFQYVABIAAAYAAABMYXB0b3AFAAAATW91c2UIAAAAS2V5Ym9hcmQVABUIFQgsFQYV" +
	"EBUGFQYcNgAWABgFTW91c2UYCEtleWJvYXJkAAAAAgMkABUEFTAVMEwVBhUAEgAAUrgehes/" +
	"j0A9CtejcP09QI/C9Shc/1NAFQAVCBUILBUGFRAVBhUGHBgIUrgehes/j0AYCD0K16Nw/T1A" +
	"FgAWABgIUrgehes/j0AYCD0K16Nw/T1AAAAAAgMkABUEGUw1BBgGc2NoZW1hFQYAFQQlABgC" +
	"aWQlJEysE0ARAAAAFQwlABgEbmFtZSUATBwAAAAVCiUAGAVwcmljZQAWBhkcGTwm2gEcFQQZ" +
	"NRAABhkYAmlkFQAWBhbSARbSASZUJggcGAgDAAAAAAAAABgIAQAAAAAAAAAWABYAGAgDAAAA" +
	"AAAAABgIAQAAAAAAAAAAGSwVBBUAFQIAFQAVEBUCAAAAJowDHBUMGTUQAAYZGARuYW1lFQAW" +
	"BhayARayASa0AibaARw2ABYAGAVNb3VzZRgIS2V5Ym9hcmQAGSwVBBUAFQIAFQAVEBUCAAAA" +
	"Jt4EHBUKGTUQAAYZGAVwcmljZRUAFgYW0gEW0gEm2AMmjAMcGAhSuB6F6z+PQBgIPQrXowAS" +
	"AEAWABYAGAhSuB6F6z+PQBgIPQrXo3D9PUAAGSwVBBUAFQIAFQAVEBUCAAAAFtYEFgYmCBbW" +
	"BBQAABkMGCJwYXJxdWV0LWdvIHZlcnNpb24gMTguMC4wLVNOQVBTSE9UGTwcAAAcAAAcAAAA" +
	"kQEAAFBBUjE="

// TestDamagedParquetIsAnErrorNotAPanic pins that boundary at every door a
// caller comes through.
func TestDamagedParquetIsAnErrorNotAPanic(t *testing.T) {
	t.Parallel()

	data, err := base64.StdEncoding.DecodeString(damagedParquet)
	require.NoError(t, err)

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "damaged.parquet")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	t.Run("through OpenContext", func(t *testing.T) {
		t.Parallel()

		var db *sql.DB
		var err error
		require.NotPanics(t, func() { db, err = OpenContext(ctx, path) })
		if db != nil {
			defer db.Close()
		}
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrParsing)
	})

	t.Run("through AddReader", func(t *testing.T) {
		t.Parallel()

		require.NotPanics(t, func() {
			built, err := NewBuilder().
				AddReader(bytes.NewReader(data), "t", FileTypeParquet).Build(ctx)
			if err != nil {
				return
			}
			db, err := built.Open(ctx)
			if db != nil {
				assert.NoError(t, db.Close())
			}
			assert.Error(t, err)
		})
	})
}

// TestParquetDamageThatWasAlreadyReported keeps the recover above from hiding a
// regression in the paths that report properly on their own.
func TestParquetDamageThatWasAlreadyReported(t *testing.T) {
	t.Parallel()

	good, err := os.ReadFile(filepath.Join("parser", "testdata", "products.parquet"))
	if os.IsNotExist(err) {
		t.Skip("no parquet fixture")
	}
	require.NoError(t, err)

	flip := func(b []byte, i int) []byte {
		out := make([]byte, len(b))
		copy(out, b)
		out[i] ^= 0xFF
		return out
	}

	for name, data := range map[string][]byte{
		"four bytes":                   good[:4],
		"truncated by one byte":        good[:len(good)-1],
		"truncated to half":            good[:len(good)/2],
		"a flipped byte in the footer": flip(good, len(good)/2),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "damaged.parquet")
			require.NoError(t, os.WriteFile(path, data, 0o600))
			db, err := OpenContext(context.Background(), path)
			if db != nil {
				defer db.Close()
			}
			require.Error(t, err)
		})
	}

	t.Run("the undamaged file still loads", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "products.parquet")
		require.NoError(t, os.WriteFile(path, good, 0o600)) //nolint:gosec // Test path is constructed from t.TempDir()
		db, err := OpenContext(context.Background(), path)
		require.NoError(t, err)
		defer db.Close()
		var n int
		require.NoError(t, db.QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM products`).Scan(&n))
		assert.Positive(t, n)
	})
}

// unboundedParquet is the file from the fuzzing run that never finished
// loading: 433 bytes that end with the format's mark and begin with something
// else.
const unboundedParquet = "MDAwMBUYFQAVCBUILBUGFRAVMBUwHBhvFTAZQTkEGAYwMDAwMDAVBjAVBCMwGAIwMDcwMDAw" +
	"MDAwMDAVDCMwGAQwMDAwIzBMHDAwMBUKJTAYBTAwMDAwMBYwGRwZMCbaMBwVMBkwMDAwGRgC" +
	"MDAVABYwFtIwFtIBJjAmCBwYIDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwWAgw" +
	"MDAwMDAwMDAZIBUwFTAVMDAVMBUwFTAwMDAmjDAcFTAZMDAwMBkYBDAwMDAVABYwFrIwFrIB" +
	"JrQwJtoBHDYwFjAYBTAwMDAwGAgwMDAwMDAwMDAZIBUwFTAVMDAVMBUwFTAwMDAm3jAcFTAZ" +
	"MDAwMBkYBTAwMDAwFQAWMBbSMBbSASbYMCaMAxwYCDAwMDAwMDAwGAgwMDAwMDAwMBYwFjAY" +
	"CDAwMDAwMDAwGAgwMDAwMDAwMDAZIBUwFTAVMDAVMBUwFTAwMDAW1jAWBiYwFtYwFDAwGQwY" +
	"IjAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAZMBwwMBwwMDAwMDCRAQAAUEFS" +
	"MQ=="

// TestParquetWithoutTheHeaderMarkIsRefused pins the check that keeps damaged
// input away from the reader's metadata parsing. The format begins and ends with
// a four-byte mark; the reader this package uses checks only the trailing one,
// so a file that ends "PAR1" and begins with anything was read as Parquet -- and
// that is the shape that panicked and that allocated without stopping.
func TestParquetWithoutTheHeaderMarkIsRefused(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	for name, data := range map[string][]byte{
		"only the trailing mark": []byte("\x00\x00\x00\x00 not parquet PAR1"),
		"neither mark":           []byte("not a parquet file at all"),
		"the mark in the middle": []byte("xxxxPAR1xxxx"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "t.parquet")
			require.NoError(t, os.WriteFile(path, data, 0o600))

			db, err := OpenContext(ctx, path)
			if db != nil {
				assert.NoError(t, db.Close())
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not a parquet file")
		})
	}

	t.Run("the file that allocated without stopping", func(t *testing.T) {
		t.Parallel()

		// 433 bytes, found by fuzzing: it ends with the mark and begins "0000",
		// and reading it grew the heap by hundreds of megabytes a second.
		data, err := base64.StdEncoding.DecodeString(unboundedParquet)
		require.NoError(t, err)

		path := filepath.Join(t.TempDir(), "unbounded.parquet")
		require.NoError(t, os.WriteFile(path, data, 0o600))

		done := make(chan error, 1)
		go func() {
			db, err := OpenContext(ctx, path)
			if db != nil {
				_ = db.Close()
			}
			done <- err
		}()
		select {
		case err := <-done:
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not a parquet file")
		case <-time.After(10 * time.Second):
			t.Fatal("a 433 byte file held the load for ten seconds")
		}
	})
}
