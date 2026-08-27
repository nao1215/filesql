package filesql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nao1215/filesql/internal/reader"
	"github.com/nao1215/filesql/parser"
	"github.com/parquet-go/parquet-go"
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

// TestParquetUint64ColumnLoadsExactly pins the type a UINT64 column is declared
// as. Declared INTEGER, a value past int64 max was converted to REAL by
// SQLite's affinity — 18446744073709551615 loaded as 1.8446744073709552e+19 —
// so the column is TEXT for the same reason DECIMAL and UUID are: it is the
// only type that holds every value the schema admits. The type follows the
// schema, not the values, so a UINT64 column of small values is TEXT too.
func TestParquetUint64ColumnLoadsExactly(t *testing.T) {
	t.Parallel()

	type row struct {
		U64 uint64 `parquet:"u64"`
		U32 uint32 `parquet:"u32"`
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[row](&buf)
	_, err := w.Write([]row{
		{U64: math.MaxUint64, U32: math.MaxUint32},
		{U64: 42, U32: 7},
	})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	dir := t.TempDir()
	path := filepath.Join(dir, "uints.parquet")
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))

	ctx := context.Background()
	db, err := OpenContext(ctx, path)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var kinds, values string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT group_concat(typeof(u64)), group_concat(quote(u64)) FROM uints`).Scan(&kinds, &values))
	assert.Equal(t, "text,text", kinds)
	assert.Equal(t, "'18446744073709551615','42'", values)

	// UINT32 fits in int64, so it keeps the numeric column.
	var u32Kind string
	var u32 int64
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT typeof(u32), u32 FROM uints ORDER BY u32 DESC LIMIT 1`).Scan(&u32Kind, &u32))
	assert.Equal(t, "integer", u32Kind)
	assert.Equal(t, int64(math.MaxUint32), u32)
}

// TestParquetDumpKeepsAMixedColumnExact pins the type a dump gives a column
// that mixes int64 and float64 values, which SQLite's per-value typing allows.
// Widening to DOUBLE is lossless only while every integer survives a float64
// round-trip; 9007199254740993 does not, and the dump used to write
// 9007199254740992 into the file. Such a column is written as STRING, the same
// answer a number-beside-text column already gets, while a losslessly widenable
// mix keeps DOUBLE.
func TestParquetDumpKeepsAMixedColumnExact(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		first    any // the two values of column v, inserted in order
		second   any
		wantType string // parquet physical type assertParquetSchema reports
		wantBack string // group_concat(quote(v)) after reloading the dump
	}{
		{"an integer past 2^53 beside a float", int64(9007199254740993), 0.5, "utf8", "'9007199254740993','0.5'"},
		{"a negative integer past 2^53 beside a float", int64(-9007199254740993), 0.5, "utf8", "'-9007199254740993','0.5'"},
		{"a small integer beside a float", int64(42), 0.5, "float64", "42.0,0.5"},
		{"2^53 itself beside a float", int64(9007199254740992), 0.5, "float64", "9007199254740992.0,0.5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			dir := t.TempDir()
			src, err := sql.Open("sqlite", filepath.Join(dir, "src.db"))
			require.NoError(t, err)
			defer func() { _ = src.Close() }()
			_, err = src.ExecContext(ctx, `CREATE TABLE mix (v);`)
			require.NoError(t, err)
			_, err = src.ExecContext(ctx, `INSERT INTO mix VALUES (?), (?);`, tt.first, tt.second)
			require.NoError(t, err)

			out := filepath.Join(dir, "out")
			require.NoError(t, DumpDatabase(src, out, NewDumpOptions().WithFormat(OutputFormatParquet)))
			assertParquetSchema(t, filepath.Join(out, "mix.parquet"), map[string]string{"v": tt.wantType})

			back, err := OpenContext(ctx, filepath.Join(out, "mix.parquet"))
			require.NoError(t, err)
			defer func() { _ = back.Close() }()
			var got string
			require.NoError(t, back.QueryRowContext(ctx,
				`SELECT group_concat(quote(v)) FROM mix`).Scan(&got))
			assert.Equal(t, tt.wantBack, got)
		})
	}
}

// TestParquetDumpKeepsANumericColumnWithABlank pins the case one missing entry
// used to decide. SQLite stores a blank in a numeric column as the empty string,
// since "" has no numeric value to convert to, and the column's type was read
// from its values: one blank made the whole column a Parquet STRING, so a column
// of numbers reached the next tool as digits it compares and sorts as text, and
// reloading it here gave a TEXT column.
func TestParquetDumpKeepsANumericColumnWithABlank(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "t.csv")
	require.NoError(t, os.WriteFile(source, []byte("amount,qty,note\n2.50,7,here\n,,\n"), 0o600))

	db, err := OpenContext(ctx, source)
	require.NoError(t, err)

	out := filepath.Join(dir, "out")
	require.NoError(t, DumpDatabase(db, out, NewDumpOptions().WithFormat(OutputFormatParquet)))
	require.NoError(t, db.Close())

	assertParquetSchema(t, filepath.Join(out, "t.parquet"),
		map[string]string{"amount": "float64", "qty": "int64", "note": "utf8"})

	back, err := OpenContext(ctx, filepath.Join(out, "t.parquet"))
	require.NoError(t, err)
	defer back.Close()

	var amountKind, amount string
	require.NoError(t, back.QueryRowContext(ctx,
		"SELECT typeof(amount), amount FROM t WHERE qty = 7").Scan(&amountKind, &amount))
	assert.Equal(t, "real", amountKind)
	assert.Equal(t, "2.5", amount)

	// The blank has no spelling in a numeric column, so it comes back as the
	// null it means. A text column keeps the empty string it held.
	var blanks int
	require.NoError(t, back.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM t WHERE amount IS NULL AND qty IS NULL AND note = ''").Scan(&blanks))
	assert.Equal(t, 1, blanks, "the blank row is a null in the numeric columns and an empty string in the text one")
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

// assertParquetSchema checks the type of each named column in a Parquet file,
// spelled "int64", "float64" or "utf8".
func assertParquetSchema(t *testing.T, path string, want map[string]string) {
	t.Helper()

	data, err := os.ReadFile(path) //nolint:gosec // test-owned path
	if err != nil {
		t.Fatal(err)
	}
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	counts := map[string]int{}
	types := map[string]string{}
	for _, field := range file.Schema().Fields() {
		counts[field.Name()]++
		switch field.Type().Kind() {
		case parquet.Int64:
			types[field.Name()] = "int64"
		case parquet.Double:
			types[field.Name()] = "float64"
		case parquet.ByteArray:
			types[field.Name()] = "utf8"
		default:
			types[field.Name()] = field.Type().String()
		}
	}
	for name, wantType := range want {
		if counts[name] != 1 {
			t.Fatalf("column %q appears %d times in the parquet schema", name, counts[name])
		}
		if got := types[name]; got != wantType {
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

			if got := reader.SQLiteFloatText(tt.value, tt.bitSize); got != tt.want {
				t.Errorf("SQLiteFloatText(%v, %d) = %q, want %q", tt.value, tt.bitSize, got, tt.want)
			}
		})
	}
}

// writeFloat64Parquet writes a one-column Parquet file of doubles, so a test can
// hand the reader a value SQLite would not have let through on the way in.
func writeFloat64Parquet(t *testing.T, path, column string, values []float64) {
	t.Helper()

	schema := parquet.NewSchema("table", parquet.Group{
		column: parquet.Optional(parquet.Leaf(parquet.DoubleType)),
	})
	f, err := os.Create(path) //nolint:gosec // test-owned path
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	w := parquet.NewGenericWriter[any](f, schema)
	rows := make([]parquet.Row, 0, len(values))
	for _, v := range values {
		rows = append(rows, parquet.Row{parquet.DoubleValue(v).Level(0, 1, 0)})
	}
	if _, err := w.WriteRows(rows); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close parquet: %v", err)
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

// TestADamagedParquetFooterIsAnErrorNotAPanic pins that a Parquet file this
// package did not write cannot end the process.
//
// The Arrow library raises a nil dereference while parsing the footer of some
// damaged files, and the guard against that covered the table read alone, so a
// file whose footer was damaged crashed before the guard was reached. The
// offsets below are into the fixture this repository ships and were found by
// mutating it at random: of 400 files with one to four bytes changed, these four
// crashed and none of the other 396 did anything worse than fail. The magic
// bytes, the length and the footer offset are untouched in every one of them,
// which is why nothing checked before the handover rejects them.
func TestADamagedParquetFooterIsAnErrorNotAPanic(t *testing.T) {
	t.Parallel()

	fixture, err := os.ReadFile(filepath.Join("testdata", "products.parquet"))
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string]map[int]byte{
		"undamaged":                    {},
		"one byte of the schema":       {317: 19},
		"two bytes, schema and page":   {146: 19, 317: 19},
		"two bytes of the row group":   {351: 108, 669: 88},
		"three bytes across the file":  {102: 133, 333: 100, 486: 31},
		"three bytes, schema and page": {157: 171, 321: 76, 673: 8},
	}

	for name, damage := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			data := make([]byte, len(fixture))
			copy(data, fixture)
			for offset, value := range damage {
				data[offset] = value
			}

			path := filepath.Join(t.TempDir(), "products.parquet")
			if err := os.WriteFile(path, data, 0o600); err != nil { //nolint:gosec // path is under t.TempDir()
				t.Fatal(err)
			}

			db, err := OpenContext(context.Background(), path)
			if db != nil {
				defer db.Close()
			}
			if len(damage) == 0 {
				// The fixture as it stands has to load, so a guard that refused
				// every Parquet file would be caught here rather than passing.
				if err != nil {
					t.Fatalf("the undamaged fixture failed to load: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("a damaged parquet file loaded")
			}
			if !errors.Is(err, ErrParsing) {
				t.Fatalf("the failure is not reportable as a parse error: %v", err)
			}
		})
	}
}

// slowParquet is 433 bytes that hold OpenContext for as long as it is left to
// run, allocating roughly 350MiB a second while it does. It was found by
// fuzzing the Parquet reader, and its header was repaired afterwards so that the
// magic-bytes check does not answer for it: what makes it dangerous is its
// metadata, which declares column chunks lying past the end of a file this size.
//
// It is held as base64 rather than as a file under testdata so that what is
// wrong with it stays next to the test that says so.
const slowParquet = "UEFSMRUYFQAVCBUILBUGFRAVMBUwHBhvFTAZQTkEGAYwMDAwMDAVBjAVBCMwGAIwMDcwMDAw" +
	"MDAwMDAVDCMwGAQwMDAwIzBMHDAwMBUKJTAYBTAwMDAwMBYwGRwZMCbaMBwVMBkwMDAwGRgC" +
	"MDAVABYwFtIwFtIBJjAmCBwYIDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwWAgw" +
	"MDAwMDAwMDAZIBUwFTAVMDAVMBUwFTAwMDAmjDAcFTAZMDAwMBkYBDAwMDAVABYwFrIwFrIB" +
	"JrQwJtoBHDYwFjAYBTAwMDAwGAgwMDAwMDAwMDAZIBUwFTAVMDAVMBUwFTAwMDAm3jAcFTAZ" +
	"MDAwMBkYBTAwMDAwFQAWMBbSMBbSASbYMCaMAxwYCDAwMDAwMDAwGAgwMDAwMDAwMBYwFjAY" +
	"CDAwMDAwMDAwGAgwMDAwMDAwMDAZIBUwFTAVMDAVMBUwFTAwMDAW1jAWBiYwFtYwFDAwGQwY" +
	"IjAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAZMBwwMBwwMDAwMDCRAQAAUEFS" +
	"MQ=="

// TestADamagedParquetFileFailsQuickly pins that a Parquet file fails in time
// bounded by its own size.
//
// The file above declares 24 rows and column chunks of about three kilobytes
// inside 433 bytes, and its chunks begin at offsets 3098 and 3116, which are
// past the end of it. Handed to the decoder as it stands, it never returns: the
// page decoding neither checks the context it is given nor bounds what it
// allocates against the file it is reading, so a caller who bounds their own
// work cannot bound this and neither can a deadline passed down from here. A
// column chunk that does not lie inside the file is refused before any of it is
// decoded.
func TestADamagedParquetFileFailsQuickly(t *testing.T) {
	t.Parallel()

	data, err := base64.StdEncoding.DecodeString(slowParquet)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "slow.parquet")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		db, openErr := OpenContext(context.Background(), path)
		if db != nil {
			_ = db.Close()
		}
		done <- openErr
	}()

	select {
	case openErr := <-done:
		if openErr == nil {
			t.Fatal("a file whose column chunks lie outside it loaded")
		}
		if !errors.Is(openErr, ErrParsing) {
			t.Fatalf("the failure is not reportable as a parse error: %v", openErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a 433 byte file has held OpenContext for 10s")
	}
}

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

	// Order by name so the asserted row order is deterministic regardless of how
	// the dump/reload path returns rows.
	rows, err := rdb.QueryContext(ctx, "SELECT id FROM t ORDER BY name")
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

// TestParquetNullsAgreeBetweenTheParserAndTheLoader holds the two readings of
// one file against each other. The parser reports which cells hold nothing and
// the loader stores those cells as SQL NULL, so they have to be the same cells:
// the mask is what the loader binds as nil, and a caller comparing a parsed
// table against a loaded one would otherwise find them disagreeing about a
// column of nulls.
func TestParquetNullsAgreeBetweenTheParserAndTheLoader(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	src := filepath.Join(t.TempDir(), "t.csv")
	require.NoError(t, os.WriteFile(src, []byte("label,amount\nx,1\ny,2\n"), 0o600))

	db, err := OpenContext(ctx, src)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO t (label, amount) VALUES (NULL, 3), ('', 4)`)
	require.NoError(t, err)

	out := t.TempDir()
	require.NoError(t, DumpDatabase(db, out, NewDumpOptions().WithFormat(OutputFormatParquet)))
	require.NoError(t, db.Close())

	written := filepath.Join(out, "t.parquet")
	raw, err := os.ReadFile(written) //nolint:gosec // a file this test just wrote
	require.NoError(t, err)
	parsed, err := parser.Parse(bytes.NewReader(raw), parser.Parquet)
	require.NoError(t, err)
	require.NotNil(t, parsed.Nulls)

	reloaded, err := OpenContext(ctx, written)
	require.NoError(t, err)
	defer reloaded.Close()

	rows, err := reloaded.QueryContext(ctx, `SELECT typeof(label) FROM t ORDER BY amount`)
	require.NoError(t, err)
	defer rows.Close()

	var stored []bool
	for rows.Next() {
		var kind string
		require.NoError(t, rows.Scan(&kind))
		stored = append(stored, kind == "null")
	}
	require.NoError(t, rows.Err())

	parsedNulls := make([]bool, len(parsed.Nulls))
	for i, row := range parsed.Nulls {
		parsedNulls[i] = row[0]
	}
	assert.Equal(t, stored, parsedNulls,
		"the cells the parser marks are the cells the loader stores as NULL")
	assert.Equal(t, []bool{false, false, true, false}, stored,
		"one null and one empty string, in that order")
}
