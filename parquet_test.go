package filesql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nao1215/filesql/internal/parser"
	"github.com/nao1215/filesql/internal/reader"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
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
	db, err := Open(ctx, filepath.Join("testdata", "products.parquet"))
	if err != nil {
		t.Fatalf("Open: %v", err)
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
	if err := DumpDatabase(context.Background(), src, out, NewDumpOptions().WithFormat(OutputFormatParquet)); err != nil {
		t.Fatalf("DumpDatabase: %v", err)
	}

	db, err := Open(ctx, filepath.Join(out, "codes.parquet"))
	if err != nil {
		t.Fatalf("Open: %v", err)
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
	csvDB, err := Open(ctx, csvPath)
	if err != nil {
		t.Fatalf("Open(csv): %v", err)
	}
	defer func() { _ = csvDB.Close() }()

	out := filepath.Join(dir, "out")
	if err := DumpDatabase(context.Background(), csvDB, out, NewDumpOptions().WithFormat(OutputFormatParquet)); err != nil {
		t.Fatalf("DumpDatabase: %v", err)
	}
	parquetDB, err := Open(ctx, filepath.Join(out, "goods.parquet"))
	if err != nil {
		t.Fatalf("Open(parquet): %v", err)
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

// TestParquetAndXLSXAgreeOnABooleanColumn covers the two typed formats this
// package reads. A workbook stores a boolean as 1 or 0 and draws it TRUE or
// FALSE, and loading the drawing made the column text: WHERE flag matched no
// row, SUM over it answered zero, and a join against the same column read from
// Parquet found nothing. Neither the values nor the errors said so.
func TestParquetAndXLSXAgreeOnABooleanColumn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()

	type flagRow struct {
		Flag bool   `parquet:"flag"`
		Name string `parquet:"name"`
	}
	parquetPath := filepath.Join(dir, "flags.parquet")
	pf, err := os.Create(parquetPath) //nolint:gosec // path from t.TempDir
	require.NoError(t, err)
	w := parquet.NewGenericWriter[flagRow](pf)
	_, err = w.Write([]flagRow{{Flag: true, Name: "yes"}, {Flag: false, Name: "no"}})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, pf.Close())

	bookPath := filepath.Join(dir, "flags.xlsx")
	f := excelize.NewFile()
	require.NoError(t, f.SetSheetName("Sheet1", "flags"))
	require.NoError(t, f.SetCellValue("flags", "A1", "flag"))
	require.NoError(t, f.SetCellValue("flags", "B1", "name"))
	require.NoError(t, f.SetCellBool("flags", "A2", true))
	require.NoError(t, f.SetCellValue("flags", "B2", "yes"))
	require.NoError(t, f.SetCellBool("flags", "A3", false))
	require.NoError(t, f.SetCellValue("flags", "B3", "no"))
	require.NoError(t, f.SaveAs(bookPath))
	require.NoError(t, f.Close())

	for _, path := range []string{parquetPath, bookPath} {
		db, err := Open(ctx, path)
		require.NoError(t, err, path)

		var declared, storage, first string
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT type FROM pragma_table_info('flags') WHERE name = 'flag'`).Scan(&declared))
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT typeof(flag), CAST(flag AS TEXT) FROM flags ORDER BY name DESC LIMIT 1`).Scan(&storage, &first))

		var truthy, sum int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM flags WHERE flag`).Scan(&truthy))
		require.NoError(t, db.QueryRowContext(ctx, `SELECT COALESCE(SUM(flag), 0) FROM flags`).Scan(&sum))

		assert.Equal(t, "INTEGER", declared, "%s: a boolean column is an integer column", path)
		assert.Equal(t, "integer", storage, "%s", path)
		assert.Equal(t, "1", first, "%s: a true loads as 1", path)
		assert.Equal(t, 1, truthy, "%s: WHERE flag finds the true row", path)
		assert.Equal(t, 1, sum, "%s: SUM counts the true row", path)
		require.NoError(t, db.Close())
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
	if err := DumpDatabase(context.Background(), db, out, NewDumpOptions().WithFormat(OutputFormatParquet)); err != nil {
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
	db, err := Open(ctx, path)
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
			require.NoError(t, DumpDatabase(context.Background(), src, out, NewDumpOptions().WithFormat(OutputFormatParquet)))
			assertParquetSchema(t, filepath.Join(out, "mix.parquet"), map[string]string{"v": tt.wantType})

			back, err := Open(ctx, filepath.Join(out, "mix.parquet"))
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

	db, err := Open(ctx, source)
	require.NoError(t, err)

	out := filepath.Join(dir, "out")
	require.NoError(t, DumpDatabase(context.Background(), db, out, NewDumpOptions().WithFormat(OutputFormatParquet)))
	require.NoError(t, db.Close())

	assertParquetSchema(t, filepath.Join(out, "t.parquet"),
		map[string]string{"amount": "float64", "qty": "int64", "note": "utf8"})

	back, err := Open(ctx, filepath.Join(out, "t.parquet"))
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
	if err := DumpDatabase(context.Background(), db, out, NewDumpOptions().WithFormat(OutputFormatParquet)); err != nil {
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
	if err := DumpDatabase(context.Background(), db, out, NewDumpOptions().WithFormat(OutputFormatParquet)); err != nil {
		t.Fatalf("DumpDatabase: %v", err)
	}
	assertParquetSchema(t, filepath.Join(out, "m.parquet"), map[string]string{"v": "float64"})

	reloaded, err := Open(ctx, filepath.Join(out, "m.parquet"))
	if err != nil {
		t.Fatalf("Open: %v", err)
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

	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
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

	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
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
		// The plain form runs from a ten-thousandth up to a hundred
		// quadrillion, which is where SQLite's own rendering leaves it. Go's
		// shortest 'g' leaves it at a million, so a load and a dump with
		// nothing in between rewrote 2500000 as 2.5e+06.
		{name: "just below the million Go leaves plain form at", value: 999999.5, bitSize: 64, want: "999999.5"},
		{name: "just above it", value: 1000000.5, bitSize: 64, want: "1000000.5"},
		{name: "a whole number keeps the suffix that keeps its column REAL", value: 2500000, bitSize: 64, want: "2500000.0"},
		{name: "a hundred million", value: 123456789.5, bitSize: 64, want: "123456789.5"},
		{name: "a ten-thousandth is the lower edge", value: 0.0001, bitSize: 64, want: "0.0001"},
		{name: "below it the exponent form is what SQLite writes", value: 0.00001, bitSize: 64, want: "1e-05"},
		{name: "the upper edge is plain", value: 1e16, bitSize: 64, want: "10000000000000000.0"},
		{name: "past it the exponent form again", value: 1e20, bitSize: 64, want: "1e+20"},
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

	t.Run("through Open", func(t *testing.T) {
		t.Parallel()

		var db *sql.DB
		var err error
		require.NotPanics(t, func() { db, err = Open(ctx, path) })
		if db != nil {
			defer db.Close()
		}
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrParsing)
	})

	t.Run("through AddReader", func(t *testing.T) {
		t.Parallel()

		require.NotPanics(t, func() {
			built, err := buildForTest(
				ctx, NewBuilder().
					AddReader(bytes.NewReader(data), "t", FileTypeParquet))

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
			db, err := Open(context.Background(), path)
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
		db, err := Open(context.Background(), path)
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

			db, err := Open(ctx, path)
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
			db, err := Open(ctx, path)
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

			db, err := Open(context.Background(), path)
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

// slowParquet is 433 bytes that hold Open for as long as it is left to
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
		db, openErr := Open(context.Background(), path)
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
		t.Fatal("a 433 byte file has held Open for 10s")
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
	if err := DumpDatabase(context.Background(), db, out, opts); err != nil {
		t.Fatalf("DumpDatabase: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(out, "*.parquet"))
	if err != nil || len(files) != 1 {
		t.Fatalf("expected one parquet file, got %v (err %v)", files, err)
	}

	rdb, err := Open(ctx, files[0])
	if err != nil {
		t.Fatalf("Open: %v", err)
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
	if err := DumpDatabase(context.Background(), db, out, NewDumpOptions().WithFormat(OutputFormatParquet)); err != nil {
		t.Fatalf("DumpDatabase: %v", err)
	}
	files, err := filepath.Glob(filepath.Join(out, "*.parquet"))
	if err != nil || len(files) != 1 {
		t.Fatalf("expected one parquet file, got %v (err %v)", files, err)
	}

	rdb, err := Open(ctx, files[0])
	if err != nil {
		t.Fatalf("Open: %v", err)
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

	db, err := Open(ctx, src)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO t (label, amount) VALUES (NULL, 3), ('', 4)`)
	require.NoError(t, err)

	out := t.TempDir()
	require.NoError(t, DumpDatabase(context.Background(), db, out, NewDumpOptions().WithFormat(OutputFormatParquet)))
	require.NoError(t, db.Close())

	written := filepath.Join(out, "t.parquet")
	raw, err := os.ReadFile(written) //nolint:gosec // a file this test just wrote
	require.NoError(t, err)
	parsed, err := parser.Parse(bytes.NewReader(raw), parser.Parquet)
	require.NoError(t, err)
	require.NotNil(t, parsed.Nulls)

	reloaded, err := Open(ctx, written)
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

// TestParquetOverwriteKeepsTheSchema pins that a save writing a Parquet file
// back to itself leaves the schema it found.
//
// A Parquet file's types are the reason it is a Parquet file. The save wrote
// every column as one of three shapes -- int64, double, string, all optional --
// so a DECIMAL came back a string, a TIMESTAMP, a DATE and a BOOLEAN came back
// integers, and a required column came back nullable, with no edit needed to
// cause it. The values were unaffected, so a reload through this package said
// nothing was wrong; the damage was to what every other reader of the file sees.
func TestParquetOverwriteKeepsTheSchema(t *testing.T) {
	t.Parallel()

	var uuid [16]byte
	for i := range uuid {
		uuid[i] = byte(i * 17)
	}

	for _, tt := range []struct {
		name   string
		node   parquet.Node
		values []parquet.Value
		// stmt is run before the save; an empty one saves with nothing edited.
		stmt string
	}{
		{"a boolean", parquet.Leaf(parquet.BooleanType), []parquet.Value{
			parquet.BooleanValue(true), parquet.BooleanValue(false)}, ""},
		{"a date", parquet.Date(), []parquet.Value{
			parquet.Int32Value(19797), parquet.Int32Value(0)}, ""},
		{"a timestamp", parquet.Timestamp(parquet.Millisecond), []parquet.Value{
			parquet.Int64Value(1710460800000), parquet.Int64Value(0)}, ""},
		{"a decimal on an int64", parquet.Decimal(2, 12, parquet.Int64Type), []parquet.Value{
			parquet.Int64Value(12345), parquet.Int64Value(-5)}, ""},
		{"a decimal on an int32", parquet.Decimal(2, 9, parquet.Int32Type), []parquet.Value{
			parquet.Int32Value(12345), parquet.Int32Value(0)}, ""},
		{"a uuid", parquet.UUID(), []parquet.Value{
			parquet.FixedLenByteArrayValue(uuid[:])}, ""},
		{"an unsigned integer", parquet.Uint(32), []parquet.Value{
			parquet.Int32Value(-1), parquet.Int32Value(0)}, ""},
		{"a narrow integer", parquet.Int(16), []parquet.Value{
			parquet.Int32Value(-32768), parquet.Int32Value(32767)}, ""},
		{"a 32-bit float", parquet.Leaf(parquet.FloatType), []parquet.Value{
			parquet.FloatValue(1.5), parquet.FloatValue(0.1)}, ""},
		{"an enum", parquet.Enum(), []parquet.Value{
			parquet.ByteArrayValue([]byte("RED"))}, ""},
		{"a time", parquet.Time(parquet.Millisecond), []parquet.Value{
			parquet.Int32Value(86399999)}, ""},
		// The same, with the caller having changed a value: the column keeps
		// its type and holds what they set.
		{"a boolean the caller changed", parquet.Leaf(parquet.BooleanType), []parquet.Value{
			parquet.BooleanValue(true), parquet.BooleanValue(false)}, "UPDATE t SET v = 0 WHERE v = 1"},
		{"a decimal the caller changed", parquet.Decimal(2, 12, parquet.Int64Type), []parquet.Value{
			parquet.Int64Value(12345), parquet.Int64Value(-5)}, "UPDATE t SET v = '999.99' WHERE v = '123.45'"},
		{"a date the caller changed", parquet.Date(), []parquet.Value{
			parquet.Int32Value(19797), parquet.Int32Value(0)}, "UPDATE t SET v = 20000 WHERE v = 19797"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "t.parquet")
			writeOneFieldParquet(t, path, tt.node, tt.values)

			want := parquetSchemaText(t, path)
			require.NoError(t, autoSaveOverwrite(t, []string{path}, statements(tt.stmt)...))
			assert.Equal(t, want, parquetSchemaText(t, path))
		})
	}

	t.Run("a required column of text holding an empty string stays required", func(t *testing.T) {
		t.Parallel()

		// Empty text is a value in a column of bytes and a missing cell
		// everywhere else. Reading it as missing wrote a null over the empty
		// string, and made the column optional to hold the null it invented.
		path := filepath.Join(t.TempDir(), "t.parquet")
		writeOneFieldParquet(t, path, parquet.String(), []parquet.Value{
			parquet.ByteArrayValue([]byte("")), parquet.ByteArrayValue([]byte("x"))})

		require.NoError(t, autoSaveOverwrite(t, []string{path}))

		assert.Equal(t, []string{"v STRING optional=false"}, parquetSchemaText(t, path))
		assert.Equal(t, []string{"", "x"}, parquetColumnText(t, path))
		assert.Equal(t, []string{`""`, `"x"`}, parquetStoredCells(t, path),
			"the empty string is a value the file holds, not a null")
	})

	t.Run("a required column stays required", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "t.parquet")
		writeOneFieldParquet(t, path, parquet.Leaf(parquet.Int64Type), []parquet.Value{parquet.Int64Value(1)})

		want := parquetSchemaText(t, path)
		require.Contains(t, want[0], "optional=false")
		require.NoError(t, autoSaveOverwrite(t, []string{path}))
		assert.Equal(t, want, parquetSchemaText(t, path))
	})

	t.Run("a required column holding a null the caller set becomes optional", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "t.parquet")
		writeOneFieldParquet(t, path, parquet.Date(), []parquet.Value{parquet.Int32Value(19797)})

		require.NoError(t, autoSaveOverwrite(t, []string{path}, "UPDATE t SET v = NULL"))

		// The type is what the file said; only what the data now demands
		// changes, since a required column cannot hold the null the caller put
		// in it.
		assert.Equal(t, []string{"v DATE optional=true"}, parquetSchemaText(t, path))
	})
}

// TestParquetOverwriteFallsBackRatherThanWriteAWrongValue pins the boundary
// that makes the schema worth keeping at all: a value the caller set that the
// file's type cannot hold sends the column back to the plainer shape rather
// than being narrowed into a different number.
func TestParquetOverwriteFallsBackRatherThanWriteAWrongValue(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		node parquet.Node
		seed []parquet.Value
		stmt string
		// want is the value the column holds afterwards, whatever type it
		// ended up written as. The value is what must not change.
		want string
	}{
		{
			// 999999999999.99 unscaled is past int32, and narrowing it wrote
			// 276447231, which reads back as 2764472.31.
			name: "a decimal past what its physical type holds",
			node: parquet.Decimal(2, 9, parquet.Int32Type),
			seed: []parquet.Value{parquet.Int32Value(12345)},
			stmt: "UPDATE t SET v = '999999999999.99'",
			want: "999999999999.99",
		},
		{
			name: "a decimal with more places than its scale",
			node: parquet.Decimal(2, 9, parquet.Int32Type),
			seed: []parquet.Value{parquet.Int32Value(12345)},
			stmt: "UPDATE t SET v = '1.234'",
			want: "1.234",
		},
		{
			// parquet-go does not enforce an annotation's range, so this is
			// the one an explicit check has to catch.
			name: "an integer past the width its annotation names",
			node: parquet.Int(8),
			seed: []parquet.Value{parquet.Int32Value(12)},
			stmt: "UPDATE t SET v = 300",
			want: "300",
		},
		{
			name: "a boolean set to something that is not one",
			node: parquet.Leaf(parquet.BooleanType),
			seed: []parquet.Value{parquet.BooleanValue(true)},
			stmt: "UPDATE t SET v = 2",
			want: "2",
		},
		{
			name: "a uuid set to text that is not one",
			node: parquet.UUID(),
			seed: []parquet.Value{parquet.FixedLenByteArrayValue(make([]byte, 16))},
			stmt: "UPDATE t SET v = 'not a uuid'",
			want: "not a uuid",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "t.parquet")
			writeOneFieldParquet(t, path, tt.node, tt.seed)
			require.NoError(t, autoSaveOverwrite(t, []string{path}, tt.stmt))

			db, err := Open(t.Context(), path)
			require.NoError(t, err)
			defer db.Close()

			var got string
			require.NoError(t, db.QueryRowContext(t.Context(), `SELECT v FROM t`).Scan(&got))
			assert.Equal(t, tt.want, got, "the value the caller set is what the file holds")
		})
	}
}

// statements drops an empty statement, so a case that edits nothing runs none.
func statements(stmt string) []string {
	if stmt == "" {
		return nil
	}
	return []string{stmt}
}

// writeOneFieldParquet writes a file of a single required column named v.
func writeOneFieldParquet(t *testing.T, path string, node parquet.Node, values []parquet.Value) {
	t.Helper()

	schema := parquet.NewSchema("t", parquet.Group{"v": parquet.Required(node)})
	f, err := os.Create(path) //nolint:gosec // the path is the test's own temporary directory
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	writer := parquet.NewGenericWriter[any](f, schema)
	rows := make([]parquet.Row, 0, len(values))
	for _, value := range values {
		rows = append(rows, parquet.Row{value.Level(0, 0, 0)})
	}
	_, err = writer.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
}

// parquetSchemaText is a file's schema as one line per field, which is what a
// reader of the file sees and what a save must not change.
func parquetSchemaText(t *testing.T, path string) []string {
	t.Helper()

	f, err := os.Open(path) //nolint:gosec // the path is the test's own temporary directory
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	info, err := f.Stat()
	require.NoError(t, err)
	file, err := parquet.OpenFile(f, info.Size())
	require.NoError(t, err)

	out := make([]string, 0, len(file.Schema().Fields()))
	for _, field := range file.Schema().Fields() {
		out = append(out, fmt.Sprintf("%s %v optional=%v", field.Name(), field.Type(), field.Optional()))
	}
	return out
}

// TestParquetOverwriteLeavesWhatItCannotRebuild pins the boundary of keeping a
// file's schema. Three types are rendered by a load in a form that says less
// than the value they store, so nothing can rebuild them: a list or a map
// becomes text such as "[1 2 3]", an INT96 becomes nanoseconds since the Unix
// epoch rather than the Julian day and offset it holds, and a FLOAT16 is
// widened to 32 bits. A column of one of those is written the way every column
// was written before the schema was kept at all.
func TestParquetOverwriteLeavesWhatItCannotRebuild(t *testing.T) {
	t.Parallel()

	t.Run("a list", func(t *testing.T) {
		t.Parallel()

		type listRow struct {
			ID   int64   `parquet:"id"`
			Tags []int32 `parquet:"tags,list"`
		}
		path := filepath.Join(t.TempDir(), "t.parquet")
		writeTypedParquet(t, path, []listRow{{ID: 1, Tags: []int32{1, 2, 3}}})

		require.NoError(t, autoSaveOverwrite(t, []string{path}))

		// The flat column beside it keeps its type; the list does not, and the
		// values still read back.
		schema := parquetSchemaText(t, path)
		assert.Equal(t, "id INT(64,true) optional=false", schema[0])

		db, err := Open(t.Context(), path)
		require.NoError(t, err)
		defer db.Close()
		var id int64
		var tags string
		require.NoError(t, db.QueryRowContext(t.Context(), `SELECT id, tags FROM t`).Scan(&id, &tags))
		assert.Equal(t, int64(1), id)
		assert.Equal(t, "[1 2 3]", tags)
	})

	// An INT96 and a FLOAT16 are the other two, and neither is reachable from
	// here: the library underneath writes no INT96 column and has no way to put
	// a FLOAT16 annotation on one, so the guard against them is for a file
	// another writer produced.
}

// TestParquetOverwriteKeepsFixedBytes covers the fixed-length bytes a file may
// hold with no annotation at all, which a load renders as the bytes themselves
// and a save can therefore write back as what they were.
func TestParquetOverwriteKeepsFixedBytes(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "t.parquet")
	writeOneFieldParquet(t, path, parquet.Leaf(parquet.FixedLenByteArrayType(2)),
		[]parquet.Value{parquet.FixedLenByteArrayValue([]byte{0x00, 0x3e})})

	want := parquetSchemaText(t, path)
	before := parquetColumnText(t, path)
	require.NoError(t, autoSaveOverwrite(t, []string{path}))

	assert.Equal(t, want, parquetSchemaText(t, path))
	assert.Equal(t, before, parquetColumnText(t, path))
}

// TestParquetOverwriteKeepsAnInfinity covers the one spelling of a number this
// package writes that is not a number: a REAL column holding an infinity is
// stored as a literal SQLite overflows while reading it, and rebuilding the
// value has to read that spelling back.
func TestParquetOverwriteKeepsAnInfinity(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "t.parquet")
	writeOneFieldParquet(t, path, parquet.Leaf(parquet.DoubleType), []parquet.Value{
		parquet.DoubleValue(math.Inf(1)), parquet.DoubleValue(math.Inf(-1)), parquet.DoubleValue(1.5)})

	want := parquetSchemaText(t, path)
	require.NoError(t, autoSaveOverwrite(t, []string{path}))
	assert.Equal(t, want, parquetSchemaText(t, path))

	db, err := Open(t.Context(), path)
	require.NoError(t, err)
	defer db.Close()
	rows, err := db.QueryContext(t.Context(), `SELECT v FROM t`)
	require.NoError(t, err)
	defer rows.Close()
	got := make([]float64, 0, 3)
	for rows.Next() {
		var v float64
		require.NoError(t, rows.Scan(&v))
		got = append(got, v)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []float64{math.Inf(1), math.Inf(-1), 1.5}, got)
}

// TestParquetExportIgnoresAnyFileAlreadyThere pins that only a save writing
// back to the file it loaded keeps a schema. An export names its own
// destination, and a file that happens to sit there is not one it read.
func TestParquetExportIgnoresAnyFileAlreadyThere(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "t.parquet")
	writeOneFieldParquet(t, path, parquet.Date(), []parquet.Value{parquet.Int32Value(19797)})

	db := openWithTable(t, "CREATE TABLE t (v INTEGER)", "INSERT INTO t VALUES (19797)")
	require.NoError(t, DumpDatabase(context.Background(), db, dir, NewDumpOptions().WithFormat(OutputFormatParquet)))

	assert.Equal(t, []string{"v INT(64,true) optional=true"}, parquetSchemaText(t, path),
		"an export writes the table it was given, not the file that was there")
}

// writeTypedParquet writes a file from a Go value, for a shape a hand-built
// schema cannot express as easily.
func writeTypedParquet[T any](t *testing.T, path string, rows []T) {
	t.Helper()

	f, err := os.Create(path) //nolint:gosec // the path is the test's own temporary directory
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	writer := parquet.NewGenericWriter[T](f)
	_, err = writer.Write(rows)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
}

// TestParquetOverwriteKeepsADecimalOnFixedBytes covers the decimal a column
// stores across fixed bytes rather than in an integer, which is how a decimal
// wider than eighteen digits is held. The unscaled value goes back big-endian
// in two's complement, so the sign is the part worth pinning: a negative value
// written as if it were positive reads back as an enormous positive one.
func TestParquetOverwriteKeepsADecimalOnFixedBytes(t *testing.T) {
	t.Parallel()

	node := parquet.Decimal(4, 38, parquet.FixedLenByteArrayType(16))

	t.Run("the values come back as themselves", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "t.parquet")
		writeOneFieldParquet(t, path, node, []parquet.Value{
			fixedDecimal(t, "-123456789012345678901234"),
			fixedDecimal(t, "1"),
			fixedDecimal(t, "0"),
			fixedDecimal(t, "-1"),
		})

		want := parquetSchemaText(t, path)
		before := parquetColumnText(t, path)
		require.NoError(t, autoSaveOverwrite(t, []string{path}))

		assert.Equal(t, want, parquetSchemaText(t, path))
		assert.Equal(t, before, parquetColumnText(t, path))
		assert.Equal(t, []string{"-12345678901234567890.1234", "0.0001", "0.0000", "-0.0001"}, before)
	})

	t.Run("a value the fixed width cannot hold falls back", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "t.parquet")
		writeOneFieldParquet(t, path, node, []parquet.Value{fixedDecimal(t, "1")})

		// Thirty-nine digits is past the precision the column declares, so the
		// value is what must survive rather than the type.
		require.NoError(t, autoSaveOverwrite(t, []string{path},
			"UPDATE t SET v = '99999999999999999999999999999999999.9999'"))
		assert.Equal(t, []string{"99999999999999999999999999999999999.9999"}, parquetColumnText(t, path))
	})
}

// TestTwosComplementBytes covers the packing directly, including the widths
// that cannot hold a value, which a file is unlikely to produce but a caller's
// edit can ask for.
func TestTwosComplementBytes(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		value string
		width int
		want  []byte
		ok    bool
	}{
		{"zero", "0", 2, []byte{0x00, 0x00}, true},
		{"one", "1", 2, []byte{0x00, 0x01}, true},
		{"minus one", "-1", 2, []byte{0xff, 0xff}, true},
		{"the most negative the width holds", "-32768", 2, []byte{0x80, 0x00}, true},
		{"the most positive the width holds", "32767", 2, []byte{0x7f, 0xff}, true},
		{"one past the positive end", "32768", 2, nil, false},
		{"one past the negative end", "-32769", 2, nil, false},
		{"no width at all", "1", 0, nil, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			n, ok := new(big.Int).SetString(tt.value, 10)
			require.True(t, ok)
			got, ok := twosComplementBytes(n, tt.width)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

// fixedDecimal packs an unscaled integer the way a decimal on sixteen fixed
// bytes stores it, for building the file a test starts from.
func fixedDecimal(t *testing.T, digits string) parquet.Value {
	t.Helper()

	const width = 16
	n, ok := new(big.Int).SetString(digits, 10)
	require.True(t, ok)
	packed, ok := twosComplementBytes(n, width)
	require.True(t, ok)
	return parquet.FixedLenByteArrayValue(packed)
}

// parquetStoredCells is the column v as the file itself holds it, which is the
// only place a null and an empty string look different.
func parquetStoredCells(t *testing.T, path string) []string {
	t.Helper()

	f, err := os.Open(path) //nolint:gosec // the path is the test's own temporary directory
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	info, err := f.Stat()
	require.NoError(t, err)

	rows, err := parquet.Read[any](f, info.Size())
	require.NoError(t, err)
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		fields, ok := row.(map[string]any)
		require.True(t, ok, "a row reads back as its fields")
		value := fields["v"]
		if value == nil {
			out = append(out, "<null>")
			continue
		}
		out = append(out, fmt.Sprintf("%q", value))
	}
	return out
}

// parquetColumnText is the column v of a file, as a load reads it.
func parquetColumnText(t *testing.T, path string) []string {
	t.Helper()

	db, err := Open(t.Context(), path)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	rows, err := db.QueryContext(t.Context(), `SELECT v FROM t`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var out []string
	for rows.Next() {
		var v sql.NullString
		require.NoError(t, rows.Scan(&v))
		out = append(out, v.String)
	}
	require.NoError(t, rows.Err())
	return out
}
