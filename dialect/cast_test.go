package dialect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// castDB opens an in-memory SQLite database with the helper functions
// registered, so a translated cast runs the way a caller's query would.
func castDB(t *testing.T) *sql.DB {
	t.Helper()
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// runDialect translates query for d and runs it, returning the single scalar it
// selects.
func runDialect(t *testing.T, db *sql.DB, d Dialect, query string) (sql.NullString, error) {
	t.Helper()
	translated, err := Translate(d, query)
	if err != nil {
		return sql.NullString{}, err
	}
	var got sql.NullString
	err = db.QueryRowContext(context.Background(), translated).Scan(&got)
	return got, err
}

// TestCastSemantics locks in the parts of a cast that SQLite's own CAST gets
// wrong for the source dialect: rounding instead of truncation, honoring the
// length and scale of a parameterized type, and validating a value the target
// type cannot represent instead of quietly coercing it.
func TestCastSemantics(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name    string
		dialect Dialect
		query   string
		want    string
		null    bool
	}{
		// A fractional value rounds on the way to an integer type in every
		// dialect; SQLite truncates toward zero.
		{"mysql rounds to signed", MySQL, `SELECT CAST(1.9 AS SIGNED)`, "2", false},
		{"mysql rounds a half away from zero", MySQL, `SELECT CAST(2.5 AS SIGNED)`, "3", false},
		{"postgresql rounds to integer", PostgreSQL, `SELECT CAST(1.9 AS INTEGER)`, "2", false},
		{"postgresql rounds a half to even", PostgreSQL, `SELECT CAST(2.5 AS INTEGER)`, "2", false},
		{"postgresql rounds an odd half up", PostgreSQL, `SELECT CAST(3.5 AS INTEGER)`, "4", false},
		{"googlesql rounds to int64", GoogleSQL, `SELECT CAST(1.9 AS INT64)`, "2", false},

		// MySQL coerces where the others raise.
		{"mysql coerces a non-numeric string", MySQL, `SELECT CAST('abc' AS SIGNED)`, "0", false},
		{"mysql takes a numeric prefix", MySQL, `SELECT CAST('12abc' AS SIGNED)`, "12", false},
		{"mysql takes the prefix before a second point", MySQL, `SELECT CAST('1.2.3' AS SIGNED)`, "1", false},
		{"mysql reads a version string as a number", MySQL, `SELECT CAST('10.5.2' AS DOUBLE)`, "10.5", false},
		{"mysql reads an exponent whole", MySQL, `SELECT CAST('1e5' AS DOUBLE)`, "100000", false},
		{"mysql nulls an invalid date", MySQL, `SELECT CAST('not a date' AS DATE)`, "", true},
		{"mysql keeps a valid date", MySQL, `SELECT CAST('2026-01-15' AS DATE)`, "2026-01-15", false},

		// A parameterized type keeps its scale and length.
		{"mysql decimal scale", MySQL, `SELECT CAST('3.567' AS DECIMAL(10,2))`, "3.57", false},
		{"mysql bare decimal is scale zero", MySQL, `SELECT CAST(1.5 AS DECIMAL)`, "2", false},
		{"mysql char length", MySQL, `SELECT CAST('abcdefghijk' AS CHAR(3))`, "abc", false},
		{"postgresql varchar length", PostgreSQL, `SELECT 'abcdef'::varchar(3)`, "abc", false},

		// PostgreSQL boolean literals survive instead of collapsing to 0.
		{"postgresql true", PostgreSQL, `SELECT 'true'::boolean`, "1", false},
		{"postgresql false", PostgreSQL, `SELECT 'false'::boolean`, "0", false},
		{"postgresql on", PostgreSQL, `SELECT 'on'::boolean`, "1", false},
		{"postgresql valid uuid", PostgreSQL, `SELECT '3F2504E0-4F89-11D3-9A0C-0305E82C3301'::uuid`, "3f2504e0-4f89-11d3-9a0c-0305e82c3301", false},
		{"postgresql valid json", PostgreSQL, `SELECT '{"a":1}'::jsonb`, `{"a":1}`, false},
		{"postgresql valid date", PostgreSQL, `SELECT '2026-01-15'::date`, "2026-01-15", false},
		{"postgresql null stays null", PostgreSQL, `SELECT NULL::integer`, "", true},

		// SAFE_CAST answers NULL where CAST would raise, which is its purpose.
		{"safe_cast invalid int64", GoogleSQL, `SELECT SAFE_CAST('abc' AS INT64)`, "", true},
		{"safe_cast valid int64", GoogleSQL, `SELECT SAFE_CAST('42' AS INT64)`, "42", false},
		{"safe_cast invalid float64", GoogleSQL, `SELECT SAFE_CAST('abc' AS FLOAT64)`, "", true},
		{"safe_cast valid bool", GoogleSQL, `SELECT SAFE_CAST('true' AS BOOL)`, "1", false},
		{"safe_cast invalid bool", GoogleSQL, `SELECT SAFE_CAST('nope' AS BOOL)`, "", true},
		{"safe_cast invalid date", GoogleSQL, `SELECT SAFE_CAST('2026-13-40' AS DATE)`, "", true},
		{"safe_cast valid date", GoogleSQL, `SELECT SAFE_CAST('2026-01-15' AS DATE)`, "2026-01-15", false},
		{"safe_cast invalid timestamp", GoogleSQL, `SELECT SAFE_CAST('not-a-timestamp' AS TIMESTAMP)`, "", true},
		{"safe_cast valid timestamp", GoogleSQL, `SELECT SAFE_CAST('2026-01-15 10:30:00' AS TIMESTAMP)`, "2026-01-15 10:30:00", false},

		// A value past the integer range is not an integer. MySQL clamps to the
		// bound of the type, which is the answer it gives with a warning; the
		// dialects that raise are covered in TestCastRejectsInvalidValues.
		{"mysql clamps a value above the range", MySQL, `SELECT CAST(1e30 AS SIGNED)`, "9223372036854775807", false},
		{"mysql clamps a value below the range", MySQL, `SELECT CAST(-1e30 AS SIGNED)`, "-9223372036854775808", false},
		{"mysql clamps an infinity", MySQL, `SELECT CAST(1e308*10 AS SIGNED)`, "9223372036854775807", false},
		{"mysql clamps a string past the range", MySQL, `SELECT CAST('99999999999999999999' AS SIGNED)`, "9223372036854775807", false},
		{"mysql keeps a value inside the range", MySQL, `SELECT CAST(9.2e18 AS SIGNED)`, "9200000000000000000", false},
		{"mysql keeps the largest integer", MySQL, `SELECT CAST(9223372036854775807 AS SIGNED)`, "9223372036854775807", false},
		{"mysql keeps the smallest integer", MySQL, `SELECT CAST(-9223372036854775808 AS SIGNED)`, "-9223372036854775808", false},
		{"safe_cast nulls a value past the range", GoogleSQL, `SELECT SAFE_CAST(1e30 AS INT64)`, "", true},
		{"safe_cast nulls a string past the range", GoogleSQL, `SELECT SAFE_CAST('99999999999999999999' AS INT64)`, "", true},
		{"safe_cast keeps a value inside the range", GoogleSQL, `SELECT SAFE_CAST(9.2e18 AS INT64)`, "9200000000000000000", false},
		// A digit string one past the range: no float64 tells it from the bound
		// itself, so the answer has to come from the integer parse.
		{"mysql clamps the string below the range", MySQL, `SELECT CAST('-9223372036854775809' AS SIGNED)`, "-9223372036854775808", false},
		{"mysql clamps the string above the range", MySQL, `SELECT CAST('9223372036854775808' AS SIGNED)`, "9223372036854775807", false},
		{"mysql keeps the string at the lower bound", MySQL, `SELECT CAST('-9223372036854775808' AS SIGNED)`, "-9223372036854775808", false},
		{"safe_cast nulls the string below the range", GoogleSQL, `SELECT SAFE_CAST('-9223372036854775809' AS INT64)`, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if tt.null {
				if got.Valid {
					t.Fatalf("%s = %q, want NULL", tt.query, got.String)
				}
				return
			}
			if !got.Valid || got.String != tt.want {
				t.Fatalf("%s = %v, want %q", tt.query, got, tt.want)
			}
		})
	}
}

// TestCastRejectsInvalidValues covers the casts that must fail. PostgreSQL and
// GoogleSQL raise for a value the target type cannot represent; letting it
// through is what made a validating query report success on bad rows.
// TestCastTargetsMatchTheEngine pins the three cast targets that used to hand
// the value back or answer a plausible wrong one, and the two MySQL spellings
// of the cast that reached SQLite raw. Every MySQL value was read from
// mysql:8.4 and every PostgreSQL one from postgres:17-alpine.
func TestCastTargetsMatchTheEngine(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		dialect Dialect
		query   string
		want    string
		null    bool
	}{
		// YEAR used to map onto the text conversion, so the cast handed its
		// argument straight back.
		{MySQL, `SELECT CAST('2024-03-05' AS YEAR)`, "2024", false},
		{MySQL, `SELECT CAST(1.9 AS YEAR)`, "2002", false},
		{MySQL, `SELECT CAST('12abc' AS YEAR)`, "2012", false},
		{MySQL, `SELECT CAST(70 AS YEAR)`, "1970", false},
		{MySQL, `SELECT CAST(69 AS YEAR)`, "2069", false},
		{MySQL, `SELECT CAST(1901 AS YEAR)`, "1901", false},
		{MySQL, `SELECT CAST(2155 AS YEAR)`, "2155", false},
		{MySQL, `SELECT CAST(100 AS YEAR)`, "", true},
		{MySQL, `SELECT CAST(1900 AS YEAR)`, "", true},
		{MySQL, `SELECT CAST(2156 AS YEAR)`, "", true},
		{MySQL, `SELECT CAST('1900-01-01' AS YEAR)`, "", true},
		{MySQL, `SELECT CAST(0 AS YEAR)`, "0", false},
		{MySQL, `SELECT CAST('abc' AS YEAR)`, "", true},
		{MySQL, `SELECT CAST('' AS YEAR)`, "", true},
		{MySQL, `SELECT CAST(-5 AS YEAR)`, "", true},
		{MySQL, `SELECT CAST(10000 AS YEAR)`, "", true},

		// A TIME with no clock in it is a number read right to left under MySQL
		// and a refusal under PostgreSQL, where formatting a date as a time
		// answered a midnight a caller cannot tell from a real one.
		{MySQL, `SELECT CAST('13:45:56' AS TIME)`, "13:45:56", false},
		{MySQL, `SELECT CAST('2024-03-05 13:45:56' AS TIME)`, "13:45:56", false},
		{MySQL, `SELECT CAST('2024-03-05' AS TIME)`, "00:20:24", false},
		{MySQL, `SELECT CAST(123456 AS TIME)`, "12:34:56", false},
		{MySQL, `SELECT CAST('12abc' AS TIME)`, "00:00:12", false},
		{MySQL, `SELECT CAST(' 7 ' AS TIME)`, "00:00:07", false},
		{MySQL, `SELECT CAST('-5' AS TIME)`, "-00:00:05", false},
		{MySQL, `SELECT CAST(-1.9 AS TIME)`, "-00:00:02", false},
		{MySQL, `SELECT CAST(9999999 AS TIME)`, "", true},
		{MySQL, `SELECT CAST('abc' AS TIME)`, "", true},

		// The precision of DECIMAL(p,s) bounds the magnitude. Applying the
		// scale and ignoring the precision let a value the type cannot hold
		// through unchanged.
		{MySQL, `SELECT CAST(12345 AS DECIMAL(3,0))`, "999", false},
		{MySQL, `SELECT CAST(-12345 AS DECIMAL(3,0))`, "-999", false},
		{MySQL, `SELECT CAST('2024-03-05' AS DECIMAL(4,1))`, "999.9", false},
		{MySQL, `SELECT CAST(1.5 AS DECIMAL(10,2))`, "1.5", false},

		// CONVERT and the BINARY prefix are the cast by MySQL's other two
		// spellings, and reach the same helper.
		{MySQL, `SELECT CONVERT('12abc', SIGNED)`, "12", false},
		{MySQL, `SELECT CONVERT('12abc', CHAR(3))`, "12a", false},
		{MySQL, `SELECT CONVERT('12abc', TIME)`, "00:00:12", false},
		{MySQL, `SELECT CONVERT('abc' USING utf8mb4)`, "abc", false},
		{MySQL, `SELECT HEX(BINARY 'abc')`, "616263", false},
		{MySQL, `SELECT HEX(CHAR(65, 66 USING utf8mb4))`, "4142", false},
	}

	for _, tt := range tests {
		got, err := runDialect(t, db, tt.dialect, tt.query)
		if err != nil {
			t.Errorf("%v: %s: %v", tt.dialect, tt.query, err)
			continue
		}
		if tt.null {
			if got.Valid {
				t.Errorf("%v: %s = %q, want NULL", tt.dialect, tt.query, got.String)
			}
			continue
		}
		if !got.Valid || got.String != tt.want {
			t.Errorf("%v: %s = %v, want %q", tt.dialect, tt.query, got, tt.want)
		}
	}

	refused := []struct {
		dialect Dialect
		query   string
	}{
		{PostgreSQL, `SELECT CAST('2024-03-05' AS time)`},
		{PostgreSQL, `SELECT CAST(12345 AS numeric(3,0))`},
		{PostgreSQL, `SELECT CAST('{1,2}' AS int[])`},
		{PostgreSQL, `SELECT '{1,2,3}'::int[]`},
		{MySQL, `SELECT CONVERT('abc' USING latin1)`},
	}
	for _, tt := range refused {
		if _, err := runDialect(t, db, tt.dialect, tt.query); err == nil {
			t.Errorf("%v: %s: want a refusal, got none", tt.dialect, tt.query)
		}
	}

	// A column named binary, and the type name inside a cast, are not the
	// prefix operator.
	kept := []string{
		"SELECT CAST('abc' AS BINARY)",
		"SELECT `binary` FROM t",
	}
	for _, query := range kept {
		if _, err := Translate(MySQL, query); err != nil {
			t.Errorf("Translate(mysql, %q): %v", query, err)
		}
	}
}

func TestCastRejectsInvalidValues(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name    string
		dialect Dialect
		query   string
	}{
		{"postgresql integer", PostgreSQL, `SELECT CAST('abc' AS INTEGER)`},
		{"postgresql boolean", PostgreSQL, `SELECT 'nope'::boolean`},
		{"postgresql date", PostgreSQL, `SELECT 'not-a-date'::date`},
		{"postgresql timestamp", PostgreSQL, `SELECT 'not-a-time'::timestamp`},
		{"postgresql time", PostgreSQL, `SELECT 'not-a-time'::time`},
		{"postgresql uuid", PostgreSQL, `SELECT 'not-a-uuid'::uuid`},
		{"postgresql uuid wrong group length", PostgreSQL, `SELECT '3f2504e0-4f89-11d3-9a0c-0305e82c33'::uuid`},
		{"postgresql uuid non-hex", PostgreSQL, `SELECT 'zf2504e0-4f89-11d3-9a0c-0305e82c3301'::uuid`},
		{"postgresql jsonb", PostgreSQL, `SELECT '{bad json}'::jsonb`},
		{"postgresql float", PostgreSQL, `SELECT 'abc'::float8`},
		{"googlesql int64", GoogleSQL, `SELECT CAST('abc' AS INT64)`},
		{"googlesql date", GoogleSQL, `SELECT CAST('2026-13-40' AS DATE)`},

		// A value past the integer range is a value the type cannot represent,
		// which is what these two dialects raise for.
		{"postgresql integer above the range", PostgreSQL, `SELECT (1e30)::bigint`},
		{"postgresql integer below the range", PostgreSQL, `SELECT (-1e30)::bigint`},
		{"postgresql integer from a string past the range", PostgreSQL, `SELECT '99999999999999999999'::bigint`},
		{"googlesql int64 above the range", GoogleSQL, `SELECT CAST(1e30 AS INT64)`},
		{"googlesql int64 from an infinity", GoogleSQL, `SELECT CAST(1e308*10 AS INT64)`},
		{"postgresql integer one below the range", PostgreSQL, `SELECT '-9223372036854775809'::bigint`},
		{"postgresql integer one above the range", PostgreSQL, `SELECT '9223372036854775808'::bigint`},
		{"googlesql int64 one below the range", GoogleSQL, `SELECT CAST('-9223372036854775809' AS INT64)`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := runDialect(t, db, tt.dialect, tt.query); err == nil {
				t.Fatalf("%s should fail", tt.query)
			}
		})
	}
}

// TestCastUnknownTypePassesThrough keeps a type this package does not model
// running as a plain SQLite CAST rather than failing the whole query.
func TestCastUnknownTypePassesThrough(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dialect Dialect
		query   string
		want    string
	}{
		// A pass-through still renames the column when the spelling changes, so
		// the original text comes back as an alias.
		{PostgreSQL, "SELECT a::inet", `SELECT CAST(a AS inet) AS "a::inet"`},
		{PostgreSQL, "SELECT CAST(a AS inet)", "SELECT CAST(a AS inet)"},
		{MySQL, "SELECT CAST(x AS GEOMETRY)", "SELECT CAST(x AS GEOMETRY)"},
		{GoogleSQL, "SELECT SAFE_CAST(x AS GEOGRAPHY)", `SELECT CAST(x AS GEOGRAPHY) AS "SAFE_CAST(x AS GEOGRAPHY)"`},
	}
	for _, tt := range tests {
		got, err := Translate(tt.dialect, tt.query)
		if err != nil {
			t.Fatalf("Translate(%s, %q): %v", tt.dialect, tt.query, err)
		}
		if got != tt.want {
			t.Fatalf("Translate(%s, %q) = %q, want %q", tt.dialect, tt.query, got, tt.want)
		}
	}
}

func TestParseCastTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target     string
		wantName   string
		wantParams []int
	}{
		{"INTEGER", "INTEGER", nil},
		{" decimal(10,2) ", "decimal", []int{10, 2}},
		{"CHAR(3)", "CHAR", []int{3}},
		{"varchar(n)", "varchar", nil},
		{"CHAR(", "CHAR(", nil},
	}
	for _, tt := range tests {
		name, params := parseCastTarget(tt.target)
		if name != tt.wantName || !reflect.DeepEqual(params, tt.wantParams) {
			t.Fatalf("parseCastTarget(%q) = (%q, %v), want (%q, %v)", tt.target, name, params, tt.wantName, tt.wantParams)
		}
	}
}

func TestNumericPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want float64
	}{
		{"12abc", 12},
		{"-3.5x", -3.5},
		{"abc", 0},
		{"", 0},
		{"+7", 7},
		{"..", 0},
		// A second dot ends the number rather than voiding it: MySQL answers
		// 1.2 for '1.2.3'+0, not 0.
		{"1.2.3", 1.2},
		{"10.5.2", 10.5},
		{".5.5", 0.5},
		{"1.", 1},
		{"1.2.", 1.2},
		// An exponent is part of the number MySQL reads, and a broken one is
		// not: '1e5'+0 is 100000 while '1e'+0 and '1e+'+0 are both 1.
		{"1e5", 100000},
		{"1E5", 100000},
		{"1e+5", 100000},
		{"2.5e-3", 0.0025},
		{"1e", 1},
		{"1e+", 1},
		{"1e5x", 100000},
		{"1.2.3e4", 1.2},
		{".e5", 0},
		{"--3", 0},
		{"-", 0},
		// A run past the range of a float64 is still a number, and MySQL
		// answers the bound of the type for it rather than 0.
		{"1e999abc", math.MaxFloat64},
		{"-1e999", -math.MaxFloat64},
	}
	for _, tt := range tests {
		if got := numericPrefix(tt.in); got != tt.want {
			t.Fatalf("numericPrefix(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

// TestCastValueUnknownDialect covers the defensive branch where a target type
// has no entry for the dialect: the value passes through unchanged.
func TestCastValueUnknownDialect(t *testing.T) {
	t.Parallel()

	got, err := castValue(MySQL, "GEOMETRY", "x")
	if err != nil {
		t.Fatalf("castValue: %v", err)
	}
	if got != "x" {
		t.Fatalf("castValue = %v, want %q", got, "x")
	}
}

// TestCastErrorIsInvalidCast keeps the sentinel usable with errors.Is, which is
// how SAFE_CAST tells an invalid value from a genuine failure.
func TestCastErrorIsInvalidCast(t *testing.T) {
	t.Parallel()

	_, err := castValue(PostgreSQL, "integer", "abc")
	if !errors.Is(err, ErrInvalidCast) {
		t.Fatalf("castValue error = %v, want ErrInvalidCast", err)
	}
}

// TestRoundForDialect covers the range rule directly, because SQLite has no way
// to carry a NaN into a query: a NaN real comes back as NULL, so the value can
// only reach the conversion from Go.
func TestRoundForDialect(t *testing.T) {
	t.Parallel()

	nan := math.NaN()
	tests := []struct {
		name    string
		dialect Dialect
		value   float64
		strict  bool
		want    int64
		wantErr bool
	}{
		{name: "mysql clamps NaN to zero", dialect: MySQL, value: nan, want: 0},
		{name: "googlesql rejects NaN", dialect: GoogleSQL, value: nan, strict: true, wantErr: true},
		{name: "mysql clamps above the range", dialect: MySQL, value: 1e30, want: math.MaxInt64},
		{name: "mysql clamps below the range", dialect: MySQL, value: -1e30, want: math.MinInt64},
		{name: "mysql clamps an infinity", dialect: MySQL, value: math.Inf(1), want: math.MaxInt64},
		{name: "mysql clamps a negative infinity", dialect: MySQL, value: math.Inf(-1), want: math.MinInt64},
		{name: "postgresql rejects above the range", dialect: PostgreSQL, value: 1e30, strict: true, wantErr: true},
		{name: "postgresql rejects below the range", dialect: PostgreSQL, value: -1e30, strict: true, wantErr: true},
		// The bound itself: no float64 holds the largest integer, so the nearest
		// one above the range is 2^63 and it does not fit, while -2^63 is exact
		// and does.
		{name: "the upper bound does not fit", dialect: MySQL, value: 9223372036854775808.0, want: math.MaxInt64},
		{name: "the lower bound fits", dialect: MySQL, value: -9223372036854775808.0, want: math.MinInt64},
		{name: "a value inside the range converts", dialect: MySQL, value: 9.2e18, want: 9200000000000000000},
		{name: "mysql rounds a half away from zero", dialect: MySQL, value: 2.5, want: 3},
		{name: "postgresql rounds a half to even", dialect: PostgreSQL, value: 2.5, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := roundForDialect(tt.dialect, tt.value, tt.strict)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidCast) {
					t.Fatalf("roundForDialect(%v, %v) error = %v, want ErrInvalidCast", tt.dialect, tt.value, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("roundForDialect(%v, %v) error = %v", tt.dialect, tt.value, err)
			}
			if got != driver.Value(tt.want) {
				t.Fatalf("roundForDialect(%v, %v) = %v, want %v", tt.dialect, tt.value, got, tt.want)
			}
		})
	}
}

// TestCastStringPastTheFloatRange covers the string a float64 cannot hold
// either. strconv.ParseFloat answers such a string with an infinity and
// ErrRange, and reading that as a parse failure sent the value down MySQL's
// numeric-prefix path, where it came back as 0 rather than as the bound of the
// type.
func TestCastStringPastTheFloatRange(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	huge := strings.Repeat("9", 400)

	got, err := runDialect(t, db, MySQL, "SELECT CAST('"+huge+"' AS SIGNED)")
	if err != nil {
		t.Fatalf("mysql: %v", err)
	}
	if want := "9223372036854775807"; got.String != want {
		t.Errorf("mysql clamps a string past the float range: got %v, want %q", got, want)
	}

	// The engine returns the helper's message as a SQL error rather than as the
	// wrapped Go error, which is why these assert on failing rather than on the
	// sentinel; the message is checked in TestCastErrorIsInvalidCast.
	if _, err := runDialect(t, db, PostgreSQL, "SELECT '"+huge+"'::bigint"); err == nil {
		t.Error("postgresql must reject a string past the float range")
	}
	if _, err := runDialect(t, db, GoogleSQL, "SELECT CAST('"+huge+"' AS INT64)"); err == nil {
		t.Error("googlesql must reject a string past the float range")
	}
}

// TestCastToBlob covers the BLOB target, which every dialect spells differently
// but which all of them mean as "the value's bytes".
func TestCastToBlob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value driver.Value
		want  string
		null  bool
	}{
		{name: "bytes pass through", value: []byte("abc"), want: "abc"},
		{name: "a string becomes its bytes", value: "abc", want: "abc"},
		{name: "a number becomes its digits", value: int64(255), want: "255"},
		{name: "a time becomes its written form", value: time.Date(2026, 7, 28, 13, 5, 9, 0, time.UTC), want: "2026-07-28 13:05:09"},
		{name: "a NULL has no bytes", value: nil, null: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := castToBlob(tt.value)
			if err != nil {
				t.Fatalf("castToBlob(%v) error: %v", tt.value, err)
			}
			if tt.null {
				if got != nil {
					t.Fatalf("castToBlob(%v) = %v, want NULL", tt.value, got)
				}
				return
			}
			b, ok := got.([]byte)
			if !ok {
				t.Fatalf("castToBlob(%v) = %T, want []byte", tt.value, got)
			}
			if string(b) != tt.want {
				t.Fatalf("castToBlob(%v) = %q, want %q", tt.value, b, tt.want)
			}
		})
	}
}

// TestCastToBool covers the two answers a non-boolean value gets. MySQL takes
// anything and reads it for truthiness; the other dialects refuse a value that
// is not a boolean, because silently reading "maybe" as false is a wrong answer
// rather than a missing one.
func TestCastToBool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   driver.Value
		strict  bool
		want    int64
		wantErr bool
	}{
		{name: "a true boolean", value: true, want: 1},
		{name: "a false boolean", value: false, want: 0},
		{name: "a non-zero integer", value: int64(7), want: 1},
		{name: "zero", value: int64(0), want: 0},
		{name: "a non-zero float", value: 0.5, want: 1},
		{name: "a zero float", value: 0.0, want: 0},
		{name: "the word yes", value: " YES ", want: 1},
		{name: "the word off", value: "off", want: 0},
		{name: "a word that is not a boolean reads as the number it spells", value: "maybe", want: 0},
		{name: "a word with a number in front of it is truthy", value: "1 maybe", want: 1},
		{name: "an empty value is not truthy", value: "", want: 0},
		{name: "a word that is not a boolean, strictly", value: "maybe", strict: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := castToBool(PostgreSQL, tt.value, tt.strict)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("castToBool(%v, strict) = %v, want an error", tt.value, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("castToBool(%v) error: %v", tt.value, err)
			}
			if got != tt.want {
				t.Fatalf("castToBool(%v) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}
