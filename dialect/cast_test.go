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
