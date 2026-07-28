package dialect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"regexp"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestRegisterFunctionsIdempotent(t *testing.T) {
	// Not parallel: touches the process-global driver registration.
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() second call error: %v", err)
	}
}

// TestUDFExecution runs each registered UDF through a real SQLite connection so
// the registration wiring and argument coercion are exercised end to end.
func TestUDFExecution(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"regexp match", `SELECT 'abc123' REGEXP '[0-9]+'`, "1"},
		{"regexp no match", `SELECT 'abc' REGEXP '[0-9]+'`, "0"},
		{"regexp func form", `SELECT REGEXP('^a', 'abc')`, "1"},
		{"if true", `SELECT IF(1, 'yes', 'no')`, "yes"},
		{"if false", `SELECT IF(0, 'yes', 'no')`, "no"},
		{"if null cond", `SELECT IF(NULL, 'yes', 'no')`, "no"},
		{"date_format", `SELECT DATE_FORMAT('2026-07-28 13:05:09', '%Y/%m/%d %H:%i')`, "2026/07/28 13:05"},
		{"date_format month name", `SELECT DATE_FORMAT('2026-07-28', '%M %e, %Y')`, "July 28, 2026"},
		{"str_to_date", `SELECT STR_TO_DATE('2026-07-28', '%Y-%m-%d')`, "2026-07-28 00:00:00"},
		{"datediff", `SELECT DATEDIFF('2026-07-28', '2026-07-25')`, "3"},
		{"date_part year", `SELECT DATE_PART('year', '2026-07-28')`, "2026"},
		{"year", `SELECT YEAR('2026-07-28')`, "2026"},
		{"month", `SELECT MONTH('2026-07-28')`, "7"},
		{"day", `SELECT DAY('2026-07-28')`, "28"},
		{"hour", `SELECT HOUR('2026-07-28 13:05:09')`, "13"},
		{"dayofweek", `SELECT DAYOFWEEK('2026-07-28')`, "3"}, // Tuesday -> 3
		{"weekday", `SELECT WEEKDAY('2026-07-28')`, "1"},     // Tuesday -> 1
		{"dayofyear", `SELECT DAYOFYEAR('2026-01-10')`, "10"},
		{"locate", `SELECT LOCATE('b', 'abcabc')`, "2"},
		{"locate with pos", `SELECT LOCATE('b', 'abcabc', 3)`, "5"},
		{"locate not found", `SELECT LOCATE('z', 'abc')`, "0"},
		{"lpad", `SELECT LPAD('7', 3, '0')`, "007"},
		{"rpad", `SELECT RPAD('7', 3, '-')`, "7--"},
		{"lpad truncates", `SELECT LPAD('abcdef', 3, 'x')`, "abc"},
		{"substring_index pos", `SELECT SUBSTRING_INDEX('a.b.c.d', '.', 2)`, "a.b"},
		{"substring_index neg", `SELECT SUBSTRING_INDEX('a.b.c.d', '.', -2)`, "c.d"},
		{"repeat", `SELECT REPEAT('ab', 3)`, "ababab"},
		{"space", `SELECT '[' || SPACE(3) || ']'`, "[   ]"},
		{"truncate", `SELECT TRUNCATE(3.14159, 2)`, "3.14"},
		{"truncate negative digits", `SELECT TRUNCATE(1234.5, -2)`, "1200"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got sql.NullString
			if err := db.QueryRowContext(context.Background(), tt.sql).Scan(&got); err != nil {
				t.Fatalf("%s: query error: %v", tt.sql, err)
			}
			if got.String != tt.want {
				t.Fatalf("%s = %q, want %q", tt.sql, got.String, tt.want)
			}
		})
	}
}

// TestPostgreSQLUDFExecution runs the PostgreSQL helper UDFs through a real
// SQLite connection.
func TestPostgreSQLUDFExecution(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"to_char date", `SELECT TO_CHAR('2026-07-28 13:05:09', 'YYYY-MM-DD HH24:MI:SS')`, "2026-07-28 13:05:09"},
		{"to_char month name", `SELECT TO_CHAR('2026-07-28', 'FMMonth DD, YYYY')`, "July 28, 2026"},
		{"to_date", `SELECT TO_DATE('28/07/2026', 'DD/MM/YYYY')`, "2026-07-28"},
		{"date_trunc month", `SELECT DATE_TRUNC('month', '2026-07-28 13:05:09')`, "2026-07-01 00:00:00"},
		{"date_trunc year", `SELECT DATE_TRUNC('year', '2026-07-28')`, "2026-01-01 00:00:00"},
		{"date_trunc quarter", `SELECT DATE_TRUNC('quarter', '2026-07-28')`, "2026-07-01 00:00:00"},
		{"date_trunc week", `SELECT DATE_TRUNC('week', '2026-07-28')`, "2026-07-27 00:00:00"}, // Monday
		{"split_part", `SELECT SPLIT_PART('a,b,c', ',', 2)`, "b"},
		{"split_part neg", `SELECT SPLIT_PART('a,b,c', ',', -1)`, "c"},
		{"split_part out of range", `SELECT SPLIT_PART('a,b', ',', 5)`, ""},
		{"initcap", `SELECT INITCAP('hello WORLD_foo')`, "Hello World_Foo"},
		{"strpos", `SELECT STRPOS('abcabc', 'c')`, "3"},
		{"strpos not found", `SELECT STRPOS('abc', 'z')`, "0"},
		{"left", `SELECT LEFT('abcdef', 3)`, "abc"},
		{"left negative", `SELECT LEFT('abcdef', -2)`, "abcd"},
		{"right", `SELECT RIGHT('abcdef', 2)`, "ef"},
		{"right negative", `SELECT RIGHT('abcdef', -2)`, "cdef"},
		{"regexp_replace", `SELECT REGEXP_REPLACE('foobar', 'o+', 'O')`, "fObar"},
		{"regexp_replace backref", `SELECT REGEXP_REPLACE('2026-07', '(\d+)-(\d+)', '\2/\1')`, "07/2026"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got sql.NullString
			if err := db.QueryRowContext(context.Background(), tt.sql).Scan(&got); err != nil {
				t.Fatalf("%s: query error: %v", tt.sql, err)
			}
			if got.String != tt.want {
				t.Fatalf("%s = %q, want %q", tt.sql, got.String, tt.want)
			}
		})
	}
}

// TestGoogleSQLUDFExecution runs the GoogleSQL helper UDFs through a real SQLite
// connection.
func TestGoogleSQLUDFExecution(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"safe_divide", `SELECT SAFE_DIVIDE(10, 4)`, "2.5"},
		{"safe_divide by zero", `SELECT IFNULL(CAST(SAFE_DIVIDE(1, 0) AS TEXT), 'null')`, "null"},
		{"starts_with true", `SELECT STARTS_WITH('foobar', 'foo')`, "1"},
		{"starts_with false", `SELECT STARTS_WITH('foobar', 'bar')`, "0"},
		{"ends_with", `SELECT ENDS_WITH('foobar', 'bar')`, "1"},
		{"regexp_contains", `SELECT REGEXP_CONTAINS('abc123', '[0-9]+')`, "1"},
		{"regexp_extract group", `SELECT REGEXP_EXTRACT('id=42', '=(\d+)')`, "42"},
		{"regexp_extract whole", `SELECT REGEXP_EXTRACT('abc123', '[0-9]+')`, "123"},
		{"date_diff day", `SELECT DATE_DIFF('2026-07-28', '2026-07-25', 'day')`, "3"},
		{"date_diff week", `SELECT DATE_DIFF('2026-07-28', '2026-07-14', 'week')`, "2"},
		{"date_diff month", `SELECT DATE_DIFF('2026-07-01', '2026-01-01', 'month')`, "6"},
		{"date_diff quarter", `SELECT DATE_DIFF('2026-07-01', '2026-01-01', 'quarter')`, "2"},
		{"date_diff year", `SELECT DATE_DIFF('2026-01-01', '2020-01-01', 'year')`, "6"},
		{"timestamp_diff hour", `SELECT TIMESTAMP_DIFF('2026-01-01 05:00:00', '2026-01-01 00:00:00', 'hour')`, "5"},
		{"timestamp_diff minute", `SELECT TIMESTAMP_DIFF('2026-01-01 00:30:00', '2026-01-01 00:00:00', 'minute')`, "30"},
		{"timestamp_diff second", `SELECT TIMESTAMP_DIFF('2026-01-01 00:00:10', '2026-01-01 00:00:01', 'second')`, "9"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got sql.NullString
			if err := db.QueryRowContext(context.Background(), tt.sql).Scan(&got); err != nil {
				t.Fatalf("%s: query error: %v", tt.sql, err)
			}
			if got.String != tt.want {
				t.Fatalf("%s = %q, want %q", tt.sql, got.String, tt.want)
			}
		})
	}

	// GENERATE_UUID returns a well-formed v4 UUID.
	var uuid string
	if err := db.QueryRowContext(context.Background(), `SELECT GENERATE_UUID()`).Scan(&uuid); err != nil {
		t.Fatalf("GENERATE_UUID: %v", err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(uuid) {
		t.Fatalf("GENERATE_UUID() = %q, not a v4 UUID", uuid)
	}
}

// TestUDFNullHandling verifies that NULL arguments propagate to NULL for the
// string/number helpers.
func TestUDFNullHandling(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	for _, q := range []string{
		`SELECT DATE_FORMAT(NULL, '%Y')`,
		`SELECT LPAD(NULL, 3, '0')`,
		`SELECT REPEAT(NULL, 2)`,
		`SELECT YEAR(NULL)`,
		`SELECT TRUNCATE(NULL, 2)`,
		`SELECT DATEDIFF('2026-01-01', NULL)`,
		`SELECT TO_CHAR(NULL, 'YYYY')`,
		`SELECT TO_CHAR('2026-01-01', NULL)`,
		`SELECT TO_DATE(NULL, 'YYYY')`,
		`SELECT DATE_TRUNC('day', NULL)`,
		`SELECT DATE_TRUNC(NULL, '2026-01-01')`,
		`SELECT SPLIT_PART(NULL, ',', 1)`,
		`SELECT INITCAP(NULL)`,
		`SELECT STRPOS(NULL, 'a')`,
		`SELECT LEFT(NULL, 2)`,
		`SELECT REGEXP_REPLACE(NULL, 'a', 'b')`,
		`SELECT DATE_PART('year', NULL)`,
		`SELECT DATE_PART(NULL, '2026-01-01')`,
		`SELECT SAFE_DIVIDE(NULL, 2)`,
		`SELECT STARTS_WITH(NULL, 'a')`,
		`SELECT REGEXP_CONTAINS(NULL, 'a')`,
		`SELECT REGEXP_EXTRACT('abc', 'z')`,
		`SELECT DATE_DIFF(NULL, '2026-01-01', 'day')`,
	} {
		var got sql.NullString
		if err := db.QueryRowContext(context.Background(), q).Scan(&got); err != nil {
			t.Fatalf("%s: query error: %v", q, err)
		}
		if got.Valid {
			t.Fatalf("%s = %q, want NULL", q, got.String)
		}
	}
}

// TestNonDeterministicUDFs checks the shape of the non-deterministic helpers
// through a real connection.
func TestNonDeterministicUDFs(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tests := []struct {
		sql     string
		pattern string
	}{
		{`SELECT NOW()`, `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`},
		{`SELECT CURDATE()`, `^\d{4}-\d{2}-\d{2}$`},
		{`SELECT CURTIME()`, `^\d{2}:\d{2}:\d{2}$`},
		{`SELECT CAST(RAND() < 1 AS INTEGER)`, `^1$`},
	}
	for _, tt := range tests {
		var got string
		if err := db.QueryRowContext(context.Background(), tt.sql).Scan(&got); err != nil {
			t.Fatalf("%s: %v", tt.sql, err)
		}
		if !regexp.MustCompile(tt.pattern).MatchString(got) {
			t.Fatalf("%s = %q, want match %s", tt.sql, got, tt.pattern)
		}
	}
}

// TestDateFormatSpecialAndParts covers the DATE_FORMAT specifiers without a
// direct Go layout and the extra DATE_PART units.
func TestDateFormatSpecialAndParts(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tests := []struct {
		sql  string
		want string
	}{
		{`SELECT DATE_FORMAT('2026-03-10', '%j')`, "069"},
		{`SELECT DATE_FORMAT('2026-07-28', '%w')`, "2"},
		{`SELECT DATE_FORMAT('2026-07-28', '100%%')`, "100%"},
		{`SELECT DATE_FORMAT('2026-07-28', '%Z')`, "Z"},
		{`SELECT DATE_PART('quarter', '2026-07-28')`, "3"},
		{`SELECT DATE_PART('week', '2026-01-05')`, "2"},
		{`SELECT DATE_PART('epoch', '1970-01-01 00:00:01')`, "1"},
		{`SELECT DATE_PART('dow', '2026-07-28')`, "3"},
		{`SELECT DATE_PART('doy', '2026-01-10')`, "10"},
	}
	for _, tt := range tests {
		var got sql.NullString
		if err := db.QueryRowContext(context.Background(), tt.sql).Scan(&got); err != nil {
			t.Fatalf("%s: %v", tt.sql, err)
		}
		if got.String != tt.want {
			t.Fatalf("%s = %q, want %q", tt.sql, got.String, tt.want)
		}
	}
}

func TestDatePartUnsupported(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	for _, q := range []string{
		`SELECT DATE_PART('century', '2026-07-28')`,
		`SELECT DATE_TRUNC('decade', '2026-07-28')`,
		`SELECT DATE_DIFF('2026-01-01', '2020-01-01', 'century')`,
	} {
		var got sql.NullString
		if err := db.QueryRowContext(context.Background(), q).Scan(&got); err == nil {
			t.Fatalf("%s should error", q)
		}
	}
}

// TestEdgeCaseUDFs covers boundary branches: zero/empty inputs and truthiness of
// several value types.
func TestEdgeCaseUDFs(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tests := []struct {
		sql  string
		want string
	}{
		{`SELECT SPACE(0)`, ""},
		{`SELECT SPACE(-1)`, ""},
		{`SELECT REPEAT('x', 0)`, ""},
		{`SELECT SUBSTRING_INDEX('a.b.c', '.', 0)`, ""},
		{`SELECT SUBSTRING_INDEX('a.b', '', 1)`, ""},
		{`SELECT SUBSTRING_INDEX('a.b.c', '.', 9)`, "a.b.c"},
		{`SELECT LPAD('x', 5, '')`, "x"},
		{`SELECT IF(2.5, 'y', 'n')`, "y"},
		{`SELECT IF('0', 'y', 'n')`, "n"},
		{`SELECT IF('text', 'y', 'n')`, "y"},
		{`SELECT IF(CAST('' AS BLOB), 'y', 'n')`, "n"},
	}
	for _, tt := range tests {
		var got sql.NullString
		if err := db.QueryRowContext(context.Background(), tt.sql).Scan(&got); err != nil {
			t.Fatalf("%s: %v", tt.sql, err)
		}
		if got.String != tt.want {
			t.Fatalf("%s = %q, want %q", tt.sql, got.String, tt.want)
		}
	}
}

func TestRegexpInvalidPattern(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var got sql.NullString
	err = db.QueryRowContext(context.Background(), `SELECT 'x' REGEXP '('`).Scan(&got)
	if err == nil {
		t.Fatal("REGEXP with an invalid pattern should error")
	}
}

func TestRandInRange(t *testing.T) {
	t.Parallel()
	for range 1000 {
		v := randFloat()
		if v < 0 || v >= 1 {
			t.Fatalf("randFloat() = %v, want [0,1)", v)
		}
	}
}

// TestToStringCoercions exercises the driver.Value coercion helpers directly for
// the types the SQLite driver can hand a UDF.
func TestValueCoercions(t *testing.T) {
	t.Parallel()
	if s, ok := toString(driver.Value(int64(42))); !ok || s != "42" {
		t.Fatalf("toString(int64) = %q, %v", s, ok)
	}
	if s, ok := toString(driver.Value([]byte("hi"))); !ok || s != "hi" {
		t.Fatalf("toString([]byte) = %q, %v", s, ok)
	}
	if _, ok := toString(driver.Value(nil)); ok {
		t.Fatal("toString(nil) should report false")
	}
	if n, ok := toInt(driver.Value("15")); !ok || n != 15 {
		t.Fatalf("toInt(string) = %d, %v", n, ok)
	}
	if _, ok := toInt(driver.Value("nope")); ok {
		t.Fatal("toInt(non-numeric) should report false")
	}
	if f, ok := toFloat(driver.Value(int64(3))); !ok || f != 3 {
		t.Fatalf("toFloat(int64) = %v, %v", f, ok)
	}

	// Remaining type branches.
	if s, ok := toString(driver.Value(3.5)); !ok || s != "3.5" {
		t.Fatalf("toString(float64) = %q, %v", s, ok)
	}
	if s, ok := toString(driver.Value(true)); !ok || s != "1" {
		t.Fatalf("toString(bool) = %q, %v", s, ok)
	}
	if s, ok := toString(driver.Value(time.Date(2026, 7, 28, 1, 2, 3, 0, time.UTC))); !ok || s != "2026-07-28 01:02:03" {
		t.Fatalf("toString(time.Time) = %q, %v", s, ok)
	}
	if n, ok := toInt(driver.Value(4.9)); !ok || n != 4 {
		t.Fatalf("toInt(float64) = %d, %v", n, ok)
	}
	if n, ok := toInt(driver.Value(true)); !ok || n != 1 {
		t.Fatalf("toInt(bool) = %d, %v", n, ok)
	}
	if _, ok := toInt(driver.Value(nil)); ok {
		t.Fatal("toInt(nil) should report false")
	}
	if f, ok := toFloat(driver.Value("2.5")); !ok || f != 2.5 {
		t.Fatalf("toFloat(string) = %v, %v", f, ok)
	}
	if f, ok := toFloat(driver.Value([]byte("6"))); !ok || f != 6 {
		t.Fatalf("toFloat([]byte) = %v, %v", f, ok)
	}
	if _, ok := toFloat(driver.Value(nil)); ok {
		t.Fatal("toFloat(nil) should report false")
	}
}
