package dialect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

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
		{"str_to_date", `SELECT STR_TO_DATE('2026-07-28', '%Y-%m-%d')`, "2026-07-28"},
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
		{"least numeric", `SELECT LEAST(3, 1, 2)`, "1"},
		{"least string", `SELECT LEAST('pear', 'apple')`, "apple"},
		{"greatest numeric", `SELECT GREATEST(3, 1, 2)`, "3"},
		{"greatest mixed sign", `SELECT GREATEST(-5, -1)`, "-1"},
		{"reverse", `SELECT REVERSE('abc')`, "cba"},
		{"reverse multibyte", `SELECT REVERSE('あいう')`, "ういあ"},
		{"find_in_set", `SELECT FIND_IN_SET('b', 'a,b,c')`, "2"},
		{"find_in_set missing", `SELECT FIND_IN_SET('z', 'a,b,c')`, "0"},
		{"field", `SELECT FIELD('b', 'a', 'b', 'c')`, "2"},
		{"field missing", `SELECT FIELD('z', 'a', 'b')`, "0"},
		{"elt", `SELECT ELT(2, 'a', 'b')`, "b"},
		{"monthname", `SELECT MONTHNAME('2026-02-05')`, "February"},
		{"dayname", `SELECT DAYNAME('2026-02-05')`, "Thursday"},
		{"last_day", `SELECT LAST_DAY('2026-02-05')`, "2026-02-28"},
		{"last_day leap year", `SELECT LAST_DAY('2028-02-05')`, "2028-02-29"},
		{"unix_timestamp", `SELECT UNIX_TIMESTAMP('2026-01-01 00:00:00')`, "1767225600"},
		{"from_unixtime", `SELECT FROM_UNIXTIME(0)`, "1970-01-01 00:00:00"},
		{"from_unixtime with format", `SELECT FROM_UNIXTIME(0, '%Y/%m/%d')`, "1970/01/01"},
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
		{"regexp_replace with flags", `SELECT REGEXP_REPLACE('a1b2', '[0-9]', '', 'g')`, "ab"},
		{"regexp_replace case-insensitive flag", `SELECT REGEXP_REPLACE('ABC', 'b', 'x', 'i')`, "AxC"},
		{"md5", `SELECT MD5('a')`, "0cc175b9c0f1b6a831c399e269772661"},
		{"ascii", `SELECT ASCII('A')`, "65"},
		{"ascii empty", `SELECT ASCII('')`, "0"},
		{"chr", `SELECT CHR(65)`, "A"},
		{"translate", `SELECT TRANSLATE('abc', 'ab', 'xy')`, "xyc"},
		{"translate deletes unmapped", `SELECT TRANSLATE('abcd', 'abc', 'x')`, "xd"},
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
		{"format_date", `SELECT FORMAT_DATE('%Y-%m', '2026-03-15')`, "2026-03"},
		{"format_date month name", `SELECT FORMAT_DATE('%B %d, %Y', '2026-03-15')`, "March 15, 2026"},
		{"format_timestamp minutes", `SELECT FORMAT_TIMESTAMP('%H:%M:%S', '2026-03-15 10:20:30')`, "10:20:30"},
		{"parse_date", `SELECT PARSE_DATE('%Y-%m-%d', '2026-03-15')`, "2026-03-15"},
		{"parse_timestamp", `SELECT PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2026-03-15 10:20:30')`, "2026-03-15 10:20:30"},
		{"unix_seconds", `SELECT UNIX_SECONDS('2026-01-01 00:00:00')`, "1767225600"},
		{"unix_millis", `SELECT UNIX_MILLIS('2026-01-01 00:00:00')`, "1767225600000"},
		{"timestamp_seconds", `SELECT TIMESTAMP_SECONDS(0)`, "1970-01-01 00:00:00"},
		{"timestamp_millis", `SELECT TIMESTAMP_MILLIS(1000)`, "1970-01-01 00:00:01"},
		{"to_hex", `SELECT TO_HEX('ab')`, "6162"},
		{"is_nan false", `SELECT IS_NAN(1.0)`, "0"},
		{"is_nan true", `SELECT IS_NAN('nan')`, "1"},
		{"is_nan non-numeric text", `SELECT IS_NAN('abc')`, "0"},
		{"safe_add", `SELECT SAFE_ADD(1, 2)`, "3"},
		{"safe_add overflow", `SELECT IFNULL(CAST(SAFE_ADD(9223372036854775807, 1) AS TEXT), 'null')`, "null"},
		{"safe_subtract", `SELECT SAFE_SUBTRACT(1, 2)`, "-1"},
		{"safe_multiply", `SELECT SAFE_MULTIPLY(3, 4)`, "12"},
		{"safe_multiply overflow", `SELECT IFNULL(CAST(SAFE_MULTIPLY(9223372036854775807, 2) AS TEXT), 'null')`, "null"},
		{"safe_negate", `SELECT SAFE_NEGATE(5)`, "-5"},
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
		`SELECT LOCATE('a', 'abc', NULL)`,
		`SELECT LOCATE(NULL, 'abc')`,
		`SELECT STRPOS(NULL, 'a')`,
		`SELECT INITCAP(NULL)`,
		`SELECT unicode_upper(NULL)`,
		`SELECT unicode_lower(NULL)`,
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
		`SELECT LEAST(1, NULL)`,
		`SELECT GREATEST(NULL, 'a')`,
		`SELECT REVERSE(NULL)`,
		`SELECT FIND_IN_SET(NULL, 'a,b')`,
		`SELECT ELT(0, 'a', 'b')`,
		`SELECT ELT(3, 'a', 'b')`,
		`SELECT MONTHNAME(NULL)`,
		`SELECT DAYNAME('not a date')`,
		`SELECT LAST_DAY(NULL)`,
		`SELECT UNIX_TIMESTAMP(NULL)`,
		`SELECT FROM_UNIXTIME(NULL)`,
		`SELECT MD5(NULL)`,
		`SELECT ASCII(NULL)`,
		`SELECT CHR(NULL)`,
		`SELECT TRANSLATE(NULL, 'a', 'b')`,
		`SELECT FORMAT_DATE('%Y', NULL)`,
		`SELECT PARSE_DATE('%Y-%m-%d', 'nope')`,
		`SELECT UNIX_SECONDS(NULL)`,
		`SELECT TIMESTAMP_SECONDS(NULL)`,
		`SELECT TO_HEX(NULL)`,
		`SELECT IS_NAN(NULL)`,
		`SELECT SAFE_ADD(NULL, 1)`,
		`SELECT SAFE_NEGATE(NULL)`,
		`SELECT REGEXP_REPLACE(NULL, 'a', 'b', 'g')`,
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
		// 2026-07-28 is a Tuesday. PostgreSQL spells the field dow and numbers
		// it from Sunday=0; MySQL spells it dayofweek and numbers it from
		// Sunday=1; PostgreSQL's isodow runs Monday=1 through Sunday=7.
		{`SELECT DATE_PART('dow', '2026-07-28')`, "2"},
		{`SELECT DATE_PART('isodow', '2026-07-28')`, "2"},
		{`SELECT DATE_PART('dayofweek', '2026-07-28')`, "3"},
		{`SELECT DATE_PART('dow', '2026-08-02')`, "0"},
		{`SELECT DATE_PART('isodow', '2026-08-02')`, "7"},
		{`SELECT DATE_PART('dayofweek', '2026-08-02')`, "1"},
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
		`SELECT DATE_PART('fortnight', '2026-07-28')`,
		`SELECT DATE_TRUNC('fortnight', '2026-07-28')`,
		`SELECT DATE_DIFF('2026-01-01', '2020-01-01', 'century')`,
		`SELECT CHR(-1)`,
		`SELECT CHR(2000000)`,
		`SELECT FIELD('a')`,
		`SELECT ELT(1)`,
		`SELECT REGEXP_REPLACE('a', 'a')`,
		`SELECT REGEXP_REPLACE('a', 'a', 'b', 'g', 'extra')`,
		`SELECT UNIX_TIMESTAMP('2026-01-01', 'extra')`,
		`SELECT FROM_UNIXTIME(0, '%Y', 'extra')`,
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
		{`SELECT IF('text', 'y', 'n')`, "n"}, // a string with no number in it is zero
		{`SELECT IF(CAST('' AS BLOB), 'y', 'n')`, "n"},
		{`SELECT LEAST(5)`, "5"},
		{`SELECT GREATEST('10', '9')`, "10"},        // numeric strings compare numerically
		{`SELECT GREATEST('b10', 'b9')`, "b9"},      // mixed content falls back to text order
		{`SELECT FIND_IN_SET('', 'a,,b')`, "2"},     // an empty field is still a field
		{`SELECT FIELD(1, '1', 'x')`, "1"},          // arguments are compared as text
		{`SELECT TRANSLATE('abc', '', 'x')`, "abc"}, // an empty from-set changes nothing
		{`SELECT REVERSE('')`, ""},
		{`SELECT ASCII('あ')`, "12354"},
		{`SELECT CHR(12354)`, "あ"},
		{`SELECT LAST_DAY('2026-12-31')`, "2026-12-31"},
		{`SELECT SAFE_ADD(1.5, 2.0)`, "3.5"},
		{`SELECT SAFE_NEGATE(1.5)`, "-1.5"},
		{`SELECT REGEXP_REPLACE('a1b2', '[0-9]', 'X', '')`, "aXb2"}, // no "g": first match only
		// A boundary-dependent pattern matches inside the source but not in the
		// matched text on its own, so the first-match path has to expand against
		// the source.
		{`SELECT REGEXP_REPLACE('ab', '\Bb', 'X', '')`, "aX"},
		{`SELECT REGEXP_REPLACE('ab', 'z', 'X', '')`, "ab"},
		{`SELECT ELT(4294967298, 'a', 'b')`, ""}, // an index past int32 must stay out of range
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

	// A string in a numeric context is the number its leading digits spell.
	for in, want := range map[string]float64{
		"abc": 0, "true": 0, "": 0, " 1 ": 1, "1abc": 1, "0abc": 0,
		"-1": -1, "+2": 2, ".5": 0.5, "1e2": 100, "1e": 1, "0.0": 0, ".": 0,
	} {
		if got := leadingNumber(in); got != want {
			t.Errorf("leadingNumber(%q) = %v, want %v", in, got, want)
		}
	}

	// A NUL byte cannot survive the round trip through SQLite's text values, so
	// the escape for it is checked here rather than through a query.
	if got, err := fnMySQLQuote([]driver.Value{"a\x00b"}); err != nil || got != `'a\0b'` {
		t.Errorf(`fnMySQLQuote("a\x00b") = %v, %v, want 'a\0b'`, got, err)
	}
}

// TestStringHelpersCountCharacters pins the helpers that measure a string to
// the unit the dialects measure in. All three count characters, and counting
// bytes cut a character in half in the padding pair and returned a position
// nothing could use in the search pair.
func TestStringHelpersCountCharacters(t *testing.T) {
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
		// Padding: the length is a count of characters, and the fill is cut at
		// a character boundary.
		{"lpad pads to a character count", `SELECT LPAD('日本', 4, '*')`, "**日本"},
		{"rpad pads to a character count", `SELECT RPAD('日本', 4, '*')`, "日本**"},
		{"lpad cuts at a character", `SELECT LPAD('日本語', 2, '*')`, "日本"},
		{"rpad cuts at a character", `SELECT RPAD('日本語', 2, '*')`, "日本"},
		{"lpad fills with a multibyte pad", `SELECT LPAD('abc', 5, '日')`, "日日abc"},
		{"rpad fills with a multibyte pad", `SELECT RPAD('abc', 5, '日')`, "abc日日"},
		{"lpad fills an empty subject", `SELECT LPAD('', 3, '日')`, "日日日"},
		{"lpad repeats a longer pad", `SELECT LPAD('a', 4, 'xy')`, "xyxa"},
		// The ASCII cases that must not change.
		{"lpad ascii", `SELECT LPAD('abc', 5, '*')`, "**abc"},
		{"rpad ascii", `SELECT RPAD('abc', 5, '*')`, "abc**"},
		{"lpad cuts a long subject", `SELECT LPAD('abcdef', 3, '*')`, "abc"},
		{"lpad with an empty pad", `SELECT LPAD('abc', 5, '')`, "abc"},
		{"lpad of the same length", `SELECT LPAD('abc', 3, '*')`, "abc"},

		// Search: the position is a count of characters.
		{"locate finds a character position", `SELECT LOCATE('本', '日本語')`, "2"},
		{"locate at the start", `SELECT LOCATE('日', '日本語')`, "1"},
		{"locate at the end", `SELECT LOCATE('語', '日本語')`, "3"},
		{"locate from a character position", `SELECT LOCATE('語', '日本語', 2)`, "3"},
		{"locate skips a match", `SELECT LOCATE('本', '本日本', 2)`, "3"},
		{"locate not found", `SELECT LOCATE('x', '日本語')`, "0"},
		{"locate past the end", `SELECT LOCATE('語', '日本語', 4)`, "0"},
		{"strpos finds a character position", `SELECT STRPOS('日本語', '語')`, "3"},
		{"strpos not found", `SELECT STRPOS('日本語', 'x')`, "0"},
		// The ASCII cases that must not change.
		{"locate ascii", `SELECT LOCATE('b', 'abcabc')`, "2"},
		{"locate ascii with pos", `SELECT LOCATE('b', 'abcabc', 3)`, "5"},
		{"strpos ascii", `SELECT STRPOS('abc', 'c')`, "3"},

		// Case: a letter is a letter whatever its script.
		{"initcap keeps an accent as a letter", `SELECT INITCAP('école du soir')`, "École Du Soir"},
		{"initcap lowercases the rest of a word", `SELECT INITCAP('ÉCOLE')`, "École"},
		{"initcap leaves text with no case alone", `SELECT INITCAP('日本語 の 文字')`, "日本語 の 文字"},
		{"initcap ascii", `SELECT INITCAP('hello world')`, "Hello World"},
		{"initcap a digit-led word", `SELECT INITCAP('123abc def')`, "123abc Def"},
		{"unicode_upper folds beyond ascii", `SELECT unicode_upper('école')`, "ÉCOLE"},
		{"unicode_lower folds beyond ascii", `SELECT unicode_lower('ÉCOLE')`, "école"},
		{"unicode_upper leaves caseless text alone", `SELECT unicode_upper('日本語')`, "日本語"},
		{"unicode_upper ascii", `SELECT unicode_upper('abc')`, "ABC"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got sql.NullString
			if err := db.QueryRowContext(context.Background(), tt.sql).Scan(&got); err != nil {
				t.Fatalf("%s: %v", tt.sql, err)
			}
			if !got.Valid || got.String != tt.want {
				t.Errorf("%s = %v, want %q", tt.sql, got, tt.want)
			}
			if got.Valid && !utf8.ValidString(got.String) {
				t.Errorf("%s returned bytes that are not UTF-8: %x", tt.sql, got.String)
			}
		})
	}
}

// TestDialectBoundariesFollowTheirEngine pins the boundaries where MySQL and
// PostgreSQL give different answers to the same call, and where both differ from
// what SQLite's own function would do. Every expected value here was taken from
// MySQL 8.4 and PostgreSQL 17 rather than from the documentation, because the
// documentation does not describe most of these cases.
func TestDialectBoundariesFollowTheirEngine(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name     string
		dialect  Dialect
		query    string
		want     string
		wantNull bool
		wantErr  bool
	}{
		// LPAD and RPAD: a negative length is NULL in MySQL and an empty string
		// in PostgreSQL; an empty pad that cannot reach the length is an empty
		// string in MySQL and the input unchanged in PostgreSQL.
		{name: "mysql pad refuses a negative length", dialect: MySQL, query: `SELECT LPAD('abc', -1, 'x')`, wantNull: true},
		{name: "postgresql pad empties a negative length", dialect: PostgreSQL, query: `SELECT lpad('abc', -1, 'x')`, want: ""},
		{name: "mysql pad gives up on an empty pad", dialect: MySQL, query: `SELECT LPAD('abc', 5, '')`, want: ""},
		{name: "mysql rpad gives up on an empty pad", dialect: MySQL, query: `SELECT RPAD('abc', 5, '')`, want: ""},
		{name: "postgresql pad keeps the input on an empty pad", dialect: PostgreSQL, query: `SELECT lpad('abc', 5, '')`, want: "abc"},
		{name: "an empty pad still truncates", dialect: MySQL, query: `SELECT LPAD('abcdef', 3, '')`, want: "abc"},
		{name: "postgresql pads with spaces by default", dialect: PostgreSQL, query: `SELECT lpad('abc', 5)`, want: "  abc"},
		{name: "postgresql rpads with spaces by default", dialect: PostgreSQL, query: `SELECT rpad('abc', 5)`, want: "abc  "},
		{name: "pad still counts characters", dialect: MySQL, query: `SELECT LPAD('日本', 4, '*')`, want: "**日本"},

		// SUBSTRING: MySQL reads position 0 as no position at all, PostgreSQL
		// counts from 1 and lets a start below 1 consume the requested length.
		{name: "mysql substring at position zero", dialect: MySQL, query: `SELECT SUBSTRING('abcdef', 0)`, want: ""},
		{name: "mysql substring at position zero with a length", dialect: MySQL, query: `SELECT SUBSTRING('abcdef', 0, 3)`, want: ""},
		{name: "mysql substring from the end", dialect: MySQL, query: `SELECT SUBSTRING('abcdef', -2, 1)`, want: "e"},
		{name: "mysql substring with no length", dialect: MySQL, query: `SELECT SUBSTRING('abcdef', 3)`, want: "cdef"},
		{name: "mysql substring with a zero length", dialect: MySQL, query: `SELECT SUBSTRING('abcdef', 2, 0)`, want: ""},
		{name: "postgresql substring before the start", dialect: PostgreSQL, query: `SELECT substr('abcdef', -1, 3)`, want: "a"},
		{name: "postgresql substring at position zero", dialect: PostgreSQL, query: `SELECT substr('abcdef', 0, 3)`, want: "ab"},
		{name: "postgresql substring with no count", dialect: PostgreSQL, query: `SELECT substr('abcdef', 0)`, want: "abcdef"},
		{name: "postgresql substring refuses a negative count", dialect: PostgreSQL, query: `SELECT substr('abcdef', 2, -1)`, wantErr: true},
		// Either bound can be any integer literal the query held, and the sum
		// that decides where the result ends must not wrap around them.
		{name: "postgresql substring from far below the string", dialect: PostgreSQL, query: `SELECT substr('abcdef', -9223372036854775808, 3)`, want: ""},
		{name: "postgresql substring with a count past the range", dialect: PostgreSQL, query: `SELECT substr('abcdef', 2, 9223372036854775807)`, want: "bcdef"},
		{name: "mysql substring with a length past the range", dialect: MySQL, query: `SELECT SUBSTRING('abcdef', 2, 9223372036854775807)`, want: "bcdef"},
		{name: "postgresql keyword form", dialect: PostgreSQL, query: `SELECT SUBSTRING('abcdef' FROM 0 FOR 3)`, want: "ab"},
		{name: "substring still counts characters", dialect: MySQL, query: `SELECT SUBSTRING('日本語', 2, 1)`, want: "本"},

		// A fractional count is rounded by MySQL, not truncated.
		{name: "repeat rounds its count up", dialect: MySQL, query: `SELECT REPEAT('ab', 2.7)`, want: "ababab"},
		{name: "repeat rounds its count down", dialect: MySQL, query: `SELECT REPEAT('ab', 2.4)`, want: "abab"},
		{name: "repeat rounds a half away from zero", dialect: MySQL, query: `SELECT REPEAT('ab', 2.5)`, want: "ababab"},
		{name: "space rounds its count", dialect: MySQL, query: `SELECT CONCAT('[', SPACE(3.7), ']')`, want: "[    ]"},
		{name: "elt rounds its index", dialect: MySQL, query: `SELECT ELT(2.7, 'a', 'b', 'c')`, want: "c"},

		// An empty set holds nothing, including the empty string.
		{name: "find_in_set in an empty set", dialect: MySQL, query: `SELECT FIND_IN_SET('', '')`, want: "0"},
		{name: "find_in_set finds an empty element", dialect: MySQL, query: `SELECT FIND_IN_SET('', 'a,,c')`, want: "2"},

		// PostgreSQL refuses a zero field position rather than answering.
		{name: "split_part refuses a zero position", dialect: PostgreSQL, query: `SELECT split_part('a.b.c', '.', 0)`, wantErr: true},
		{name: "split_part counts from the end", dialect: PostgreSQL, query: `SELECT split_part('a.b.c', '.', -1)`, want: "c"},

		// The day of the week is numbered differently by each spelling.
		{name: "postgresql dow on a Sunday", dialect: PostgreSQL, query: `SELECT DATE_PART('dow', '2026-08-02')`, want: "0"},
		{name: "postgresql extract dow on a Sunday", dialect: PostgreSQL, query: `SELECT EXTRACT(DOW FROM TIMESTAMP '2026-08-02')`, want: "0"},
		{name: "postgresql isodow on a Sunday", dialect: PostgreSQL, query: `SELECT DATE_PART('isodow', '2026-08-02')`, want: "7"},
		{name: "mysql dayofweek on a Sunday", dialect: MySQL, query: `SELECT DAYOFWEEK('2026-08-02')`, want: "1"},
		{name: "googlesql dayofweek on a Sunday", dialect: GoogleSQL, query: `SELECT EXTRACT(DAYOFWEEK FROM TIMESTAMP '2026-08-02')`, want: "1"},

		// A value the date functions cannot read is answered per dialect:
		// MySQL warns and answers NULL, BigQuery refuses the query by name.
		{name: "mysql extract answers null for a malformed date", dialect: MySQL, query: `SELECT EXTRACT(YEAR FROM 'not-a-date')`, wantNull: true},
		{name: "mysql extract reads the year", dialect: MySQL, query: `SELECT EXTRACT(YEAR FROM '2024-02-29')`, want: "2024"},
		{name: "googlesql extract refuses a malformed date", dialect: GoogleSQL, query: `SELECT EXTRACT(YEAR FROM 'not-a-date')`, wantErr: true},
		{name: "googlesql extract of null is null", dialect: GoogleSQL, query: `SELECT EXTRACT(HOUR FROM NULL)`, wantNull: true},
		{name: "googlesql date_trunc week of a malformed date is null", dialect: GoogleSQL, query: `SELECT DATE_TRUNC('not-a-date', WEEK)`, wantNull: true},

		// A call nested in one of the keyword forms is translated by the
		// dialect's own pass. MySQL CONCAT is NULL when an argument is NULL;
		// PostgreSQL's skips NULLs, so reading one as the other is visible here.
		{name: "concat inside the substring keyword form", dialect: MySQL, query: `SELECT SUBSTRING(CONCAT('a', NULL) FROM 1 FOR 5)`, wantNull: true},
		{name: "concat in the position needle", dialect: MySQL, query: `SELECT POSITION(CONCAT('b', NULL) IN 'abc')`, wantNull: true},
		{name: "concat in the position haystack", dialect: MySQL, query: `SELECT POSITION('b' IN CONCAT('abc', NULL))`, wantNull: true},
		{name: "mysql length counts bytes inside substring", dialect: MySQL, query: `SELECT SUBSTRING(CAST(LENGTH('日') AS CHAR) FROM 1)`, want: "3"},

		// MySQL's default collation folds case, and its regular expressions
		// match under that collation. LIKE already follows it here; REGEXP has
		// to as well, or one dialect answers two ways about the same letter.
		{name: "mysql regexp folds case", dialect: MySQL, query: `SELECT 'abc' REGEXP 'B'`, want: "1"},
		{name: "mysql rlike folds case", dialect: MySQL, query: `SELECT 'ABC' RLIKE 'b'`, want: "1"},
		{name: "mysql regexp folds beyond ascii", dialect: MySQL, query: `SELECT 'école' REGEXP 'É'`, want: "1"},
		{name: "mysql regexp_replace folds case", dialect: MySQL, query: `SELECT REGEXP_REPLACE('aAa', 'a', 'X')`, want: "XXX"},
		{name: "mysql regexp keeps an inline flag", dialect: MySQL, query: `SELECT 'abc' REGEXP '(?-i)B'`, want: "0"},
		{name: "postgresql regexp stays case-sensitive", dialect: PostgreSQL, query: `SELECT CASE WHEN 'abc' ~ 'B' THEN 1 ELSE 0 END`, want: "0"},
		{name: "googlesql regexp stays case-sensitive", dialect: GoogleSQL, query: `SELECT REGEXP_CONTAINS('abc', 'B')`, want: "0"},

		// QUOTE exists to produce a literal the source engine can read back,
		// and MySQL escapes with a backslash where SQLite doubles the quote.
		{name: "mysql quote escapes with a backslash", dialect: MySQL, query: `SELECT QUOTE('O''Reilly')`, want: `'O\'Reilly'`},
		{name: "mysql quote doubles a backslash", dialect: MySQL, query: `SELECT QUOTE('a\\b')`, want: `'a\\b'`},
		{name: "mysql quote quotes a number", dialect: MySQL, query: `SELECT QUOTE(1.5)`, want: "'1.5'"},
		{name: "mysql quote of an empty string", dialect: MySQL, query: `SELECT QUOTE('')`, want: "''"},
		{name: "mysql quote of null is the word", dialect: MySQL, query: `SELECT QUOTE(NULL)`, want: "NULL"},
		{name: "mysql quote escapes a control z", dialect: MySQL, query: `SELECT QUOTE(UNHEX('1A'))`, want: `'\Z'`},
		{name: "postgresql quote is left to sqlite", dialect: PostgreSQL, query: `SELECT QUOTE('a''b')`, want: "'a''b'"},

		// ASCII answers a byte in MySQL and a code point in PostgreSQL, and ORD
		// is the one that answers the whole character to a MySQL caller.
		{name: "mysql ascii answers the first byte", dialect: MySQL, query: `SELECT ASCII('Ā')`, want: "196"},
		{name: "mysql ascii of a multibyte character", dialect: MySQL, query: `SELECT ASCII('日')`, want: "230"},
		{name: "mysql ascii of an empty string", dialect: MySQL, query: `SELECT ASCII('')`, want: "0"},
		{name: "mysql ascii reads a number as its digits", dialect: MySQL, query: `SELECT ASCII(255)`, want: "50"},
		{name: "mysql ord answers the character", dialect: MySQL, query: `SELECT ORD('日')`, want: "15112101"},
		{name: "postgresql ascii answers the code point", dialect: PostgreSQL, query: `SELECT ascii('Ā')`, want: "256"},

		// HEX reads a number as unsigned and rounds a fraction, and it hexes
		// the bytes of anything that arrives as a string.
		{name: "mysql hex of a negative number", dialect: MySQL, query: `SELECT HEX(-1)`, want: "FFFFFFFFFFFFFFFF"},
		{name: "mysql hex of a larger negative number", dialect: MySQL, query: `SELECT HEX(-255)`, want: "FFFFFFFFFFFFFF01"},
		{name: "mysql hex rounds up", dialect: MySQL, query: `SELECT HEX(1.9)`, want: "2"},
		{name: "mysql hex rounds a half away from zero", dialect: MySQL, query: `SELECT HEX(2.5)`, want: "3"},
		{name: "mysql hex rounds toward zero from below one", dialect: MySQL, query: `SELECT HEX(-0.4)`, want: "0"},
		{name: "mysql hex of a numeric string hexes its bytes", dialect: MySQL, query: `SELECT HEX('255')`, want: "323535"},
		{name: "mysql hex of a number hexes its value", dialect: MySQL, query: `SELECT HEX(255)`, want: "FF"},
		{name: "mysql hex of a string hexes its bytes", dialect: MySQL, query: `SELECT HEX('abc')`, want: "616263"},
		{name: "mysql unhex reads hex back", dialect: MySQL, query: `SELECT HEX(UNHEX(HEX(-1)))`, want: "FFFFFFFFFFFFFFFF"},

		// MySQL shifts an unsigned 64-bit value; SQLite shifts a signed one and
		// reads a negative count as a shift the other way.
		{name: "mysql shifts zeros in from the left", dialect: MySQL, query: `SELECT -1 >> 1`, want: "9223372036854775807"},
		{name: "mysql shift right of a negative value", dialect: MySQL, query: `SELECT -8 >> 1`, want: "9223372036854775804"},
		{name: "mysql shift right past the width", dialect: MySQL, query: `SELECT -1 >> 64`, want: "0"},
		{name: "mysql shift by a negative count", dialect: MySQL, query: `SELECT 1 >> -1`, want: "0"},
		{name: "mysql shift left past the width", dialect: MySQL, query: `SELECT -1 << 64`, want: "0"},
		{name: "mysql shift left into the top bit", dialect: MySQL, query: `SELECT 1 << 63`, want: "-9223372036854775808"},
		{name: "mysql shift right of a positive value", dialect: MySQL, query: `SELECT 16 >> 2`, want: "4"},
		// A shift binds looser than the arithmetic on both sides of it and
		// tighter than "&" and "|", so the operand a rewrite takes has to reach
		// across the one and stop at the other. Every want was read from MySQL
		// 8.4 rather than derived.
		{name: "mysql shift takes the sum on its left", dialect: MySQL, query: `SELECT 1 + 2 >> 1`, want: "1"},
		{name: "mysql shift takes the sum on its right", dialect: MySQL, query: `SELECT 4 >> 1 + 1`, want: "1"},
		{name: "mysql shift takes the product on its left", dialect: MySQL, query: `SELECT 2 * 3 >> 1`, want: "3"},
		{name: "mysql shift takes the difference on its right", dialect: MySQL, query: `SELECT 8 >> 3 - 1`, want: "2"},
		{name: "mysql shift takes the remainder on its left", dialect: MySQL, query: `SELECT 7 % 4 >> 1`, want: "1"},
		{name: "mysql shift takes the quotient on its left", dialect: MySQL, query: `SELECT 100 / 4 >> 2`, want: "6"},
		{name: "mysql shift binds tighter than or", dialect: MySQL, query: `SELECT 1 | 4 >> 1`, want: "3"},
		{name: "mysql shift binds tighter than and", dialect: MySQL, query: `SELECT 3 & 12 >> 2`, want: "3"},
		{name: "mysql shift chains to the left", dialect: MySQL, query: `SELECT 16 >> 2 >> 1`, want: "2"},
		{name: "mysql shift left chains to the left", dialect: MySQL, query: `SELECT 1 << 2 << 3`, want: "32"},
		{name: "mysql shift of a complement", dialect: MySQL, query: `SELECT ~0 >> 60`, want: "15"},
		{name: "mysql shift of a negative sum", dialect: MySQL, query: `SELECT -8 >> 1 + 1`, want: "4611686018427387902"},
		{name: "mysql shift by a product", dialect: MySQL, query: `SELECT -1 >> 1 * 2`, want: "4611686018427387903"},
		{name: "mysql shift after a keyword and a sign", dialect: MySQL, query: `SELECT 1 WHERE -1 >> 1 = 9223372036854775807`, want: "1"},
		{name: "mysql shift of a column expression", dialect: MySQL, query: `SELECT n - 1 >> 1 FROM (SELECT 9 AS n)`, want: "4"},

		// A string in boolean context is the number its leading digits spell,
		// which is what MySQL and SQLite both do and what IF did not.
		{name: "if reads a non-numeric string as zero", dialect: MySQL, query: `SELECT IF('abc', 1, 0)`, want: "0"},
		{name: "if reads a boolean word as zero", dialect: MySQL, query: `SELECT IF('true', 1, 0)`, want: "0"},
		{name: "if reads a numeric prefix", dialect: MySQL, query: `SELECT IF('1abc', 1, 0)`, want: "1"},
		{name: "if reads a zero prefix as zero", dialect: MySQL, query: `SELECT IF('0abc', 1, 0)`, want: "0"},
		{name: "if reads a zero decimal as zero", dialect: MySQL, query: `SELECT IF('0.0', 1, 0)`, want: "0"},
		{name: "if trims before reading", dialect: MySQL, query: `SELECT IF(' 1 ', 1, 0)`, want: "1"},
		{name: "if reads an empty string as zero", dialect: MySQL, query: `SELECT IF('', 1, 0)`, want: "0"},
		{name: "if reads a zero byte as zero", dialect: MySQL, query: `SELECT IF(X'00', 1, 0)`, want: "0"},

		// NULL flows through the new helpers rather than being answered for.
		{name: "mysql regexp of a null pattern", dialect: MySQL, query: `SELECT 'a' REGEXP NULL`, wantNull: true},
		{name: "mysql shift of a null value", dialect: MySQL, query: `SELECT NULL >> 1`, wantNull: true},
		{name: "mysql shift by a null count", dialect: MySQL, query: `SELECT 1 << NULL`, wantNull: true},
		{name: "mysql ascii of null", dialect: MySQL, query: `SELECT ASCII(NULL)`, wantNull: true},
		{name: "mysql hex of null", dialect: MySQL, query: `SELECT HEX(NULL)`, wantNull: true},
		{name: "mysql quote of null is the word", dialect: MySQL, query: `SELECT QUOTE(NULL)`, want: "NULL"},

		// The match type still selects the other flags, and an unknown one is
		// reported rather than folded into the default.
		{name: "match type m anchors each line", dialect: MySQL, query: "SELECT REGEXP_REPLACE('a" + "\n" + "b', 'a$', 'X', 1, 1, 'm')", want: "X\nb"},
		{name: "match type n lets a dot cross a newline", dialect: MySQL, query: "SELECT REGEXP_REPLACE('a" + "\n" + "b', 'a.b', 'X', 1, 1, 'n')", want: "X"},
		{name: "match type c turns folding off", dialect: MySQL, query: `SELECT REGEXP_REPLACE('aAa', 'a', 'X', 1, 0, 'c')`, want: "XAX"},
		{name: "an unknown match type is refused", dialect: MySQL, query: `SELECT REGEXP_REPLACE('a', 'a', 'X', 1, 0, 'z')`, wantErr: true},
		{name: "an invalid pattern is refused", dialect: MySQL, query: `SELECT 'a' REGEXP '('`, wantErr: true},

		// GREATEST and LEAST: PostgreSQL ignores its NULL arguments, MySQL and
		// BigQuery answer NULL for the whole call. An empty cell loads as NULL,
		// so under the wrong rule a row missing one column reports no extreme.
		{name: "postgresql greatest skips a null", dialect: PostgreSQL, query: `SELECT GREATEST(1, NULL, 2)`, want: "2"},
		{name: "postgresql least skips a null", dialect: PostgreSQL, query: `SELECT LEAST(1, NULL, 2)`, want: "1"},
		{name: "mysql greatest answers null", dialect: MySQL, query: `SELECT GREATEST(1, NULL, 2)`, wantNull: true},
		{name: "mysql least answers null", dialect: MySQL, query: `SELECT LEAST(1, NULL, 2)`, wantNull: true},
		{name: "googlesql greatest answers null", dialect: GoogleSQL, query: `SELECT GREATEST(1, NULL, 2)`, wantNull: true},
		{name: "googlesql least answers null", dialect: GoogleSQL, query: `SELECT LEAST(1, NULL, 2)`, wantNull: true},
		// The NULLs go before the numeric-or-string choice is made, so a
		// string list still orders as strings, and a list of nothing but NULL
		// has no extreme to answer with.
		{name: "postgresql greatest of strings skips a null", dialect: PostgreSQL, query: `SELECT GREATEST('a', NULL, 'b')`, want: "b"},
		{name: "postgresql greatest of only nulls", dialect: PostgreSQL, query: `SELECT GREATEST(NULL, NULL)`, wantNull: true},
		{name: "postgresql least of only nulls", dialect: PostgreSQL, query: `SELECT LEAST(NULL, NULL)`, wantNull: true},
		{name: "postgresql greatest with only a trailing value", dialect: PostgreSQL, query: `SELECT GREATEST(NULL, NULL, 7)`, want: "7"},
		{name: "postgresql greatest of one argument", dialect: PostgreSQL, query: `SELECT GREATEST(3)`, want: "3"},

		// PostgreSQL's coarse and sub-second date parts. Every want was read
		// from PostgreSQL 17.10 rather than derived; the centuries and
		// millennia count from 1, which is the off-by-one these units invite.
		{name: "postgresql millennium", dialect: PostgreSQL, query: `SELECT DATE_PART('millennium', TIMESTAMP '2024-03-05 10:11:12')`, want: "3"},
		{name: "postgresql millennium at its first year", dialect: PostgreSQL, query: `SELECT DATE_PART('millennium', DATE '2000-01-01')`, want: "2"},
		{name: "postgresql century", dialect: PostgreSQL, query: `SELECT DATE_PART('century', DATE '2024-03-05')`, want: "21"},
		{name: "postgresql century of a year ending in 00", dialect: PostgreSQL, query: `SELECT DATE_PART('century', DATE '2000-06-01')`, want: "20"},
		{name: "postgresql century of a year ending in 01", dialect: PostgreSQL, query: `SELECT DATE_PART('century', DATE '2001-06-01')`, want: "21"},
		{name: "postgresql decade", dialect: PostgreSQL, query: `SELECT DATE_PART('decade', DATE '2024-01-01')`, want: "202"},
		{name: "postgresql isoyear inside its own year", dialect: PostgreSQL, query: `SELECT DATE_PART('isoyear', DATE '2024-01-01')`, want: "2024"},
		{name: "postgresql isoyear borrowed from the year before", dialect: PostgreSQL, query: `SELECT DATE_PART('isoyear', DATE '2023-01-01')`, want: "2022"},
		{name: "postgresql milliseconds carry the seconds", dialect: PostgreSQL, query: `SELECT DATE_PART('milliseconds', TIMESTAMP '2024-03-05 10:11:12.5')`, want: "12500"},
		{name: "postgresql microseconds carry the seconds", dialect: PostgreSQL, query: `SELECT DATE_PART('microseconds', TIMESTAMP '2024-03-05 10:11:12.5')`, want: "12500000"},
		{name: "postgresql second carries its fraction", dialect: PostgreSQL, query: `SELECT DATE_PART('second', TIMESTAMP '2024-03-05 10:11:12.5')`, want: "12.5"},
		{name: "postgresql second without a fraction", dialect: PostgreSQL, query: `SELECT DATE_PART('second', TIMESTAMP '2024-03-05 10:11:12')`, want: "12"},
		// The other two dialects number seconds whole, so the fraction above
		// must not have reached the shared helper.
		{name: "mysql second is whole", dialect: MySQL, query: `SELECT SECOND('2024-03-05 10:11:12.5')`, want: "12"},
		{name: "googlesql second is whole", dialect: GoogleSQL, query: `SELECT EXTRACT(SECOND FROM TIMESTAMP '2024-03-05 10:11:12.5')`, want: "12"},
		{name: "googlesql isoyear still answers", dialect: GoogleSQL, query: `SELECT EXTRACT(ISOYEAR FROM DATE '2023-01-01')`, want: "2022"},

		{name: "postgresql date_trunc decade", dialect: PostgreSQL, query: `SELECT DATE_TRUNC('decade', TIMESTAMP '2024-03-05 10:11:12')`, want: "2020-01-01 00:00:00"},
		{name: "postgresql date_trunc century", dialect: PostgreSQL, query: `SELECT DATE_TRUNC('century', TIMESTAMP '2024-03-05 10:11:12')`, want: "2001-01-01 00:00:00"},
		{name: "postgresql date_trunc millennium", dialect: PostgreSQL, query: `SELECT DATE_TRUNC('millennium', TIMESTAMP '2024-03-05 10:11:12')`, want: "2001-01-01 00:00:00"},
		{name: "postgresql date_trunc millisecond", dialect: PostgreSQL, query: `SELECT DATE_TRUNC('millisecond', TIMESTAMP '2024-03-05 10:11:12.123456')`, want: "2024-03-05 10:11:12.123"},
		{name: "postgresql date_trunc microsecond", dialect: PostgreSQL, query: `SELECT DATE_TRUNC('microsecond', TIMESTAMP '2024-03-05 10:11:12.123456')`, want: "2024-03-05 10:11:12.123456"},
		{name: "postgresql date_trunc millisecond of a whole second", dialect: PostgreSQL, query: `SELECT DATE_TRUNC('millisecond', TIMESTAMP '2024-03-05 10:11:12')`, want: "2024-03-05 10:11:12"},

		// BETWEEN SYMMETRIC takes its bounds in either order. A NULL bound
		// makes the whole comparison NULL, which is what keeps the rewrite off
		// PostgreSQL's NULL-skipping LEAST and GREATEST.
		{name: "symmetric with the bounds reversed", dialect: PostgreSQL, query: `SELECT 5 BETWEEN SYMMETRIC 7 AND 3`, want: "1"},
		{name: "symmetric with the bounds in order", dialect: PostgreSQL, query: `SELECT 5 BETWEEN SYMMETRIC 3 AND 7`, want: "1"},
		{name: "symmetric outside the range", dialect: PostgreSQL, query: `SELECT 9 BETWEEN SYMMETRIC 7 AND 3`, want: "0"},
		{name: "not between symmetric", dialect: PostgreSQL, query: `SELECT 5 NOT BETWEEN SYMMETRIC 7 AND 3`, want: "0"},
		{name: "symmetric with a null bound", dialect: PostgreSQL, query: `SELECT 5 BETWEEN SYMMETRIC NULL AND 3`, wantNull: true},
		{name: "asymmetric keeps the written order", dialect: PostgreSQL, query: `SELECT 5 BETWEEN ASYMMETRIC 7 AND 3`, want: "0"},
		{name: "symmetric over strings", dialect: PostgreSQL, query: `SELECT 'b' BETWEEN SYMMETRIC 'c' AND 'a'`, want: "1"},
		{name: "symmetric with a call as a bound", dialect: PostgreSQL, query: `SELECT 5 BETWEEN SYMMETRIC ABS(-7) AND 3`, want: "1"},
		{name: "symmetric with arithmetic in a bound", dialect: PostgreSQL, query: `SELECT 5 BETWEEN SYMMETRIC 3+1 AND 7`, want: "1"},

		// The operator spelling of date arithmetic answers what the function
		// spelling answers.
		{name: "mysql interval operator adds a day", dialect: MySQL, query: `SELECT DATE '2026-01-01' + INTERVAL 1 DAY`, want: "2026-01-02"},
		{name: "mysql interval function adds a day", dialect: MySQL, query: `SELECT DATE_ADD('2026-01-01', INTERVAL 1 DAY)`, want: "2026-01-02"},
		{name: "mysql interval operator subtracts", dialect: MySQL, query: `SELECT DATE '2026-01-01' - INTERVAL 1 DAY`, want: "2025-12-31"},
		{name: "mysql interval operator crosses a month", dialect: MySQL, query: `SELECT DATE '2026-01-31' + INTERVAL 1 MONTH`, want: "2026-02-28"},
		{name: "mysql interval operator with a negative amount", dialect: MySQL, query: `SELECT DATE '2026-01-01' + INTERVAL -1 DAY`, want: "2025-12-31"},
		{name: "googlesql interval operator adds a day", dialect: GoogleSQL, query: `SELECT DATE '2026-01-01' + INTERVAL 1 DAY`, want: "2026-01-02"},
		// The amount is an expression, and a CASE inside it holds the words
		// that otherwise end the scan for the unit.
		{name: "mysql interval amount is a case expression", dialect: MySQL, query: `SELECT DATE '2026-01-01' + INTERVAL CASE WHEN 1 THEN 1 ELSE 2 END DAY`, want: "2026-01-02"},
		{name: "mysql interval amount is arithmetic", dialect: MySQL, query: `SELECT DATE '2026-01-01' + INTERVAL 1 + 1 DAY`, want: "2026-01-03"},

		// SUBSTRING(s FROM p) with a pattern extracts the match, and returns
		// the first capture group when the pattern has one. Every want was
		// read from PostgreSQL 17.10.
		{name: "postgresql substring extracts a match", dialect: PostgreSQL, query: `SELECT SUBSTRING('abc123' FROM '[0-9]+')`, want: "123"},
		{name: "postgresql substring returns the capture group", dialect: PostgreSQL, query: `SELECT SUBSTRING('abc123' FROM 'b(c)1')`, want: "c"},
		{name: "postgresql substring with no match", dialect: PostgreSQL, query: `SELECT SUBSTRING('abc123' FROM '[x]+')`, wantNull: true},
		// A numeric-looking pattern is still a pattern, because PostgreSQL
		// reads the operand's type rather than its value.
		{name: "postgresql substring reads a numeric string as a pattern", dialect: PostgreSQL, query: `SELECT SUBSTRING('abc123' FROM '2')`, want: "2"},
		{name: "postgresql substring reads a number as a position", dialect: PostgreSQL, query: `SELECT SUBSTRING('abc123' FROM 2)`, want: "bc123"},
		{name: "postgresql substring with a length is positional", dialect: PostgreSQL, query: `SELECT SUBSTRING('abc123' FROM 2 FOR 3)`, want: "bc1"},
		// The other two dialects have only the positional reading.
		{name: "mysql substring from a string operand", dialect: MySQL, query: `SELECT SUBSTRING('abc123' FROM 2)`, want: "bc123"},
		// A non-literal operand carries no kind into the translation, so the
		// helper reads it from the value: a number is a position and anything
		// else is a pattern.
		{name: "postgresql substring from a computed position", dialect: PostgreSQL, query: `SELECT SUBSTRING('abc123' FROM 1 + 1)`, want: "bc123"},
		{name: "postgresql substring from a column pattern", dialect: PostgreSQL, query: `SELECT SUBSTRING('abc123' FROM p) FROM (SELECT '[0-9]+' AS p)`, want: "123"},
		{name: "postgresql substring from a column position", dialect: PostgreSQL, query: `SELECT SUBSTRING('abc123' FROM p) FROM (SELECT 2 AS p)`, want: "bc123"},

		// A distance between two times is counted in a unit the answer fits,
		// rather than through a time.Duration, whose int64 of nanoseconds
		// saturates about 292 years out. 9999-12-31 is the ordinary "no
		// expiry" sentinel, so counting the days to it is a query people
		// write, and it used to answer 106751 -- the bound -- for every unit.
		{name: "date_diff in days over a millennium", dialect: GoogleSQL, query: `SELECT DATE_DIFF(DATE '9999-12-31', DATE '2024-03-05', DAY)`, want: "2913109"},
		{name: "date_diff in hours over a millennium", dialect: GoogleSQL, query: `SELECT DATE_DIFF(DATE '9999-12-31', DATE '2024-03-05', HOUR)`, want: "69914616"},
		{name: "date_diff in minutes over a millennium", dialect: GoogleSQL, query: `SELECT DATE_DIFF(DATE '9999-12-31', DATE '2024-03-05', MINUTE)`, want: "4194876960"},
		{name: "date_diff in seconds over a millennium", dialect: GoogleSQL, query: `SELECT DATE_DIFF(DATE '9999-12-31', DATE '2024-03-05', SECOND)`, want: "251692617600"},
		{name: "date_diff in weeks over a millennium", dialect: GoogleSQL, query: `SELECT DATE_DIFF(DATE '9999-12-31', DATE '2024-03-05', WEEK)`, want: "416158"},
		{name: "date_diff in days backwards over a millennium", dialect: GoogleSQL, query: `SELECT DATE_DIFF(DATE '2024-03-05', DATE '9999-12-31', DAY)`, want: "-2913109"},
		{name: "mysql datediff over a millennium", dialect: MySQL, query: `SELECT DATEDIFF('9999-12-31', '2024-03-05')`, want: "2913109"},
		// The day either side of where the saturation used to begin.
		{name: "date_diff at the old bound", dialect: GoogleSQL, query: `SELECT DATE_DIFF(DATE '2262-04-11', DATE '1970-01-01', DAY)`, want: "106751"},
		{name: "date_diff one day past the old bound", dialect: GoogleSQL, query: `SELECT DATE_DIFF(DATE '2262-04-12', DATE '1970-01-01', DAY)`, want: "106752"},
		{name: "unix_date past the old bound", dialect: GoogleSQL, query: `SELECT UNIX_DATE(DATE '2300-01-01')`, want: "120530"},
		{name: "unix_date before the old bound", dialect: GoogleSQL, query: `SELECT UNIX_DATE(DATE '1600-01-01')`, want: "-135140"},
		{name: "date_from_unix_date round trips past the old bound", dialect: GoogleSQL, query: `SELECT DATE_FROM_UNIX_DATE(120530)`, want: "2300-01-01"},
		{name: "date_bin with a far origin", dialect: PostgreSQL, query: `SELECT DATE_BIN('1 day', TIMESTAMP '2400-03-05 10:11:12', TIMESTAMP '1970-01-01 00:00:00')`, want: "2400-03-05 00:00:00"},
		{name: "week extraction past the old bound", dialect: GoogleSQL, query: `SELECT EXTRACT(WEEK(MONDAY) FROM DATE '2400-03-05')`, want: "9"},

		// A greatest common divisor is not negative. The magnitude of the
		// smallest int64 is not an int64, so taking the sign off it used to
		// leave it negative and Euclid carried that sign into the answer.
		{name: "gcd at the smallest int64", dialect: PostgreSQL, query: `SELECT GCD(-9223372036854775808, 6)`, want: "2"},
		{name: "gcd of the smallest int64 and zero", dialect: PostgreSQL, query: `SELECT GCD(-9223372036854775808, 0)`, wantErr: true},
		{name: "gcd of zero and the smallest int64", dialect: PostgreSQL, query: `SELECT GCD(0, -9223372036854775808)`, wantErr: true},
		{name: "gcd of the smallest int64 with itself", dialect: PostgreSQL, query: `SELECT GCD(-9223372036854775808, -9223372036854775808)`, wantErr: true},
		{name: "lcm at the smallest int64", dialect: PostgreSQL, query: `SELECT LCM(-9223372036854775808, 1)`, wantErr: true},

		// TO_CHAR reads its argument to tell a date from a number. The
		// template cannot say: a digit in a date template is literal text and
		// a letter in a numeric one is too.
		{name: "a date template holding a fixed time", dialect: PostgreSQL, query: `SELECT TO_CHAR(TIMESTAMP '2024-03-05 13:04:05', 'YYYY-MM-DD 00:00:00')`, want: "2024-03-05 00:00:00"},
		{name: "a date template holding a zone in its text", dialect: PostgreSQL, query: `SELECT TO_CHAR(TIMESTAMP '2024-03-05 13:04:05', 'HH24:MI (UTC+0)')`, want: "13:04 (UTC+0)"},
		{name: "a date template holding a digit", dialect: PostgreSQL, query: `SELECT TO_CHAR(DATE '2024-03-05', 'FMDay 0')`, want: "Tuesday 0"},
		{name: "a numeric template on a date is literal text", dialect: PostgreSQL, query: `SELECT TO_CHAR(DATE '2024-03-05', '9999')`, want: "9999"},
		{name: "a date template on a number is literal text", dialect: PostgreSQL, query: `SELECT TO_CHAR(2024, 'YYYY')`, want: "YYYY"},
		{name: "text that is neither still reads the template", dialect: PostgreSQL, query: `SELECT TO_CHAR('2024', '9999')`, want: " 2024"},

		// A sign is told from the binary operator by what stands before it.
		{name: "a sign after a closing paren is binary", dialect: MySQL, query: `SELECT (4) - 1 >> 1`, want: "1"},
		{name: "a sign after a quoted name is binary", dialect: MySQL, query: "SELECT `n` - 1 >> 1 FROM (SELECT 9 AS n)", want: "4"},
		{name: "a sign after an operator is a sign", dialect: MySQL, query: `SELECT 8 * -1 >> 1`, want: "9223372036854775804"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("%s: expected an error, got %q", tt.query, got.String)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			// NULL and the empty string are the answer these dialects
			// disagree on, so they are told apart rather than folded together.
			if got.Valid == tt.wantNull {
				t.Fatalf("%s returned valid=%v (%q), want null=%v", tt.query, got.Valid, got.String, tt.wantNull)
			}
			if tt.wantNull {
				return
			}
			if got.String != tt.want {
				t.Errorf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestDateFormatMySQLSpecifiers pins DATE_FORMAT against MySQL 8.4 one
// specifier at a time. Twelve of them used to be written as the letter itself,
// which is what MySQL does for a specifier it does not know, so a format string
// asking for a time came back holding a "T" and looked like it had worked.
//
// The week specifiers carry most of the weight, because MySQL numbers weeks
// four ways: %U and %V start the week on Sunday, %u and %v on Monday, %U and %u
// number from zero, and %V and %v borrow the previous year's last week, whose
// year %X and %x give. The dates below are the ones where those four disagree.
//
// Every want was read from MySQL 8.4 rather than derived.
func TestDateFormatMySQLSpecifiers(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Each row is one date and what MySQL writes for the twelve specifiers, in
	// the order of the header below.
	specifiers := []string{"%f", "%k", "%l", "%r", "%T", "%D", "%u", "%U", "%v", "%V", "%x", "%X"}
	rows := []struct {
		date string
		want []string
	}{
		{"2024-02-29 13:05:09.123456", []string{"123456", "13", "1", "01:05:09 PM", "13:05:09", "29th", "09", "08", "09", "08", "2024", "2024"}},
		{"2024-12-31", []string{"000000", "0", "12", "12:00:00 AM", "00:00:00", "31st", "53", "52", "01", "52", "2025", "2024"}},
		{"2024-01-01", []string{"000000", "0", "12", "12:00:00 AM", "00:00:00", "1st", "01", "00", "01", "53", "2024", "2023"}},
		{"2023-01-01", []string{"000000", "0", "12", "12:00:00 AM", "00:00:00", "1st", "00", "01", "52", "01", "2022", "2023"}},
		{"2021-01-01", []string{"000000", "0", "12", "12:00:00 AM", "00:00:00", "1st", "00", "00", "53", "52", "2020", "2020"}},
		{"2015-01-01", []string{"000000", "0", "12", "12:00:00 AM", "00:00:00", "1st", "01", "00", "01", "52", "2015", "2014"}},
		{"2016-01-03", []string{"000000", "0", "12", "12:00:00 AM", "00:00:00", "3rd", "00", "01", "53", "01", "2015", "2016"}},
		{"2000-01-02", []string{"000000", "0", "12", "12:00:00 AM", "00:00:00", "2nd", "00", "01", "52", "01", "1999", "2000"}},
		{"2010-01-01", []string{"000000", "0", "12", "12:00:00 AM", "00:00:00", "1st", "00", "00", "53", "52", "2009", "2009"}},
		{"2012-12-31", []string{"000000", "0", "12", "12:00:00 AM", "00:00:00", "31st", "53", "53", "01", "53", "2013", "2012"}},
		{"2017-01-01", []string{"000000", "0", "12", "12:00:00 AM", "00:00:00", "1st", "00", "01", "52", "01", "2016", "2017"}},
		{"1999-12-31", []string{"000000", "0", "12", "12:00:00 AM", "00:00:00", "31st", "52", "52", "52", "52", "1999", "1999"}},
		{"2024-06-03 09:07:00", []string{"000000", "9", "9", "09:07:00 AM", "09:07:00", "3rd", "23", "22", "23", "22", "2024", "2024"}},
		{"2026-08-21 00:30:00", []string{"000000", "0", "12", "12:30:00 AM", "00:30:00", "21st", "34", "33", "34", "33", "2026", "2026"}},
	}

	for _, row := range rows {
		for i, spec := range specifiers {
			query := `SELECT DATE_FORMAT('` + row.date + `', '` + spec + `')`
			var got sql.NullString
			if err := db.QueryRowContext(context.Background(), query).Scan(&got); err != nil {
				t.Fatalf("%s: %v", query, err)
			}
			if got.String != row.want[i] {
				t.Errorf("%s = %q, want %q", query, got.String, row.want[i])
			}
		}
	}
}

// TestDateFormatOrdinalSuffixes pins %D over the days whose suffix is not the
// one their last digit suggests: 11, 12 and 13 take "th" where 1, 2 and 3 take
// "st", "nd" and "rd". Read from MySQL 8.4.
func TestDateFormatOrdinalSuffixes(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	want := map[string]string{
		"01": "1st", "02": "2nd", "03": "3rd", "04": "4th",
		"11": "11th", "12": "12th", "13": "13th",
		"21": "21st", "22": "22nd", "23": "23rd",
		"30": "30th", "31": "31st",
	}
	for day, spelled := range want {
		query := `SELECT DATE_FORMAT('2024-01-` + day + `', '%D')`
		var got sql.NullString
		if err := db.QueryRowContext(context.Background(), query).Scan(&got); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if got.String != spelled {
			t.Errorf("%s = %q, want %q", query, got.String, spelled)
		}
	}
}

// TestDateFormatKeepsAnUnknownSpecifierAsItsLetter pins the fallback that hid
// the twelve above as deliberate: MySQL writes an unknown specifier as the
// letter itself, and so does this.
func TestDateFormatKeepsAnUnknownSpecifierAsItsLetter(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var got sql.NullString
	query := `SELECT DATE_FORMAT('2024-02-29', '%q%%%Z')`
	if err := db.QueryRowContext(context.Background(), query).Scan(&got); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	if got.String != "q%Z" {
		t.Fatalf("%s = %q, want %q", query, got.String, "q%Z")
	}
}

// TestGoogleSQLSubstrFollowsBigQuery pins BigQuery's rule for SUBSTR beside the
// two the other dialects follow, so the three are stated together.
//
// BigQuery reads position 0 as position 1, counts a negative position back from
// the end, and clamps a position that lands before the string to its start with
// the length measured from there. MySQL answers an empty string for position 0,
// and PostgreSQL lets the out-of-range prefix consume the length.
//
// The values were read from BigQuery through goccy/bigquery-emulator, except
// the multibyte rows: it slices bytes there and returns broken UTF-8, which is
// its own defect. A position is a character position in all three dialects.
func TestGoogleSQLSubstrFollowsBigQuery(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tests := []struct {
		call                      string
		google, mysql, postgresql string
	}{
		{"'abcdef', 0, 2", "ab", "", "a"},
		{"'abcdef', 0, 1", "a", "", ""},
		{"'abcdef', 0, 3", "abc", "", "ab"},
		{"'abcdef', 1, 2", "ab", "ab", "ab"},
		{"'abcdef', 3, 100", "cdef", "cdef", "cdef"},
		{"'abcdef', 10, 2", "", "", ""},
		{"'abcdef', 1, 0", "", "", ""},
		{"'abcdef', -2, 3", "ef", "ef", ""},
		{"'abcdef', -7, 2", "ab", "", ""},
		{"'abcdef', -10, 3", "abc", "", ""},
		{"'abcdef', -100, 100", "abcdef", "", ""},
		{"'日本語です', 2, 2", "本語", "本語", "本語"},
		{"'日本語です', 0, 2", "日本", "", "日"},
	}

	for _, tt := range tests {
		for _, helper := range []struct {
			fn   string
			want string
		}{
			{"googlesql_substr", tt.google},
			{"mysql_substr", tt.mysql},
			{"postgresql_substr", tt.postgresql},
		} {
			query := "SELECT " + helper.fn + "(" + tt.call + ")"
			var got sql.NullString
			if err := db.QueryRowContext(context.Background(), query).Scan(&got); err != nil {
				t.Errorf("%s: %v", query, err)
				continue
			}
			if !got.Valid {
				t.Errorf("%s = NULL, want %q", query, helper.want)
				continue
			}
			if got.String != helper.want {
				t.Errorf("%s = %q, want %q", query, got.String, helper.want)
			}
		}
	}
}

// TestRoundHonorsANegativeDigitCount pins that rounding to a ten, a hundred or a
// thousand happens. MySQL, PostgreSQL and BigQuery agree on every value below,
// so one helper answers all three; SQLite's own round() ignores a negative
// digit count, which is its documented behavior and stays the answer for
// dialect.SQLite.
//
// Read from MySQL 8.4, PostgreSQL 17 and goccy/bigquery-emulator.
func TestRoundHonorsANegativeDigitCount(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tests := map[string]string{
		"12345, -1":  "12350",
		"12345, -2":  "12300",
		"12345, -3":  "12000",
		"12345, -5":  "0",
		"-12345, -2": "-12300",
		"15, -1":     "20",
		"25, -1":     "30",
		"-15, -1":    "-20",
		"999, -3":    "1000",
		"0, -3":      "0",
		"12345, 0":   "12345",
		"1.26, 1":    "1.3",
		// A tie rounds away from zero in all three engines, which is also what
		// SQLite's own round() does.
		"0.5, 0":  "1",
		"1.5, 0":  "2",
		"2.5, 0":  "3",
		"-2.5, 0": "-3",
		"1.25, 1": "1.3",
		"1.35, 1": "1.4",
		// A digit count past what a float64 carries: the value comes back as it
		// is, and a count past the smallest power of ten it holds rounds the
		// whole value away. Both are what MySQL and BigQuery answer, and both
		// used to come back NaN from an infinite scale.
		"1.5, 400":                    "1.5",
		"12345, -400":                 "0",
		"12345, -9223372036854775808": "0",
	}

	for call, want := range tests {
		query := "SELECT dialect_round(" + call + ")"
		var got sql.NullString
		if err := db.QueryRowContext(context.Background(), query).Scan(&got); err != nil {
			t.Errorf("%s: %v", query, err)
			continue
		}
		if got.String != want {
			t.Errorf("%s = %q, want %q", query, got.String, want)
		}
	}

	var null sql.NullString
	if err := db.QueryRowContext(context.Background(), "SELECT dialect_round(NULL, -2)").Scan(&null); err != nil {
		t.Fatalf("dialect_round(NULL, -2): %v", err)
	}
	if null.Valid {
		t.Fatalf("dialect_round(NULL, -2) = %q, want NULL", null.String)
	}
}

// TestFormatDateKnowsItsSpecifiers pins FORMAT_DATE against BigQuery one
// specifier at a time. Twenty of them used to be written as the letter itself,
// which is what BigQuery does for a specifier it does not know, so a format
// string asking for a time came back holding an "X".
//
// The values were read from BigQuery through goccy/bigquery-emulator, except
// four it answers wrongly: it renders %U and %W as the ISO week, answers %w
// with 8, and drops the zero padding from %j, %U, %V and %W. Those come from
// the documented definitions instead -- %j is 001 to 366 and the week numbers
// are 00 to 53 -- and %I, which it answers with 13 where a 12-hour clock reads
// 01, was already right here.
func TestFormatDateKnowsItsSpecifiers(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Each row is one specifier and what BigQuery writes for it, for the four
	// datetimes below. The dates are the ones where the four week numberings
	// disagree with each other and where the week year is not the date's year.
	dates := []string{"2024-02-29 13:05:09", "2024-01-01 00:00:00", "2023-01-01 00:00:00", "2024-12-31 00:00:00"}
	tests := []struct {
		spec string
		want [4]string
	}{
		{"%j", [4]string{"060", "001", "001", "366"}},
		{"%s", [4]string{"1709211909", "1704067200", "1672531200", "1735603200"}},
		{"%C", [4]string{"20", "20", "20", "20"}},
		{"%Q", [4]string{"1", "1", "1", "4"}},
		{"%D", [4]string{"02/29/24", "01/01/24", "01/01/23", "12/31/24"}},
		{"%x", [4]string{"02/29/24", "01/01/24", "01/01/23", "12/31/24"}},
		{"%X", [4]string{"13:05:09", "00:00:00", "00:00:00", "00:00:00"}},
		{"%c", [4]string{"Thu Feb 29 13:05:09 2024", "Mon Jan  1 00:00:00 2024", "Sun Jan  1 00:00:00 2023", "Tue Dec 31 00:00:00 2024"}},
		{"%h", [4]string{"Feb", "Jan", "Jan", "Dec"}},
		{"%k", [4]string{"13", " 0", " 0", " 0"}},
		{"%l", [4]string{" 1", "12", "12", "12"}},
		{"%P", [4]string{"pm", "am", "am", "am"}},
		{"%u", [4]string{"4", "1", "7", "2"}},
		{"%w", [4]string{"4", "1", "0", "2"}},
		{"%G", [4]string{"2024", "2024", "2022", "2025"}},
		{"%V", [4]string{"09", "01", "52", "01"}},
		{"%U", [4]string{"08", "00", "01", "52"}},
		{"%W", [4]string{"09", "01", "00", "53"}},
		{"%n", [4]string{"\n", "\n", "\n", "\n"}},
		{"%t", [4]string{"\t", "\t", "\t", "\t"}},
	}

	for _, tt := range tests {
		for i, date := range dates {
			query := `SELECT FORMAT_DATETIME('` + tt.spec + `', '` + date + `')`
			var got sql.NullString
			if err := db.QueryRowContext(context.Background(), query).Scan(&got); err != nil {
				t.Errorf("%s: %v", query, err)
				continue
			}
			if got.String != tt.want[i] {
				t.Errorf("%s on %s = %q, want %q", tt.spec, date, got.String, tt.want[i])
			}
		}
	}
}

// TestFormatDateKeepsAnUnknownSpecifierAsItsLetter pins the fallback that hid
// the twenty above as deliberate, and TestParseDateStillParses that splitting
// rendering from parsing left parsing alone.
func TestFormatDateKeepsAnUnknownSpecifierAsItsLetter(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tests := map[string]string{
		// An unknown specifier is its own letter, and %% is a percent sign.
		`'%v%%%L'`: "v%L",
		// Text around the specifiers is copied rather than read as a format.
		// The whole string used to become a Go layout, so "2006" and "1" were
		// rendered as the year and the month: this came back as
		// "year 2024 month 2".
		`'year 2006 month 1'`: "year 2006 month 1",
		`'%Y-%m-%d'`:          "2024-02-29",
	}

	for format, want := range tests {
		query := `SELECT FORMAT_DATETIME(` + format + `, '2024-02-29 13:05:09')`
		var got sql.NullString
		if err := db.QueryRowContext(context.Background(), query).Scan(&got); err != nil {
			t.Errorf("%s: %v", query, err)
			continue
		}
		if got.String != want {
			t.Errorf("%s = %q, want %q", query, got.String, want)
		}
	}
}

// TestRoundIsRewrittenOnlyWhereADialectDisagrees pins which calls reach the
// helper. The two-argument form goes to it in every translated dialect, because
// all three answer a negative digit count the same way and SQLite answers none.
// The one-argument form is left alone: rounding to a whole number is what
// SQLite already does, and rewriting it would put a helper in front of a call
// nobody disagrees about.
func TestRoundIsRewrittenOnlyWhereADialectDisagrees(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dialect Dialect
		input   string
		want    string
	}{
		{MySQL, "SELECT ROUND(a, -2) FROM t", `SELECT dialect_round(a, -2) AS "ROUND(a, -2)" FROM t`},
		{PostgreSQL, "SELECT ROUND(a, -2) FROM t", `SELECT dialect_round(a, -2) AS "ROUND(a, -2)" FROM t`},
		{GoogleSQL, "SELECT ROUND(a, -2) FROM t", `SELECT dialect_round(a, -2) AS "ROUND(a, -2)" FROM t`},
		{MySQL, "SELECT ROUND(a) FROM t", "SELECT ROUND(a) FROM t"},
		{GoogleSQL, "SELECT ROUND(a) FROM t", "SELECT ROUND(a) FROM t"},
		{SQLite, "SELECT ROUND(a, -2) FROM t", "SELECT ROUND(a, -2) FROM t"},
	}

	for _, tt := range tests {
		got, err := Translate(tt.dialect, tt.input)
		if err != nil {
			t.Errorf("Translate(%v, %q): %v", tt.dialect, tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("Translate(%v, %q)\n  = %q\nwant %q", tt.dialect, tt.input, got, tt.want)
		}
	}
}

// TestGoogleSQLSubstrIsRoutedToItsOwnHelper pins that the GoogleSQL rewrite
// reaches the helper above rather than SQLite's substr, beside the other two
// dialects so the three stay apart.
func TestGoogleSQLSubstrIsRoutedToItsOwnHelper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dialect Dialect
		want    string
	}{
		{GoogleSQL, `SELECT googlesql_substr(s, 0, 2) AS "SUBSTR(s, 0, 2)" FROM t`},
		{MySQL, `SELECT mysql_substr(s, 0, 2) AS "SUBSTR(s, 0, 2)" FROM t`},
		{PostgreSQL, `SELECT postgresql_substr(s, 0, 2) AS "SUBSTR(s, 0, 2)" FROM t`},
		{SQLite, "SELECT SUBSTR(s, 0, 2) FROM t"},
	}

	for _, tt := range tests {
		got, err := Translate(tt.dialect, "SELECT SUBSTR(s, 0, 2) FROM t")
		if err != nil {
			t.Errorf("Translate(%v): %v", tt.dialect, err)
			continue
		}
		if got != tt.want {
			t.Errorf("Translate(%v)\n  = %q\nwant %q", tt.dialect, got, tt.want)
		}
	}
}

// TestSubstrAndRoundAnswerNullAndBadArity pins the edges the tables above do not
// reach: a NULL argument is NULL rather than an error, and a call with the wrong
// number of arguments is refused by name.
func TestSubstrAndRoundAnswerNullAndBadArity(t *testing.T) {
	t.Parallel()

	nulls := map[string][]driver.Value{
		"googlesql_substr null string": {nil, int64(1)},
		"googlesql_substr null pos":    {"abc", nil},
		"googlesql_substr null len":    {"abc", int64(1), nil},
		"dialect_round null value":     {nil, int64(-2)},
		"dialect_round null digits":    {float64(1), nil},
	}
	for name, args := range nulls {
		var got driver.Value
		var err error
		if strings.HasPrefix(name, "dialect_round") {
			got, err = fnDialectRound(args)
		} else {
			got, err = fnGoogleSQLSubstr(args)
		}
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if got != nil {
			t.Errorf("%s = %v, want NULL", name, got)
		}
	}

	if _, err := fnGoogleSQLSubstr([]driver.Value{"abc"}); err == nil {
		t.Error("googlesql_substr accepted one argument")
	}
	if _, err := fnGoogleSQLSubstr([]driver.Value{"abc", int64(1), int64(2), int64(3)}); err == nil {
		t.Error("googlesql_substr accepted four arguments")
	}
}

// TestRoundAtTheEdgesOfAFloat64 pins the digit counts where the scaling itself
// is the problem. The guard is on what the scaling produces rather than on the
// count, because the two are not the same question: at 18 digits the scale is
// finite and a value below it does round, while at 400 the scale is infinite and
// nothing can.
func TestRoundAtTheEdgesOfAFloat64(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		value   float64
		digits  int64
		want    float64
		wantErr bool
	}{
		"a value below the last place still rounds":       {value: 5e-19, digits: 18, want: 1e-18},
		"a scale too large to represent leaves the value": {value: 1.5, digits: 400, want: 1.5},
		"a large value at a large scale leaves the value": {value: 1e300, digits: 300, want: 1e300},
		"a large value rounded to a ten is unchanged":     {value: math.MaxFloat64, digits: -1, want: math.MaxFloat64},
		// Rounding the largest float64 up to the next unit of 1e308 lands past
		// what a float64 holds. BigQuery raises on the overflow, and an infinity
		// here would flow into the rest of the query as a number.
		"a result past the largest float64 is refused": {value: math.MaxFloat64, digits: -308, wantErr: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := fnDialectRound([]driver.Value{tt.value, tt.digits})
			if tt.wantErr {
				if err == nil {
					t.Fatalf("dialect_round(%v, %d) = %v, want an error", tt.value, tt.digits, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("dialect_round(%v, %d): %v", tt.value, tt.digits, err)
			}
			if got != tt.want {
				t.Fatalf("dialect_round(%v, %d) = %v, want %v", tt.value, tt.digits, got, tt.want)
			}
		})
	}
}

// TestTimestampLiteralsReachTheDateFunctions pins the GoogleSQL TIMESTAMP and
// DATETIME literal forms against BigQuery. A timezone suffix used to miss every
// layout parseTime tried, so EXTRACT answered NULL — a statement about the data
// rather than the query — and a literal with an offset must land on the UTC
// field values, which is the timezone BigQuery extracts in by default. A
// malformed literal fails by name, the way BigQuery refuses it, rather than
// joining the NULLs. Every want was read through goccy/bigquery-emulator.
func TestTimestampLiteralsReachTheDateFunctions(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name    string
		query   string
		want    string
		wantErr bool
	}{
		{name: "a plain timestamp", query: `SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-02-29 13:05:09')`, want: "13"},
		{name: "a utc suffix", query: `SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-02-29 13:05:09Z')`, want: "13"},
		{name: "a utc suffix keeps the minute", query: `SELECT EXTRACT(MINUTE FROM TIMESTAMP '2024-02-29 13:05:09Z')`, want: "5"},
		{name: "a T separator with a utc suffix", query: `SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-02-29T13:05:09Z')`, want: "13"},
		{name: "an offset normalizes to utc", query: `SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-02-29 13:05:09+09:00')`, want: "4"},
		{name: "a negative offset normalizes to utc", query: `SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-02-29 23:05:09-05:00')`, want: "4"},
		{name: "a datetime literal", query: `SELECT EXTRACT(HOUR FROM DATETIME '2024-02-29 13:05:09')`, want: "13"},
		{name: "unix_seconds reads a utc suffix", query: `SELECT UNIX_SECONDS(TIMESTAMP '2024-01-01 00:00:00Z')`, want: "1704067200"},
		{name: "a date literal keeps working", query: `SELECT EXTRACT(YEAR FROM DATE '2024-02-29')`, want: "2024"},
		{name: "a malformed literal fails by name", query: `SELECT EXTRACT(HOUR FROM TIMESTAMP 'not-a-time')`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, GoogleSQL, tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("%s = %q, want an error", tt.query, got.String)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if !got.Valid {
				t.Fatalf("%s answered NULL, want %q", tt.query, tt.want)
			}
			if got.String != tt.want {
				t.Fatalf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestWeekNumberingFollowsEachDialect pins WEEK, ISOWEEK and ISOYEAR per
// dialect, on the dates where the numberings disagree. BigQuery and MySQL begin
// the week on Sunday and number the days before the year's first Sunday as week
// 0; PostgreSQL's week is the ISO week, Monday-first with no week 0. The rows
// hold all three dialects side by side so a change to one that quietly moved
// another fails here. The BigQuery values were read through
// goccy/bigquery-emulator (its year-boundary WEEK answers disagree with
// BigQuery's own documentation, so those rows follow the documented Sunday
// rule), the MySQL values from mysql:8.4 and the PostgreSQL values from
// postgres:17.
func TestWeekNumberingFollowsEachDialect(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name    string
		dialect Dialect
		query   string
		want    string
	}{
		{name: "googlesql week mid-year", dialect: GoogleSQL, query: `SELECT EXTRACT(WEEK FROM DATE '2024-02-29')`, want: "8"},
		{name: "googlesql week in june", dialect: GoogleSQL, query: `SELECT EXTRACT(WEEK FROM DATE '2024-06-15')`, want: "23"},
		{name: "googlesql week before the first sunday", dialect: GoogleSQL, query: `SELECT EXTRACT(WEEK FROM DATE '2024-01-01')`, want: "0"},
		{name: "googlesql week at the year's end", dialect: GoogleSQL, query: `SELECT EXTRACT(WEEK FROM DATE '2024-12-31')`, want: "52"},
		{name: "googlesql week on a new year sunday", dialect: GoogleSQL, query: `SELECT EXTRACT(WEEK FROM DATE '2023-01-01')`, want: "1"},

		{name: "mysql week mid-year", dialect: MySQL, query: `SELECT EXTRACT(WEEK FROM '2024-02-29')`, want: "8"},
		{name: "mysql week in june", dialect: MySQL, query: `SELECT EXTRACT(WEEK FROM '2024-06-15')`, want: "23"},
		{name: "mysql week before the first sunday", dialect: MySQL, query: `SELECT EXTRACT(WEEK FROM '2024-01-01')`, want: "0"},
		{name: "mysql week at the year's end", dialect: MySQL, query: `SELECT EXTRACT(WEEK FROM '2024-12-31')`, want: "52"},
		{name: "mysql week on a new year sunday", dialect: MySQL, query: `SELECT EXTRACT(WEEK FROM '2023-01-01')`, want: "1"},

		{name: "postgresql week mid-year", dialect: PostgreSQL, query: `SELECT EXTRACT(WEEK FROM DATE '2024-02-29')`, want: "9"},
		{name: "postgresql week in june", dialect: PostgreSQL, query: `SELECT EXTRACT(WEEK FROM DATE '2024-06-15')`, want: "24"},
		{name: "postgresql week on new year's day", dialect: PostgreSQL, query: `SELECT EXTRACT(WEEK FROM DATE '2024-01-01')`, want: "1"},
		{name: "postgresql week at the year's end", dialect: PostgreSQL, query: `SELECT EXTRACT(WEEK FROM DATE '2024-12-31')`, want: "1"},
		{name: "postgresql week on a new year sunday", dialect: PostgreSQL, query: `SELECT EXTRACT(WEEK FROM DATE '2023-01-01')`, want: "52"},

		{name: "googlesql isoweek at the year's end", dialect: GoogleSQL, query: `SELECT EXTRACT(ISOWEEK FROM DATE '2024-12-31')`, want: "1"},
		{name: "googlesql isoyear at the year's end", dialect: GoogleSQL, query: `SELECT EXTRACT(ISOYEAR FROM DATE '2024-12-31')`, want: "2025"},
		{name: "googlesql isoweek on a new year sunday", dialect: GoogleSQL, query: `SELECT EXTRACT(ISOWEEK FROM DATE '2023-01-01')`, want: "52"},
		{name: "googlesql isoyear on a new year sunday", dialect: GoogleSQL, query: `SELECT EXTRACT(ISOYEAR FROM DATE '2023-01-01')`, want: "2022"},

		{name: "googlesql date_trunc week lands on sunday", dialect: GoogleSQL, query: `SELECT DATE_TRUNC(DATE '2024-02-29', WEEK)`, want: "2024-02-25"},
		{name: "googlesql date_trunc week from a saturday", dialect: GoogleSQL, query: `SELECT DATE_TRUNC(DATE '2024-02-24', WEEK)`, want: "2024-02-18"},
		{name: "googlesql date_trunc week crosses the year", dialect: GoogleSQL, query: `SELECT DATE_TRUNC(DATE '2024-01-01', WEEK)`, want: "2023-12-31"},
		{name: "googlesql date_trunc isoweek lands on monday", dialect: GoogleSQL, query: `SELECT DATE_TRUNC(DATE '2024-12-31', ISOWEEK)`, want: "2024-12-30"},
		{name: "googlesql date_trunc week keeps a time's shape", dialect: GoogleSQL, query: `SELECT DATE_TRUNC(TIMESTAMP '2024-02-29 13:05:09', WEEK)`, want: "2024-02-25 00:00:00"},

		{name: "postgresql date_trunc week stays on monday", dialect: PostgreSQL, query: `SELECT DATE_TRUNC('week', '2024-02-29')`, want: "2024-02-26 00:00:00"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if got.String != tt.want {
				t.Fatalf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestAddDateShapesItDoesNotKnow covers the ADDDATE calls the rewrite leaves
// alone. A call it cannot read as "value, interval" or "value, days" is passed
// through for SQLite to reject, which is what this package does with anything it
// does not recognize, rather than guessed at.
func TestAddDateShapesItDoesNotKnow(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	for _, query := range []string{
		`SELECT ADDDATE('2026-01-01')`,
		`SELECT SUBDATE('2026-01-01')`,
		`SELECT ADDDATE('2026-01-01', )`,
	} {
		t.Run(query, func(t *testing.T) {
			if _, err := runDialect(t, db, MySQL, query); err == nil {
				t.Fatalf("%s returned no error", query)
			}
		})
	}
}

// TestMySQLWeekFunctionsRejectWrongArity covers the arity guards of the week
// functions, which take one argument or two and nothing else.
func TestMySQLWeekFunctionsRejectWrongArity(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	for _, query := range []string{
		`SELECT WEEK()`,
		`SELECT WEEK('2026-01-01', 0, 0)`,
		`SELECT YEARWEEK()`,
		`SELECT YEARWEEK('2026-01-01', 0, 0)`,
	} {
		t.Run(query, func(t *testing.T) {
			if _, err := runDialect(t, db, MySQL, query); err == nil {
				t.Fatalf("%s returned no error", query)
			}
		})
	}
}

// TestMySQLDateFunctionsFollowMySQL pins TIMESTAMPDIFF, FROM_UNIXTIME, TIMEDIFF,
// STR_TO_DATE, the ADDDATE and SUBDATE synonyms and the week and quarter
// functions against MySQL 8.4. TIMESTAMPDIFF counts complete units, not
// the calendar boundaries BigQuery's DATE_DIFF counts, so the GoogleSQL rows
// stand guard beside the MySQL ones: a MySQL fix that moved the shared helper
// would fail them. FROM_UNIXTIME is NULL outside MySQL's documented range,
// TIMEDIFF answers in MySQL's TIME shape with its ±838:59:59 clamp, and
// STR_TO_DATE's shape follows the format's specifiers, refusing an incomplete
// date. Every want was read from mysql:8.4 or goccy/bigquery-emulator.
func TestMySQLDateFunctionsFollowMySQL(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name     string
		dialect  Dialect
		query    string
		want     string
		wantNull bool
	}{
		// TIMESTAMPDIFF: a span short of a complete unit counts zero.
		{name: "timestampdiff day short of 24 hours", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(DAY, '2024-01-01 23:00:00', '2024-01-02 22:00:00')`, want: "0"},
		{name: "timestampdiff week short of 7 days", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(WEEK, '2024-01-01 23:00:00', '2024-01-08 22:00:00')`, want: "0"},
		{name: "timestampdiff month to a shorter month's end", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(MONTH, '2024-01-31', '2024-02-29')`, want: "0"},
		{name: "timestampdiff month one day short", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(MONTH, '2024-01-15', '2024-03-14')`, want: "1"},
		{name: "timestampdiff month backward truncates toward zero", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(MONTH, '2024-02-29', '2024-01-31')`, want: "0"},
		{name: "timestampdiff quarter one day short", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(QUARTER, '2024-01-31', '2024-04-30')`, want: "0"},
		{name: "timestampdiff year one day short", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(YEAR, '2020-06-15', '2024-06-14')`, want: "3"},
		{name: "timestampdiff year from a leap day", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(YEAR, '2024-02-29', '2028-02-28')`, want: "3"},
		{name: "timestampdiff hour short of an hour", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(HOUR, '2024-01-01 00:30:00', '2024-01-01 01:29:00')`, want: "0"},
		{name: "timestampdiff whole day", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(DAY, '2024-01-01', '2024-01-02')`, want: "1"},
		{name: "timestampdiff backward whole days", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(DAY, '2026-01-10', '2026-01-01')`, want: "-9"},
		{name: "timestampdiff month decided by the time of day", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(MONTH, '2024-01-15 10:00:00', '2024-02-15 09:00:00')`, want: "0"},
		{name: "timestampdiff month completed by the time of day", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(MONTH, '2024-01-15 10:00:00', '2024-02-15 11:00:00')`, want: "1"},
		{name: "timestampdiff seconds", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(SECOND, '2024-01-01 00:00:00', '2024-01-01 00:01:30')`, want: "90"},
		{name: "timestampdiff minute short of a minute", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(MINUTE, '2024-01-01 00:00:59', '2024-01-01 00:01:58')`, want: "0"},
		// MySQL's DATETIME range spans nine millennia, past what a
		// time.Duration can hold.
		{name: "timestampdiff day across nine millennia", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(DAY, '1000-01-01', '9999-12-31')`, want: "3287181"},
		{name: "timestampdiff second across nine millennia", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(SECOND, '1000-01-01', '9999-12-31')`, want: "284012438400"},
		{name: "timestampdiff year across nine millennia", dialect: MySQL, query: `SELECT TIMESTAMPDIFF(YEAR, '1000-01-01', '9999-12-31')`, want: "8999"},

		// BigQuery counts boundaries for the same spans; these rows keep the
		// shared helper where it is.
		{name: "googlesql date_diff month counts the boundary", dialect: GoogleSQL, query: `SELECT DATE_DIFF(DATE '2024-02-29', DATE '2024-01-31', MONTH)`, want: "1"},
		{name: "googlesql timestamp_diff day counts the boundary", dialect: GoogleSQL, query: `SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-02 22:00:00', TIMESTAMP '2024-01-01 23:00:00', DAY)`, want: "1"},
		{name: "googlesql timestamp_diff hour is a whole interval", dialect: GoogleSQL, query: `SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-01 01:29:00', TIMESTAMP '2024-01-01 00:30:00', HOUR)`, want: "0"},

		// FROM_UNIXTIME: NULL outside 0..32536771199.
		{name: "from_unixtime refuses a negative epoch", dialect: MySQL, query: `SELECT FROM_UNIXTIME(-1)`, wantNull: true},
		{name: "from_unixtime at zero", dialect: MySQL, query: `SELECT FROM_UNIXTIME(0)`, want: "1970-01-01 00:00:00"},
		{name: "from_unixtime at the ceiling", dialect: MySQL, query: `SELECT FROM_UNIXTIME(32536771199)`, want: "3001-01-18 23:59:59"},
		{name: "from_unixtime past the ceiling", dialect: MySQL, query: `SELECT FROM_UNIXTIME(32536771200)`, wantNull: true},
		{name: "from_unixtime with a format refuses the same range", dialect: MySQL, query: `SELECT FROM_UNIXTIME(-1, '%Y')`, wantNull: true},
		{name: "from_unixtime with a format inside the range", dialect: MySQL, query: `SELECT FROM_UNIXTIME(1, '%Y')`, want: "1970"},

		// TIMEDIFF: MySQL's TIME shape, hours past 24, the ±838:59:59 clamp.
		{name: "timediff within a day", dialect: MySQL, query: `SELECT TIMEDIFF('2024-01-02 01:30:00', '2024-01-02 00:00:00')`, want: "01:30:00"},
		{name: "timediff crosses a day", dialect: MySQL, query: `SELECT TIMEDIFF('2024-01-02 01:30:00', '2024-01-01 00:00:00')`, want: "25:30:00"},
		{name: "timediff negative", dialect: MySQL, query: `SELECT TIMEDIFF('2024-01-01 00:00:00', '2024-01-02 01:30:00')`, want: "-25:30:00"},
		{name: "timediff clamps high", dialect: MySQL, query: `SELECT TIMEDIFF('2024-03-01 00:00:00', '2024-01-01 00:00:00')`, want: "838:59:59"},
		{name: "timediff clamps low", dialect: MySQL, query: `SELECT TIMEDIFF('2024-01-01 00:00:00', '2024-03-01 00:00:00')`, want: "-838:59:59"},
		{name: "timediff of two times", dialect: MySQL, query: `SELECT TIMEDIFF('13:05:09', '01:05:09')`, want: "12:00:00"},
		{name: "timediff keeps a fraction", dialect: MySQL, query: `SELECT TIMEDIFF('00:00:01.500000', '00:00:00')`, want: "00:00:01.500000"},
		// A bare TIME's hours pass 23 and carry a sign, which no calendar
		// layout reads.
		{name: "timediff of a time past 24 hours", dialect: MySQL, query: `SELECT TIMEDIFF('25:00:00', '00:00:00')`, want: "25:00:00"},
		{name: "timediff of a negative time", dialect: MySQL, query: `SELECT TIMEDIFF('-01:00:00', '00:00:00')`, want: "-01:00:00"},
		{name: "timediff of a negative time and a positive one", dialect: MySQL, query: `SELECT TIMEDIFF('-01:00:00', '01:30:00')`, want: "-02:30:00"},
		{name: "timediff of single-digit hours", dialect: MySQL, query: `SELECT TIMEDIFF('8:00:00', '0:30:00')`, want: "07:30:00"},
		{name: "timediff clamps each argument first", dialect: MySQL, query: `SELECT TIMEDIFF('2000:00:00', '1000:00:00')`, want: "00:00:00"},
		{name: "timediff refuses mixed shapes", dialect: MySQL, query: `SELECT TIMEDIFF('2024-01-01 00:00:00', '01:00:00')`, wantNull: true},
		{name: "timediff of null", dialect: MySQL, query: `SELECT TIMEDIFF(NULL, '01:00:00')`, wantNull: true},
		{name: "timediff of a malformed value", dialect: MySQL, query: `SELECT TIMEDIFF('not-a-time', '01:00:00')`, wantNull: true},

		// STR_TO_DATE: the shape follows the format's specifiers.
		{name: "str_to_date date only", dialect: MySQL, query: `SELECT STR_TO_DATE('2024-02-29', '%Y-%m-%d')`, want: "2024-02-29"},
		{name: "str_to_date time only", dialect: MySQL, query: `SELECT STR_TO_DATE('13:05:09', '%T')`, want: "13:05:09"},
		{name: "str_to_date datetime", dialect: MySQL, query: `SELECT STR_TO_DATE('2024-02-29 13:05:09', '%Y-%m-%d %H:%i:%s')`, want: "2024-02-29 13:05:09"},
		{name: "str_to_date hour only stays a time", dialect: MySQL, query: `SELECT STR_TO_DATE('07', '%H')`, want: "07:00:00"},
		{name: "str_to_date refuses a year alone", dialect: MySQL, query: `SELECT STR_TO_DATE('2024', '%Y')`, wantNull: true},
		{name: "str_to_date refuses a date without a day", dialect: MySQL, query: `SELECT STR_TO_DATE('2024-02', '%Y-%m')`, wantNull: true},
		{name: "str_to_date refuses a weekday alone", dialect: MySQL, query: `SELECT STR_TO_DATE('Monday', '%W')`, wantNull: true},
		{name: "str_to_date without a specifier is null", dialect: MySQL, query: `SELECT STR_TO_DATE('x', 'x')`, wantNull: true},

		// STR_TO_DATE's numeric specifiers are padded on output only. On input
		// each reads one digit or two, which is why a date written the way a
		// spreadsheet writes it parses in MySQL.
		{name: "str_to_date reads an unpadded month and day", dialect: MySQL, query: `SELECT STR_TO_DATE('2026-1-5', '%Y-%m-%d')`, want: "2026-01-05"},
		{name: "str_to_date reads a padded month and day", dialect: MySQL, query: `SELECT STR_TO_DATE('2026-01-05', '%Y-%m-%d')`, want: "2026-01-05"},
		{name: "str_to_date reads an unpadded day first", dialect: MySQL, query: `SELECT STR_TO_DATE('5,1,2026', '%d,%m,%Y')`, want: "2026-01-05"},
		{name: "str_to_date reads an unpadded time", dialect: MySQL, query: `SELECT STR_TO_DATE('2026-01-05 9:7:5', '%Y-%m-%d %H:%i:%s')`, want: "2026-01-05 09:07:05"},
		{name: "str_to_date reads an unpadded twelve-hour time", dialect: MySQL, query: `SELECT STR_TO_DATE('2026-01-05 1:07:05 PM', '%Y-%m-%d %h:%i:%s %p')`, want: "2026-01-05 13:07:05"},
		{name: "str_to_date still refuses an impossible date", dialect: MySQL, query: `SELECT STR_TO_DATE('2026-02-30', '%Y-%m-%d')`, wantNull: true},
		{name: "str_to_date still refuses a value that is not a date", dialect: MySQL, query: `SELECT STR_TO_DATE('abc', '%Y-%m-%d')`, wantNull: true},
		// The two-digit year keeps its width: MySQL reads 99 as 1999 and 26 as
		// 2026, which a variable-width year would not.
		{name: "str_to_date reads a two-digit year as MySQL does", dialect: MySQL, query: `SELECT STR_TO_DATE('99-1-5', '%y-%c-%e')`, want: "1999-01-05"},

		// ADDDATE and SUBDATE are MySQL's synonyms of DATE_ADD and DATE_SUB,
		// interval form and day shorthand alike.
		{name: "adddate takes an interval", dialect: MySQL, query: `SELECT ADDDATE('2026-01-31', INTERVAL 1 MONTH)`, want: "2026-02-28"},
		{name: "subdate takes an interval", dialect: MySQL, query: `SELECT SUBDATE('2026-03-31', INTERVAL 1 MONTH)`, want: "2026-02-28"},
		{name: "adddate takes a day count", dialect: MySQL, query: `SELECT ADDDATE('2026-01-31', 1)`, want: "2026-02-01"},
		{name: "subdate takes a day count", dialect: MySQL, query: `SELECT SUBDATE('2026-01-31', 1)`, want: "2026-01-30"},
		{name: "subdate of a negative day count adds", dialect: MySQL, query: `SELECT SUBDATE('2026-01-31', -1)`, want: "2026-02-01"},
		{name: "adddate agrees with date_add", dialect: MySQL, query: `SELECT ADDDATE('2026-01-31', INTERVAL 1 MONTH) = DATE_ADD('2026-01-31', INTERVAL 1 MONTH)`, want: "1"},

		// WEEK, WEEKOFYEAR, YEARWEEK and QUARTER, whose numbers EXTRACT and
		// DATE_FORMAT already reach. The year boundaries are the rows worth
		// having: MySQL lends the first days of January to the previous year in
		// the modes that have no week 0.
		{name: "week defaults to mode zero", dialect: MySQL, query: `SELECT WEEK('2026-01-01')`, want: "0"},
		{name: "week of the first full week", dialect: MySQL, query: `SELECT WEEK('2026-01-04')`, want: "1"},
		{name: "week at the end of the year", dialect: MySQL, query: `SELECT WEEK('2026-12-31')`, want: "52"},
		{name: "week in mode one", dialect: MySQL, query: `SELECT WEEK('2026-01-01', 1)`, want: "1"},
		{name: "week in mode three", dialect: MySQL, query: `SELECT WEEK('2026-01-01', 3)`, want: "1"},
		{name: "week agrees with extract", dialect: MySQL, query: `SELECT WEEK('2026-01-04') = EXTRACT(WEEK FROM '2026-01-04')`, want: "1"},
		{name: "weekofyear is the iso week", dialect: MySQL, query: `SELECT WEEKOFYEAR('2026-01-01')`, want: "1"},
		{name: "yearweek lends january to the previous year", dialect: MySQL, query: `SELECT YEARWEEK('2026-01-01')`, want: "202552"},
		{name: "yearweek of the first full week", dialect: MySQL, query: `SELECT YEARWEEK('2026-01-04')`, want: "202601"},
		{name: "yearweek at the end of the year", dialect: MySQL, query: `SELECT YEARWEEK('2026-12-31')`, want: "202652"},
		{name: "quarter of january", dialect: MySQL, query: `SELECT QUARTER('2026-01-31')`, want: "1"},
		{name: "quarter of april", dialect: MySQL, query: `SELECT QUARTER('2026-04-01')`, want: "2"},
		{name: "quarter of december", dialect: MySQL, query: `SELECT QUARTER('2026-12-31')`, want: "4"},
		{name: "quarter agrees with extract", dialect: MySQL, query: `SELECT QUARTER('2026-12-31') = EXTRACT(QUARTER FROM '2026-12-31')`, want: "1"},

		// A value that is not a date, and a mode that is not a number, answer
		// NULL rather than a zero that would read as a real week.
		{name: "week of null", dialect: MySQL, query: `SELECT WEEK(NULL)`, wantNull: true},
		{name: "week of a value that is not a date", dialect: MySQL, query: `SELECT WEEK('not-a-date')`, wantNull: true},
		{name: "week with a null mode", dialect: MySQL, query: `SELECT WEEK('2026-01-04', NULL)`, wantNull: true},
		{name: "weekofyear of null", dialect: MySQL, query: `SELECT WEEKOFYEAR(NULL)`, wantNull: true},
		{name: "yearweek of null", dialect: MySQL, query: `SELECT YEARWEEK(NULL)`, wantNull: true},
		{name: "yearweek with a null mode", dialect: MySQL, query: `SELECT YEARWEEK('2026-01-04', NULL)`, wantNull: true},
		{name: "quarter of null", dialect: MySQL, query: `SELECT QUARTER(NULL)`, wantNull: true},
		{name: "adddate of null", dialect: MySQL, query: `SELECT ADDDATE(NULL, 1)`, wantNull: true},
		{name: "adddate of a null day count", dialect: MySQL, query: `SELECT ADDDATE('2026-01-01', NULL)`, wantNull: true},
		// The mode is read as MySQL reads it, by its low three bits.
		{name: "week of a mode past the range", dialect: MySQL, query: `SELECT WEEK('2026-01-01', 8)`, want: "0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if got.Valid == tt.wantNull {
				t.Fatalf("%s returned valid=%v (%q), want null=%v", tt.query, got.Valid, got.String, tt.wantNull)
			}
			if !tt.wantNull && got.String != tt.want {
				t.Fatalf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestFunctionsFollowTheSourceDialect covers the calls SQLite spells the same
// way as a source dialect but means something else by, where passing the call
// through answered a plausible wrong value rather than an error. Every expected
// value here was read from the engine itself: mysql:8.4, postgres:17-alpine, and
// the BigQuery documentation for the GoogleSQL rows.
func TestFunctionsFollowTheSourceDialect(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name     string
		dialect  Dialect
		query    string
		want     string
		wantNull bool
		wantErr  bool
	}{
		// LOG. MySQL and GoogleSQL mean the natural logarithm by the one-argument
		// form, where SQLite means the base-10 one, and GoogleSQL writes the base
		// second where SQLite writes it first.
		{name: "mysql log is the natural logarithm", dialect: MySQL, query: `SELECT LOG(10)`, want: "2.302585092994046"},
		{name: "mysql log takes its base first", dialect: MySQL, query: `SELECT LOG(2, 8)`, want: "3"},
		{name: "mysql log10 is unchanged", dialect: MySQL, query: `SELECT LOG10(100)`, want: "2"},
		{name: "mysql log2 is unchanged", dialect: MySQL, query: `SELECT LOG2(8)`, want: "3"},
		{name: "mysql log nested in another call", dialect: MySQL, query: `SELECT ROUND(LOG(10), 3)`, want: "2.303"},
		{name: "googlesql log is the natural logarithm", dialect: GoogleSQL, query: `SELECT LOG(10)`, want: "2.302585092994046"},
		{name: "googlesql log takes its base second", dialect: GoogleSQL, query: `SELECT LOG(8, 2)`, want: "3"},
		{name: "postgresql log is the base-ten logarithm", dialect: PostgreSQL, query: `SELECT log(10)`, want: "1"},
		{name: "postgresql log takes its base first", dialect: PostgreSQL, query: `SELECT log(2, 8)`, want: "3"},
		{name: "log of null", dialect: MySQL, query: `SELECT LOG(NULL)`, wantNull: true},

		// PostgreSQL to_hex converts an integer; GoogleSQL TO_HEX hexes bytes.
		{name: "postgresql to_hex converts the integer", dialect: PostgreSQL, query: `SELECT to_hex(255)`, want: "ff"},
		{name: "postgresql to_hex of zero", dialect: PostgreSQL, query: `SELECT to_hex(0)`, want: "0"},
		{name: "postgresql to_hex reads a negative as two's complement", dialect: PostgreSQL, query: `SELECT to_hex(-1)`, want: "ffffffffffffffff"},
		{name: "postgresql to_hex of null", dialect: PostgreSQL, query: `SELECT to_hex(NULL)`, wantNull: true},
		{name: "postgresql to_hex refuses a value that is not an integer", dialect: PostgreSQL, query: `SELECT to_hex('abc')`, wantErr: true},
		{name: "postgresql to_hex refuses a fractional value", dialect: PostgreSQL, query: `SELECT to_hex(2.5)`, wantErr: true},
		{name: "postgresql to_hex reads an integer written as text", dialect: PostgreSQL, query: `SELECT to_hex('255')`, want: "ff"},
		{name: "googlesql to_hex still hexes bytes", dialect: GoogleSQL, query: `SELECT TO_HEX('ab')`, want: "6162"},

		// REGEXP_REPLACE: MySQL's fourth argument is a start position and its
		// fifth an occurrence, PostgreSQL's fourth is a flag string and its
		// three-argument form replaces one match.
		{name: "mysql regexp_replace replaces every match", dialect: MySQL, query: `SELECT REGEXP_REPLACE('aaa', 'a', 'X')`, want: "XXX"},
		{name: "mysql regexp_replace starts at a position", dialect: MySQL, query: `SELECT REGEXP_REPLACE('aaa', 'a', 'X', 2)`, want: "aXX"},
		{name: "mysql regexp_replace takes one occurrence", dialect: MySQL, query: `SELECT REGEXP_REPLACE('aaa', 'a', 'X', 1, 2)`, want: "aXa"},
		{name: "mysql regexp_replace occurrence zero means every match", dialect: MySQL, query: `SELECT REGEXP_REPLACE('aaa', 'a', 'X', 1, 0)`, want: "XXX"},
		{name: "mysql regexp_replace past the last occurrence", dialect: MySQL, query: `SELECT REGEXP_REPLACE('aaa', 'a', 'X', 1, 9)`, want: "aaa"},
		{name: "mysql regexp_replace at one past the end", dialect: MySQL, query: `SELECT REGEXP_REPLACE('aaa', 'a', 'X', 4)`, want: "aaa"},
		{name: "mysql regexp_replace counts the position in characters", dialect: MySQL, query: `SELECT REGEXP_REPLACE('日本語', '.', 'X', 2)`, want: "日XX"},
		{name: "mysql regexp_replace folds case on request", dialect: MySQL, query: `SELECT REGEXP_REPLACE('AAA', 'a', 'X', 1, 0, 'i')`, want: "XXX"},
		{name: "mysql regexp_replace refuses position zero", dialect: MySQL, query: `SELECT REGEXP_REPLACE('aaa', 'a', 'X', 0)`, wantErr: true},
		{name: "mysql regexp_replace refuses a position past the end", dialect: MySQL, query: `SELECT REGEXP_REPLACE('aaa', 'a', 'X', 5)`, wantErr: true},
		{name: "mysql regexp_replace of null", dialect: MySQL, query: `SELECT REGEXP_REPLACE(NULL, 'a', 'X')`, wantNull: true},
		{name: "postgresql regexp_replace replaces one match", dialect: PostgreSQL, query: `SELECT regexp_replace('aaa', 'a', 'X')`, want: "Xaa"},
		{name: "postgresql regexp_replace replaces all with the g flag", dialect: PostgreSQL, query: `SELECT regexp_replace('aaa', 'a', 'X', 'g')`, want: "XXX"},
		{name: "postgresql regexp_replace folds case with the i flag", dialect: PostgreSQL, query: `SELECT regexp_replace('ABC', 'b', 'x', 'i')`, want: "AxC"},
		{name: "postgresql regexp_replace expands a back reference", dialect: PostgreSQL, query: `SELECT regexp_replace('2026-07', '(\d+)-(\d+)', '\2/\1')`, want: "07/2026"},
		{name: "mysql regexp_replace expands a group reference", dialect: MySQL, query: `SELECT REGEXP_REPLACE('2026-07', '([0-9]+)-([0-9]+)', '$2/$1')`, want: "07/2026"},
		{name: "mysql regexp_replace writes a lone dollar as itself", dialect: MySQL, query: `SELECT REGEXP_REPLACE('a', 'a', '$')`, want: "$"},
		{name: "mysql regexp_replace writes an escaped dollar", dialect: MySQL, query: `SELECT REGEXP_REPLACE('a', 'a', '\\$')`, want: "$"},
		{name: "mysql regexp_replace matches across lines", dialect: MySQL, query: "SELECT REGEXP_REPLACE('a\nb', 'a.b', 'X', 1, 0, 'n')", want: "X"},
		{name: "mysql regexp_replace anchors per line", dialect: MySQL, query: "SELECT REGEXP_REPLACE('a\nb', '^b', 'X', 1, 0, 'm')", want: "a\nX"},
		{name: "mysql regexp_replace refuses a match type it cannot honor", dialect: MySQL, query: `SELECT REGEXP_REPLACE('a', 'a', 'X', 1, 0, 'u')`, wantErr: true},
		{name: "googlesql regexp_replace replaces every match", dialect: GoogleSQL, query: `SELECT REGEXP_REPLACE('aaa', 'a', 'X')`, want: "XXX"},

		// MySQL FORMAT is a number formatter, not printf.
		{name: "mysql format rounds and groups", dialect: MySQL, query: `SELECT FORMAT(1234.5678, 2)`, want: "1,234.57"},
		{name: "mysql format with no decimals", dialect: MySQL, query: `SELECT FORMAT(1234.5678, 0)`, want: "1,235"},
		{name: "mysql format keeps the sign", dialect: MySQL, query: `SELECT FORMAT(-1234.5, 1)`, want: "-1,234.5"},
		{name: "mysql format groups every three digits", dialect: MySQL, query: `SELECT FORMAT(1234567.891, 2)`, want: "1,234,567.89"},
		{name: "mysql format pads to the asked precision", dialect: MySQL, query: `SELECT FORMAT(12.3, 4)`, want: "12.3000"},
		{name: "mysql format reads a negative precision as zero", dialect: MySQL, query: `SELECT FORMAT(1234.5678, -1)`, want: "1,235"},
		{name: "mysql format leaves a short number ungrouped", dialect: MySQL, query: `SELECT FORMAT(999, 0)`, want: "999"},
		{name: "mysql format of null", dialect: MySQL, query: `SELECT FORMAT(NULL, 2)`, wantNull: true},
		{name: "mysql format refuses a locale", dialect: MySQL, query: `SELECT FORMAT(1234.5, 2, 'de_DE')`, wantErr: true},
		{name: "googlesql format still expands printf verbs", dialect: GoogleSQL, query: `SELECT FORMAT('%d items', 3)`, want: "3 items"},
		{name: "postgresql format still expands printf verbs", dialect: PostgreSQL, query: `SELECT format('%d items', 3)`, want: "3 items"},

		// LEFT and RIGHT with a negative length.
		{name: "mysql left of a negative length is empty", dialect: MySQL, query: `SELECT LEFT('abcd', -1)`, want: ""},
		{name: "mysql right of a negative length is empty", dialect: MySQL, query: `SELECT RIGHT('abcd', -1)`, want: ""},
		{name: "mysql left of zero is empty", dialect: MySQL, query: `SELECT LEFT('abcd', 0)`, want: ""},
		{name: "mysql left past the end is the whole string", dialect: MySQL, query: `SELECT LEFT('abc', 10)`, want: "abc"},
		{name: "mysql left counts characters", dialect: MySQL, query: `SELECT LEFT('日本語', 2)`, want: "日本"},
		{name: "mysql right counts characters", dialect: MySQL, query: `SELECT RIGHT('日本語', 2)`, want: "本語"},
		{name: "googlesql left refuses a negative length", dialect: GoogleSQL, query: `SELECT LEFT('abcd', -1)`, wantErr: true},
		{name: "googlesql right refuses a negative length", dialect: GoogleSQL, query: `SELECT RIGHT('abcd', -1)`, wantErr: true},
		{name: "googlesql left of zero is empty", dialect: GoogleSQL, query: `SELECT LEFT('abcd', 0)`, want: ""},
		{name: "postgresql left keeps its negative length", dialect: PostgreSQL, query: `SELECT left('abcd', -1)`, want: "abc"},
		{name: "postgresql right keeps its negative length", dialect: PostgreSQL, query: `SELECT right('abcd', -1)`, want: "bcd"},
		{name: "mysql left of null", dialect: MySQL, query: `SELECT LEFT(NULL, 1)`, wantNull: true},
		{name: "googlesql left of null", dialect: GoogleSQL, query: `SELECT LEFT(NULL, 1)`, wantNull: true},

		// GoogleSQL FORMAT's own verbs.
		{name: "googlesql format prints a string bare", dialect: GoogleSQL, query: `SELECT FORMAT('%t', 'x')`, want: "x"},
		{name: "googlesql format prints a string literal", dialect: GoogleSQL, query: `SELECT FORMAT('%T', 'x')`, want: `"x"`},
		{name: "googlesql format quotes what a literal needs quoted", dialect: GoogleSQL, query: `SELECT FORMAT('%T', 'a"b')`, want: `"a\"b"`},
		{name: "googlesql format prints a real", dialect: GoogleSQL, query: `SELECT FORMAT('%t', 1.5)`, want: "1.5"},
		{name: "googlesql format prints an integer literal", dialect: GoogleSQL, query: `SELECT FORMAT('%T', 3)`, want: "3"},
		{name: "googlesql format prints null", dialect: GoogleSQL, query: `SELECT FORMAT('%t', NULL)`, want: "NULL"},
		{name: "googlesql format prints a null literal", dialect: GoogleSQL, query: `SELECT FORMAT('%T', NULL)`, want: "NULL"},
		{name: "googlesql format mixes its verbs with printf", dialect: GoogleSQL, query: `SELECT FORMAT('%s=%T', 'a', 'b')`, want: `a="b"`},
		{name: "googlesql format keeps a literal percent", dialect: GoogleSQL, query: `SELECT FORMAT('100%% %t', 'x')`, want: "100% x"},
		{name: "googlesql format keeps the printf verbs", dialect: GoogleSQL, query: `SELECT FORMAT('%05.2f', 3.14159)`, want: "03.14"},
		{name: "googlesql format ends on a bare percent", dialect: GoogleSQL, query: `SELECT FORMAT('done %')`, want: "done %"},
		{name: "googlesql format takes a width from an argument", dialect: GoogleSQL, query: `SELECT FORMAT('%*d', 4, 7)`, want: "   7"},
		// A value with no finite decimal form has no MySQL answer either, since
		// MySQL cannot hold one; NULL says so rather than printing "+Inf".
		{name: "mysql format of a value that is not finite", dialect: MySQL, query: `SELECT FORMAT(1e400, 2)`, wantNull: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("%s = %q, want an error", tt.query, got.String)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if got.Valid == tt.wantNull {
				t.Fatalf("%s returned valid=%v (%q), want null=%v", tt.query, got.Valid, got.String, tt.wantNull)
			}
			if !tt.wantNull && got.String != tt.want {
				t.Fatalf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestCastOfAStringToAnIntegerFollowsTheSourceDialect covers the string form of
// a cast to an integer type, which each dialect reads its own way: MySQL takes
// the leading run of digits, while PostgreSQL and GoogleSQL read the whole text
// as an integer literal and raise when it is not one.
func TestCastOfAStringToAnIntegerFollowsTheSourceDialect(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name     string
		dialect  Dialect
		query    string
		want     string
		wantNull bool
		wantErr  bool
	}{
		{name: "mysql truncates a fraction", dialect: MySQL, query: `SELECT CAST('1.5' AS SIGNED)`, want: "1"},
		{name: "mysql stops at an exponent", dialect: MySQL, query: `SELECT CAST('1e3' AS SIGNED)`, want: "1"},
		{name: "mysql takes the leading digits", dialect: MySQL, query: `SELECT CAST('12abc' AS SIGNED)`, want: "12"},
		{name: "mysql keeps a leading sign", dialect: MySQL, query: `SELECT CAST('-12abc' AS SIGNED)`, want: "-12"},
		{name: "mysql reads a leading plus", dialect: MySQL, query: `SELECT CAST('+5' AS SIGNED)`, want: "5"},
		{name: "mysql reads a leading point as zero", dialect: MySQL, query: `SELECT CAST('.5' AS SIGNED)`, want: "0"},
		{name: "mysql trims surrounding spaces", dialect: MySQL, query: `SELECT CAST(' 12 ' AS SIGNED)`, want: "12"},
		{name: "mysql answers zero for a string with no number", dialect: MySQL, query: `SELECT CAST('abc' AS SIGNED)`, want: "0"},
		{name: "mysql clamps a string past the range", dialect: MySQL, query: `SELECT CAST('9223372036854775808' AS SIGNED)`, want: "9223372036854775807"},
		{name: "mysql clamps a leading run past the range", dialect: MySQL, query: `SELECT CAST('99999999999999999999abc' AS SIGNED)`, want: "9223372036854775807"},
		{name: "mysql clamps a negative run past the range", dialect: MySQL, query: `SELECT CAST('-99999999999999999999abc' AS SIGNED)`, want: "-9223372036854775808"},
		{name: "mysql still rounds a number", dialect: MySQL, query: `SELECT CAST(1.5 AS SIGNED)`, want: "2"},

		{name: "postgresql refuses a fraction", dialect: PostgreSQL, query: `SELECT CAST('1.5' AS integer)`, wantErr: true},
		{name: "postgresql refuses an exponent", dialect: PostgreSQL, query: `SELECT CAST('1e3' AS integer)`, wantErr: true},
		{name: "postgresql accepts an integer string", dialect: PostgreSQL, query: `SELECT CAST('12' AS integer)`, want: "12"},
		{name: "postgresql still rounds a number", dialect: PostgreSQL, query: `SELECT CAST(2.5 AS integer)`, want: "2"},
		{name: "postgresql reports a string past the range as out of range", dialect: PostgreSQL, query: `SELECT CAST('9223372036854775808' AS integer)`, wantErr: true},
		{name: "postgresql reports a string past the float range as out of range", dialect: PostgreSQL, query: `SELECT CAST('1e400' AS integer)`, wantErr: true},
		{name: "postgresql refuses a string that underflows to zero", dialect: PostgreSQL, query: `SELECT CAST('1e-400' AS integer)`, wantErr: true},

		{name: "googlesql refuses a fraction", dialect: GoogleSQL, query: `SELECT CAST('1.5' AS INT64)`, wantErr: true},
		{name: "googlesql refuses an exponent", dialect: GoogleSQL, query: `SELECT CAST('1e3' AS INT64)`, wantErr: true},
		{name: "googlesql accepts an integer string", dialect: GoogleSQL, query: `SELECT CAST('12' AS INT64)`, want: "12"},
		{name: "googlesql safe_cast answers null for a fraction", dialect: GoogleSQL, query: `SELECT SAFE_CAST('1.5' AS INT64)`, wantNull: true},
		{name: "googlesql safe_cast still reads an integer string", dialect: GoogleSQL, query: `SELECT SAFE_CAST('12' AS INT64)`, want: "12"},
		{name: "googlesql still rounds a number", dialect: GoogleSQL, query: `SELECT CAST(1.5 AS INT64)`, want: "2"},

		// GoogleSQL reads a hexadecimal string, which is how a column of
		// hexadecimal identifiers becomes numbers. The other two do not:
		// PostgreSQL raises for it and MySQL reads the leading digits, which
		// is the 0 its prefix rule gives.
		{name: "googlesql reads a hexadecimal string", dialect: GoogleSQL, query: `SELECT CAST('0x10' AS INT64)`, want: "16"},
		{name: "googlesql reads an upper-case prefix", dialect: GoogleSQL, query: `SELECT CAST('0X1f' AS INT64)`, want: "31"},
		{name: "googlesql reads a signed hexadecimal string", dialect: GoogleSQL, query: `SELECT CAST('-0x10' AS INT64)`, want: "-16"},
		{name: "googlesql refuses a prefix with no digits", dialect: GoogleSQL, query: `SELECT CAST('0x' AS INT64)`, wantErr: true},
		{name: "googlesql refuses hexadecimal that is not", dialect: GoogleSQL, query: `SELECT CAST('0xzz' AS INT64)`, wantErr: true},
		{name: "googlesql reads the largest hexadecimal int64", dialect: GoogleSQL, query: `SELECT CAST('0x7fffffffffffffff' AS INT64)`, want: "9223372036854775807"},
		{name: "googlesql refuses one past the largest", dialect: GoogleSQL, query: `SELECT CAST('0x8000000000000000' AS INT64)`, wantErr: true},
		{name: "googlesql reads the smallest hexadecimal int64", dialect: GoogleSQL, query: `SELECT CAST('-0x8000000000000000' AS INT64)`, want: "-9223372036854775808"},
		{name: "googlesql safe_cast reads a hexadecimal string", dialect: GoogleSQL, query: `SELECT SAFE_CAST('0x10' AS INT64)`, want: "16"},
		{name: "googlesql safe_cast answers null for bad hexadecimal", dialect: GoogleSQL, query: `SELECT SAFE_CAST('0xzz' AS INT64)`, wantNull: true},
		{name: "postgresql refuses a hexadecimal string", dialect: PostgreSQL, query: `SELECT CAST('0x10' AS integer)`, wantErr: true},
		{name: "mysql reads no digits out of a hexadecimal string", dialect: MySQL, query: `SELECT CAST('0x10' AS SIGNED)`, want: "0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("%s = %q, want an error", tt.query, got.String)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if got.Valid == tt.wantNull {
				t.Fatalf("%s returned valid=%v (%q), want null=%v", tt.query, got.Valid, got.String, tt.wantNull)
			}
			if !tt.wantNull && got.String != tt.want {
				t.Fatalf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}
