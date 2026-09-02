package runtime

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/nao1215/filesql/dialect/internal/dialects"
)

// TestCastSemantics locks in the parts of a cast that SQLite's own CAST gets
// wrong for the source dialect: rounding instead of truncation, honoring the
// length and scale of a parameterized type, and validating a value the target
// type cannot represent instead of quietly coercing it.
func TestCastSemantics(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name    string
		dialect dialects.Dialect
		query   string
		want    string
		null    bool
	}{
		// A fractional value rounds on the way to an integer type in every
		// dialect; SQLite truncates toward zero.
		{"mysql rounds to signed", dialects.MySQL, `SELECT CAST(1.9 AS SIGNED)`, "2", false},
		{"mysql rounds a half away from zero", dialects.MySQL, `SELECT CAST(2.5 AS SIGNED)`, "3", false},
		{"postgresql rounds to integer", dialects.PostgreSQL, `SELECT CAST(1.9 AS INTEGER)`, "2", false},
		{"postgresql rounds a half to even", dialects.PostgreSQL, `SELECT CAST(2.5 AS INTEGER)`, "2", false},
		{"postgresql rounds an odd half up", dialects.PostgreSQL, `SELECT CAST(3.5 AS INTEGER)`, "4", false},
		{"googlesql rounds to int64", dialects.GoogleSQL, `SELECT CAST(1.9 AS INT64)`, "2", false},

		// MySQL coerces where the others raise.
		{"mysql coerces a non-numeric string", dialects.MySQL, `SELECT CAST('abc' AS SIGNED)`, "0", false},
		{"mysql takes a numeric prefix", dialects.MySQL, `SELECT CAST('12abc' AS SIGNED)`, "12", false},
		{"mysql takes the prefix before a second point", dialects.MySQL, `SELECT CAST('1.2.3' AS SIGNED)`, "1", false},
		{"mysql reads a version string as a number", dialects.MySQL, `SELECT CAST('10.5.2' AS DOUBLE)`, "10.5", false},
		{"mysql reads an exponent whole", dialects.MySQL, `SELECT CAST('1e5' AS DOUBLE)`, "100000", false},
		{"mysql nulls an invalid date", dialects.MySQL, `SELECT CAST('not a date' AS DATE)`, "", true},
		{"mysql keeps a valid date", dialects.MySQL, `SELECT CAST('2026-01-15' AS DATE)`, "2026-01-15", false},

		// A parameterized type keeps its scale and length.
		{"mysql decimal scale", dialects.MySQL, `SELECT CAST('3.567' AS DECIMAL(10,2))`, "3.57", false},
		{"mysql bare decimal is scale zero", dialects.MySQL, `SELECT CAST(1.5 AS DECIMAL)`, "2", false},
		{"mysql char length", dialects.MySQL, `SELECT CAST('abcdefghijk' AS CHAR(3))`, "abc", false},
		{"postgresql varchar length", dialects.PostgreSQL, `SELECT 'abcdef'::varchar(3)`, "abc", false},

		// PostgreSQL boolean literals survive instead of collapsing to 0.
		{"postgresql true", dialects.PostgreSQL, `SELECT 'true'::boolean`, "1", false},
		{"postgresql false", dialects.PostgreSQL, `SELECT 'false'::boolean`, "0", false},
		{"postgresql on", dialects.PostgreSQL, `SELECT 'on'::boolean`, "1", false},
		{"postgresql valid uuid", dialects.PostgreSQL, `SELECT '3F2504E0-4F89-11D3-9A0C-0305E82C3301'::uuid`, "3f2504e0-4f89-11d3-9a0c-0305e82c3301", false},
		{"postgresql valid json", dialects.PostgreSQL, `SELECT '{"a":1}'::jsonb`, `{"a":1}`, false},
		{"postgresql valid date", dialects.PostgreSQL, `SELECT '2026-01-15'::date`, "2026-01-15", false},
		{"postgresql null stays null", dialects.PostgreSQL, `SELECT NULL::integer`, "", true},

		// A cast from BYTES to STRING reads the bytes as UTF-8, and GoogleSQL
		// raises for a sequence that is not. The other two dialects do not,
		// which is why the check is asked of one dialect only.
		{"googlesql casts bytes that are utf-8", dialects.GoogleSQL, `SELECT CAST(FROM_HEX('c2a9') AS STRING)`, "\u00a9", false},
		{"googlesql casts empty bytes", dialects.GoogleSQL, `SELECT CAST(b'' AS STRING)`, "", false},
		{"mysql keeps bytes that are not utf-8", dialects.MySQL, `SELECT HEX(CAST(UNHEX('61ff62') AS CHAR))`, "61FF62", false},
		{"postgresql keeps bytes that are not utf-8", dialects.PostgreSQL, `SELECT length(CAST(decode('61ff62', 'hex') AS text))`, "3", false},

		// The type names each dialect has that used to fall through to SQLite's
		// own CAST, where numeric affinity took the leading digits of a value
		// and answered a number. Every want was read from postgres:17 or
		// mysql:8.4.
		{"postgresql casts to a timestamp spelled in full", dialects.PostgreSQL, `SELECT '2024-01-02 03:04:05'::timestamp without time zone`, "2024-01-02 03:04:05", false},
		{"postgresql casts to a timestamp with a zone", dialects.PostgreSQL, `SELECT '2024-01-02 03:04:05'::timestamp with time zone`, "2024-01-02 03:04:05", false},
		{"postgresql casts to a time spelled in full", dialects.PostgreSQL, `SELECT '03:04:05'::time without time zone`, "03:04:05", false},
		{"postgresql casts to a time with a zone", dialects.PostgreSQL, `SELECT '03:04:05'::time with time zone`, "03:04:05", false},
		{"postgresql casts to timetz", dialects.PostgreSQL, `SELECT '03:04:05'::timetz`, "03:04:05", false},
		{"postgresql casts to character varying", dialects.PostgreSQL, `SELECT 'abc'::character varying`, "abc", false},
		{"postgresql applies a character varying length", dialects.PostgreSQL, `SELECT CAST('abc' AS character varying(2))`, "ab", false},
		{"postgresql casts to double precision", dialects.PostgreSQL, `SELECT '1.5'::double precision`, "1.5", false},
		{"postgresql keeps an address", dialects.PostgreSQL, `SELECT '192.168.0.1'::inet`, "192.168.0.1", false},
		{"postgresql keeps a network", dialects.PostgreSQL, `SELECT '192.168.0.0/24'::cidr`, "192.168.0.0/24", false},
		{"postgresql keeps a hardware address", dialects.PostgreSQL, `SELECT '08:00:2b:01:02:03'::macaddr`, "08:00:2b:01:02:03", false},
		{"postgresql keeps a document", dialects.PostgreSQL, `SELECT '<a/>'::xml`, "<a/>", false},
		{"mysql casts to double precision", dialects.MySQL, `SELECT CAST('1.5' AS DOUBLE PRECISION)`, "1.5", false},
		{"mysql casts to a national char", dialects.MySQL, `SELECT CAST('abc' AS NATIONAL CHAR)`, "abc", false},
		{"mysql applies a longtext length", dialects.MySQL, `SELECT CAST('abcd' AS CHARACTER(2))`, "ab", false},
		{"googlesql casts to an interval", dialects.GoogleSQL, `SELECT CAST('1' AS INTERVAL)`, "1", false},

		// A boolean written as a literal is a boolean, not the 1 SQLite stores
		// it as. Only the literal can be told apart: a boolean that is computed
		// reaches a helper as int64, so the reading is decided while lowering.
		// The PostgreSQL wants were read from postgres:17, the MySQL ones from
		// mysql:8.4, and the GoogleSQL ones taken from the ZetaSQL rule that
		// casting BOOL to STRING "returns \"true\" if x is TRUE, \"false\"
		// otherwise".
		{"postgresql writes a true as its word", dialects.PostgreSQL, `SELECT true::text`, "true", false},
		{"postgresql writes a false as its word", dialects.PostgreSQL, `SELECT false::text`, "false", false},
		{"postgresql writes a boolean into a varchar", dialects.PostgreSQL, `SELECT CAST(true AS varchar)`, "true", false},
		{"postgresql truncates the word to the char length", dialects.PostgreSQL, `SELECT CAST(true AS char(2))`, "tr", false},
		{"postgresql keeps a boolean cast to an integer", dialects.PostgreSQL, `SELECT true::int`, "1", false},
		{"googlesql writes a boolean as its word", dialects.GoogleSQL, `SELECT CAST(TRUE AS STRING)`, "true", false},
		{"googlesql writes a false as its word", dialects.GoogleSQL, `SELECT CAST(FALSE AS STRING)`, "false", false},
		{"googlesql keeps a boolean cast to an integer", dialects.GoogleSQL, `SELECT CAST(TRUE AS INT64)`, "1", false},
		{"mysql writes a boolean as json", dialects.MySQL, `SELECT CAST(TRUE AS JSON)`, "true", false},
		{"mysql writes a boolean to char as the number", dialects.MySQL, `SELECT CAST(TRUE AS CHAR)`, "1", false},
		// The boundary: a boolean that is not a literal is the number SQLite
		// stores, in every dialect.
		{"postgresql keeps a computed boolean a number", dialects.PostgreSQL, `SELECT (1 = 1)::text`, "1", false},

		// A bit-string literal names bits, not the text of the digits a caller
		// wrote, and PostgreSQL reads those bits as a base-2 number on the way
		// to an integer. Every want was read from postgres:17.
		{"postgresql reads a bit string as base two", dialects.PostgreSQL, `SELECT B'1010'::int`, "10", false},
		{"postgresql reads a hexadecimal bit string", dialects.PostgreSQL, `SELECT X'41'::int`, "65", false},
		{"postgresql reads a bit string as a bigint", dialects.PostgreSQL, `SELECT B'1010'::bigint`, "10", false},
		// An integer target holds a fixed width, and a bit string that fills a
		// signed one is its negative value.
		{"postgresql reads a full-width integer as negative", dialects.PostgreSQL, `SELECT B'` + "1" + strings.Repeat("0", 31) + `'::int`, "-2147483648", false},
		{"postgresql reads a full-width bigint as negative", dialects.PostgreSQL, `SELECT B'` + strings.Repeat("1", 64) + `'::bigint`, "-1", false},
		{"postgresql reads 32 ones as an integer", dialects.PostgreSQL, `SELECT B'` + strings.Repeat("1", 32) + `'::int`, "-1", false},
		{"postgresql writes a bit string as its digits", dialects.PostgreSQL, `SELECT X'41'::text`, "01000001", false},
		{"postgresql keeps a binary bit string as its digits", dialects.PostgreSQL, `SELECT B'1010'::text`, "1010", false},
		{"postgresql counts the bits of a bit string", dialects.PostgreSQL, `SELECT length(X'41')`, "8", false},
		{"postgresql concatenates two bit strings", dialects.PostgreSQL, `SELECT X'41' || B'1'`, "010000011", false},
		{"postgresql compares two spellings of one bit string", dialects.PostgreSQL, `SELECT (X'41' = B'01000001')`, "1", false},
		{"postgresql reads a bit string of nothing", dialects.PostgreSQL, `SELECT B''::text`, "", false},
		{"postgresql reads a bit string of nothing as zero", dialects.PostgreSQL, `SELECT B''::int`, "0", false},
		{"postgresql reads a bit string of nothing as a zero bigint", dialects.PostgreSQL, `SELECT B''::bigint`, "0", false},
		{"postgresql reads a bit string in lower case", dialects.PostgreSQL, `SELECT x'ab'::text`, "10101011", false},
		{"postgresql reads a bit string written b in lower case", dialects.PostgreSQL, `SELECT b'11'::int`, "3", false},

		// A cast to bytea reads the two input formats PostgreSQL defines for
		// one, so that building a value from a literal and building it with
		// decode() answer the same bytes. Every want was read from postgres:17.
		{"postgresql bytea reads the hex format", dialects.PostgreSQL, `SELECT encode('\x4142'::bytea, 'hex')`, "4142", false},
		{"postgresql bytea allows space between hex pairs", dialects.PostgreSQL, `SELECT encode('\x41 42'::bytea, 'hex')`, "4142", false},
		{"postgresql bytea allows more than one space", dialects.PostgreSQL, `SELECT encode('\x4142  4344'::bytea, 'hex')`, "41424344", false},
		{"postgresql bytea allows space before the first pair", dialects.PostgreSQL, `SELECT encode('\x 41'::bytea, 'hex')`, "41", false},
		{"postgresql bytea reads hex in either case", dialects.PostgreSQL, `SELECT encode('\xAbCd'::bytea, 'hex')`, "abcd", false},
		{"postgresql bytea reads an octal escape", dialects.PostgreSQL, `SELECT encode('a\102b'::bytea, 'hex')`, "614262", false},
		{"postgresql bytea reads a doubled backslash", dialects.PostgreSQL, `SELECT encode('a\\b'::bytea, 'hex')`, "615c62", false},
		{"postgresql bytea keeps a string with no escape", dialects.PostgreSQL, `SELECT encode('AB'::bytea, 'hex')`, "4142", false},
		{"postgresql bytea counts the bytes it decoded", dialects.PostgreSQL, `SELECT octet_length('\x4142'::bytea)`, "2", false},
		{"postgresql bytea reads nothing", dialects.PostgreSQL, `SELECT encode('\x'::bytea, 'hex')`, "", false},
		{"postgresql bytea agrees with decode", dialects.PostgreSQL, `SELECT ('\x4142'::bytea = decode('4142', 'hex'))`, "1", false},

		// SAFE_CAST answers NULL where CAST would raise, which is its purpose.
		{"safe_cast bytes that are not utf-8", dialects.GoogleSQL, `SELECT SAFE_CAST(FROM_HEX('61ff62') AS STRING)`, "", true},
		{"safe_cast invalid int64", dialects.GoogleSQL, `SELECT SAFE_CAST('abc' AS INT64)`, "", true},
		{"safe_cast valid int64", dialects.GoogleSQL, `SELECT SAFE_CAST('42' AS INT64)`, "42", false},
		{"safe_cast invalid float64", dialects.GoogleSQL, `SELECT SAFE_CAST('abc' AS FLOAT64)`, "", true},
		{"safe_cast valid bool", dialects.GoogleSQL, `SELECT SAFE_CAST('true' AS BOOL)`, "1", false},
		{"safe_cast invalid bool", dialects.GoogleSQL, `SELECT SAFE_CAST('nope' AS BOOL)`, "", true},
		{"safe_cast invalid date", dialects.GoogleSQL, `SELECT SAFE_CAST('2026-13-40' AS DATE)`, "", true},
		{"safe_cast valid date", dialects.GoogleSQL, `SELECT SAFE_CAST('2026-01-15' AS DATE)`, "2026-01-15", false},
		{"safe_cast invalid timestamp", dialects.GoogleSQL, `SELECT SAFE_CAST('not-a-timestamp' AS TIMESTAMP)`, "", true},
		{"safe_cast valid timestamp", dialects.GoogleSQL, `SELECT SAFE_CAST('2026-01-15 10:30:00' AS TIMESTAMP)`, "2026-01-15 10:30:00", false},

		// A value past the integer range is not an integer. MySQL clamps to the
		// bound of the type, which is the answer it gives with a warning; the
		// dialects that raise are covered in TestCastRejectsInvalidValues.
		{"mysql clamps a value above the range", dialects.MySQL, `SELECT CAST(1e30 AS SIGNED)`, "9223372036854775807", false},
		{"mysql clamps a value below the range", dialects.MySQL, `SELECT CAST(-1e30 AS SIGNED)`, "-9223372036854775808", false},
		{"mysql clamps an infinity", dialects.MySQL, `SELECT CAST(1e308*10 AS SIGNED)`, "9223372036854775807", false},
		{"mysql clamps a string past the range", dialects.MySQL, `SELECT CAST('99999999999999999999' AS SIGNED)`, "9223372036854775807", false},
		{"mysql keeps a value inside the range", dialects.MySQL, `SELECT CAST(9.2e18 AS SIGNED)`, "9200000000000000000", false},
		{"mysql keeps the largest integer", dialects.MySQL, `SELECT CAST(9223372036854775807 AS SIGNED)`, "9223372036854775807", false},
		{"mysql keeps the smallest integer", dialects.MySQL, `SELECT CAST(-9223372036854775808 AS SIGNED)`, "-9223372036854775808", false},
		{"safe_cast nulls a value past the range", dialects.GoogleSQL, `SELECT SAFE_CAST(1e30 AS INT64)`, "", true},
		{"safe_cast nulls a string past the range", dialects.GoogleSQL, `SELECT SAFE_CAST('99999999999999999999' AS INT64)`, "", true},
		{"safe_cast keeps a value inside the range", dialects.GoogleSQL, `SELECT SAFE_CAST(9.2e18 AS INT64)`, "9200000000000000000", false},
		// A digit string one past the range: no float64 tells it from the bound
		// itself, so the answer has to come from the integer parse.
		{"mysql clamps the string below the range", dialects.MySQL, `SELECT CAST('-9223372036854775809' AS SIGNED)`, "-9223372036854775808", false},
		{"mysql clamps the string above the range", dialects.MySQL, `SELECT CAST('9223372036854775808' AS SIGNED)`, "9223372036854775807", false},
		{"mysql keeps the string at the lower bound", dialects.MySQL, `SELECT CAST('-9223372036854775808' AS SIGNED)`, "-9223372036854775808", false},
		{"safe_cast nulls the string below the range", dialects.GoogleSQL, `SELECT SAFE_CAST('-9223372036854775809' AS INT64)`, "", true},
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
		dialect dialects.Dialect
		query   string
		want    string
		null    bool
	}{
		// YEAR used to map onto the text conversion, so the cast handed its
		// argument straight back.
		{dialects.MySQL, `SELECT CAST('2024-03-05' AS YEAR)`, "2024", false},
		{dialects.MySQL, `SELECT CAST(1.9 AS YEAR)`, "2002", false},
		{dialects.MySQL, `SELECT CAST('12abc' AS YEAR)`, "2012", false},
		{dialects.MySQL, `SELECT CAST(70 AS YEAR)`, "1970", false},
		{dialects.MySQL, `SELECT CAST(69 AS YEAR)`, "2069", false},
		{dialects.MySQL, `SELECT CAST(1901 AS YEAR)`, "1901", false},
		{dialects.MySQL, `SELECT CAST(2155 AS YEAR)`, "2155", false},
		{dialects.MySQL, `SELECT CAST(100 AS YEAR)`, "", true},
		{dialects.MySQL, `SELECT CAST(1900 AS YEAR)`, "", true},
		{dialects.MySQL, `SELECT CAST(2156 AS YEAR)`, "", true},
		{dialects.MySQL, `SELECT CAST('1900-01-01' AS YEAR)`, "", true},
		{dialects.MySQL, `SELECT CAST(0 AS YEAR)`, "0", false},
		{dialects.MySQL, `SELECT CAST('abc' AS YEAR)`, "", true},
		{dialects.MySQL, `SELECT CAST('' AS YEAR)`, "", true},
		{dialects.MySQL, `SELECT CAST(-5 AS YEAR)`, "", true},
		{dialects.MySQL, `SELECT CAST(10000 AS YEAR)`, "", true},

		// A TIME with no clock in it is a number read right to left under MySQL
		// and a refusal under PostgreSQL, where formatting a date as a time
		// answered a midnight a caller cannot tell from a real one.
		{dialects.MySQL, `SELECT CAST('13:45:56' AS TIME)`, "13:45:56", false},
		{dialects.MySQL, `SELECT CAST('2024-03-05 13:45:56' AS TIME)`, "13:45:56", false},
		{dialects.MySQL, `SELECT CAST('2024-03-05' AS TIME)`, "00:20:24", false},
		{dialects.MySQL, `SELECT CAST(123456 AS TIME)`, "12:34:56", false},
		{dialects.MySQL, `SELECT CAST('12abc' AS TIME)`, "00:00:12", false},
		{dialects.MySQL, `SELECT CAST(' 7 ' AS TIME)`, "00:00:07", false},
		{dialects.MySQL, `SELECT CAST('-5' AS TIME)`, "-00:00:05", false},
		{dialects.MySQL, `SELECT CAST(-1.9 AS TIME)`, "-00:00:02", false},
		{dialects.MySQL, `SELECT CAST(9999999 AS TIME)`, "", true},
		{dialects.MySQL, `SELECT CAST('abc' AS TIME)`, "", true},

		// The precision of DECIMAL(p,s) bounds the magnitude. Applying the
		// scale and ignoring the precision let a value the type cannot hold
		// through unchanged.
		{dialects.MySQL, `SELECT CAST(12345 AS DECIMAL(3,0))`, "999", false},
		{dialects.MySQL, `SELECT CAST(-12345 AS DECIMAL(3,0))`, "-999", false},
		{dialects.MySQL, `SELECT CAST('2024-03-05' AS DECIMAL(4,1))`, "999.9", false},
		{dialects.MySQL, `SELECT CAST(1.5 AS DECIMAL(10,2))`, "1.5", false},

		// CONVERT and the BINARY prefix are the cast by MySQL's other two
		// spellings, and reach the same helper.
		{dialects.MySQL, `SELECT CONVERT('12abc', SIGNED)`, "12", false},
		{dialects.MySQL, `SELECT CONVERT('12abc', CHAR(3))`, "12a", false},
		{dialects.MySQL, `SELECT CONVERT('12abc', TIME)`, "00:00:12", false},
		{dialects.MySQL, `SELECT CONVERT('abc' USING utf8mb4)`, "abc", false},
		{dialects.MySQL, `SELECT HEX(BINARY 'abc')`, "616263", false},
		{dialects.MySQL, `SELECT HEX(CHAR(65, 66 USING utf8mb4))`, "4142", false},
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
		dialect dialects.Dialect
		query   string
	}{
		{dialects.PostgreSQL, `SELECT CAST('2024-03-05' AS time)`},
		{dialects.PostgreSQL, `SELECT CAST(12345 AS numeric(3,0))`},
		{dialects.PostgreSQL, `SELECT CAST('{1,2}' AS int[])`},
		{dialects.PostgreSQL, `SELECT '{1,2,3}'::int[]`},
		{dialects.MySQL, `SELECT CONVERT('abc' USING latin1)`},
		{dialects.GoogleSQL, `SELECT CAST(FROM_HEX('61ff62') AS STRING)`},
		{dialects.GoogleSQL, `SELECT CAST(FROM_HEX('ff') AS STRING)`},
		{dialects.PostgreSQL, `SELECT '\x414'::bytea`},
		{dialects.PostgreSQL, `SELECT '\X4142'::bytea`},
		{dialects.PostgreSQL, `SELECT 'a\x'::bytea`},
		{dialects.PostgreSQL, `SELECT 'a\9'::bytea`},
		// A type name no dialect has is refused rather than answered by
		// SQLite's affinity, which made text the number 0.
		{dialects.PostgreSQL, `SELECT 'a'::nosuchtype`},
		{dialects.PostgreSQL, `SELECT '(1,2)'::point`},
		{dialects.PostgreSQL, `SELECT '[1,2)'::int4range`},
		{dialects.PostgreSQL, `SELECT 'a'::int64`},
		{dialects.MySQL, `SELECT CAST('a' AS NOSUCHTYPE)`},
		{dialects.MySQL, `SELECT CAST('a' AS POINT)`},
		{dialects.GoogleSQL, `SELECT CAST('a' AS GEOGRAPHY)`},
		{dialects.GoogleSQL, `SELECT CAST('a' AS NOSUCHTYPE)`},
		// PostgreSQL casts a bit string to an integer and to nothing else
		// numeric, and a bit string past 64 bits does not fit in one.
		{dialects.PostgreSQL, `SELECT B'1010'::numeric`},
		{dialects.PostgreSQL, `SELECT B'1010'::smallint`},
		{dialects.PostgreSQL, `SELECT B'1010'::float8`},
		{dialects.PostgreSQL, `SELECT B'` + strings.Repeat("1", 65) + `'::int`},
		{dialects.PostgreSQL, `SELECT B'` + strings.Repeat("1", 33) + `'::int`},
		{dialects.PostgreSQL, `SELECT B'` + strings.Repeat("1", 65) + `'::bigint`},
		// Whitespace in the hexadecimal form of a bytea is allowed between
		// pairs and nowhere else.
		{dialects.PostgreSQL, `SELECT '\x4 142'::bytea`},
		{dialects.PostgreSQL, `SELECT '\x412 3'::bytea`},
		{dialects.PostgreSQL, `SELECT B'1012'`},
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
		if _, err := Translate(dialects.MySQL, query); err != nil {
			t.Errorf("Translate(mysql, %q): %v", query, err)
		}
	}
}

func TestCastRejectsInvalidValues(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name    string
		dialect dialects.Dialect
		query   string
	}{
		{"postgresql integer", dialects.PostgreSQL, `SELECT CAST('abc' AS INTEGER)`},
		{"postgresql boolean", dialects.PostgreSQL, `SELECT 'nope'::boolean`},
		{"postgresql date", dialects.PostgreSQL, `SELECT 'not-a-date'::date`},
		{"postgresql timestamp", dialects.PostgreSQL, `SELECT 'not-a-time'::timestamp`},
		{"postgresql time", dialects.PostgreSQL, `SELECT 'not-a-time'::time`},
		{"postgresql uuid", dialects.PostgreSQL, `SELECT 'not-a-uuid'::uuid`},
		{"postgresql uuid wrong group length", dialects.PostgreSQL, `SELECT '3f2504e0-4f89-11d3-9a0c-0305e82c33'::uuid`},
		{"postgresql uuid non-hex", dialects.PostgreSQL, `SELECT 'zf2504e0-4f89-11d3-9a0c-0305e82c3301'::uuid`},
		{"postgresql jsonb", dialects.PostgreSQL, `SELECT '{bad json}'::jsonb`},
		{"postgresql float", dialects.PostgreSQL, `SELECT 'abc'::float8`},
		{"googlesql int64", dialects.GoogleSQL, `SELECT CAST('abc' AS INT64)`},
		{"googlesql date", dialects.GoogleSQL, `SELECT CAST('2026-13-40' AS DATE)`},

		// A value past the integer range is a value the type cannot represent,
		// which is what these two dialects raise for.
		{"postgresql integer above the range", dialects.PostgreSQL, `SELECT (1e30)::bigint`},
		{"postgresql integer below the range", dialects.PostgreSQL, `SELECT (-1e30)::bigint`},
		{"postgresql integer from a string past the range", dialects.PostgreSQL, `SELECT '99999999999999999999'::bigint`},
		{"googlesql int64 above the range", dialects.GoogleSQL, `SELECT CAST(1e30 AS INT64)`},
		{"googlesql int64 from an infinity", dialects.GoogleSQL, `SELECT CAST(1e308*10 AS INT64)`},
		{"postgresql integer one below the range", dialects.PostgreSQL, `SELECT '-9223372036854775809'::bigint`},
		{"postgresql integer one above the range", dialects.PostgreSQL, `SELECT '9223372036854775808'::bigint`},
		{"googlesql int64 one below the range", dialects.GoogleSQL, `SELECT CAST('-9223372036854775809' AS INT64)`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := runDialect(t, db, tt.dialect, tt.query); err == nil {
				t.Fatalf("%s should fail", tt.query)
			}
		})
	}
}

// TestTextSurvivesEveryTextCastTarget is the invariant behind the list rather
// than the list itself: a cast to a target this package converts to text must
// answer the text. A target that reaches no helper falls back to SQLite's own
// CAST, and SQLite reads text as a number for a type it has never heard of,
// which is how '192.168.0.1' cast to inet became 192.168.
func TestTextSurvivesEveryTextCastTarget(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	const value = "192.168.0.1"
	for _, d := range []dialects.Dialect{dialects.MySQL, dialects.PostgreSQL, dialects.GoogleSQL} {
		for _, name := range TextCastTargetNames(d) {
			query := fmt.Sprintf("SELECT CAST('%s' AS %s)", value, name)
			got, err := runDialect(t, db, d, query)
			if err != nil {
				t.Errorf("%v: %s: %v", d, query, err)
				continue
			}
			if !got.Valid || got.String != value {
				t.Errorf("%v: %s = %v, want %q", d, query, got, value)
			}
		}
	}
}

// TestCastRefusesATypeItDoesNotModel covers the other half of a cast target:
// SQLite's own CAST is not a conversion to a type it has never heard of. It
// applies numeric affinity, so a value cast to one came back as the number its
// leading digits spell -- '192.168.0.1' as 192.168 -- and the value was gone
// with nothing said. Every engine here raises for a type it does not have.
func TestCastRefusesATypeItDoesNotModel(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect dialects.Dialect
		query   string
	}{
		{dialects.PostgreSQL, "SELECT a::nosuchtype"},
		{dialects.PostgreSQL, "SELECT CAST(a AS point)"},
		{dialects.PostgreSQL, "SELECT a::int64"},
		{dialects.MySQL, "SELECT CAST(x AS GEOMETRY)"},
		{dialects.MySQL, "SELECT CAST(x AS INET)"},
		{dialects.GoogleSQL, "SELECT SAFE_CAST(x AS GEOGRAPHY)"},
		{dialects.GoogleSQL, "SELECT CAST(x AS BYTEA)"},
	} {
		if got, err := Translate(tt.dialect, tt.query); err == nil {
			t.Errorf("Translate(%s, %q) = %q, want a refusal", tt.dialect, tt.query, got)
		}
	}

	// A type the dialect does have still reaches the helper, with the original
	// text kept as the column's name where the spelling changed.
	for _, tt := range []struct {
		dialect dialects.Dialect
		query   string
		want    string
	}{
		{dialects.PostgreSQL, "SELECT a::inet", `SELECT postgresql_cast(a, 'inet') AS "a::inet"`},
		{dialects.MySQL, "SELECT CAST(x AS LONGTEXT)", `SELECT mysql_cast(x, 'LONGTEXT') AS "CAST(x AS LONGTEXT)"`},
	} {
		got, err := Translate(tt.dialect, tt.query)
		if err != nil {
			t.Fatalf("Translate(%s, %q): %v", tt.dialect, tt.query, err)
		}
		if got != tt.want {
			t.Errorf("Translate(%s, %q) = %q, want %q", tt.dialect, tt.query, got, tt.want)
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

	got, err := castValue(dialects.MySQL, "GEOMETRY", "x")
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

	_, err := castValue(dialects.PostgreSQL, "integer", "abc")
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
		dialect dialects.Dialect
		value   float64
		strict  bool
		want    int64
		wantErr bool
	}{
		{name: "mysql clamps NaN to zero", dialect: dialects.MySQL, value: nan, want: 0},
		{name: "googlesql rejects NaN", dialect: dialects.GoogleSQL, value: nan, strict: true, wantErr: true},
		{name: "mysql clamps above the range", dialect: dialects.MySQL, value: 1e30, want: math.MaxInt64},
		{name: "mysql clamps below the range", dialect: dialects.MySQL, value: -1e30, want: math.MinInt64},
		{name: "mysql clamps an infinity", dialect: dialects.MySQL, value: math.Inf(1), want: math.MaxInt64},
		{name: "mysql clamps a negative infinity", dialect: dialects.MySQL, value: math.Inf(-1), want: math.MinInt64},
		{name: "postgresql rejects above the range", dialect: dialects.PostgreSQL, value: 1e30, strict: true, wantErr: true},
		{name: "postgresql rejects below the range", dialect: dialects.PostgreSQL, value: -1e30, strict: true, wantErr: true},
		// The bound itself: no float64 holds the largest integer, so the nearest
		// one above the range is 2^63 and it does not fit, while -2^63 is exact
		// and does.
		{name: "the upper bound does not fit", dialect: dialects.MySQL, value: 9223372036854775808.0, want: math.MaxInt64},
		{name: "the lower bound fits", dialect: dialects.MySQL, value: -9223372036854775808.0, want: math.MinInt64},
		{name: "a value inside the range converts", dialect: dialects.MySQL, value: 9.2e18, want: 9200000000000000000},
		{name: "mysql rounds a half away from zero", dialect: dialects.MySQL, value: 2.5, want: 3},
		{name: "postgresql rounds a half to even", dialect: dialects.PostgreSQL, value: 2.5, want: 2},
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

	got, err := runDialect(t, db, dialects.MySQL, "SELECT CAST('"+huge+"' AS SIGNED)")
	if err != nil {
		t.Fatalf("mysql: %v", err)
	}
	if want := "9223372036854775807"; got.String != want {
		t.Errorf("mysql clamps a string past the float range: got %v, want %q", got, want)
	}

	// The engine returns the helper's message as a SQL error rather than as the
	// wrapped Go error, which is why these assert on failing rather than on the
	// sentinel; the message is checked in TestCastErrorIsInvalidCast.
	if _, err := runDialect(t, db, dialects.PostgreSQL, "SELECT '"+huge+"'::bigint"); err == nil {
		t.Error("postgresql must reject a string past the float range")
	}
	if _, err := runDialect(t, db, dialects.GoogleSQL, "SELECT CAST('"+huge+"' AS INT64)"); err == nil {
		t.Error("googlesql must reject a string past the float range")
	}
}

// TestCastToBlob covers the BLOB target, which every dialect spells differently
// but which all of them mean as "the value's bytes" -- except PostgreSQL, whose
// bytea has two input formats a string is read in.
func TestCastToBlob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect dialects.Dialect
		value   driver.Value
		want    string
		null    bool
	}{
		{name: "bytes pass through", dialect: dialects.MySQL, value: []byte("abc"), want: "abc"},
		{name: "a string becomes its bytes", dialect: dialects.MySQL, value: "abc", want: "abc"},
		{name: "a number becomes its digits", dialect: dialects.MySQL, value: int64(255), want: "255"},
		{name: "a time becomes its written form", dialect: dialects.MySQL, value: time.Date(2026, 7, 28, 13, 5, 9, 0, time.UTC), want: "2026-07-28 13:05:09"},
		{name: "a NULL has no bytes", dialect: dialects.MySQL, value: nil, null: true},
		{name: "googlesql keeps a backslash", dialect: dialects.GoogleSQL, value: `\x4142`, want: `\x4142`},
		{name: "postgresql reads the hex format", dialect: dialects.PostgreSQL, value: `\x4142`, want: "AB"},
		{name: "postgresql reads an octal escape", dialect: dialects.PostgreSQL, value: `a\102b`, want: "aBb"},
		{name: "postgresql keeps a string with no escape", dialect: dialects.PostgreSQL, value: "abc", want: "abc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := castToBlob(tt.dialect, tt.value)
			if err != nil {
				t.Fatalf("castToBlob(%v, %v) error: %v", tt.dialect, tt.value, err)
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

			got, err := castToBool(dialects.PostgreSQL, tt.value, tt.strict)
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
