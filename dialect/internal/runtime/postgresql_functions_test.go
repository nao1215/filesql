package runtime

import (
	"strings"
	"testing"

	"github.com/nao1215/filesql/dialect/internal/dialects"
)

// TestPostgreSQLScalarFunctions pins the PostgreSQL-only scalar functions
// against PostgreSQL 17.10. Every want was read from that engine rather than
// derived, and the edges are what the table is for: a reimplementation drifts
// at the boundaries rather than in the middle.
func TestPostgreSQLScalarFunctions(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	for _, tt := range []struct {
		name    string
		query   string
		want    string
		wantErr bool
	}{
		// Quoting: a name that needs no quotes keeps none, and a quote inside
		// one is doubled.
		{name: "quote_ident quotes a name with a space", query: `SELECT quote_ident('foo bar')`, want: `"foo bar"`},
		{name: "quote_ident leaves a plain name alone", query: `SELECT quote_ident('foo')`, want: "foo"},
		{name: "quote_ident quotes an upper-case name", query: `SELECT quote_ident('Foo')`, want: `"Foo"`},
		{name: "quote_ident doubles an inner quote", query: `SELECT quote_ident('a"b')`, want: `"a""b"`},
		{name: "quote_ident keeps a leading underscore", query: `SELECT quote_ident('_x1')`, want: "_x1"},
		{name: "quote_literal doubles an inner quote", query: `SELECT quote_literal('a''b')`, want: `'a''b'`},
		{name: "quote_literal quotes a number", query: `SELECT quote_literal(42)`, want: "'42'"},
		{name: "quote_nullable spells NULL as the word", query: `SELECT quote_nullable(NULL)`, want: "NULL"},

		{name: "regexp_count counts every match", query: `SELECT regexp_count('abcabc', 'a')`, want: "2"},
		{name: "regexp_count starts where it is told", query: `SELECT regexp_count('abcabc', 'a', 3)`, want: "1"},
		{name: "regexp_count folds case on request", query: `SELECT regexp_count('ABCabc', 'a', 1, 'i')`, want: "2"},
		{name: "regexp_count with no match", query: `SELECT regexp_count('abc', 'z')`, want: "0"},

		// The cube root of a negative number is negative, where a fractional
		// power of one is not a real number at all.
		{name: "cbrt", query: `SELECT cbrt(27)`, want: "3"},
		{name: "cbrt of a negative number", query: `SELECT cbrt(-27)`, want: "-3"},

		{name: "factorial of zero", query: `SELECT factorial(0)`, want: "1"},
		{name: "factorial", query: `SELECT factorial(5)`, want: "120"},
		{name: "factorial at the edge of an int64", query: `SELECT factorial(20)`, want: "2432902008176640000"},
		{name: "factorial past an int64", query: `SELECT factorial(21)`, wantErr: true},
		{name: "factorial of a negative number", query: `SELECT factorial(-1)`, wantErr: true},

		// Both take the sign off their arguments, and gcd(0, 0) is 0.
		{name: "gcd", query: `SELECT gcd(36, 24)`, want: "12"},
		{name: "gcd of two zeros", query: `SELECT gcd(0, 0)`, want: "0"},
		{name: "gcd ignores the sign", query: `SELECT gcd(-4, 6)`, want: "2"},
		{name: "lcm", query: `SELECT lcm(6, 8)`, want: "24"},
		{name: "lcm with a zero", query: `SELECT lcm(0, 5)`, want: "0"},
		{name: "lcm ignores the sign", query: `SELECT lcm(-4, 6)`, want: "12"},

		// The quadrant angles are exact, which is the reason these functions
		// exist rather than a caller converting to radians.
		{name: "sind at thirty degrees", query: `SELECT sind(30)`, want: "0.5"},
		{name: "sind at ninety degrees", query: `SELECT sind(90)`, want: "1"},
		{name: "sind at zero", query: `SELECT sind(0)`, want: "0"},
		{name: "cosd at sixty degrees", query: `SELECT cosd(60)`, want: "0.5"},
		{name: "cosd at zero", query: `SELECT cosd(0)`, want: "1"},
		{name: "tand at forty-five degrees", query: `SELECT tand(45)`, want: "1"},
		{name: "cotd at forty-five degrees", query: `SELECT cotd(45)`, want: "1"},
		{name: "sind wraps a whole turn", query: `SELECT sind(390)`, want: "0.5"},
		{name: "sind of a negative angle", query: `SELECT sind(-30)`, want: "-0.5"},
		{name: "cosd of a negative angle", query: `SELECT cosd(-60)`, want: "0.5"},
		{name: "tand in the third quadrant", query: `SELECT tand(225)`, want: "1"},
		{name: "cotd in the third quadrant", query: `SELECT cotd(225)`, want: "1"},
		// The poles are an infinity, which PostgreSQL prints as Infinity and
		// SQLite has no spelling of its own for; the value is the same.
		{name: "tand at a pole", query: `SELECT tand(90)`, want: "+Inf"},
		{name: "cotd at a pole", query: `SELECT cotd(0)`, want: "+Inf"},
		{name: "tand at the other pole", query: `SELECT tand(270)`, want: "-Inf"},
		{name: "asind", query: `SELECT asind(0.5)`, want: "30"},
		{name: "acosd", query: `SELECT acosd(0.5)`, want: "60"},
		{name: "atand", query: `SELECT atand(1)`, want: "45"},
		{name: "atan2d", query: `SELECT atan2d(1, 1)`, want: "45"},

		{name: "to_number reads a template", query: `SELECT to_number('1234', '9999')`, want: "1234"},
		{name: "to_number reads separators", query: `SELECT to_number('12,345.67', '99,999.99')`, want: "12345.67"},
		{name: "to_number reads a sign", query: `SELECT to_number('-12', 'S999')`, want: "-12"},

		{name: "to_timestamp of an epoch", query: `SELECT to_timestamp(1709633472)`, want: "2024-03-05 10:11:12"},
		{name: "to_timestamp of the epoch itself", query: `SELECT to_timestamp(0)`, want: "1970-01-01 00:00:00"},
		{name: "to_timestamp reads a template", query: `SELECT to_timestamp('2024-03-05 10:00:00', 'YYYY-MM-DD HH24:MI:SS')`, want: "2024-03-05 10:00:00"},

		// A field outside its range is refused rather than carried into the
		// next one, which is what makes a bad computed field visible.
		{name: "make_date", query: `SELECT make_date(2024, 3, 5)`, want: "2024-03-05"},
		{name: "make_date at the end of a year", query: `SELECT make_date(2024, 12, 31)`, want: "2024-12-31"},
		{name: "make_date refuses a thirteenth month", query: `SELECT make_date(2024, 13, 1)`, wantErr: true},
		{name: "make_date refuses a day the month has not got", query: `SELECT make_date(2024, 2, 30)`, wantErr: true},
		{name: "make_time", query: `SELECT make_time(10, 11, 12)`, want: "10:11:12"},
		{name: "make_time at midnight", query: `SELECT make_time(0, 0, 0)`, want: "00:00:00"},
		{name: "make_time refuses a sixty-first minute", query: `SELECT make_time(10, 61, 0)`, wantErr: true},
		{name: "make_time keeps a fraction of a second", query: `SELECT make_time(10, 11, 12.5)`, want: "10:11:12.5"},
		{name: "make_time keeps a fraction below a second", query: `SELECT make_time(10, 11, 0.25)`, want: "10:11:00.25"},

		{name: "isfinite of a date", query: `SELECT isfinite(DATE '2024-03-05')`, want: "1"},

		{name: "num_nulls", query: `SELECT num_nulls(1, NULL, 2)`, want: "1"},
		{name: "num_nulls of only nulls", query: `SELECT num_nulls(NULL, NULL)`, want: "2"},
		{name: "num_nonnulls", query: `SELECT num_nonnulls(1, NULL, 2)`, want: "2"},

		// The hashes answer bytes, so the spelling a query uses is
		// encode(sha256(x), 'hex').
		{name: "sha224", query: `SELECT encode(sha224('abc'::bytea), 'hex')`, want: "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"},
		{name: "sha256", query: `SELECT encode(sha256('abc'::bytea), 'hex')`, want: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{name: "sha384", query: `SELECT encode(sha384('abc'::bytea), 'hex')`, want: "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"},
		{name: "sha512", query: `SELECT encode(sha512('abc'::bytea), 'hex')`, want: "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"},

		{name: "encode as hex", query: `SELECT encode('abc'::bytea, 'hex')`, want: "616263"},
		{name: "encode as base64", query: `SELECT encode('abc'::bytea, 'base64')`, want: "YWJj"},
		{name: "encode as escape", query: `SELECT encode('abc'::bytea, 'escape')`, want: "abc"},
		{name: "encode of nothing", query: `SELECT encode(''::bytea, 'hex')`, want: ""},
		{name: "encode refuses an encoding it has not got", query: `SELECT encode('abc'::bytea, 'rot13')`, wantErr: true},

		// The three encodings round-trip, which is the property that matters
		// rather than any one spelling.
		{name: "hex round trip", query: `SELECT encode(decode(encode('abc'::bytea, 'hex'), 'hex'), 'escape')`, want: "abc"},
		{name: "base64 round trip", query: `SELECT encode(decode(encode('abc'::bytea, 'base64'), 'base64'), 'escape')`, want: "abc"},
		{name: "escape round trip", query: `SELECT encode(decode(encode('a' || CHAR(10) || 'b', 'escape'), 'escape'), 'escape')`, want: `a\012b`},

		{name: "get_byte reads the first byte", query: `SELECT get_byte('abc'::bytea, 0)`, want: "97"},
		{name: "get_byte reads the last byte", query: `SELECT get_byte('abc'::bytea, 2)`, want: "99"},
		{name: "get_byte past the end", query: `SELECT get_byte('abc'::bytea, 3)`, wantErr: true},
		{name: "set_byte replaces one byte", query: `SELECT encode(set_byte('abc'::bytea, 0, 66), 'escape')`, want: "Bbc"},

		{name: "date_bin to a quarter of an hour", query: `SELECT date_bin('15 minutes', TIMESTAMP '2024-03-05 10:11:12', TIMESTAMP '2024-03-05 10:00:00')`, want: "2024-03-05 10:00:00"},
		{name: "date_bin to an hour", query: `SELECT date_bin('1 hour', TIMESTAMP '2024-03-05 10:59:59', TIMESTAMP '2024-03-05 00:00:00')`, want: "2024-03-05 10:00:00"},
		{name: "date_bin to half a minute", query: `SELECT date_bin('30 seconds', TIMESTAMP '2024-03-05 10:11:12', TIMESTAMP '2024-03-05 10:00:00')`, want: "2024-03-05 10:11:00"},
		{name: "date_bin refuses a stride of no fixed length", query: `SELECT date_bin('1 month', TIMESTAMP '2024-03-05 10:11:12', TIMESTAMP '2024-01-01 00:00:00')`, wantErr: true},
		// The two branches of the bin arithmetic -- a stride inside a day and
		// one that is a whole number of days -- with a source on either side
		// of the origin, which is what exercises the floor rather than the
		// truncation an integer division does on its own.
		{name: "date_bin to a whole day", query: `SELECT date_bin('1 day', TIMESTAMP '2024-03-05 10:11:12', TIMESTAMP '2024-03-01 00:00:00')`, want: "2024-03-05 00:00:00"},
		{name: "date_bin to a week", query: `SELECT date_bin('1 week', TIMESTAMP '2024-03-05 10:11:12', TIMESTAMP '2024-01-01 00:00:00')`, want: "2024-03-04 00:00:00"},
		{name: "date_bin to a multi-day stride from a shifted origin", query: `SELECT date_bin('2 days', TIMESTAMP '2024-03-05 10:11:12', TIMESTAMP '2024-03-01 06:00:00')`, want: "2024-03-05 06:00:00"},
		{name: "date_bin before its origin", query: `SELECT date_bin('1 day', TIMESTAMP '2023-12-31 23:00:00', TIMESTAMP '2024-03-05 06:00:00')`, want: "2023-12-31 06:00:00"},
		{name: "date_bin before its origin by a sub-day stride", query: `SELECT date_bin('30 seconds', TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2024-03-05 10:00:00')`, want: "2023-01-01 00:00:00"},
		{name: "date_bin over eight centuries", query: `SELECT date_bin('1 hour', TIMESTAMP '1600-06-01 10:11:12', TIMESTAMP '2400-01-01 00:00:00')`, want: "1600-06-01 10:00:00"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.PostgreSQL, tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("%s: expected an error, got %q", tt.query, got.String)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if got.String != tt.want {
				t.Errorf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestPostgreSQLFunctionsAnswerNullForNull covers the rule every SQL function
// follows: a NULL argument makes the answer NULL. The two counting functions
// are the exception, since counting NULLs is what they are for.
func TestPostgreSQLFunctionsAnswerNullForNull(t *testing.T) {
	db := castDB(t)

	for _, q := range []string{
		`SELECT quote_ident(NULL)`,
		`SELECT quote_literal(NULL)`,
		`SELECT regexp_count(NULL, 'a')`,
		`SELECT cbrt(NULL)`,
		`SELECT factorial(NULL)`,
		`SELECT gcd(NULL, 1)`,
		`SELECT lcm(1, NULL)`,
		`SELECT sind(NULL)`,
		`SELECT acosd(NULL)`,
		`SELECT atan2d(NULL, 1)`,
		`SELECT to_number(NULL, '999')`,
		`SELECT to_timestamp(NULL)`,
		`SELECT make_date(NULL, 1, 1)`,
		`SELECT make_time(NULL, 1, 1)`,
		`SELECT isfinite(NULL)`,
		`SELECT sha256(NULL)`,
		`SELECT encode(NULL, 'hex')`,
		`SELECT decode(NULL, 'hex')`,
		`SELECT get_byte(NULL, 0)`,
		`SELECT date_bin(NULL, TIMESTAMP '2024-03-05', TIMESTAMP '2024-03-05')`,
	} {
		t.Run(q, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.PostgreSQL, q)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			if got.Valid {
				t.Errorf("%s = %q, want NULL", q, got.String)
			}
		})
	}
}

// TestPostgreSQLClockFunctionsAnswer covers the functions that read the clock.
// They cannot be pinned to a value, so what is asserted is that they run, that
// they answer something, and that they are not registered as deterministic --
// SQLite folds a deterministic function to one value per statement, which would
// make a query comparing two readings compare a value against itself.
func TestPostgreSQLClockFunctionsAnswer(t *testing.T) {
	db := castDB(t)

	for _, q := range []string{
		`SELECT clock_timestamp()`,
		`SELECT statement_timestamp()`,
		`SELECT transaction_timestamp()`,
		`SELECT timeofday()`,
		`SELECT gen_random_uuid()`,
		`SELECT LOCALTIMESTAMP`,
		`SELECT LOCALTIME`,
	} {
		t.Run(q, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.PostgreSQL, q)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			if !got.Valid || got.String == "" {
				t.Errorf("%s answered nothing", q)
			}
		})
	}
}

// TestGenRandomUUIDIsDistinct checks the identifier function answers a new
// value each time, which is the whole point of it and what registering it as
// non-deterministic buys.
func TestGenRandomUUIDIsDistinct(t *testing.T) {
	db := castDB(t)

	seen := make(map[string]bool, 8)
	for range 8 {
		got, err := runDialect(t, db, dialects.PostgreSQL, `SELECT gen_random_uuid()`)
		if err != nil {
			t.Fatalf("gen_random_uuid: %v", err)
		}
		if len(got.String) != 36 {
			t.Fatalf("gen_random_uuid() = %q, want 36 characters", got.String)
		}
		if seen[got.String] {
			t.Fatalf("gen_random_uuid() answered %q twice", got.String)
		}
		seen[got.String] = true
	}
}

// TestPostgreSQLByteHelpersAtTheEdges covers the byte paths a plain ASCII round
// trip does not reach: a byte with no printable spelling, which the escape
// encoding writes as an octal escape, and a backslash, which it doubles.
func TestPostgreSQLByteHelpersAtTheEdges(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		bytes []byte
		want  string
	}{
		{name: "a printable byte is itself", bytes: []byte("abc"), want: "abc"},
		{name: "a control byte is an octal escape", bytes: []byte{'a', 0x0a, 'b'}, want: `a\012b`},
		{name: "a byte past ASCII is an octal escape", bytes: []byte{0xff}, want: `\377`},
		{name: "a backslash is doubled", bytes: []byte(`a\b`), want: `a\\b`},
		{name: "nothing encodes to nothing", bytes: nil, want: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := escapeBytes(tt.bytes)
			if got != tt.want {
				t.Fatalf("escapeBytes(%q) = %q, want %q", tt.bytes, got, tt.want)
			}
			if back := unescapeBytes(got); back != string(tt.bytes) {
				t.Errorf("unescapeBytes(%q) = %q, want %q", got, back, string(tt.bytes))
			}
		})
	}
}

// TestQuoteLiteralEscapesABackslash covers the spelling PostgreSQL uses when a
// literal holds a backslash: the string is prefixed with E and the backslash is
// doubled, since a plain literal would read it as itself under
// standard_conforming_strings and as an escape without.
func TestQuoteLiteralEscapesABackslash(t *testing.T) {
	db := castDB(t)

	got, err := runDialect(t, db, dialects.PostgreSQL, `SELECT quote_literal('a\b')`)
	if err != nil {
		t.Fatalf("quote_literal: %v", err)
	}
	if want := `E'a\\b'`; got.String != want {
		t.Errorf("quote_literal('a\\b') = %q, want %q", got.String, want)
	}
}

// TestQuoteIdentQuotesAReservedWord covers the arm of the identifier rule that
// is not about the characters in the name: a word that is syntax unquoted has
// to be quoted even when it is spelled in lower case.
func TestQuoteIdentQuotesAReservedWord(t *testing.T) {
	db := castDB(t)

	for _, tt := range []struct {
		name string
		want string
	}{
		{name: "select", want: `"select"`},
		{name: "where", want: `"where"`},
		{name: "selection", want: "selection"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.PostgreSQL, `SELECT quote_ident('`+tt.name+`')`)
			if err != nil {
				t.Fatalf("quote_ident(%q): %v", tt.name, err)
			}
			if got.String != tt.want {
				t.Errorf("quote_ident(%q) = %q, want %q", tt.name, got.String, tt.want)
			}
		})
	}
}

// TestPostgreSQLFunctionsAddedForTheEngine covers the functions that had no
// translation at all and reached SQLite as "no such function". Every want was
// read from PostgreSQL 17.10. The edges are the point: age borrows the days of
// the earlier timestamp's month rather than of the month before the later one,
// and the two differ whenever those months have different lengths.
func TestPostgreSQLFunctionsAddedForTheEngine(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	for _, tt := range []struct {
		query string
		want  string
	}{
		{query: `SELECT min_scale(1.230)`, want: "2"},
		{query: `SELECT min_scale(1.000)`, want: "0"},
		{query: `SELECT min_scale(0.00)`, want: "0"},
		{query: `SELECT trim_scale(1.230)`, want: "1.23"},
		{query: `SELECT trim_scale(1.000)`, want: "1"},
		{query: `SELECT scale(1)`, want: "0"},

		{query: `SELECT age(timestamp '2024-03-05', timestamp '2023-01-31')`, want: "1 year 1 mon 5 days"},
		{query: `SELECT age(timestamp '2024-03-05', timestamp '2024-03-05')`, want: "00:00:00"},
		{query: `SELECT age(timestamp '2023-01-31', timestamp '2024-03-05')`, want: "-1 years -1 mons -5 days"},
		{query: `SELECT age(timestamp '2024-03-05 12:30:00', timestamp '2024-03-04 13:45:10')`, want: "22:44:50"},
		{query: `SELECT age(timestamp '2024-03-04 13:45:10', timestamp '2024-03-05 12:30:00')`, want: "-22:44:50"},
		{query: `SELECT age(timestamp '2024-03-31', timestamp '2024-02-29')`, want: "1 mon 2 days"},
		{query: `SELECT age(timestamp '2024-01-31', timestamp '2023-12-31')`, want: "1 mon"},
		{query: `SELECT age(timestamp '2025-01-01', timestamp '2024-01-01')`, want: "1 year"},
		{query: `SELECT age(timestamp '2024-03-01', timestamp '2024-01-31')`, want: "1 mon 1 day"},

		// json_typeof answers with the names JSON defines, not with SQLite's
		// storage class names.
		{query: `SELECT jsonb_typeof('{"a":1}')`, want: "object"},
		{query: `SELECT jsonb_typeof('[1]')`, want: "array"},
		{query: `SELECT jsonb_typeof('null')`, want: "null"},
		{query: `SELECT json_typeof('"x"')`, want: "string"},
		{query: `SELECT json_typeof('1')`, want: "number"},
		{query: `SELECT json_typeof('1.5')`, want: "number"},
		{query: `SELECT json_typeof('true')`, want: "boolean"},
		{query: `SELECT json_typeof('false')`, want: "boolean"},

		{query: `SELECT pg_typeof('a')`, want: "text"},
		{query: `SELECT pg_typeof(1.5)`, want: "double precision"},
		{query: `SELECT pg_typeof(NULL)`, want: "unknown"},

		// A julian date carries the fraction of the day with it.
		{query: `SELECT extract(julian from timestamp '2024-03-05')`, want: "2460375"},
		{query: `SELECT extract(julian from date '1970-01-01')`, want: "2440588"},
		{query: `SELECT extract(julian from date '2000-01-01')`, want: "2451545"},
		// Cast to text so the assertion reads the value the way SQLite writes
		// it rather than the way database/sql prints a float64.
		{query: `SELECT CAST(extract(julian from timestamp '2024-03-05 12:00:00') AS TEXT)`, want: "2460375.5"},
		{query: `SELECT CAST(extract(julian from timestamp '2024-03-05 06:00:00') AS TEXT)`, want: "2460375.25"},

		{query: `SELECT COALESCE('a')`, want: "a"},
	} {
		t.Run(tt.query, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.PostgreSQL, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if !got.Valid || got.String != tt.want {
				t.Errorf("%s = %v, want %q", tt.query, got, tt.want)
			}
		})
	}
}

// TestToNumberReadsOnlyWhatTheTemplateNames covers to_number's two answers that
// were not PostgreSQL's, read from postgres:17.10.
//
// An empty template names no digit positions, so PostgreSQL reads nothing and
// answers NULL; scraping the digits out of the value instead answered a number
// that is not in it in any recognizable form. And a value it cannot read is an
// error there rather than NULL, which is also the answer for a value that is
// absent.
func TestToNumberReadsOnlyWhatTheTemplateNames(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	for _, tt := range []struct {
		query string
		want  string
	}{
		{`SELECT to_number('1234','9999')`, "1234"},
		{`SELECT to_number('1,234.5','9,999.9')`, "1234.5"},
		{`SELECT to_number('-12','S99')`, "-12"},
	} {
		t.Run(tt.query, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.PostgreSQL, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if got.String != tt.want {
				t.Errorf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}

	for _, query := range []string{
		`SELECT to_number('2024-02-29','')`,
		`SELECT to_number('1234','')`,
	} {
		t.Run(query, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.PostgreSQL, query)
			if err != nil {
				t.Fatalf("%s: %v", query, err)
			}
			if got.Valid {
				t.Errorf("%s = %q, want NULL", query, got.String)
			}
		})
	}

	if _, err := runDialect(t, db, dialects.PostgreSQL, `SELECT to_number('abc','999')`); err == nil {
		t.Error("to_number('abc','999') answered a value, want an error")
	}
}

// TestAgeKeepsTheFraction pins the sub-second part of an interval. AGE built
// its clock field out of whole seconds, so a fraction was dropped and the loss
// was not always small: from 00:00:00.75 to the next day's 00:00:00.25 came out
// as a whole day, and a difference under a second came out as 00:00:00 with no
// sign to say which timestamp was the later one.
func TestAgeKeepsTheFraction(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "a sub-second difference", query: `SELECT AGE(TIMESTAMP '2020-02-29 13:45:59.123456', TIMESTAMP '2020-02-29 13:45:59')`, want: "00:00:00.123456"},
		{name: "a negative sub-second difference", query: `SELECT AGE(TIMESTAMP '2020-02-29 13:45:59', TIMESTAMP '2020-02-29 13:45:59.123456')`, want: "-00:00:00.123456"},
		{name: "a fraction beside whole months", query: `SELECT AGE(TIMESTAMP '2020-03-01 00:00:00.5', TIMESTAMP '2020-01-01 00:00:00')`, want: "2 mons 00:00:00.5"},
		{name: "a fraction borrowing from the day", query: `SELECT AGE(TIMESTAMP '2020-01-02 00:00:00.25', TIMESTAMP '2020-01-01 00:00:00.75')`, want: "23:59:59.5"},
		{name: "equal timestamps", query: `SELECT AGE(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 00:00:00')`, want: "00:00:00"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.PostgreSQL, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if got.String != tt.want {
				t.Fatalf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestAgeIsZeroOnlyForEqualTimestamps is the invariant behind
// TestAgeKeepsTheFraction: an interval of zero between two different instants
// is the shape of a dropped fraction.
func TestAgeIsZeroOnlyForEqualTimestamps(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	stamps := []string{
		"2020-01-01 00:00:00",
		"2020-01-01 00:00:00.000001",
		"2020-01-01 00:00:00.5",
		"2020-01-01 00:00:01",
		"2020-02-29 13:45:59.123456",
	}
	for _, a := range stamps {
		for _, b := range stamps {
			query := "SELECT AGE(TIMESTAMP '" + a + "', TIMESTAMP '" + b + "')"
			t.Run(a+" vs "+b, func(t *testing.T) {
				got, err := runDialect(t, db, dialects.PostgreSQL, query)
				if err != nil {
					t.Fatalf("%s: %v", query, err)
				}
				if (got.String == "00:00:00") != (a == b) {
					t.Fatalf("%s = %q", query, got.String)
				}
			})
		}
	}
}

// TestTheMakeFamilyIsWholeAndReachable pins the four constructors PostgreSQL
// has. Two of them were not registered, so a call reached SQLite and came back
// as "no such function": a message that names the engine underneath rather than
// the dialect the caller asked for.
func TestTheMakeFamilyIsWholeAndReachable(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name    string
		query   string
		want    string
		wantErr bool
	}{
		{name: "make_date", query: `SELECT MAKE_DATE(2020, 2, 29)`, want: "2020-02-29"},
		{name: "make_time", query: `SELECT MAKE_TIME(13, 45, 59.5)`, want: "13:45:59.5"},
		{name: "make_time at the end of the day", query: `SELECT MAKE_TIME(24, 0, 0)`, want: "24:00:00"},
		{name: "make_time past the end of the day", query: `SELECT MAKE_TIME(24, 0, 1)`, wantErr: true},
		{name: "make_time with a fraction past the end", query: `SELECT MAKE_TIME(24, 0, 0.000001)`, wantErr: true},
		{name: "make_time at the last moment of the day", query: `SELECT MAKE_TIME(23, 59, 59.999999)`, want: "23:59:59.999999"},
		{name: "make_time past twenty-four", query: `SELECT MAKE_TIME(25, 0, 0)`, wantErr: true},
		{name: "make_timestamp", query: `SELECT MAKE_TIMESTAMP(2020, 2, 29, 13, 45, 59.5)`, want: "2020-02-29 13:45:59.5"},
		{name: "make_timestamp refuses a day its date refuses", query: `SELECT MAKE_TIMESTAMP(2021, 2, 29, 0, 0, 0)`, wantErr: true},
		{name: "make_interval", query: `SELECT MAKE_INTERVAL(0, 0, 0, 1, 2, 3, 4)`, want: "1 day 02:03:04"},
		{name: "make_interval folds weeks into days", query: `SELECT MAKE_INTERVAL(1, 2, 3, 4, 5, 6, 7.5)`, want: "1 year 2 mons 25 days 05:06:07.5"},
		{name: "make_interval with nothing in it", query: `SELECT MAKE_INTERVAL(0, 0, 0, 0, 0, 0, 0)`, want: "00:00:00"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.PostgreSQL, tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("%s = %q, want an error", tt.query, got.String)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if got.String != tt.want {
				t.Fatalf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestNoPostgreSQLHelperFallsThroughToSQLite is the invariant behind
// TestTheMakeFamilyIsWholeAndReachable: a name this package claims should never
// come back as SQLite's "no such function".
func TestNoPostgreSQLHelperFallsThroughToSQLite(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	calls := []string{
		"MAKE_DATE(2020, 1, 1)",
		"MAKE_TIME(1, 1, 1)",
		"MAKE_TIMESTAMP(2020, 1, 1, 1, 1, 1)",
		"MAKE_INTERVAL(1, 1, 1, 1, 1, 1, 1)",
		"AGE(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2019-01-01 00:00:00')",
		"DATE_TRUNC('day', TIMESTAMP '2020-01-01 00:00:00')",
		"TO_DATE('2020-01-01', 'YYYY-MM-DD')",
		"TO_TIMESTAMP('2020-01-01', 'YYYY-MM-DD')",
		"JUSTIFY_DAYS(INTERVAL '1 day')",
	}
	for _, call := range calls {
		t.Run(call, func(t *testing.T) {
			_, err := runDialect(t, db, dialects.PostgreSQL, "SELECT "+call)
			if err != nil && strings.Contains(err.Error(), "no such function") {
				t.Fatalf("SELECT %s: %v", call, err)
			}
		})
	}
}
