package runtime

import (
	"testing"

	"github.com/nao1215/filesql/dialect/internal/dialects"
)

// TestGoogleSQLDateAndTimeFunctions pins the GoogleSQL date and time work
// against BigQuery. Every want was read from BigQuery rather than derived,
// except where a note says the emulator used to read it disagrees with the
// documented definition and the definition was taken.
func TestGoogleSQLDateAndTimeFunctions(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	for _, tt := range []struct {
		name    string
		query   string
		want    string
		wantErr bool
	}{
		// The constructors. SQLite has date(), time() and datetime() of its
		// own, which read a time value and modifiers, so these used to answer
		// NULL for the fields BigQuery builds a value from.
		{name: "date from fields", query: `SELECT DATE(2024, 3, 5)`, want: "2024-03-05"},
		{name: "date from a timestamp", query: `SELECT DATE(TIMESTAMP '2024-03-05 13:04:05')`, want: "2024-03-05"},
		{name: "date refuses a thirteenth month", query: `SELECT DATE(2024, 13, 1)`, wantErr: true},
		{name: "date refuses a day the month has not got", query: `SELECT DATE(2024, 2, 30)`, wantErr: true},
		{name: "datetime from fields", query: `SELECT DATETIME(2024, 3, 5, 13, 4, 5)`, want: "2024-03-05 13:04:05"},
		{name: "datetime from a date and a time", query: `SELECT DATETIME(DATE '2024-03-05', TIME '13:04:05')`, want: "2024-03-05 13:04:05"},
		{name: "datetime from a timestamp", query: `SELECT DATETIME(TIMESTAMP '2024-03-05 13:04:05')`, want: "2024-03-05 13:04:05"},
		{name: "time from fields", query: `SELECT TIME(13, 4, 5)`, want: "13:04:05"},
		{name: "time at midnight", query: `SELECT TIME(0, 0, 0)`, want: "00:00:00"},
		{name: "time refuses a sixty-first minute", query: `SELECT TIME(10, 61, 0)`, wantErr: true},
		{name: "time from a datetime", query: `SELECT TIME(DATETIME '2024-03-05 13:04:05')`, want: "13:04:05"},
		{name: "timestamp from a string", query: `SELECT TIMESTAMP('2024-03-05 13:04:05')`, want: "2024-03-05 13:04:05"},
		{name: "string of a timestamp", query: `SELECT STRING(TIMESTAMP '2024-03-05 13:04:05')`, want: "2024-03-05 13:04:05"},
		// A time zone would shift the instant, and filesql carries none, so
		// the forms that take one are refused rather than answered unshifted.
		{name: "timestamp refuses a time zone", query: `SELECT TIMESTAMP('2024-03-05 13:04:05', 'UTC')`, wantErr: true},
		{name: "datetime refuses a time zone", query: `SELECT DATETIME(TIMESTAMP '2024-03-05 13:04:05', 'UTC')`, wantErr: true},
		// An arity no signature has is the caller's mistake rather than a
		// value the function could not build.
		{name: "datetime refuses an arity it has not got", query: `SELECT DATETIME(2024, 3, 5, 13)`, wantErr: true},
		{name: "date refuses an arity it has not got", query: `SELECT DATE(2024, 3)`, wantErr: true},
		{name: "time refuses an arity it has not got", query: `SELECT TIME(13, 4)`, wantErr: true},
		{name: "timestamp refuses an arity it has not got", query: `SELECT TIMESTAMP('2024-03-05', 'UTC', 'x')`, wantErr: true},
		{name: "datetime of a value that is not one", query: `SELECT DATETIME('not a datetime')`, want: ""},
		{name: "timestamp of a value that is not one", query: `SELECT TIMESTAMP('not a timestamp')`, want: ""},
		{name: "string of a value that is not a timestamp", query: `SELECT STRING('not a timestamp')`, want: ""},

		// Days since the epoch, negative before it.
		{name: "unix_date", query: `SELECT UNIX_DATE(DATE '2024-03-05')`, want: "19787"},
		{name: "unix_date at the epoch", query: `SELECT UNIX_DATE(DATE '1970-01-01')`, want: "0"},
		{name: "unix_date before the epoch", query: `SELECT UNIX_DATE(DATE '1969-12-31')`, want: "-1"},
		{name: "date_from_unix_date", query: `SELECT DATE_FROM_UNIX_DATE(19787)`, want: "2024-03-05"},
		{name: "date_from_unix_date at the epoch", query: `SELECT DATE_FROM_UNIX_DATE(0)`, want: "1970-01-01"},
		{name: "date_from_unix_date before the epoch", query: `SELECT DATE_FROM_UNIX_DATE(-1)`, want: "1969-12-31"},

		// LAST_DAY takes a part. WEEK begins on Sunday and so ends on
		// Saturday, while ISOWEEK begins on Monday and ends on Sunday, which
		// is the pair this function invites getting backwards.
		{name: "last_day defaults to the month", query: `SELECT LAST_DAY(DATE '2024-02-05')`, want: "2024-02-29"},
		{name: "last_day of a month", query: `SELECT LAST_DAY(DATE '2024-03-05', MONTH)`, want: "2024-03-31"},
		{name: "last_day of a week", query: `SELECT LAST_DAY(DATE '2024-03-05', WEEK)`, want: "2024-03-09"},
		{name: "last_day of an ISO week", query: `SELECT LAST_DAY(DATE '2024-03-05', ISOWEEK)`, want: "2024-03-10"},
		{name: "last_day of a week from a named day", query: `SELECT LAST_DAY(DATE '2024-03-05', WEEK(MONDAY))`, want: "2024-03-10"},
		{name: "last_day of a quarter", query: `SELECT LAST_DAY(DATE '2024-03-05', QUARTER)`, want: "2024-03-31"},
		{name: "last_day of a year", query: `SELECT LAST_DAY(DATE '2024-03-05', YEAR)`, want: "2024-12-31"},
		// An ISO year ends on the Sunday that closes its last ISO week, which
		// is rarely 31 December: ISO year 2024 runs to 2024-12-29, and
		// 2021-01-01 belongs to ISO year 2020, which runs to 2021-01-03. The
		// emulator answers 31 December for both, falling back to the calendar
		// year, so the definition was taken.
		{name: "last_day of an ISO year", query: `SELECT LAST_DAY(DATE '2024-03-05', ISOYEAR)`, want: "2024-12-29"},
		{name: "last_day of the ISO year a January day belongs to", query: `SELECT LAST_DAY(DATE '2021-01-01', ISOYEAR)`, want: "2021-01-03"},
		{name: "last_day refuses a part it has not got", query: `SELECT LAST_DAY(DATE '2024-03-05', FORTNIGHT)`, wantErr: true},

		// The DATETIME family answers what the TIMESTAMP family answers, since
		// both are a point in time and the zone BigQuery separates them by is
		// not carried here.
		{name: "datetime_add", query: `SELECT DATETIME_ADD(DATETIME '2024-03-05 13:04:05', INTERVAL 1 HOUR)`, want: "2024-03-05 14:04:05"},
		{name: "datetime_sub", query: `SELECT DATETIME_SUB(DATETIME '2024-03-05 13:04:05', INTERVAL 1 DAY)`, want: "2024-03-04 13:04:05"},
		{name: "datetime_diff", query: `SELECT DATETIME_DIFF(DATETIME '2024-03-05 13:04:05', DATETIME '2024-03-05 12:04:05', MINUTE)`, want: "60"},
		{name: "datetime_trunc", query: `SELECT DATETIME_TRUNC(DATETIME '2024-03-05 13:04:05', HOUR)`, want: "2024-03-05 13:00:00"},

		// A TIME is a time of day, so its arithmetic wraps around midnight
		// rather than moving to another day.
		{name: "time_add", query: `SELECT TIME_ADD(TIME '13:04:05', INTERVAL 1 HOUR)`, want: "14:04:05"},
		{name: "time_add wraps past midnight", query: `SELECT TIME_ADD(TIME '23:04:05', INTERVAL 2 HOUR)`, want: "01:04:05"},
		{name: "time_sub wraps before midnight", query: `SELECT TIME_SUB(TIME '00:04:05', INTERVAL 1 HOUR)`, want: "23:04:05"},
		{name: "time_diff in hours", query: `SELECT TIME_DIFF(TIME '13:04:05', TIME '12:04:05', HOUR)`, want: "1"},
		{name: "time_diff in minutes", query: `SELECT TIME_DIFF(TIME '13:04:05', TIME '12:04:05', MINUTE)`, want: "60"},
		{name: "time_diff is signed", query: `SELECT TIME_DIFF(TIME '12:04:05', TIME '13:04:05', HOUR)`, want: "-1"},
		{name: "time_trunc to an hour", query: `SELECT TIME_TRUNC(TIME '13:04:05', HOUR)`, want: "13:00:00"},
		{name: "time_trunc to a minute", query: `SELECT TIME_TRUNC(TIME '13:04:05', MINUTE)`, want: "13:04:00"},
		{name: "time_trunc to a millisecond", query: `SELECT TIME_TRUNC(TIME '13:04:05.123', MILLISECOND)`, want: "13:04:05.123"},
		{name: "time_trunc to a microsecond", query: `SELECT TIME_TRUNC(TIME '13:04:05.123456', MICROSECOND)`, want: "13:04:05.123456"},
		{name: "time_trunc to a second drops the fraction", query: `SELECT TIME_TRUNC(TIME '13:04:05.123', SECOND)`, want: "13:04:05"},
		{name: "time_add keeps a fraction of a second", query: `SELECT TIME_ADD(TIME '13:04:05.5', INTERVAL 1 HOUR)`, want: "14:04:05.5"},
		// A whole number of days changes nothing, and an amount past what a
		// Duration holds is reduced before it is multiplied out rather than
		// wrapping to some other time. The emulator overflows on both of
		// these and answers a time with a fraction of a second in it.
		{name: "time_add of a whole day", query: `SELECT TIME_ADD(TIME '13:04:05', INTERVAL 24 HOUR)`, want: "13:04:05"},
		{name: "time_add of many whole days", query: `SELECT TIME_ADD(TIME '13:04:05', INTERVAL 3000000 HOUR)`, want: "13:04:05"},
		{name: "time_sub of many whole days", query: `SELECT TIME_SUB(TIME '13:04:05', INTERVAL 3000000 HOUR)`, want: "13:04:05"},
		{name: "time_add past a day", query: `SELECT TIME_ADD(TIME '13:04:05', INTERVAL 25 HOUR)`, want: "14:04:05"},
		{name: "time_add of the largest int64 of seconds", query: `SELECT TIME_ADD(TIME '13:04:05', INTERVAL 9223372036854775807 SECOND)`, want: "04:34:12"},
		{name: "current_datetime refuses a time zone", query: `SELECT CURRENT_DATETIME('UTC')`, wantErr: true},
		{name: "time_diff in seconds", query: `SELECT TIME_DIFF(TIME '13:04:05', TIME '13:04:04', SECOND)`, want: "1"},
		{name: "the time family refuses a calendar unit", query: `SELECT TIME_TRUNC(TIME '13:04:05', MONTH)`, wantErr: true},

		// The week parts. WEEK begins on Sunday and the days before the year's
		// first Sunday are week 0; WEEK(<WEEKDAY>) is the same rule from
		// another day, and 2024-01-01 is a Monday, which is where the two
		// disagree most.
		{name: "extract week from monday", query: `SELECT EXTRACT(WEEK(MONDAY) FROM DATE '2024-01-01')`, want: "1"},
		{name: "extract week from sunday", query: `SELECT EXTRACT(WEEK(SUNDAY) FROM DATE '2024-01-01')`, want: "0"},
		{name: "extract the plain week", query: `SELECT EXTRACT(WEEK FROM DATE '2024-01-01')`, want: "0"},
		{name: "extract the ISO week", query: `SELECT EXTRACT(ISOWEEK FROM DATE '2024-01-01')`, want: "1"},
		{name: "extract week from friday", query: `SELECT EXTRACT(WEEK(FRIDAY) FROM DATE '2024-03-05')`, want: "9"},
		{name: "extract refuses a weekday it has not got", query: `SELECT EXTRACT(WEEK(FUNDAY) FROM DATE '2024-01-01')`, wantErr: true},
		{name: "date_trunc to a week from a named day", query: `SELECT DATE_TRUNC(DATE '2024-03-05', WEEK(MONDAY))`, want: "2024-03-04"},
		{name: "date_trunc to a week", query: `SELECT DATE_TRUNC(DATE '2024-03-05', WEEK)`, want: "2024-03-03"},
		{name: "date_trunc to an ISO week", query: `SELECT DATE_TRUNC(DATE '2024-03-05', ISOWEEK)`, want: "2024-03-04"},

		// DATE_DIFF counts the boundaries crossed, so a Saturday and the
		// Sunday after it are one week apart.
		{name: "date_diff over a week boundary", query: `SELECT DATE_DIFF(DATE '2024-01-07', DATE '2024-01-06', WEEK)`, want: "1"},
		{name: "date_diff inside one week", query: `SELECT DATE_DIFF(DATE '2024-01-06', DATE '2024-01-01', WEEK)`, want: "0"},
		{name: "date_diff in ISO weeks", query: `SELECT DATE_DIFF(DATE '2024-03-05', DATE '2024-01-01', ISOWEEK)`, want: "9"},
		{name: "date_diff in weeks from a named day", query: `SELECT DATE_DIFF(DATE '2024-01-08', DATE '2024-01-07', WEEK(MONDAY))`, want: "1"},
		{name: "date_diff in quarters inside one", query: `SELECT DATE_DIFF(DATE '2024-03-05', DATE '2024-01-01', QUARTER)`, want: "0"},
		{name: "date_diff over a quarter boundary", query: `SELECT DATE_DIFF(DATE '2024-04-01', DATE '2024-03-31', QUARTER)`, want: "1"},
		{name: "date_diff in ISO years", query: `SELECT DATE_DIFF(DATE '2024-03-05', DATE '2024-01-01', ISOYEAR)`, want: "0"},
		{name: "date_diff refuses a unit it has not got", query: `SELECT DATE_DIFF(DATE '2024-03-05', DATE '2024-01-01', FORTNIGHT)`, wantErr: true},

		// BigQuery's MILLISECOND and MICROSECOND are the fraction of a second
		// alone; PostgreSQL's are the seconds field with the fraction scaled
		// into it, and it answers to both spellings.
		{name: "extract a millisecond", query: `SELECT EXTRACT(MILLISECOND FROM DATETIME '2024-03-05 13:04:05.123')`, want: "123"},
		{name: "extract a microsecond", query: `SELECT EXTRACT(MICROSECOND FROM DATETIME '2024-03-05 13:04:05.123')`, want: "123000"},

		// The digests answer bytes, so TO_HEX(MD5(x)) is the digest rather than
		// the hexadecimal of its hexadecimal.
		{name: "md5 answers bytes", query: `SELECT TO_HEX(MD5('abc'))`, want: "900150983cd24fb0d6963f7d28e17f72"},
		{name: "md5 is sixteen bytes", query: `SELECT LENGTH(MD5('abc'))`, want: "16"},
		{name: "sha1 answers bytes", query: `SELECT TO_HEX(SHA1('abc'))`, want: "a9993e364706816aba3e25717850c26c9cd0d89d"},
		{name: "sha256 was already right", query: `SELECT TO_HEX(SHA256('abc'))`, want: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{name: "md5 of an empty string", query: `SELECT TO_HEX(MD5(''))`, want: "d41d8cd98f00b204e9800998ecf8427e"},
		{name: "md5 hashes bytes rather than characters", query: `SELECT LENGTH(MD5('日本'))`, want: "16"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.GoogleSQL, tt.query)
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

// TestDigestsFollowTheirDialect keeps MD5 and SHA1 answering what each engine
// answers: bytes in BigQuery, hexadecimal text in PostgreSQL and MySQL.
func TestDigestsFollowTheirDialect(t *testing.T) {
	db := castDB(t)

	for _, tt := range []struct {
		dialect dialects.Dialect
		query   string
		want    string
	}{
		{dialects.PostgreSQL, `SELECT MD5('abc')`, "900150983cd24fb0d6963f7d28e17f72"},
		{dialects.PostgreSQL, `SELECT LENGTH(MD5('abc'))`, "32"},
		{dialects.MySQL, `SELECT MD5('abc')`, "900150983cd24fb0d6963f7d28e17f72"},
		{dialects.MySQL, `SELECT SHA1('abc')`, "a9993e364706816aba3e25717850c26c9cd0d89d"},
		{dialects.MySQL, `SELECT LENGTH(SHA1('abc'))`, "40"},
	} {
		t.Run(string(tt.dialect)+" "+tt.query, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if got.String != tt.want {
				t.Errorf("%s under %s = %q, want %q", tt.query, tt.dialect, got.String, tt.want)
			}
		})
	}
}

// TestSafePrefixRunsAnyHelper covers BigQuery's SAFE. prefix, which turns an
// error into a NULL. It used to be honored for five names -- and those five are
// the separate SAFE_ADD family rather than the prefix at all -- so every other
// function it was written in front of failed to translate.
func TestSafePrefixRunsAnyHelper(t *testing.T) {
	db := castDB(t)

	for _, tt := range []struct {
		name     string
		query    string
		want     string
		wantNull bool
		wantErr  bool
	}{
		// A call that succeeds is unchanged by the prefix.
		{name: "safe substr", query: `SELECT SAFE.SUBSTR('abc', 1, 2)`, want: "ab"},
		{name: "safe parse_date that parses", query: `SELECT SAFE.PARSE_DATE('%Y-%m-%d', '2024-03-05')`, want: "2024-03-05"},
		{name: "safe date that is a date", query: `SELECT SAFE.DATE(2024, 3, 5)`, want: "2024-03-05"},
		// A call that would raise answers NULL instead, which is the point.
		{name: "safe parse_date that does not parse", query: `SELECT SAFE.PARSE_DATE('%Y-%m-%d', 'nope')`, wantNull: true},
		{name: "safe date of a month that is not one", query: `SELECT SAFE.DATE(2024, 13, 1)`, wantNull: true},
		{name: "safe mod by zero", query: `SELECT SAFE.MOD(1, 0)`, wantNull: true},
		{name: "the same call raises without the prefix", query: `SELECT MOD(1, 0)`, wantErr: true},
		// The five underscore names keep their dotted spelling, which this
		// package answered to before the prefix was general.
		{name: "safe divide", query: `SELECT SAFE.DIVIDE(1, 0)`, wantNull: true},
		{name: "safe add", query: `SELECT SAFE.ADD(1, 2)`, want: "3"},
		{name: "nested safe arithmetic", query: `SELECT SAFE.MULTIPLY(SAFE.ADD(1, 2), 3)`, want: "9"},
		// A function SQLite computes itself is out of reach, so the prefix is
		// dropped and the call runs as written.
		{name: "safe on a sqlite builtin", query: `SELECT SAFE.LENGTH('abc')`, want: "3"},
		// A name nothing defines still says so, rather than becoming a NULL.
		{name: "safe on a name nothing defines", query: `SELECT SAFE.NOSUCHFN(1)`, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.GoogleSQL, tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("%s: expected an error, got %q", tt.query, got.String)
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
				t.Errorf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestGoogleSQLScalarFunctions pins the GoogleSQL-only scalar functions. Every
// want was read from BigQuery except where a note says the emulator used to
// read them disagrees with the documented definition, in which case the
// definition was taken.
func TestGoogleSQLScalarFunctions(t *testing.T) {
	db := castDB(t)

	for _, tt := range []struct {
		name    string
		query   string
		want    string
		wantErr bool
	}{
		// INSTR takes a position and an occurrence, where SQLite's own takes
		// two arguments and nothing else.
		{name: "instr finds the first match", query: `SELECT INSTR('abcabc', 'b')`, want: "2"},
		{name: "instr starts where it is told", query: `SELECT INSTR('abcabc', 'b', 3)`, want: "5"},
		{name: "instr counts occurrences", query: `SELECT INSTR('abcabc', 'b', 1, 2)`, want: "5"},
		{name: "instr with no match", query: `SELECT INSTR('abcabc', 'z')`, want: "0"},
		{name: "instr past the occurrences", query: `SELECT INSTR('abcabc', 'b', 1, 3)`, want: "0"},
		// A negative position searches backwards from that far from the end,
		// and the answer is still counted from the beginning. The emulator
		// used to read these answers disagrees -- it answers 4 here, a
		// position 'b' does not occupy at all -- so the documented reading was
		// taken.
		{name: "instr searches backwards", query: `SELECT INSTR('abcabc', 'b', -1)`, want: "5"},
		{name: "instr searches backwards for a later occurrence", query: `SELECT INSTR('abcabc', 'b', -1, 2)`, want: "2"},
		{name: "instr counts characters rather than bytes", query: `SELECT INSTR('日本語', '語')`, want: "3"},
		{name: "instr refuses a zero position", query: `SELECT INSTR('abcabc', 'b', 0)`, wantErr: true},

		// CONTAINS_SUBSTR normalizes to NFKC and casefolds, so it matches
		// across case and not across accents.
		{name: "contains_substr across case", query: `SELECT CONTAINS_SUBSTR('alphabet', 'PHA')`, want: "1"},
		{name: "contains_substr with the case the other way", query: `SELECT CONTAINS_SUBSTR('AlphaBet', 'pha')`, want: "1"},
		{name: "contains_substr does not cross an accent", query: `SELECT CONTAINS_SUBSTR('café', 'cafe')`, want: "0"},
		{name: "contains_substr with no match", query: `SELECT CONTAINS_SUBSTR('alphabet', 'zz')`, want: "0"},

		// NORMALIZE takes its mode as a bare keyword, which the rewrite turns
		// into an argument.
		{name: "normalize defaults to NFC", query: `SELECT LENGTH(NORMALIZE('a` + "\u0301" + `'))`, want: "1"},
		{name: "normalize to NFC", query: `SELECT LENGTH(NORMALIZE('a` + "\u0301" + `', NFC))`, want: "1"},
		{name: "normalize to NFD", query: `SELECT LENGTH(NORMALIZE('a` + "\u0301" + `', NFD))`, want: "2"},
		{name: "normalize to NFKC", query: `SELECT NORMALIZE('` + "\ufb01" + `', NFKC)`, want: "fi"},
		{name: "normalize_and_casefold folds case", query: `SELECT NORMALIZE_AND_CASEFOLD('AbC')`, want: "abc"},
		{name: "normalize refuses a mode it has not got", query: `SELECT NORMALIZE('a', NFQ)`, wantErr: true},

		// EDIT_DISTANCE writes its cap as a named argument, which SQLite has
		// no syntax for.
		{name: "edit_distance", query: `SELECT EDIT_DISTANCE('kitten', 'sitting')`, want: "3"},
		{name: "edit_distance of equal strings", query: `SELECT EDIT_DISTANCE('abc', 'abc')`, want: "0"},
		{name: "edit_distance from nothing", query: `SELECT EDIT_DISTANCE('', 'abc')`, want: "3"},
		{name: "edit_distance with a cap", query: `SELECT EDIT_DISTANCE('kitten', 'sitting', max_distance => 2)`, want: "2"},
		{name: "edit_distance with a cap and no spaces", query: `SELECT EDIT_DISTANCE('kitten', 'sitting', max_distance=>2)`, want: "2"},
		{name: "edit_distance counts characters", query: `SELECT EDIT_DISTANCE('日本', '日和')`, want: "1"},

		// The byte encoders and their inverses.
		{name: "from_hex round trips", query: `SELECT TO_HEX(FROM_HEX('616263'))`, want: "616263"},
		{name: "to_base32", query: `SELECT TO_BASE32(b'abc')`, want: "MFRGG==="},
		{name: "to_base32 of nothing", query: `SELECT TO_BASE32(b'')`, want: ""},
		{name: "from_base32 round trips", query: `SELECT TO_HEX(FROM_BASE32('MFRGG==='))`, want: "616263"},
		{name: "from_hex refuses text that is not hexadecimal", query: `SELECT FROM_HEX('zz')`, wantErr: true},
		{name: "from_base32 refuses text that is not base32", query: `SELECT FROM_BASE32('!!')`, wantErr: true},

		{name: "to_json_string quotes a string", query: `SELECT TO_JSON_STRING('a')`, want: `"a"`},
		{name: "to_json_string of a number", query: `SELECT TO_JSON_STRING(1)`, want: "1"},
		{name: "to_json_string of null", query: `SELECT TO_JSON_STRING(NULL)`, want: "null"},

		// IEEE_DIVIDE answers where "/" raises under this dialect.
		{name: "ieee_divide by zero", query: `SELECT IEEE_DIVIDE(1, 0)`, want: "+Inf"},
		{name: "ieee_divide a negative by zero", query: `SELECT IEEE_DIVIDE(-1, 0)`, want: "-Inf"},
		{name: "ieee_divide of two zeros", query: `SELECT IEEE_DIVIDE(0, 0)`, want: ""},
		{name: "ieee_divide of two numbers", query: `SELECT IEEE_DIVIDE(6, 3)`, want: "2"},
		{name: "the operator still raises", query: `SELECT 1 / 0`, wantErr: true},
		{name: "is_inf of an infinity", query: `SELECT IS_INF(IEEE_DIVIDE(1, 0))`, want: "1"},
		{name: "is_inf of a number", query: `SELECT IS_INF(1)`, want: "0"},

		// The reciprocal trigonometric functions.
		{name: "csc", query: `SELECT CSC(1)`, want: "1.1883951057781212"},
		{name: "sec", query: `SELECT SEC(1)`, want: "1.8508157176809255"},
		{name: "csch", query: `SELECT CSCH(1)`, want: "0.8509181282393216"},
		{name: "sech", query: `SELECT SECH(1)`, want: "0.6480542736638855"},
		{name: "coth", query: `SELECT COTH(1)`, want: "1.3130352854993315"},

		{name: "error raises with its message", query: `SELECT ERROR('boom')`, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.GoogleSQL, tt.query)
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

// TestGoogleSQLFunctionsAnswerNullForNull covers the rule every SQL function
// follows: a NULL argument makes the answer NULL.
// TestGoogleSQLSoundexIsNotMySQLs pins the three places BigQuery's SOUNDEX
// differs from MySQL's, all of them read off the BigQuery emulator: it stops at
// three digits, it keeps the first letter's case, and it sees no letter outside
// ASCII. The coding rule itself is the same, so the two share one
// implementation and differ only by their rules table.
func TestGoogleSQLSoundexIsNotMySQLs(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		dialect dialects.Dialect
		query   string
		want    string
		null    bool
	}{
		{dialects.GoogleSQL, `SELECT SOUNDEX('Ashcraft')`, "A261", false},
		{dialects.GoogleSQL, `SELECT SOUNDEX('Hello World')`, "H464", false},
		{dialects.GoogleSQL, `SELECT SOUNDEX('abcdefghijklmnopqrstuvwxyz')`, "a123", false},
		{dialects.GoogleSQL, `SELECT SOUNDEX('hello')`, "h400", false},
		{dialects.GoogleSQL, `SELECT SOUNDEX('Tymczak')`, "T520", false},
		{dialects.GoogleSQL, `SELECT SOUNDEX('Pfister')`, "P236", false},
		{dialects.GoogleSQL, `SELECT SOUNDEX('Honeyman')`, "H500", false},
		{dialects.GoogleSQL, `SELECT SOUNDEX('')`, "", false},
		{dialects.GoogleSQL, `SELECT SOUNDEX('123')`, "", false},
		{dialects.GoogleSQL, `SELECT SOUNDEX('éèê')`, "", false},
		{dialects.GoogleSQL, `SELECT SOUNDEX(NULL)`, "", true},

		// The same three inputs under MySQL, so the two cannot be given one
		// rule by a later change.
		{dialects.MySQL, `SELECT SOUNDEX('Hello World')`, "H4643", false},
		{dialects.MySQL, `SELECT SOUNDEX('hello')`, "H400", false},
		{dialects.MySQL, `SELECT SOUNDEX('éèê')`, "é000", false},
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
}

// TestGoogleSQLRefusesANegativeLength pins one rule across the six functions
// that take a length. BigQuery refuses a negative one in every one of them, and
// this package used to answer an error from two, the empty string from two and
// NULL from one, so a computed length that came out negative meant something
// different depending on which function read it.
func TestGoogleSQLRefusesANegativeLength(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	refused := []string{
		`SELECT LEFT('hello', -1)`,
		`SELECT RIGHT('hello', -1)`,
		`SELECT SUBSTR('hello', 2, -1)`,
		`SELECT REPEAT('hello', -1)`,
		`SELECT LPAD('hello', -1, 'xy')`,
		`SELECT RPAD('hello', -1, 'xy')`,
	}
	for _, query := range refused {
		if _, err := runDialect(t, db, dialects.GoogleSQL, query); err == nil {
			t.Errorf("%s: want an error for the negative length, got none", query)
		}
	}

	// A length of zero is not negative and keeps each function's own answer,
	// and the same calls under MySQL keep MySQL's rules rather than BigQuery's.
	kept := []struct {
		dialect dialects.Dialect
		query   string
		want    string
	}{
		{dialects.GoogleSQL, `SELECT REPEAT('hello', 0)`, ""},
		{dialects.GoogleSQL, `SELECT SUBSTR('hello', 2, 0)`, ""},
		{dialects.GoogleSQL, `SELECT LPAD('hello', 8, 'xy')`, "xyxhello"},
		{dialects.MySQL, `SELECT REPEAT('hello', -1)`, ""},
	}
	for _, tt := range kept {
		got, err := runDialect(t, db, tt.dialect, tt.query)
		if err != nil {
			t.Errorf("%v: %s: %v", tt.dialect, tt.query, err)
			continue
		}
		if !got.Valid || got.String != tt.want {
			t.Errorf("%v: %s = %v, want %q", tt.dialect, tt.query, got, tt.want)
		}
	}

	// MySQL answers NULL for a negative pad length, which is its own rule.
	got, err := runDialect(t, db, dialects.MySQL, `SELECT LPAD('hello', -1, 'xy')`)
	if err != nil {
		t.Fatalf("mysql LPAD with a negative length: %v", err)
	}
	if got.Valid {
		t.Errorf("mysql LPAD('hello', -1, 'xy') = %q, want NULL", got.String)
	}
}

func TestGoogleSQLFunctionsAnswerNullForNull(t *testing.T) {
	db := castDB(t)

	for _, q := range []string{
		`SELECT INSTR(NULL, 'a')`,
		`SELECT CONTAINS_SUBSTR(NULL, 'a')`,
		`SELECT NORMALIZE(NULL)`,
		`SELECT NORMALIZE_AND_CASEFOLD(NULL)`,
		`SELECT EDIT_DISTANCE(NULL, 'a')`,
		`SELECT FROM_HEX(NULL)`,
		`SELECT TO_BASE32(NULL)`,
		`SELECT FROM_BASE32(NULL)`,
		`SELECT IEEE_DIVIDE(NULL, 1)`,
		`SELECT IS_INF(NULL)`,
		`SELECT CSC(NULL)`,
		`SELECT MD5(NULL)`,
		`SELECT DATE(NULL, 1, 1)`,
		`SELECT TIME(NULL, 1, 1)`,
		`SELECT UNIX_DATE(NULL)`,
		`SELECT DATE_FROM_UNIX_DATE(NULL)`,
		`SELECT LAST_DAY(NULL)`,
		`SELECT TIME_DIFF(NULL, TIME '12:00:00', HOUR)`,
	} {
		t.Run(q, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.GoogleSQL, q)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			if got.Valid {
				t.Errorf("%s = %q, want NULL", q, got.String)
			}
		})
	}
}

// TestCurrentDatetimeReadsTheClock covers the one GoogleSQL function that
// cannot be pinned to a value: what is asserted is that it runs and answers
// something shaped like a datetime. It is registered as non-deterministic, or
// SQLite would fold it to one value per statement.
func TestCurrentDatetimeReadsTheClock(t *testing.T) {
	db := castDB(t)

	got, err := runDialect(t, db, dialects.GoogleSQL, `SELECT CURRENT_DATETIME()`)
	if err != nil {
		t.Fatalf("CURRENT_DATETIME(): %v", err)
	}
	if !got.Valid || len(got.String) != len("2006-01-02 15:04:05") {
		t.Fatalf("CURRENT_DATETIME() = %q, want a datetime", got.String)
	}
}
