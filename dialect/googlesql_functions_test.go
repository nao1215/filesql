package dialect

import "testing"

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
			got, err := runDialect(t, db, GoogleSQL, tt.query)
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
		dialect Dialect
		query   string
		want    string
	}{
		{PostgreSQL, `SELECT MD5('abc')`, "900150983cd24fb0d6963f7d28e17f72"},
		{PostgreSQL, `SELECT LENGTH(MD5('abc'))`, "32"},
		{MySQL, `SELECT MD5('abc')`, "900150983cd24fb0d6963f7d28e17f72"},
		{MySQL, `SELECT SHA1('abc')`, "a9993e364706816aba3e25717850c26c9cd0d89d"},
		{MySQL, `SELECT LENGTH(SHA1('abc'))`, "40"},
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
