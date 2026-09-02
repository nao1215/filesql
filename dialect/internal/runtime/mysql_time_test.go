package runtime

import (
	"fmt"
	"strings"
	"testing"

	"github.com/nao1215/filesql/dialect/internal/dialects"
)

// TestMySQLTimeFunctions pins the TIME functions against MySQL 8.4. Every want
// below was read from a running MySQL 8.4.11 rather than derived. A MySQL TIME
// is a signed span rather than a point on a clock, so the cases carry the parts
// that go with that: an hour field past 24, a negative span, the clamp at
// 838:59:59, and the colonless form where the last two digits are the seconds.
func TestMySQLTimeFunctions(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		query    string
		want     string
		wantNull bool
		wantErr  bool
	}{
		// SEC_TO_TIME and TIME_TO_SEC
		{query: `SELECT SEC_TO_TIME(3661)`, want: "01:01:01"},
		{query: `SELECT SEC_TO_TIME(-3661)`, want: "-01:01:01"},
		{query: `SELECT SEC_TO_TIME(0)`, want: "00:00:00"},
		{query: `SELECT SEC_TO_TIME(360000)`, want: "100:00:00"},
		{query: `SELECT SEC_TO_TIME(59)`, want: "00:00:59"},
		{query: `SELECT SEC_TO_TIME(3020399)`, want: "838:59:59"},
		{query: `SELECT SEC_TO_TIME(3020400)`, want: "838:59:59"},
		{query: `SELECT SEC_TO_TIME(-3020400)`, want: "-838:59:59"},
		{query: `SELECT SEC_TO_TIME(NULL)`, wantNull: true},
		{query: `SELECT SEC_TO_TIME(90.7)`, want: "00:01:30.7"},
		{query: `SELECT TIME_TO_SEC('01:01:01')`, want: "3661"},
		{query: `SELECT TIME_TO_SEC('-01:01:01')`, want: "-3661"},
		{query: `SELECT TIME_TO_SEC('100:00:00')`, want: "360000"},
		{query: `SELECT TIME_TO_SEC('00:00:00')`, want: "0"},
		{query: `SELECT TIME_TO_SEC('2024-01-01 01:00:00')`, want: "3600"},
		{query: `SELECT TIME_TO_SEC('not-a-time')`, wantNull: true},
		{query: `SELECT TIME_TO_SEC(NULL)`, wantNull: true},
		{query: `SELECT TIME_TO_SEC('12:30')`, want: "45000"},
		{query: `SELECT TIME_TO_SEC('1:2:3')`, want: "3723"},

		// TO_DAYS and FROM_DAYS
		{query: `SELECT TO_DAYS('2024-02-29')`, want: "739310"},
		{query: `SELECT TO_DAYS('1000-01-01')`, want: "365243"},
		{query: `SELECT TO_DAYS('0001-01-01')`, want: "366"},
		{query: `SELECT TO_DAYS('9999-12-31')`, want: "3652424"},
		{query: `SELECT TO_DAYS('2024-02-29 13:00:00')`, want: "739310"},
		{query: `SELECT TO_DAYS('not-a-date')`, wantNull: true},
		{query: `SELECT TO_DAYS(NULL)`, wantNull: true},
		{query: `SELECT FROM_DAYS(739310)`, want: "2024-02-29"},
		{query: `SELECT FROM_DAYS(366)`, want: "0001-01-01"},
		{query: `SELECT FROM_DAYS(1)`, want: "0000-00-00"},
		{query: `SELECT FROM_DAYS(3652424)`, want: "9999-12-31"},
		{query: `SELECT FROM_DAYS(-1)`, want: "0000-00-00"},
		{query: `SELECT FROM_DAYS(NULL)`, wantNull: true},

		// MAKEDATE and MAKETIME
		{query: `SELECT MAKEDATE(2024, 60)`, want: "2024-02-29"},
		{query: `SELECT MAKEDATE(2024, 1)`, want: "2024-01-01"},
		{query: `SELECT MAKEDATE(2024, 366)`, want: "2024-12-31"},
		{query: `SELECT MAKEDATE(2024, 367)`, want: "2025-01-01"},
		{query: `SELECT MAKEDATE(2024, 0)`, wantNull: true},
		{query: `SELECT MAKEDATE(2024, -1)`, wantNull: true},
		{query: `SELECT MAKEDATE(99, 1)`, want: "1999-01-01"},
		{query: `SELECT MAKEDATE(NULL, 1)`, wantNull: true},
		{query: `SELECT MAKETIME(12, 30, 45)`, want: "12:30:45"},
		{query: `SELECT MAKETIME(-1, 30, 0)`, want: "-01:30:00"},
		{query: `SELECT MAKETIME(100, 0, 0)`, want: "100:00:00"},
		{query: `SELECT MAKETIME(0, 0, 0)`, want: "00:00:00"},
		{query: `SELECT MAKETIME(838, 59, 59)`, want: "838:59:59"},
		{query: `SELECT MAKETIME(839, 0, 0)`, want: "838:59:59"},
		{query: `SELECT MAKETIME(1, 60, 0)`, wantNull: true},
		{query: `SELECT MAKETIME(1, 0, 60)`, wantNull: true},
		{query: `SELECT MAKETIME(1, -1, 0)`, wantNull: true},
		{query: `SELECT MAKETIME(NULL, 0, 0)`, wantNull: true},

		// PERIOD_ADD and PERIOD_DIFF
		{query: `SELECT PERIOD_ADD(202401, 2)`, want: "202403"},
		{query: `SELECT PERIOD_ADD(202401, -1)`, want: "202312"},
		{query: `SELECT PERIOD_ADD(9902, 1)`, want: "199903"},
		{query: `SELECT PERIOD_ADD(202412, 1)`, want: "202501"},
		{query: `SELECT PERIOD_ADD(202401, 0)`, want: "202401"},
		{query: `SELECT PERIOD_ADD(NULL, 1)`, wantNull: true},
		{query: `SELECT PERIOD_DIFF(202403, 202401)`, want: "2"},
		{query: `SELECT PERIOD_DIFF(202401, 202403)`, want: "-2"},
		{query: `SELECT PERIOD_DIFF(9902, 199901)`, want: "1"},
		{query: `SELECT PERIOD_DIFF(202401, 202301)`, want: "12"},
		{query: `SELECT PERIOD_DIFF(NULL, 202301)`, wantNull: true},

		// TIME_FORMAT
		{query: `SELECT TIME_FORMAT('12:30:45', '%H:%i:%s')`, want: "12:30:45"},
		{query: `SELECT TIME_FORMAT('100:00:00', '%H:%i')`, want: "100:00"},
		{query: `SELECT TIME_FORMAT('12:30:45', '%h %p')`, want: "12 PM"},
		{query: `SELECT TIME_FORMAT('00:30:45', '%h %p')`, want: "12 AM"},
		{query: `SELECT TIME_FORMAT('12:30:45', '%T')`, want: "12:30:45"},
		{query: `SELECT TIME_FORMAT('12:30:45', '%r')`, want: "12:30:45 PM"},
		{query: `SELECT TIME_FORMAT('-01:30:45', '%H:%i:%s')`, want: "-01:30:45"},
		{query: `SELECT TIME_FORMAT('12:30:45', '%k %l')`, want: "12 12"},
		{query: `SELECT TIME_FORMAT('12:30:45', '%Y')`, want: "0000"},
		{query: `SELECT TIME_FORMAT(NULL, '%H')`, wantNull: true},

		// ADDTIME, SUBTIME and MICROSECOND
		{query: `SELECT ADDTIME('2024-01-01 10:00:00', '02:30:00')`, want: "2024-01-01 12:30:00"},
		{query: `SELECT ADDTIME('01:00:00', '01:00:00')`, want: "02:00:00"},
		{query: `SELECT ADDTIME('01:00:00', '-02:00:00')`, want: "-01:00:00"},
		{query: `SELECT ADDTIME('2024-01-01 23:00:00', '02:00:00')`, want: "2024-01-02 01:00:00"},
		{query: `SELECT ADDTIME('10:00:00', '100:00:00')`, want: "110:00:00"},
		{query: `SELECT ADDTIME(NULL, '01:00:00')`, wantNull: true},
		{query: `SELECT ADDTIME('2024-01-01 10:00:00', 'not-a-time')`, wantNull: true},
		{query: `SELECT SUBTIME('01:00:00', '02:00:00')`, want: "-01:00:00"},
		{query: `SELECT SUBTIME('2024-01-01 10:00:00', '02:30:00')`, want: "2024-01-01 07:30:00"},
		{query: `SELECT SUBTIME('2024-01-01 00:30:00', '01:00:00')`, want: "2023-12-31 23:30:00"},
		{query: `SELECT SUBTIME(NULL, '01:00:00')`, wantNull: true},
		{query: `SELECT MICROSECOND('2024-01-01 12:00:00.123')`, want: "123000"},
		{query: `SELECT MICROSECOND('12:00:00.123456')`, want: "123456"},
		{query: `SELECT MICROSECOND('12:00:00')`, want: "0"},
		{query: `SELECT MICROSECOND('2024-01-01')`, want: "0"},
		{query: `SELECT MICROSECOND(NULL)`, wantNull: true},
		{query: `SELECT MICROSECOND('not-a-time')`, wantNull: true},

		// The edges, measured the same way.
		{query: `SELECT SEC_TO_TIME(90.75)`, want: "00:01:30.75"},
		{query: `SELECT SEC_TO_TIME(90.7777777)`, want: "00:01:30.777778"},
		{query: `SELECT SEC_TO_TIME(-0.5)`, want: "-00:00:00.5"},
		{query: `SELECT TIME_TO_SEC('12:30:45.9')`, want: "45045"},
		{query: `SELECT TIME_TO_SEC('-12:30:45.9')`, want: "-45045"},
		{query: `SELECT TIME_TO_SEC('839:00:00')`, want: "3020399"},
		{query: `SELECT TIME_TO_SEC('00:60:00')`, wantNull: true},
		{query: `SELECT TIME_TO_SEC('00:00:60')`, wantNull: true},
		{query: `SELECT TIME_TO_SEC('12')`, want: "12"},
		{query: `SELECT MAKETIME(1, 0, 1.5)`, want: "01:00:01.5"},
		{query: `SELECT MAKETIME(0, 0, 59.999999)`, want: "00:00:59.999999"},
		{query: `SELECT MAKETIME(-838, 59, 59)`, want: "-838:59:59"},
		{query: `SELECT MAKETIME(1, 59, 59)`, want: "01:59:59"},
		{query: `SELECT TIME_FORMAT('13:04:05.123456', '%f')`, want: "123456"},
		{query: `SELECT TIME_FORMAT('13:04:05.123456', '%H %k %h %I %l %i %s %S %p')`, want: "13 13 01 01 1 04 05 05 PM"},
		{query: `SELECT TIME_FORMAT('13:04:05', '%r')`, want: "01:04:05 PM"},
		{query: `SELECT TIME_FORMAT('00:04:05', '%r')`, want: "12:04:05 AM"},
		{query: `SELECT TIME_FORMAT('12:04:05', '%r')`, want: "12:04:05 PM"},
		{query: `SELECT TIME_FORMAT('23:04:05', '%r')`, want: "11:04:05 PM"},
		{query: `SELECT TIME_FORMAT('100:04:05', '%r')`, want: "04:04:05 AM"},
		{query: `SELECT TIME_FORMAT('100:04:05', '%H %k %h %p')`, want: "100 100 04 AM"},
		{query: `SELECT TIME_FORMAT('-100:04:05', '%H %k %T')`, want: "-100 100 100:04:05"},
		{query: `SELECT TIME_FORMAT('13:04:05', '%c %d %e %m %y %Y')`, want: "0 00 0 00 00 0000"},
		{query: `SELECT TIME_FORMAT('13:04:05', '%M')`, wantNull: true},
		{query: `SELECT TIME_FORMAT('13:04:05', '%q %Z %A')`, want: "q Z A"},
		{query: `SELECT TIME_FORMAT('13:04:05', '%')`, want: "%"},
		{query: `SELECT TIME_FORMAT('13:04:05', '')`, wantNull: true},
		{query: `SELECT TO_DAYS('2000-02-29')`, want: "730544"},
		{query: `SELECT TO_DAYS('1900-03-01')`, want: "694020"},
		{query: `SELECT TO_DAYS('1970-01-01')`, want: "719528"},
		{query: `SELECT FROM_DAYS(719528)`, want: "1970-01-01"},
		{query: `SELECT FROM_DAYS(3652424)`, want: "9999-12-31"},
		{query: `SELECT FROM_DAYS(366)`, want: "0001-01-01"},
		{query: `SELECT FROM_DAYS(0)`, want: "0000-00-00"},
		{query: `SELECT MAKEDATE(2000, 366)`, want: "2000-12-31"},
		{query: `SELECT MAKEDATE(1900, 366)`, want: "1901-01-01"},
		{query: `SELECT MAKEDATE(0, 1)`, want: "2000-01-01"},
		{query: `SELECT MAKEDATE(69, 1)`, want: "2069-01-01"},
		{query: `SELECT MAKEDATE(70, 1)`, want: "1970-01-01"},
		{query: `SELECT MAKEDATE(9999, 365)`, want: "9999-12-31"},
		{query: `SELECT PERIOD_ADD(202401, 24)`, want: "202601"},
		{query: `SELECT PERIOD_ADD(202401, -24)`, want: "202201"},
		{query: `SELECT PERIOD_ADD(1, 1)`, want: "200002"},
		{query: `SELECT PERIOD_ADD(9912, 1)`, want: "200001"},
		{query: `SELECT PERIOD_DIFF(200001, 199901)`, want: "12"},
		{query: `SELECT PERIOD_DIFF(1, 12)`, want: "-11"},
		{query: `SELECT ADDTIME('-01:00:00', '00:30:00')`, want: "-00:30:00"},
		{query: `SELECT ADDTIME('2024-02-28 23:00:00', '25:00:00')`, want: "2024-03-01 00:00:00"},
		{query: `SELECT SUBTIME('2024-03-01 00:00:00', '00:00:01')`, want: "2024-02-29 23:59:59"},
		{query: `SELECT SUBTIME('00:00:00', '838:59:59')`, want: "-838:59:59"},
		{query: `SELECT MICROSECOND('12:00:00.000001')`, want: "1"},
		{query: `SELECT MICROSECOND('-12:00:00.5')`, want: "500000"},
		{query: `SELECT MICROSECOND(123)`, want: "0"},

		// A value that is not a TIME is NULL rather than a salvaged prefix.
		{query: `SELECT TIME_TO_SEC('')`, wantNull: true},
		{query: `SELECT TIME_TO_SEC('a:b:c')`, wantNull: true},
		{query: `SELECT TIME_TO_SEC('1:2:3:4')`, wantNull: true},
		{query: `SELECT TIME_TO_SEC('01::01')`, wantNull: true},
		{query: `SELECT TIME_TO_SEC('01:01:01.abc')`, wantNull: true},
		{query: `SELECT TIME_TO_SEC('-')`, wantNull: true},
		{query: `SELECT TIME_TO_SEC('.5')`, wantNull: true},
		{query: `SELECT TIME_TO_SEC('1234567')`, wantNull: true},
		{query: `SELECT TIME_TO_SEC('12.')`, want: "12"},
		{query: `SELECT TIME_TO_SEC('12:30:45.1234567')`, want: "45045"},
		{query: `SELECT ADDTIME('bogus', '01:00:00')`, wantNull: true},
		{query: `SELECT ADDTIME('2024-01-01 10:00:00', '2024-01-01 01:00:00')`, wantNull: true},
		{query: `SELECT SEC_TO_TIME(-3020400.5)`, want: "-838:59:59"},
		{query: `SELECT FROM_DAYS(365)`, want: "0000-00-00"},
		{query: `SELECT FROM_DAYS(3652425)`, wantNull: true},
		{query: `SELECT MAKEDATE(-1, 1)`, wantNull: true},
		{query: `SELECT MAKEDATE(10000, 1)`, wantNull: true},
		{query: `SELECT MICROSECOND('2024-01-01')`, want: "0"},
		{query: `SELECT TIME_FORMAT('12:30:45', '%%H %s')`, want: "%H 45"},
		{query: `SELECT TIME_FORMAT('12:30:45', 'x%')`, want: "x%"},
		{query: `SELECT SEC_TO_TIME(NULL)`, wantNull: true},
		{query: `SELECT MAKETIME(1, NULL, 1)`, wantNull: true},
		{query: `SELECT TIME_FORMAT(NULL, '%H')`, wantNull: true},
		{query: `SELECT TIME_FORMAT('12:30:45', NULL)`, wantNull: true},
		{query: `SELECT TIME_FORMAT('12:30:45', '')`, wantNull: true},
		{query: `SELECT ADDTIME(NULL, NULL)`, wantNull: true},
		{query: `SELECT MICROSECOND('bogus')`, wantNull: true},
		{query: `SELECT PERIOD_ADD(NULL, NULL)`, wantNull: true},
		{query: `SELECT PERIOD_DIFF(202401, NULL)`, wantNull: true},
		{query: `SELECT TO_DAYS(NULL)`, wantNull: true},
		{query: `SELECT FROM_DAYS(NULL)`, wantNull: true},
		{query: `SELECT MAKEDATE(NULL, NULL)`, wantNull: true},

		// A period whose month is not a month is refused rather than answered,
		// which is what MySQL does with it.
		{query: `SELECT PERIOD_ADD(202413, 1)`, wantErr: true},
		{query: `SELECT PERIOD_ADD(0, 1)`, wantErr: true},
		{query: `SELECT PERIOD_ADD(-1, 1)`, wantErr: true},
		{query: `SELECT PERIOD_DIFF(202400, 202401)`, wantErr: true},
		{query: `SELECT PERIOD_DIFF(202401, 202400)`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.MySQL, tt.query)
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
			if tt.wantNull {
				return
			}
			if got.String != tt.want {
				t.Errorf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestTimeFormatSpecifiers walks every letter TIME_FORMAT can be given, because
// the ones that answer NULL, the ones that answer a zero field and the ones
// that answer the letter itself cannot be told apart by reading the
// documentation. Every want was read from MySQL 8.4.
func TestTimeFormatSpecifiers(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	// The letters MySQL refuses a TIME for, because they name a part of a date
	// a span does not have.
	refused := "abjuvwxDMUVWX"
	// The letters that name a part of a date TIME_FORMAT prints as empty.
	zeroed := map[byte]string{'c': "0", 'd': "00", 'e': "0", 'm': "00", 'y': "00", 'Y': "0000"}
	// The letters that name a part of the time of day.
	timed := map[byte]string{
		'f': "123456", 'h': "01", 'i': "04", 'k': "13", 'l': "1", 'p': "PM",
		'r': "01:04:05 PM", 's': "05", 'H': "13", 'I': "01", 'S': "05", 'T': "13:04:05",
	}

	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	for i := range len(letters) {
		spec := letters[i]
		query := "SELECT TIME_FORMAT('13:04:05.123456', '%" + string(spec) + "')"
		t.Run(string(spec), func(t *testing.T) {
			got, err := runDialect(t, db, dialects.MySQL, query)
			if err != nil {
				t.Fatalf("%s: %v", query, err)
			}
			switch {
			case strings.IndexByte(refused, spec) >= 0:
				if got.Valid {
					t.Errorf("%s = %q, want NULL", query, got.String)
				}
			case zeroed[spec] != "":
				if got.String != zeroed[spec] {
					t.Errorf("%s = %q, want %q", query, got.String, zeroed[spec])
				}
			case timed[spec] != "":
				if got.String != timed[spec] {
					t.Errorf("%s = %q, want %q", query, got.String, timed[spec])
				}
			default:
				// A letter that names nothing is written as itself.
				if got.String != string(spec) {
					t.Errorf("%s = %q, want %q", query, got.String, string(spec))
				}
			}
		})
	}
}

// TestMySQLTimeRelations checks what the TIME functions promise about each
// other rather than one more measured value: the pairs that invert, and the
// sign that TIME_FORMAT puts in front of a whole result rather than on each
// field it prints.
func TestMySQLTimeRelations(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	queries := []struct {
		name  string
		query string
	}{
		{"seconds round-trip", `SELECT TIME_TO_SEC(SEC_TO_TIME(n)) = n FROM (SELECT 3661 AS n)`},
		{"negative seconds round-trip", `SELECT TIME_TO_SEC(SEC_TO_TIME(n)) = n FROM (SELECT -3661 AS n)`},
		{"seconds round-trip past a day", `SELECT TIME_TO_SEC(SEC_TO_TIME(n)) = n FROM (SELECT 360000 AS n)`},
		{"seconds at the top of the range", `SELECT TIME_TO_SEC(SEC_TO_TIME(n)) = n FROM (SELECT 3020399 AS n)`},
		{"days round-trip", `SELECT FROM_DAYS(TO_DAYS(d)) = d FROM (SELECT '2024-02-29' AS d)`},
		{"days round-trip at the end of the range", `SELECT FROM_DAYS(TO_DAYS(d)) = d FROM (SELECT '9999-12-31' AS d)`},
		{"maketime inverts time_to_sec", `SELECT MAKETIME(1, 1, 1) = SEC_TO_TIME(3661)`},
		{"makedate names the first of january", `SELECT MAKEDATE(2024, 1) = '2024-01-01'`},
		{"period_diff inverts period_add", `SELECT PERIOD_DIFF(PERIOD_ADD(202401, 7), 202401) = 7`},
		{"addtime inverts subtime", `SELECT SUBTIME(ADDTIME(t, '02:30:00'), '02:30:00') = t FROM (SELECT '10:00:00' AS t)`},
		{"addtime inverts subtime on a datetime", `SELECT SUBTIME(ADDTIME(t, '02:30:00'), '02:30:00') = t FROM (SELECT '2024-01-01 10:00:00' AS t)`},
		{"the sign leads the whole result", `SELECT TIME_FORMAT('-01:30:45', '%k %p') = '-1 AM'`},
		{"a positive time carries no sign", `SELECT TIME_FORMAT('01:30:45', '%k %p') = '1 AM'`},
		{"microsecond is the fraction time_to_sec drops", `SELECT MICROSECOND(t) = 500000 AND TIME_TO_SEC(t) = 45045 FROM (SELECT '12:30:45.5' AS t)`},
	}
	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.MySQL, q.query)
			if err != nil {
				t.Fatalf("%s: %v", q.query, err)
			}
			if got.String != "1" {
				t.Errorf("%s = %q, want 1", q.query, got.String)
			}
		})
	}
}

// TestMySQLTimeDivergences records the answers this package deliberately does
// not copy from MySQL, so a later change cannot drift away from them without
// saying so.
func TestMySQLTimeDivergences(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name  string
		query string
		want  string
		mysql string
	}{
		// MySQL carries a fractional-seconds precision on the type of each
		// argument and prints that many digits whether or not there is a
		// fraction. SQLite has no such type, so a fraction this package never
		// saw is not invented. A fraction that is in a value is a different
		// matter: arithmetic on one answers all six digits, the way MySQL does.
		{"a whole number of seconds has no fraction", `SELECT SEC_TO_TIME('3661')`, "01:01:01", "01:01:01.000000"},
		{"a fraction written in a value keeps all six digits", `SELECT ADDTIME('12:00:00', '00:00:00.5')`, "12:00:00.500000", "12:00:00.500000"},
		{"maketime of three strings", `SELECT MAKETIME('1', '2', '3')`, "01:02:03", "01:02:03.000000"},

		// An ISO timestamp is read as a datetime, where MySQL coerces the whole
		// string and answers a number built from its digits.
		{"an iso timestamp keeps its time of day", `SELECT TIME_TO_SEC('2024-01-01T01:00:00')`, "3600", "1224"},

		// MySQL's calendar has no 29 February in the year zero, so its day
		// numbers before the first of March that year are one ahead of the
		// proleptic Gregorian ones. Every date from 0001-01-01 onwards agrees,
		// and MySQL documents TO_DAYS as unreliable before 1582 in any case.
		{"the first day of the year zero", `SELECT TO_DAYS('0000-01-01')`, "0", "1"},
		{"the first of march in the year zero", `SELECT TO_DAYS('0000-03-01')`, "60", "60"},
		{"the first of january in year one", `SELECT TO_DAYS('0001-01-01')`, "366", "366"},

		// MySQL salvages a prefix from some strings that are not a TIME, and
		// answers NULL for others. A value this cannot read is NULL, which is
		// the answer MySQL itself gives for the ones that look least like one.
		{"a time with a fourth field", `SELECT TIME_TO_SEC('1:2:3:4')`, "", "3723"},
		{"a time with an empty field", `SELECT TIME_TO_SEC('01::01')`, "", "1"},
		{"a fraction that is not digits", `SELECT TIME_TO_SEC('01:01:01.abc')`, "", "3661"},
		{"a bare fraction", `SELECT TIME_TO_SEC('.5')`, "", "0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.MySQL, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if tt.want == "" {
				if got.Valid {
					t.Errorf("%s = %q, want NULL (MySQL 8.4 answers %q)", tt.query, got.String, tt.mysql)
				}
				return
			}
			if got.String != tt.want {
				t.Errorf("%s = %q, want %q (MySQL 8.4 answers %q)", tt.query, got.String, tt.want, tt.mysql)
			}
		})
	}
}

// TestMySQLTimeValuesKeepTheirFraction pins the sub-second digits that used to
// be dropped on the way out, and the compound date parts and interval units
// that used to be refused. Every expected value was read from mysql:8.4.
func TestMySQLTimeValuesKeepTheirFraction(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		query string
		want  string
	}{
		// The fraction written in the value survives the helpers that move or
		// take apart the value.
		{`SELECT TIME('2024-03-05 01:02:03.123456')`, "01:02:03.123456"},
		{`SELECT TIME('13:45:56')`, "13:45:56"},
		{`SELECT TIME('2024-03-05')`, "00:20:24"},
		{`SELECT DATE_ADD('2024-03-05 13:45:56.123456', INTERVAL 1 DAY)`, "2024-03-06 13:45:56.123456"},
		{`SELECT DATE_SUB('2024-03-05 13:45:56.123456', INTERVAL 1 HOUR)`, "2024-03-05 12:45:56.123456"},
		{`SELECT ADDTIME('2024-03-05 13:45:56.123456', '01:00:00')`, "2024-03-05 14:45:56.123456"},
		{`SELECT TIMESTAMPADD(SECOND, 1, '2024-03-05 13:45:56.123456')`, "2024-03-05 13:45:57.123456"},
		{`SELECT MICROSECOND(DATE_ADD('2024-03-05 13:45:56.123456', INTERVAL 1 DAY))`, "123456"},

		// EXTRACT of MICROSECOND is the fraction alone, not the seconds field
		// multiplied out, which is PostgreSQL's rule and was answered for both.
		{`SELECT EXTRACT(MICROSECOND FROM '2024-03-05 13:45:56.123456')`, "123456"},
		{`SELECT EXTRACT(SECOND FROM '2024-03-05 13:45:56.123456')`, "56"},

		// The compound part names run their fields together as one number.
		{`SELECT EXTRACT(SECOND_MICROSECOND FROM '2024-03-05 13:45:56.123456')`, "56123456"},
		{`SELECT EXTRACT(MINUTE_MICROSECOND FROM '2024-03-05 13:45:56.123456')`, "4556123456"},
		{`SELECT EXTRACT(MINUTE_SECOND FROM '2024-03-05 13:45:56.123456')`, "4556"},
		{`SELECT EXTRACT(HOUR_MICROSECOND FROM '2024-03-05 13:45:56.123456')`, "134556123456"},
		{`SELECT EXTRACT(HOUR_SECOND FROM '2024-03-05 13:45:56.123456')`, "134556"},
		{`SELECT EXTRACT(HOUR_MINUTE FROM '2024-03-05 13:45:56.123456')`, "1345"},
		{`SELECT EXTRACT(DAY_MICROSECOND FROM '2024-03-05 13:45:56.123456')`, "5134556123456"},
		{`SELECT EXTRACT(DAY_SECOND FROM '2024-03-05 13:45:56.123456')`, "5134556"},
		{`SELECT EXTRACT(DAY_MINUTE FROM '2024-03-05 13:45:56.123456')`, "51345"},
		{`SELECT EXTRACT(DAY_HOUR FROM '2024-03-05 13:45:56.123456')`, "513"},
		{`SELECT EXTRACT(YEAR_MONTH FROM '2024-03-05 13:45:56.123456')`, "202403"},

		// A compound INTERVAL unit carries its fields in one value, and a value
		// shorter than the unit names is read from the right.
		{`SELECT DATE_ADD('2024-01-31', INTERVAL '1:30' HOUR_MINUTE)`, "2024-01-31 01:30:00"},
		{`SELECT DATE_ADD('2024-01-31', INTERVAL '2-3' YEAR_MONTH)`, "2026-04-30"},
		{`SELECT DATE_ADD('2024-01-31', INTERVAL '1 2' DAY_HOUR)`, "2024-02-01 02:00:00"},
		{`SELECT DATE_ADD('2024-01-31 10:00:00', INTERVAL '1:10' DAY_SECOND)`, "2024-01-31 10:01:10"},
		{`SELECT DATE_ADD('2024-01-31 10:00:00', INTERVAL '1 2:3:4' DAY_SECOND)`, "2024-02-01 12:03:04"},
		{`SELECT DATE_SUB('2024-03-31', INTERVAL '1:30' HOUR_MINUTE)`, "2024-03-30 22:30:00"},
		{`SELECT DATE_ADD('2024-01-31 10:00:00', INTERVAL '5.123456' SECOND_MICROSECOND)`, "2024-01-31 10:00:05.123456"},

		// MICROSECOND is a unit of its own everywhere a unit is taken.
		{`SELECT DATE_ADD('2024-01-31', INTERVAL 1 MICROSECOND)`, "2024-01-31 00:00:00.000001"},
		{`SELECT TIMESTAMPADD(MICROSECOND, 1, '2024-01-31 10:00:00')`, "2024-01-31 10:00:00.000001"},
		{`SELECT TIMESTAMPDIFF(MICROSECOND, '2024-01-01', '2025-03-05')`, "37065600000000"},
	}

	for _, tt := range tests {
		got, err := runDialect(t, db, dialects.MySQL, tt.query)
		if err != nil {
			t.Errorf("%s: %v", tt.query, err)
			continue
		}
		if !got.Valid || got.String != tt.want {
			t.Errorf("%s = %v, want %q", tt.query, got, tt.want)
		}
	}
}

// TestMySQLTimeFunctionsAddedForTheEngine covers the time functions that had no
// translation at all. Every want was read from MySQL 8.4.11. CONVERT_TZ is
// limited to fixed offsets because a named zone needs the zone tables MySQL
// loads, which SQLite has no equivalent of; a named zone answers NULL, which is
// also what MySQL answers when those tables are not loaded.
func TestMySQLTimeFunctionsAddedForTheEngine(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	for _, tt := range []struct {
		query    string
		want     string
		wantNull bool
	}{
		// TO_SECONDS counts from year 0, so 1970-01-01 is not zero.
		{query: `SELECT TO_SECONDS('2024-03-05')`, want: "63876816000"},
		{query: `SELECT TO_SECONDS('1970-01-01 00:00:00')`, want: "62167219200"},
		{query: `SELECT TO_SECONDS('2024-03-05 01:02:03')`, want: "63876819723"},
		{query: `SELECT TO_SECONDS('bad')`, wantNull: true},

		{query: `SELECT TIMESTAMP('2024-03-05')`, want: "2024-03-05 00:00:00"},
		{query: `SELECT TIMESTAMP('2024-03-05 01:02:03.5')`, want: "2024-03-05 01:02:03.5"},
		{query: `SELECT TIMESTAMP('2024-03-05','01:02:03')`, want: "2024-03-05 01:02:03"},

		{query: `SELECT CONVERT_TZ('2024-03-05 12:00:00','+00:00','+09:00')`, want: "2024-03-05 21:00:00"},
		{query: `SELECT CONVERT_TZ('2024-03-05 12:00:00','+09:00','+00:00')`, want: "2024-03-05 03:00:00"},
		{query: `SELECT CONVERT_TZ('2024-03-05 12:00:00','UTC','+05:30')`, want: "2024-03-05 17:30:00"},
		{query: `SELECT CONVERT_TZ('bad','+00:00','+09:00')`, wantNull: true},
		{query: `SELECT CONVERT_TZ('2024-03-05 12:00:00','Asia/Tokyo','+00:00')`, wantNull: true},
	} {
		t.Run(tt.query, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.MySQL, tt.query)
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

// TestMySQLDateIsThisPackagesOwnReading pins that DATE() reads a value the way
// every other date helper here reads it. The name is SQLite's own too, and left
// alone it reached SQLite's date(): DATE('2020-02-31') answered 2020-03-02
// where YEAR() of the same string answered NULL, DATE('now') answered today,
// and a second argument was taken as a SQLite modifier.
func TestMySQLDateIsThisPackagesOwnReading(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name     string
		query    string
		want     string
		wantNull bool
		wantErr  bool
	}{
		{name: "an impossible day", query: `SELECT DATE('2020-02-31')`, wantNull: true},
		{name: "a day past a non-leap february", query: `SELECT DATE('2021-02-29')`, wantNull: true},
		{name: "the now keyword", query: `SELECT DATE('now')`, wantNull: true},
		{name: "a modifier argument", query: `SELECT DATE('2020-02-29', '+1 day')`, wantErr: true},
		{name: "a date", query: `SELECT DATE('2020-02-29')`, want: "2020-02-29"},
		{name: "a datetime", query: `SELECT DATE('2020-02-29 13:45:59')`, want: "2020-02-29"},
		{name: "an unpadded date", query: `SELECT DATE('2020-1-2')`, want: "2020-01-02"},
		{name: "a compact date", query: `SELECT DATE(20200229)`, want: "2020-02-29"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.MySQL, tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("%s = %q, want an error", tt.query, got.String)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if tt.wantNull {
				if got.Valid {
					t.Fatalf("%s = %q, want NULL", tt.query, got.String)
				}
				return
			}
			if got.String != tt.want {
				t.Fatalf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestEveryDateHelperAgreesOnAMalformedDate is the invariant behind
// TestMySQLDateIsThisPackagesOwnReading: one reading of a date string serves
// every MySQL date function, so a string one of them refuses is a string all of
// them refuse.
func TestEveryDateHelperAgreesOnAMalformedDate(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	helpers := []string{"DATE", "YEAR", "MONTH", "DAY", "LAST_DAY", "DAYNAME", "TO_DAYS", "QUARTER"}
	malformed := []string{"2020-02-31", "2021-02-29", "2020-13-01", "not a date", "now", ""}
	for _, value := range malformed {
		for _, fn := range helpers {
			query := "SELECT " + fn + "('" + value + "')"
			t.Run(fn+" "+value, func(t *testing.T) {
				got, err := runDialect(t, db, dialects.MySQL, query)
				if err != nil {
					t.Fatalf("%s: %v", query, err)
				}
				if got.Valid {
					t.Fatalf("%s = %q, want NULL as the other helpers answer", query, got.String)
				}
			})
		}
	}
}

// TestYearMonthIntervalMovesInOneStep pins that a YEAR_MONTH interval is one
// amount of months. Moving the years and then the months clamped a month end
// twice, so 2020-02-29 plus INTERVAL '1-2' YEAR_MONTH landed on 2021-04-28: the
// year reached 2021-02-28 first and the two months followed from there.
func TestYearMonthIntervalMovesInOneStep(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "a leap day forward", query: `SELECT DATE_ADD('2020-02-29', INTERVAL '1-2' YEAR_MONTH)`, want: "2021-04-29"},
		{name: "the same amount in months", query: `SELECT DATE_ADD('2020-02-29', INTERVAL 14 MONTH)`, want: "2021-04-29"},
		{name: "the same amount with no years", query: `SELECT DATE_ADD('2020-02-29', INTERVAL '0-14' YEAR_MONTH)`, want: "2021-04-29"},
		{name: "a leap day backward", query: `SELECT DATE_SUB('2020-02-29', INTERVAL '1-2' YEAR_MONTH)`, want: "2018-12-29"},
		{name: "a month end onto a shorter month", query: `SELECT DATE_ADD('2020-01-31', INTERVAL '1-1' YEAR_MONTH)`, want: "2021-02-28"},
		{name: "a datetime keeps its clock", query: `SELECT DATE_ADD('2020-02-29 13:45:59', INTERVAL '1-2' YEAR_MONTH)`, want: "2021-04-29 13:45:59"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.MySQL, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if got.String != tt.want {
				t.Fatalf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestAYearMonthIntervalEqualsItsMonths is the invariant behind
// TestYearMonthIntervalMovesInOneStep: the three spellings of one amount are one
// amount.
func TestAYearMonthIntervalEqualsItsMonths(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	dates := []string{"2020-01-31", "2020-02-29", "2020-03-31", "2019-11-30", "2020-12-31"}
	for _, date := range dates {
		for years := range 3 {
			for months := range 4 {
				compound := fmt.Sprintf("SELECT DATE_ADD('%s', INTERVAL '%d-%d' YEAR_MONTH)", date, years, months)
				plain := fmt.Sprintf("SELECT DATE_ADD('%s', INTERVAL %d MONTH)", date, years*12+months)
				t.Run(fmt.Sprintf("%s %d-%d", date, years, months), func(t *testing.T) {
					a, err := runDialect(t, db, dialects.MySQL, compound)
					if err != nil {
						t.Fatalf("%s: %v", compound, err)
					}
					b, err := runDialect(t, db, dialects.MySQL, plain)
					if err != nil {
						t.Fatalf("%s: %v", plain, err)
					}
					if a.String != b.String {
						t.Fatalf("%s = %q but %s = %q", compound, a.String, plain, b.String)
					}
				})
			}
		}
	}
}

// TestEveryDatetimeHelperStaysInsideTheRange pins the range check on the
// helpers that add a duration without going through the interval arithmetic.
// Each of them could put a year of five digits or a year zero into a value
// nothing here can read back.
func TestEveryDatetimeHelperStaysInsideTheRange(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	queries := []string{
		`SELECT TIMESTAMP('9999-12-31 23:59:59', '00:00:01')`,
		`SELECT ADDTIME('9999-12-31 23:59:59', '00:00:01')`,
		`SELECT SUBTIME('0001-01-01 00:00:00', '00:00:01')`,
		`SELECT CONVERT_TZ('9999-12-31 23:59:59', '+00:00', '+09:00')`,
		`SELECT DATE_ADD('2020-01-01', INTERVAL 9223372036854775807 YEAR)`,
		`SELECT DATE_ADD('2020-01-01', INTERVAL 9223372036854775807 QUARTER)`,
		`SELECT DATE_ADD('2020-01-01', INTERVAL 768614336404564650 YEAR)`,
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.MySQL, query)
			if err != nil {
				t.Fatalf("%s: %v", query, err)
			}
			if got.Valid {
				t.Fatalf("%s = %q, want NULL", query, got.String)
			}
		})
	}
}
