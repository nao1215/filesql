package runtime

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/nao1215/filesql/dialect/internal/dialects"
)

// TestDateArithmeticSemantics locks in the date arithmetic that SQLite's
// datetime() modifier gets wrong for the source dialects: a month-end overflow
// that rolls forward instead of clamping, and a date that grows a time of day.
func TestDateArithmeticSemantics(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name    string
		dialect dialects.Dialect
		query   string
		want    string
	}{
		// Every dialect clamps a month-end overflow to the last day of the
		// target month; SQLite rolls it forward into the next one.
		{"mysql clamps a month end", dialects.MySQL, `SELECT DATE_ADD('2026-01-31', INTERVAL 1 MONTH)`, "2026-02-28"},
		{"mysql clamps going backward", dialects.MySQL, `SELECT DATE_SUB('2026-03-31', INTERVAL 1 MONTH)`, "2026-02-28"},
		{"mysql clamps a leap year", dialects.MySQL, `SELECT DATE_ADD('2028-01-31', INTERVAL 1 MONTH)`, "2028-02-29"},
		{"mysql clamps across a year", dialects.MySQL, `SELECT DATE_ADD('2026-01-31', INTERVAL 13 MONTH)`, "2027-02-28"},
		{"googlesql clamps a month end", dialects.GoogleSQL, `SELECT DATE_ADD(DATE '2026-01-31', INTERVAL 1 MONTH)`, "2026-02-28"},
		{"googlesql clamps going backward", dialects.GoogleSQL, `SELECT DATE_SUB(DATE '2026-03-31', INTERVAL 1 MONTH)`, "2026-02-28"},

		// A date stays a date; only a time-grained unit promotes it.
		{"mysql keeps a date a date", dialects.MySQL, `SELECT DATE_ADD('2026-01-01', INTERVAL 1 DAY)`, "2026-01-02"},
		{"googlesql keeps a date a date", dialects.GoogleSQL, `SELECT DATE_ADD(DATE '2026-01-01', INTERVAL 1 DAY)`, "2026-01-02"},
		{"mysql promotes on an hour", dialects.MySQL, `SELECT DATE_ADD('2026-01-01', INTERVAL 1 HOUR)`, "2026-01-01 01:00:00"},
		{"mysql keeps a datetime a datetime", dialects.MySQL, `SELECT DATE_ADD('2026-01-01 10:00:00', INTERVAL 1 DAY)`, "2026-01-02 10:00:00"},

		// The units and the negative literal that used to be rejected outright.
		{"mysql week", dialects.MySQL, `SELECT DATE_ADD('2026-01-01', INTERVAL 1 WEEK)`, "2026-01-08"},
		{"mysql quarter", dialects.MySQL, `SELECT DATE_ADD('2026-01-01', INTERVAL 1 QUARTER)`, "2026-04-01"},
		{"mysql negative interval", dialects.MySQL, `SELECT DATE_ADD('2026-01-01', INTERVAL -1 DAY)`, "2025-12-31"},
		{"mysql explicit positive interval", dialects.MySQL, `SELECT DATE_ADD('2026-01-01', INTERVAL +1 DAY)`, "2026-01-02"},
		// MySQL accepts any expression as the amount, not only a literal.
		{"mysql interval from a column", dialects.MySQL, `SELECT DATE_ADD('2026-01-01', INTERVAL n DAY) FROM (SELECT 3 AS n) t`, "2026-01-04"},
		{"mysql interval from an expression", dialects.MySQL, `SELECT DATE_ADD('2026-01-01', INTERVAL (1 + 2) DAY)`, "2026-01-04"},
		{"mysql date_sub from a column", dialects.MySQL, `SELECT DATE_SUB('2026-01-10', INTERVAL n DAY) FROM (SELECT 3 AS n) t`, "2026-01-07"},
		{"googlesql week", dialects.GoogleSQL, `SELECT DATE_ADD(DATE '2026-01-01', INTERVAL 1 WEEK)`, "2026-01-08"},

		// MySQL's TIMESTAMPDIFF counts forward from its second argument, the
		// reverse of DATE_DIFF's order.
		{"mysql timestampdiff", dialects.MySQL, `SELECT TIMESTAMPDIFF(DAY, '2026-01-01', '2026-01-10')`, "9"},
		{"mysql timestampdiff backward", dialects.MySQL, `SELECT TIMESTAMPDIFF(DAY, '2026-01-10', '2026-01-01')`, "-9"},
		{"mysql timestampadd", dialects.MySQL, `SELECT TIMESTAMPADD(DAY, 3, '2026-01-01')`, "2026-01-04"},
		{"mysql timestampadd hour", dialects.MySQL, `SELECT TIMESTAMPADD(HOUR, 2, '2026-01-01 00:00:00')`, "2026-01-01 02:00:00"},

		// PostgreSQL has no DATE_ADD; the interval operator is its only date
		// arithmetic. It answers a timestamp whatever it was given, where the
		// other two dialects leave a date a date, so the shape is per dialect
		// rather than per unit. Every want here was read from postgres:17.
		{"postgresql adds a day", dialects.PostgreSQL, `SELECT DATE '2026-01-15' + INTERVAL '1 day'`, "2026-01-16 00:00:00"},
		{"postgresql subtracts a month", dialects.PostgreSQL, `SELECT DATE '2026-03-31' - INTERVAL '1 month'`, "2026-02-28 00:00:00"},
		{"postgresql adds a compound interval", dialects.PostgreSQL, `SELECT DATE '2026-01-01' + INTERVAL '1 year 6 months'`, "2027-07-01 00:00:00"},
		{"postgresql adds weeks", dialects.PostgreSQL, `SELECT DATE '2026-01-31' + INTERVAL '2 weeks'`, "2026-02-14 00:00:00"},
		{"postgresql promotes on an hour", dialects.PostgreSQL, `SELECT DATE '2026-01-01' + INTERVAL '2 hours'`, "2026-01-01 02:00:00"},
		{"postgresql adds to a cast value", dialects.PostgreSQL, `SELECT '2026-01-15'::date + INTERVAL '1 day'`, "2026-01-16 00:00:00"},
		{"postgresql keeps a timestamp's time", dialects.PostgreSQL, `SELECT TIMESTAMP '2026-01-31 08:00:00' + INTERVAL '1 month'`, "2026-02-28 08:00:00"},

		// Typed date literals, which SQLite does not parse in any dialect.
		{"mysql date literal", dialects.MySQL, `SELECT DATE '2026-01-01'`, "2026-01-01"},
		{"mysql timestamp literal", dialects.MySQL, `SELECT TIMESTAMP '2026-01-01 10:00:00'`, "2026-01-01 10:00:00"},
		{"postgresql date literal", dialects.PostgreSQL, `SELECT DATE '2026-01-01'`, "2026-01-01"},

		// GoogleSQL's DATE_TRUNC puts the value first and the part as a bare
		// keyword; PostgreSQL's spelling still works too.
		{"googlesql date_trunc", dialects.GoogleSQL, `SELECT DATE_TRUNC(DATE '2026-03-15', MONTH)`, "2026-03-01"},
		{"googlesql timestamp_trunc", dialects.GoogleSQL, `SELECT TIMESTAMP_TRUNC(TIMESTAMP '2026-03-15 10:20:30', HOUR)`, "2026-03-15 10:00:00"},
		{"googlesql datetime_trunc", dialects.GoogleSQL, `SELECT DATETIME_TRUNC(TIMESTAMP '2026-03-15 10:20:30', DAY)`, "2026-03-15 00:00:00"},
		{"postgresql date_trunc keeps its order", dialects.PostgreSQL, `SELECT DATE_TRUNC('month', '2026-03-15 10:00:00')`, "2026-03-01 00:00:00"},

		// GoogleSQL allows the date and time parts of a timestamp to be
		// extracted, not just its numeric fields.
		{"googlesql extract date", dialects.GoogleSQL, `SELECT EXTRACT(DATE FROM TIMESTAMP '2026-03-15 10:00:00')`, "2026-03-15"},
		{"googlesql extract time", dialects.GoogleSQL, `SELECT EXTRACT(TIME FROM TIMESTAMP '2026-03-15 10:20:30')`, "10:20:30"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if !got.Valid || got.String != tt.want {
				t.Fatalf("%s = %v, want %q", tt.query, got, tt.want)
			}
		})
	}
}

// TestCurrentValueParentheses covers the no-argument datetime keywords MySQL and
// GoogleSQL write as calls and SQLite accepts only bare.
func TestCurrentValueParentheses(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	for _, tt := range []struct {
		dialect dialects.Dialect
		query   string
		want    int
	}{
		{dialects.GoogleSQL, `SELECT CURRENT_DATE()`, len("2026-01-01")},
		{dialects.MySQL, `SELECT CURRENT_TIMESTAMP()`, len("2026-01-01 00:00:00")},
	} {
		got, err := runDialect(t, db, tt.dialect, tt.query)
		if err != nil {
			t.Fatalf("%s: %v", tt.query, err)
		}
		if len(got.String) != tt.want {
			t.Fatalf("%s = %q, want %d characters", tt.query, got.String, tt.want)
		}
	}
}

// TestDateArithmeticRejects reports the interval shapes the helper cannot model
// instead of computing something else.
func TestDateArithmeticRejects(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		dialect dialects.Dialect
		query   string
	}{
		{dialects.MySQL, `SELECT DATE_ADD(d, INTERVAL 1 DAY_HOUR)`},
		{dialects.PostgreSQL, `SELECT DATE '2026-01-01' + INTERVAL 'nonsense'`},
		{dialects.PostgreSQL, `SELECT DATE '2026-01-01' + INTERVAL '1 fortnight'`},
		// A fraction of a month has no fixed length, so it stays refused where
		// a fraction of a day is read.
		{dialects.PostgreSQL, `SELECT DATE '2026-01-01' + INTERVAL '1.5 months'`},
	}
	for _, tt := range tests {
		if _, err := runDialect(t, db, tt.dialect, tt.query); err == nil {
			t.Fatalf("%s should fail", tt.query)
		}
	}
}

func TestAddMonthsClamps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		from   string
		months int64
		want   string
	}{
		{"2026-01-31", 1, "2026-02-28"},
		{"2026-01-31", -1, "2025-12-31"},
		{"2026-03-31", -1, "2026-02-28"},
		{"2028-01-31", 1, "2028-02-29"},
		{"2026-01-31", 12, "2027-01-31"},
		{"2026-01-15", 0, "2026-01-15"},
		{"2026-01-31", -13, "2024-12-31"},
	}
	// An amount whose months do not fit, or whose year leaves the range a date
	// can hold, is refused rather than wrapped into a plausible date.
	for _, months := range []int64{math.MaxInt64, math.MinInt64, 1 << 40, -(1 << 40)} {
		if _, err := addMonths(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), months); err == nil {
			t.Fatalf("addMonths(2026-01-01, %d) should be refused", months)
		}
	}
	for _, tt := range tests {
		from, err := time.Parse(layoutDateOnly, tt.from)
		if err != nil {
			t.Fatalf("parse %q: %v", tt.from, err)
		}
		moved, err := addMonths(from, tt.months)
		if err != nil {
			t.Fatalf("addMonths(%s, %d): %v", tt.from, tt.months, err)
		}
		if got := moved.Format(layoutDateOnly); got != tt.want {
			t.Fatalf("addMonths(%s, %d) = %s, want %s", tt.from, tt.months, got, tt.want)
		}
	}
}

func TestParseIntervalText(t *testing.T) {
	t.Parallel()

	got, err := parseIntervalText(" 1 year 6 months ")
	if err != nil {
		t.Fatalf("parseIntervalText: %v", err)
	}
	want := []intervalTerm{{1, unitYear}, {6, unitMonth}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseIntervalText = %v, want %v", got, want)
	}

	for _, bad := range []string{"", "1", "one day", "1 fortnight", "1 day 2"} {
		if _, err := parseIntervalText(bad); err == nil {
			t.Fatalf("parseIntervalText(%q) should fail", bad)
		}
	}
}

func TestHasTimePart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   any
		want bool
	}{
		{"2026-01-01", false},
		{"2026-01-01 10:00:00", true},
		{"2026-01-01T10:00:00", true},
		{time.Now(), true},
		{nil, false},
	}
	for _, tt := range tests {
		if got := hasTimePart(tt.in); got != tt.want {
			t.Fatalf("hasTimePart(%v) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

// TestAddInterval covers the interval arithmetic the three dialects share,
// including the month clamping Go's AddDate does not do: "January 31 plus one
// month" is the last day of February, not March 3.
func TestAddInterval(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 1, 31, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		n       int64
		unit    string
		want    time.Time
		wantErr bool
	}{
		{name: "years", n: 1, unit: unitYear, want: time.Date(2027, 1, 31, 10, 0, 0, 0, time.UTC)},
		{name: "quarters", n: 1, unit: unitQuarter, want: time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)},
		{name: "months clamp to the last day", n: 1, unit: unitMonth, want: time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)},
		{name: "months backwards", n: -1, unit: unitMonth, want: time.Date(2025, 12, 31, 10, 0, 0, 0, time.UTC)},
		{name: "weeks", n: 1, unit: unitWeek, want: time.Date(2026, 2, 7, 10, 0, 0, 0, time.UTC)},
		{name: "days", n: 1, unit: unitDay, want: time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)},
		{name: "hours", n: 2, unit: unitHour, want: time.Date(2026, 1, 31, 12, 0, 0, 0, time.UTC)},
		{name: "minutes", n: 30, unit: unitMinute, want: time.Date(2026, 1, 31, 10, 30, 0, 0, time.UTC)},
		{name: "seconds", n: 45, unit: unitSecond, want: time.Date(2026, 1, 31, 10, 0, 45, 0, time.UTC)},
		{name: "a unit no dialect defines", n: 1, unit: "fortnight", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := addInterval(base, tt.n, tt.unit)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("addInterval(%d, %q) = %v, want an error", tt.n, tt.unit, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("addInterval(%d, %q) error: %v", tt.n, tt.unit, err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("addInterval(%d, %q) = %v, want %v", tt.n, tt.unit, got, tt.want)
			}
		})
	}
}

// TestDateArithmeticStaysInsideTheRange pins that arithmetic which leaves the
// year range a date can hold answers NULL rather than a string this package
// cannot read back. Adding a day to 9999-12-31 used to answer "10000-01-01",
// which YEAR() then read as NULL: the row was lost at the second function
// rather than at the first, where the caller could still see why.
func TestDateArithmeticStaysInsideTheRange(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name     string
		dialect  dialects.Dialect
		query    string
		want     string
		wantNull bool
	}{
		{name: "mysql past the last day", dialect: dialects.MySQL, query: `SELECT DATE_ADD('9999-12-31', INTERVAL 1 DAY)`, wantNull: true},
		{name: "mysql past the last second", dialect: dialects.MySQL, query: `SELECT DATE_ADD('9999-12-31 23:59:59', INTERVAL 1 SECOND)`, wantNull: true},
		{name: "mysql before the first day", dialect: dialects.MySQL, query: `SELECT DATE_SUB('0001-01-01', INTERVAL 1 DAY)`, wantNull: true},
		{name: "mysql a million years on", dialect: dialects.MySQL, query: `SELECT DATE_ADD('2020-02-29', INTERVAL 1000000 YEAR)`, wantNull: true},
		{name: "mysql a million years back", dialect: dialects.MySQL, query: `SELECT DATE_SUB('2020-02-29', INTERVAL 1000000 YEAR)`, wantNull: true},
		{name: "mysql a million months", dialect: dialects.MySQL, query: `SELECT DATE_ADD('2020-02-29', INTERVAL 1000000 MONTH)`, wantNull: true},
		{name: "mysql a compound unit past the end", dialect: dialects.MySQL, query: `SELECT DATE_ADD('9999-12-31', INTERVAL '1-0' YEAR_MONTH)`, wantNull: true},
		{name: "googlesql past the last day", dialect: dialects.GoogleSQL, query: `SELECT DATE_ADD(DATE '9999-12-31', INTERVAL 1 DAY)`, wantNull: true},
		{name: "postgresql past the last day", dialect: dialects.PostgreSQL, query: `SELECT TIMESTAMP '9999-12-31 23:59:59' + INTERVAL '1 day'`, wantNull: true},

		// The last representable day still costs nothing.
		{name: "mysql reaches the last day", dialect: dialects.MySQL, query: `SELECT DATE_ADD('9999-12-30', INTERVAL 1 DAY)`, want: "9999-12-31"},
		{name: "mysql reaches the first day", dialect: dialects.MySQL, query: `SELECT DATE_SUB('0001-01-02', INTERVAL 1 DAY)`, want: "0001-01-01"},
		{name: "googlesql reaches the last day", dialect: dialects.GoogleSQL, query: `SELECT DATE_ADD(DATE '9999-12-30', INTERVAL 1 DAY)`, want: "9999-12-31"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if tt.wantNull {
				if got.Valid {
					t.Fatalf("%s = %q, want NULL", tt.query, got.String)
				}
				return
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

// TestEveryDateArithmeticResultCanBeReadBack is the invariant behind
// TestDateArithmeticStaysInsideTheRange: whatever DATE_ADD answers, the
// helpers of this package have to be able to read. A result they cannot read
// is a row that disappears one function later with nothing said.
func TestEveryDateArithmeticResultCanBeReadBack(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	units := []string{"MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR"}
	amounts := []string{"1", "-1", "1000000", "-1000000", "999999999"}
	for _, unit := range units {
		for _, amount := range amounts {
			query := "SELECT YEAR(DATE_ADD('2020-02-29 13:45:59', INTERVAL " + amount + " " + unit + "))"
			inner := "SELECT DATE_ADD('2020-02-29 13:45:59', INTERVAL " + amount + " " + unit + ")"
			t.Run(unit+" "+amount, func(t *testing.T) {
				moved, err := runDialect(t, db, dialects.MySQL, inner)
				if err != nil {
					t.Fatalf("%s: %v", inner, err)
				}
				if !moved.Valid {
					return // Refusing the arithmetic is the answer this asks for.
				}
				read, err := runDialect(t, db, dialects.MySQL, query)
				if err != nil {
					t.Fatalf("%s: %v", query, err)
				}
				if !read.Valid {
					t.Fatalf("%s answered %q, which YEAR() then read as NULL", inner, moved.String)
				}
			})
		}
	}
}

// TestIntervalLiteralTakesEveryUnitTheTruncationTakes pins that the words a
// PostgreSQL INTERVAL literal accepts are the words the rest of the package
// accepts. Four of them used to be missing from the literal alone, two of
// which addInterval already implemented.
func TestIntervalLiteralTakesEveryUnitTheTruncationTakes(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "microsecond", query: `SELECT TIMESTAMP '2020-02-29 13:45:59' + INTERVAL '1 microsecond'`, want: "2020-02-29 13:45:59.000001"},
		{name: "microseconds", query: `SELECT TIMESTAMP '2020-02-29 13:45:59' + INTERVAL '2 microseconds'`, want: "2020-02-29 13:45:59.000002"},
		{name: "millisecond", query: `SELECT TIMESTAMP '2020-02-29 13:45:59' + INTERVAL '1 millisecond'`, want: "2020-02-29 13:45:59.001"},
		{name: "decade", query: `SELECT TIMESTAMP '2020-02-29 13:45:59' + INTERVAL '1 decade'`, want: "2030-02-28 13:45:59"},
		{name: "decades", query: `SELECT TIMESTAMP '2020-02-29 13:45:59' - INTERVAL '1 decades'`, want: "2010-02-28 13:45:59"},
		{name: "century", query: `SELECT TIMESTAMP '2020-02-29 13:45:59' + INTERVAL '1 century'`, want: "2120-02-29 13:45:59"},
		{name: "centuries", query: `SELECT TIMESTAMP '2020-02-29 13:45:59' - INTERVAL '1 centuries'`, want: "1920-02-29 13:45:59"},
		{name: "the units that already worked", query: `SELECT TIMESTAMP '2020-02-29 13:45:59' + INTERVAL '1 day 2 hours'`, want: "2020-03-01 15:45:59"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.PostgreSQL, tt.query)
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

// TestIntervalFieldsAreAppliedInPostgreSQLsOrder pins the order PostgreSQL
// applies an interval's three fields in: months, then days, then the clock.
// Applying each term as the literal wrote it gave a different day whenever a
// month landed on a month end.
func TestIntervalFieldsAreAppliedInPostgreSQLsOrder(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "days written before the month", query: `SELECT TIMESTAMP '2021-01-30' + INTERVAL '2 days 1 month'`, want: "2021-03-02 00:00:00"},
		{name: "the month written first", query: `SELECT TIMESTAMP '2021-01-30' + INTERVAL '1 month 2 days'`, want: "2021-03-02 00:00:00"},
		{name: "subtracting both", query: `SELECT TIMESTAMP '2021-03-31' - INTERVAL '1 day 1 month'`, want: "2021-02-27 00:00:00"},
		{name: "a fractional day beside a month", query: `SELECT TIMESTAMP '2021-01-31' + INTERVAL '1.5 days 1 month'`, want: "2021-03-01 12:00:00"},
		{name: "hours beside a month", query: `SELECT TIMESTAMP '2021-01-31' + INTERVAL '3 hours 1 month'`, want: "2021-02-28 03:00:00"},
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

// TestIntervalLiteralTakesPostgreSQLsAbbreviations pins the short spellings
// PostgreSQL accepts for the coarse units, which are as much a part of the unit
// vocabulary as the long ones.
func TestIntervalLiteralTakesPostgreSQLsAbbreviations(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := map[string]string{
		"1 dec":  "2030-02-28 13:45:59",
		"1 decs": "2030-02-28 13:45:59",
		"1 c":    "2120-02-29 13:45:59",
		"1 cent": "2120-02-29 13:45:59",
		"1 mil":  "3020-02-29 13:45:59",
		"1 mils": "3020-02-29 13:45:59",
		"1 y":    "2021-02-28 13:45:59",
		"1 mon":  "2020-03-29 13:45:59",
	}
	for written, want := range tests {
		query := "SELECT TIMESTAMP '2020-02-29 13:45:59' + INTERVAL '" + written + "'"
		t.Run(written, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.PostgreSQL, query)
			if err != nil {
				t.Fatalf("%s: %v", query, err)
			}
			if got.String != want {
				t.Fatalf("%s = %q, want %q", query, got.String, want)
			}
		})
	}
}
