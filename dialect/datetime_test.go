package dialect

import (
	"reflect"
	"testing"
	"time"
)

// TestDateArithmeticSemantics locks in the date arithmetic that SQLite's
// datetime() modifier gets wrong for the source dialects: a month-end overflow
// that rolls forward instead of clamping, and a date that grows a time of day.
func TestDateArithmeticSemantics(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name    string
		dialect Dialect
		query   string
		want    string
	}{
		// Every dialect clamps a month-end overflow to the last day of the
		// target month; SQLite rolls it forward into the next one.
		{"mysql clamps a month end", MySQL, `SELECT DATE_ADD('2026-01-31', INTERVAL 1 MONTH)`, "2026-02-28"},
		{"mysql clamps going backward", MySQL, `SELECT DATE_SUB('2026-03-31', INTERVAL 1 MONTH)`, "2026-02-28"},
		{"mysql clamps a leap year", MySQL, `SELECT DATE_ADD('2028-01-31', INTERVAL 1 MONTH)`, "2028-02-29"},
		{"mysql clamps across a year", MySQL, `SELECT DATE_ADD('2026-01-31', INTERVAL 13 MONTH)`, "2027-02-28"},
		{"googlesql clamps a month end", GoogleSQL, `SELECT DATE_ADD(DATE '2026-01-31', INTERVAL 1 MONTH)`, "2026-02-28"},
		{"googlesql clamps going backward", GoogleSQL, `SELECT DATE_SUB(DATE '2026-03-31', INTERVAL 1 MONTH)`, "2026-02-28"},

		// A date stays a date; only a time-grained unit promotes it.
		{"mysql keeps a date a date", MySQL, `SELECT DATE_ADD('2026-01-01', INTERVAL 1 DAY)`, "2026-01-02"},
		{"mysql promotes on an hour", MySQL, `SELECT DATE_ADD('2026-01-01', INTERVAL 1 HOUR)`, "2026-01-01 01:00:00"},
		{"mysql keeps a datetime a datetime", MySQL, `SELECT DATE_ADD('2026-01-01 10:00:00', INTERVAL 1 DAY)`, "2026-01-02 10:00:00"},

		// The units and the negative literal that used to be rejected outright.
		{"mysql week", MySQL, `SELECT DATE_ADD('2026-01-01', INTERVAL 1 WEEK)`, "2026-01-08"},
		{"mysql quarter", MySQL, `SELECT DATE_ADD('2026-01-01', INTERVAL 1 QUARTER)`, "2026-04-01"},
		{"mysql negative interval", MySQL, `SELECT DATE_ADD('2026-01-01', INTERVAL -1 DAY)`, "2025-12-31"},
		{"mysql explicit positive interval", MySQL, `SELECT DATE_ADD('2026-01-01', INTERVAL +1 DAY)`, "2026-01-02"},
		{"googlesql week", GoogleSQL, `SELECT DATE_ADD(DATE '2026-01-01', INTERVAL 1 WEEK)`, "2026-01-08"},

		// MySQL's TIMESTAMPDIFF counts forward from its second argument, the
		// reverse of DATE_DIFF's order.
		{"mysql timestampdiff", MySQL, `SELECT TIMESTAMPDIFF(DAY, '2026-01-01', '2026-01-10')`, "9"},
		{"mysql timestampdiff backward", MySQL, `SELECT TIMESTAMPDIFF(DAY, '2026-01-10', '2026-01-01')`, "-9"},
		{"mysql timestampadd", MySQL, `SELECT TIMESTAMPADD(DAY, 3, '2026-01-01')`, "2026-01-04"},
		{"mysql timestampadd hour", MySQL, `SELECT TIMESTAMPADD(HOUR, 2, '2026-01-01 00:00:00')`, "2026-01-01 02:00:00"},

		// PostgreSQL has no DATE_ADD; the interval operator is its only date
		// arithmetic.
		{"postgresql adds a day", PostgreSQL, `SELECT DATE '2026-01-15' + INTERVAL '1 day'`, "2026-01-16"},
		{"postgresql subtracts a month", PostgreSQL, `SELECT DATE '2026-03-31' - INTERVAL '1 month'`, "2026-02-28"},
		{"postgresql adds a compound interval", PostgreSQL, `SELECT DATE '2026-01-01' + INTERVAL '1 year 6 months'`, "2027-07-01"},
		{"postgresql promotes on an hour", PostgreSQL, `SELECT DATE '2026-01-01' + INTERVAL '2 hours'`, "2026-01-01 02:00:00"},
		{"postgresql adds to a cast value", PostgreSQL, `SELECT '2026-01-15'::date + INTERVAL '1 day'`, "2026-01-16"},

		// Typed date literals, which SQLite does not parse in any dialect.
		{"mysql date literal", MySQL, `SELECT DATE '2026-01-01'`, "2026-01-01"},
		{"mysql timestamp literal", MySQL, `SELECT TIMESTAMP '2026-01-01 10:00:00'`, "2026-01-01 10:00:00"},
		{"postgresql date literal", PostgreSQL, `SELECT DATE '2026-01-01'`, "2026-01-01"},

		// GoogleSQL's DATE_TRUNC puts the value first and the part as a bare
		// keyword; PostgreSQL's spelling still works too.
		{"googlesql date_trunc", GoogleSQL, `SELECT DATE_TRUNC(DATE '2026-03-15', MONTH)`, "2026-03-01"},
		{"googlesql timestamp_trunc", GoogleSQL, `SELECT TIMESTAMP_TRUNC(TIMESTAMP '2026-03-15 10:20:30', HOUR)`, "2026-03-15 10:00:00"},
		{"googlesql datetime_trunc", GoogleSQL, `SELECT DATETIME_TRUNC(TIMESTAMP '2026-03-15 10:20:30', DAY)`, "2026-03-15 00:00:00"},
		{"postgresql date_trunc keeps its order", PostgreSQL, `SELECT DATE_TRUNC('month', '2026-03-15 10:00:00')`, "2026-03-01 00:00:00"},

		// GoogleSQL allows the date and time parts of a timestamp to be
		// extracted, not just its numeric fields.
		{"googlesql extract date", GoogleSQL, `SELECT EXTRACT(DATE FROM TIMESTAMP '2026-03-15 10:00:00')`, "2026-03-15"},
		{"googlesql extract time", GoogleSQL, `SELECT EXTRACT(TIME FROM TIMESTAMP '2026-03-15 10:20:30')`, "10:20:30"},
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
		dialect Dialect
		query   string
		want    int
	}{
		{GoogleSQL, `SELECT CURRENT_DATE()`, len("2026-01-01")},
		{MySQL, `SELECT CURRENT_TIMESTAMP()`, len("2026-01-01 00:00:00")},
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
		dialect Dialect
		query   string
	}{
		{MySQL, `SELECT DATE_ADD(d, INTERVAL 1 DAY_HOUR)`},
		{MySQL, `SELECT DATE_ADD('2026-01-01', INTERVAL 'x' DAY)`},
		{PostgreSQL, `SELECT DATE '2026-01-01' + INTERVAL 'nonsense'`},
		{PostgreSQL, `SELECT DATE '2026-01-01' + INTERVAL '1 fortnight'`},
		{PostgreSQL, `SELECT DATE '2026-01-01' + INTERVAL '1.5 days'`},
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
	for _, tt := range tests {
		from, err := time.Parse(layoutDateOnly, tt.from)
		if err != nil {
			t.Fatalf("parse %q: %v", tt.from, err)
		}
		if got := addMonths(from, tt.months).Format(layoutDateOnly); got != tt.want {
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
