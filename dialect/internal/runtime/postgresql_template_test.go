package runtime

import (
	"strconv"
	"testing"
	"time"
)

// TestToCharDateTemplate pins TO_CHAR's date/time template against PostgreSQL
// 17.10, one pattern at a time. Every want was read from that engine rather
// than derived.
//
// The template used to be translated into a Go layout string and handed to
// time.Format, which could not express any of this: a Go layout has one
// spelling per field, so MONTH, Month and month were one answer instead of
// three and a name could not be padded to a fixed width, and a pattern with no
// Go equivalent was copied out as literal text -- which is how DDD came to
// answer "05D" and IYYY "I24Y". The prefix collisions below are that class of
// mistake, and the three case spellings of each name pattern are the other.
func TestToCharDateTemplate(t *testing.T) {
	t.Parallel()

	tm := time.Date(2024, 3, 5, 13, 4, 5, 123456789, time.UTC)
	for _, tt := range []struct {
		format string
		want   string
	}{
		{"YYYY-MM-DD", "2024-03-05"},
		{"YYYY", "2024"},
		{"YYY", "024"},
		{"YY", "24"},
		{"Y", "4"},
		{"Y,YYY", "2,024"},
		{"IYYY", "2024"},
		{"IYY", "024"},
		{"IY", "24"},
		{"I", "4"},

		// Each name pattern in its three case spellings.
		{"MONTH", "MARCH    "},
		{"Month", "March    "},
		{"month", "march    "},
		{"MON", "MAR"},
		{"Mon", "Mar"},
		{"mon", "mar"},
		{"DAY", "TUESDAY  "},
		{"Day", "Tuesday  "},
		{"day", "tuesday  "},
		{"DY", "TUE"},
		{"Dy", "Tue"},
		{"dy", "tue"},
		{"RM", "III "},
		{"rm", "iii "},

		// FM suppresses the padding of a name, and it binds to the one pattern
		// that follows it rather than to the whole template.
		{"FMDay", "Tuesday"},
		{"FMMonth", "March"},
		{"FMDD", "5"},
		{"FMMM", "3"},
		{"FMDDD", "65"},
		{"FMDay, DD FMMonth YYYY", "Tuesday, 05 March 2024"},
		{"DD FMDD", "05 5"},
		{"FMDD DD", "5 05"},

		// The prefix collisions: each of these shares its first characters with
		// a shorter pattern, and the shorter one used to win.
		{"DDD", "065"},
		{"DD", "05"},
		{"D", "3"},
		{"IDDD", "065"},
		{"ID", "2"},
		{"HH24", "13"},
		{"HH12", "01"},
		{"HH", "01"},
		{"SSSS", "47045"},
		{"SS", "05"},
		{"MI", "04"},
		{"MS", "123"},
		{"US", "123456"},

		{"W", "1"},
		{"WW", "10"},
		{"IW", "10"},
		{"Q", "1"},
		{"CC", "21"},
		{"J", "2460375"},
		{"AM", "PM"},
		{"am", "pm"},
		{"A.M.", "P.M."},
		{"BC", "AD"},
		{"DDTH", "05TH"},
		{"DDth", "05th"},

		// Double-quoted text is literal, including a letter that would
		// otherwise be read as a pattern.
		{`"Year:" YYYY`, "Year: 2024"},
		{`"a"YYYY"b"`, "a2024b"},
		{`"DD"DD`, "DD05"},
	} {
		t.Run(tt.format, func(t *testing.T) {
			t.Parallel()

			if got := pgFormatTime(tt.format, tm); got != tt.want {
				t.Errorf("TO_CHAR(ts, %q) = %q, want %q", tt.format, got, tt.want)
			}
		})
	}
}

// TestToCharDateTemplateOnAMorningHour covers the halves of the day and the era
// that the afternoon timestamp above cannot show.
func TestToCharDateTemplateOnAMorningHour(t *testing.T) {
	t.Parallel()

	tm := time.Date(2024, 3, 5, 1, 4, 5, 0, time.UTC)
	for _, tt := range []struct {
		format string
		want   string
	}{
		{"HH12:MI:SS AM", "01:04:05 AM"},
		{"HH12:MI:SS am", "01:04:05 am"},
		{"A.M.", "A.M."},
		{"a.m.", "a.m."},
		{"HH24", "01"},
		{"MS", "000"},
	} {
		t.Run(tt.format, func(t *testing.T) {
			t.Parallel()

			if got := pgFormatTime(tt.format, tm); got != tt.want {
				t.Errorf("TO_CHAR(ts, %q) = %q, want %q", tt.format, got, tt.want)
			}
		})
	}
}

// TestToCharNumericTemplate pins the numeric template against PostgreSQL
// 17.10. The template used to be read as digit positions and a decimal point
// alone, so FM, the group and decimal locale patterns, every sign spelling and
// the "#" overflow fill all did nothing: FM999999.00 kept the padding it was
// written to remove, 999G999G999 grouped nothing, 999D99 dropped the fraction
// it asked for, and a value too wide for its template printed in full.
func TestToCharNumericTemplate(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		value  float64
		format string
		want   string
	}{
		// Digit positions and padding.
		{12, "999", "  12"},
		{0, "999", "   0"},
		{-12, "999", " -12"},
		{12, "0999", " 0012"},
		{-12, "0999", "-0012"},
		{1234.567, "999.99", " ###.##"},
		{12.5, "999.99", "  12.50"},
		{0.5, "9.9", "  .5"},
		{0.5, "0.9", " 0.5"},

		// A value too wide for its positions.
		{100, "99", " ##"},
		{-1234, "999", "-###"},
		{1234.56, "999D99", " ###.##"},

		// FM removes the padding, and with it the column reserved for a sign
		// that is not there.
		{12345.678, "FM999999.00", "12345.68"},
		{1, "FM9", "1"},
		{0, "FM9", "0"},
		{-12, "FM999", "-12"},
		{0.25, "FM990.99", "0.25"},
		{1234.5, "FM9,999.9", "1,234.5"},
		{12, "FM999.99", "12."},

		// Group and decimal separators, and the locale spellings of both.
		{1234567, "999G999G999", "   1,234,567"},
		{1234, "9,999", " 1,234"},
		{12, "9,999", "    12"},
		{1234.56, "9999D99", " 1234.56"},

		// Every sign spelling, positive and negative.
		{12, "S999", " +12"},
		{-12, "S999", " -12"},
		{12, "999S", " 12+"},
		{-12, "999S", " 12-"},
		{-12, "MI999", "- 12"},
		{12, "MI999", "  12"},
		{12, "999MI", " 12 "},
		{-12, "999MI", " 12-"},
		{12, "PL999", "+  12"},
		{-12, "PL999", "  -12"},
		{12, "999PL", "  12+"},
		{-12, "999PL", " -12 "},
		{12, "SG999", "+ 12"},
		{-12, "SG999", "- 12"},
		{12, "999PR", "  12 "},
		{-12, "999PR", " <12>"},

		// Roman numerals, the digit shift and the ordinal suffix.
		{12, "RN", "            XII"},
		{12, "rn", "            xii"},
		{12, "FMRN", "XII"},
		{0, "RN", "###############"},
		{0.5, "9V99", "  50"},
		{1, "9TH", " 1ST"},
		{1, "9th", " 1st"},
		{-1, "9TH", "-1"},

		// Rounding is away from zero at a half, as SQL rounds and Go's own
		// formatting does not.
		{0.5, "999", "   1"},
		{1.5, "999", "   2"},
		{2.5, "999", "   3"},
		{-0.5, "999", "  -1"},
	} {
		t.Run(tt.format+"/"+trimFloat(tt.value), func(t *testing.T) {
			t.Parallel()

			if got := pgFormatNumber(tt.value, tt.format); got != tt.want {
				t.Errorf("TO_CHAR(%v, %q) = %q, want %q", tt.value, tt.format, got, tt.want)
			}
		})
	}
}

// trimFloat names a value in a subtest name.
func trimFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// TestToCharNumericTemplateUnderFillMode covers the sign spellings under FM,
// where no column is reserved for a sign that is not there. They are a
// separate arm of the layout from the padded one above and were otherwise
// reached only through the default sign.
func TestToCharNumericTemplateUnderFillMode(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		value  float64
		format string
		want   string
	}{
		{12, "FMS999", "+12"},
		{-12, "FMS999", "-12"},
		{12, "FM999S", "12+"},
		{-12, "FM999S", "12-"},
		{12, "FMSG999", "+12"},
		{-12, "FM999SG", "12-"},
		{12, "FMMI999", "12"},
		{-12, "FMMI999", "-12"},
		{-12, "FM999MI", "12-"},
		{12, "FMPL999", "+12"},
		{-12, "FMPL999", "-12"},
		{12, "FM999PL", "12+"},
		{12, "FM999PR", "12"},
		{-12, "FM999PR", "<12>"},
		{-1234, "FM999", "-###"},
	} {
		t.Run(tt.format+"/"+trimFloat(tt.value), func(t *testing.T) {
			t.Parallel()

			if got := pgFormatNumber(tt.value, tt.format); got != tt.want {
				t.Errorf("TO_CHAR(%v, %q) = %q, want %q", tt.value, tt.format, got, tt.want)
			}
		})
	}
}

// TestToCharDateTemplateAtTheEdges covers the days and eras the two fixed
// timestamps above cannot reach: a Sunday, which is the day ISO numbering and
// PostgreSQL's own D disagree most about, and a year before the common era.
func TestToCharDateTemplateAtTheEdges(t *testing.T) {
	t.Parallel()

	sunday := time.Date(2024, 3, 3, 0, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		format string
		want   string
	}{
		{"D", "1"},
		{"ID", "7"},
		{"DY", "SUN"},
		{"IW", "09"},
	} {
		t.Run("sunday/"+tt.format, func(t *testing.T) {
			t.Parallel()

			if got := pgFormatTime(tt.format, sunday); got != tt.want {
				t.Errorf("TO_CHAR(sunday, %q) = %q, want %q", tt.format, got, tt.want)
			}
		})
	}

	// Go counts 1 BC as year 0 and 44 BC as year -43, and the year patterns
	// print the year of the era rather than the year Go counts, so this is
	// 44 BC and TO_CHAR prints 0044. CC is the one field that keeps the sign.
	beforeTheEra := time.Date(-43, 3, 15, 0, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		format string
		want   string
	}{
		{"BC", "BC"},
		{"bc", "bc"},
		{"B.C.", "B.C."},
		{"YYYY", "0044"},
		{"YYY", "044"},
		{"YYYY-MM-DD BC", "0044-03-15 BC"},
		{"CC", "-01"},
		{"FMYYYY", "44"},
	} {
		t.Run("bc/"+tt.format, func(t *testing.T) {
			t.Parallel()

			if got := pgFormatTime(tt.format, beforeTheEra); got != tt.want {
				t.Errorf("TO_CHAR(bc, %q) = %q, want %q", tt.format, got, tt.want)
			}
		})
	}
}

// TestIsDateTemplateTellsTheTwoApart covers the choice TO_CHAR makes before it
// formats anything: a template with a digit position is numeric, RN is the one
// numeric template with none, and a digit inside double quotes is literal text
// and says nothing about either.
func TestIsDateTemplateTellsTheTwoApart(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		format string
		want   bool
	}{
		{"YYYY-MM-DD", true},
		{"999", false},
		{"0.9", false},
		{"RN", false},
		{"rn", false},
		{`"9" YYYY`, true},
		{`"RN" YYYY`, true},
	} {
		t.Run(tt.format, func(t *testing.T) {
			t.Parallel()

			if got := isDateTemplate(tt.format); got != tt.want {
				t.Errorf("isDateTemplate(%q) = %v, want %v", tt.format, got, tt.want)
			}
		})
	}
}
