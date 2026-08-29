package runtime

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

// This file implements dialects.PostgreSQL's TO_CHAR template language, for date/time
// values and for numbers.
//
// The date half cannot be expressed as a Go layout string, which is what it was
// written as before: a Go layout has one spelling per field, so MONTH, Month
// and month all become "January" and there is no way to ask for a fixed width,
// and a pattern with no Go equivalent has to be copied through as literal text
// -- which is how TO_CHAR(d, 'DDD') came to answer "05D", the DD substituted
// and the remaining D passed along. Formatting one pattern at a time removes
// both limits.

// The template patterns whose spelling is repeated between the scanner's table
// and the renderers that read it.
const (
	patYYYY  = "YYYY"
	patIYY   = "IYY"
	patMonth = "MONTH"
	patMon   = "MON"
	patDay   = "DAY"
	patHH24  = "HH24"
	patHH12  = "HH12"
	patYYY   = "YYY"
	patDDD   = "DDD"
	patEEEE  = "EEEE"
	patAD    = "A.D."
	patAMDot = "A.M."
	patIDDD  = "IDDD"
	patIYYY  = "IYYY"
	patBC    = "B.C."
	patPMDot = "P.M."
	patSSSS  = "SSSS"
	patSSSSS = "SSSSS"
	patFF    = "FF"
)

// pgTemplateItem is one element of a scanned template: either a pattern, whose
// spelling decides the case of a name it prints, or literal text.
type pgTemplateItem struct {
	// pattern is the canonical upper-case pattern, empty for literal text.
	pattern string
	// text is the pattern as the template spelled it, or the literal text.
	text string
	// fill is set when FM stood in front of this pattern. FM binds to the one
	// pattern that follows it rather than to the whole template, so
	// "FMDay, DD" is "Tuesday, 05" and not "Tuesday, 5".
	fill bool
}

// pgTemplatePatterns are the template patterns, longest first so the scanner
// prefers the longest match: DDD has to be tried before DD and before D, and
// IYYY before IY and I, or the tail of a pattern is copied out as literal text.
var pgTemplatePatterns = []string{ //nolint:gochecknoglobals // a fixed table read by the scanner
	"Y,YYY", patAMDot, patPMDot, patBC, patAD,
	patIDDD, patIYYY, patHH24, patHH12, patSSSSS, patSSSS, patEEEE,
	patMonth, patIYY, patDDD, "FF1", "FF2", "FF3", "FF4", "FF5", "FF6",
	patMon, patDay, "RM",
	patYYYY, patYYY, "YY",
	"HH", "MI", "SS", "MS", "US", "MM", "DD", "DY", "ID", "IW", "WW", "CC", "TH",
	patTZH, patTZM, "OF",
	"AM", "PM", "BC", "AD", "IY", "PR", "SG", "PL", "RN", "TZ",
	"Y", "I", "D", "W", "Q", "J", "V", "S", "L", "G", "C",
}

// scanPGTemplate splits a template into patterns and literal text, marking each
// pattern an FM stood in front of, and reports whether the template held an FM
// at all -- which is what the numeric half needs, since a numeric template
// formats one number and there is nothing for a per-pattern flag to
// distinguish. Text in double quotes is literal with the quotes removed, which
// is how a template asks for a letter that would otherwise be read as a
// pattern.
func scanPGTemplate(format string) (items []pgTemplateItem, fillMode bool) {
	if cached, ok := scannedTemplates.Load(format); ok {
		scan := cached.(scannedTemplate) //nolint:forcetypeassert,errcheck // the map holds only this type
		return scan.items, scan.fillMode
	}
	items, fillMode = scanPGTemplateUncached(format)
	// A template is nearly always a literal in the query, so the same string
	// arrives once per row; scanning it once is most of what TO_CHAR costs. The
	// cache is bounded because the format can be a column, and an unbounded map
	// keyed by data is a way to run out of memory on a large file.
	if scannedTemplateCount.Load() < maxScannedTemplates {
		if _, loaded := scannedTemplates.LoadOrStore(format, scannedTemplate{items: items, fillMode: fillMode}); !loaded {
			scannedTemplateCount.Add(1)
		}
	}
	return items, fillMode
}

// scannedTemplate is one cached scan. Its items are never written after they go
// into the map, so every reader shares the one slice.
type scannedTemplate struct {
	items    []pgTemplateItem
	fillMode bool
}

// maxScannedTemplates bounds the cache. A query holds a handful of distinct
// templates; a number this size is reached only by a format that comes from the
// data, and then the cache stops growing and the scan runs as it did before.
const maxScannedTemplates = 256

var (
	scannedTemplates     sync.Map     //nolint:gochecknoglobals // a process-wide cache of scanned templates
	scannedTemplateCount atomic.Int64 //nolint:gochecknoglobals // the bound on the cache above
)

func scanPGTemplateUncached(format string) (items []pgTemplateItem, fillMode bool) {
	pendingFill := false
	for i := 0; i < len(format); {
		if strings.HasPrefix(format[i:], "FM") || strings.HasPrefix(format[i:], "fm") {
			fillMode, pendingFill = true, true
			i += 2
			continue
		}
		// TM asks for the localized spelling of the pattern that follows and
		// suppresses its padding. This package has one locale, so the localized
		// name is the plain one and TM is the fill prefix by another name.
		// Copied through instead, its "T" landed in the result as literal text
		// and the scan resumed at "MMonth", where MM matched the month number:
		// to_char(ts, 'TMMonth') answered "T03onrd".
		if strings.HasPrefix(format[i:], "TM") || strings.HasPrefix(format[i:], "tm") {
			fillMode, pendingFill = true, true
			i += 2
			continue
		}
		if format[i] == '"' {
			j := i + 1
			var lit strings.Builder
			for j < len(format) && format[j] != '"' {
				if format[j] == '\\' && j+1 < len(format) {
					j++
				}
				lit.WriteByte(format[j])
				j++
			}
			items = append(items, pgTemplateItem{text: lit.String()})
			if j < len(format) {
				j++ // the closing quote
			}
			i = j
			continue
		}
		matched := false
		for _, pat := range pgTemplatePatterns {
			if len(format[i:]) < len(pat) || !strings.EqualFold(format[i:i+len(pat)], pat) {
				continue
			}
			items = append(items, pgTemplateItem{pattern: pat, text: format[i : i+len(pat)], fill: pendingFill})
			pendingFill = false
			i += len(pat)
			matched = true
			break
		}
		if !matched {
			items = append(items, pgTemplateItem{text: format[i : i+1]})
			i++
		}
	}
	return items, fillMode
}

// isDateTemplate reports whether a template describes a date or time. A
// numeric template is built from digit positions, which no date template holds,
// and RN is the one numeric template that has none: it asks for a roman
// numeral. Text in double quotes is literal and says nothing about either.
func isDateTemplate(format string) bool {
	quoted := false
	for i := range len(format) {
		switch {
		case format[i] == '"':
			quoted = !quoted
		case quoted:
		case format[i] == '9' || format[i] == '0':
			return false
		case strings.EqualFold(format[i:min(i+2, len(format))], "RN"):
			return false
		}
	}
	return true
}

// applyCase spells s the way the template spelled its pattern: an all-upper
// pattern asks for upper case, an all-lower one for lower, and anything else
// for the capitalized form. dialects.PostgreSQL reads the pattern's own case this way,
// which is why MONTH, Month and month are three different answers.
func applyCase(pattern, spelling, s string) string {
	if pattern == "" || s == "" {
		return s
	}
	upper, lower := 0, 0
	for _, r := range spelling {
		switch {
		case unicode.IsUpper(r):
			upper++
		case unicode.IsLower(r):
			lower++
		}
	}
	switch {
	case lower == 0:
		return strings.ToUpper(s)
	case upper == 0:
		return strings.ToLower(s)
	default:
		return strings.ToUpper(s[:1]) + strings.ToLower(s[1:])
	}
}

// padName pads a name to width with spaces, as dialects.PostgreSQL pads a month or a
// weekday name to the width of the longest one. FM suppresses the padding.
func padName(s string, width int, fillMode bool) string {
	if fillMode || len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

// padNumber writes n zero-padded to width, or without the padding under FM. A
// negative number keeps its sign in front of the zeros rather than behind them,
// which is how dialects.PostgreSQL prints the one field that can be negative: CC on a
// date before the common era is -01.
func padNumber(n, width int, fillMode bool) string {
	sign := ""
	if n < 0 {
		sign, n = "-", -n
	}
	s := strconv.Itoa(n)
	if fillMode {
		return sign + s
	}
	for len(s) < width {
		s = "0" + s
	}
	return sign + s
}

// romanNumerals spells 1..3999, which is all a month number or a small integer
// needs.
var romanNumerals = []struct { //nolint:gochecknoglobals // a fixed table read by the roman spelling
	value  int
	letter string
}{
	{1000, "M"}, {900, "CM"}, {500, "D"}, {400, "CD"}, {100, "C"}, {90, "XC"},
	{50, "L"}, {40, "XL"}, {10, "X"}, {9, "IX"}, {5, "V"}, {4, "IV"}, {1, "I"},
}

func toRoman(n int) string {
	if n <= 0 || n > 3999 {
		return ""
	}
	var b strings.Builder
	for _, r := range romanNumerals {
		for n >= r.value {
			b.WriteString(r.letter)
			n -= r.value
		}
	}
	return b.String()
}

// pgFormatTime renders a time against a dialects.PostgreSQL date/time template.
func pgFormatTime(format string, tm time.Time) string {
	items, _ := scanPGTemplate(format)
	var b strings.Builder
	last := 0 // the number the previous pattern printed, for TH
	for _, it := range items {
		if it.pattern == "" {
			b.WriteString(it.text)
			continue
		}
		out, n := pgTimePattern(it, tm, it.fill)
		if it.pattern == "TH" {
			b.WriteString(applyCase(it.pattern, it.text, ordinalSuffix(last)))
			continue
		}
		last = n
		b.WriteString(out)
	}
	return b.String()
}

// pgTimePattern renders one date/time pattern, returning the text and, when the
// pattern printed a number, that number, which a following TH turns into an
// ordinal.
//
//nolint:cyclop,funlen,gocyclo // one arm per template pattern; splitting it would only scatter the table
func pgTimePattern(it pgTemplateItem, tm time.Time, fillMode bool) (string, int) {
	calendarYear, month, day := tm.Date()
	rawISOYear, isoWeek := tm.ISOWeek()
	// The year patterns print the year of its era, which is what BC and AD
	// name: dialects.PostgreSQL prints TO_CHAR(DATE '0044-03-15 BC', 'YYYY') as 0044,
	// and the year Go counts is -43. CC and the era patterns read the year Go
	// counts, since one keeps the sign and the other reports it.
	year, isoYear := eraYear(calendarYear), eraYear(rawISOYear)
	switch it.pattern {
	case patHH24:
		return padNumber(tm.Hour(), 2, fillMode), tm.Hour()
	case patHH12, "HH":
		h := tm.Hour() % 12
		if h == 0 {
			h = 12
		}
		return padNumber(h, 2, fillMode), h
	case "MI":
		return padNumber(tm.Minute(), 2, fillMode), tm.Minute()
	case patSSSSS, patSSSS:
		secs := tm.Hour()*3600 + tm.Minute()*60 + tm.Second()
		return strconv.Itoa(secs), secs
	case "SS":
		return padNumber(tm.Second(), 2, fillMode), tm.Second()
	case "MS":
		return padNumber(tm.Nanosecond()/1e6, 3, false), tm.Nanosecond() / 1e6
	case "US":
		return padNumber(tm.Nanosecond()/1e3, 6, false), tm.Nanosecond() / 1e3
	case "FF1", "FF2", "FF3", "FF4", "FF5", "FF6":
		digits := int(it.pattern[2] - '0')
		frac := padNumber(tm.Nanosecond(), 9, false)
		return frac[:digits], 0
	case "AM", "PM", patAMDot, patPMDot:
		return pgMeridiem(it, tm), 0
	case "TZ", patTZH, patTZM, "OF":
		// Every value this package holds is read as UTC, which is what the
		// datetime helpers document, so the offset is zero and the zone has no
		// abbreviation to print. Copied through instead, the pattern's own
		// letters landed in the result where the offset belonged.
		return pgTimeZoneField(it.pattern), 0
	case "Y,YYY":
		return strconv.Itoa(year/1000) + "," + padNumber(year%1000, 3, false), year
	case patYYYY:
		return padNumber(year, 4, fillMode), year
	case patYYY:
		return padNumber(year%1000, 3, fillMode), year % 1000
	case "YY":
		return padNumber(year%100, 2, fillMode), year % 100
	case "Y":
		return strconv.Itoa(year % 10), year % 10
	case patIYYY:
		return padNumber(isoYear, 4, fillMode), isoYear
	case patIYY:
		return padNumber(isoYear%1000, 3, fillMode), isoYear % 1000
	case "IY":
		return padNumber(isoYear%100, 2, fillMode), isoYear % 100
	case "I":
		return strconv.Itoa(isoYear % 10), isoYear % 10
	case "BC", "AD", patBC, patAD:
		return pgEra(it, calendarYear), 0
	case patMonth:
		return applyCase(it.pattern, it.text, padName(month.String(), 9, fillMode)), int(month)
	case patMon:
		return applyCase(it.pattern, it.text, month.String()[:3]), int(month)
	case "MM":
		return padNumber(int(month), 2, fillMode), int(month)
	case "RM":
		return applyCase(it.pattern, it.text, padName(toRoman(int(month)), 4, fillMode)), int(month)
	case patDay:
		return applyCase(it.pattern, it.text, padName(tm.Weekday().String(), 9, fillMode)), 0
	case "DY":
		return applyCase(it.pattern, it.text, tm.Weekday().String()[:3]), 0
	case patDDD:
		return padNumber(tm.YearDay(), 3, fillMode), tm.YearDay()
	case patIDDD:
		n := (isoWeek-1)*7 + isoWeekday(tm)
		return padNumber(n, 3, fillMode), n
	case "DD":
		return padNumber(day, 2, fillMode), day
	case "D":
		return strconv.Itoa(int(tm.Weekday()) + 1), int(tm.Weekday()) + 1
	case "ID":
		return strconv.Itoa(isoWeekday(tm)), isoWeekday(tm)
	case "W":
		n := (day-1)/7 + 1
		return strconv.Itoa(n), n
	case "WW":
		n := (tm.YearDay()-1)/7 + 1
		return padNumber(n, 2, fillMode), n
	case "IW":
		return padNumber(isoWeek, 2, fillMode), isoWeek
	case "CC":
		return padNumber(centuryOf(calendarYear), 2, fillMode), centuryOf(calendarYear)
	case "Q":
		n := (int(month)-1)/3 + 1
		return strconv.Itoa(n), n
	case "J":
		n := julianDay(tm)
		return strconv.Itoa(n), n
	case "TH":
		return "", 0
	default:
		// A pattern with no date meaning -- a numeric one, or TZ, which needs a
		// zone dialects.SQLite does not carry -- prints as the template wrote it.
		return it.text, 0
	}
}

// pgTimeZoneField is the value of a timezone pattern for the UTC every value
// here is read as: no abbreviation, a zero offset, and no minutes.
func pgTimeZoneField(pattern string) string {
	switch pattern {
	case patTZH, "OF":
		return "+00"
	case patTZM:
		return "00"
	default:
		return ""
	}
}

// pgMeridiem prints AM or PM in the spelling the template used.
func pgMeridiem(it pgTemplateItem, tm time.Time) string {
	dotted := strings.Contains(it.pattern, ".")
	s := "AM"
	if tm.Hour() >= 12 {
		s = "PM"
	}
	if dotted {
		s = s[:1] + ".M."
	}
	return applyCase(it.pattern, it.text, s)
}

// pgEra prints AD or BC in the spelling the template used.
func pgEra(it pgTemplateItem, year int) string {
	dotted := strings.Contains(it.pattern, ".")
	s := "AD"
	if year <= 0 {
		s = "BC"
	}
	if dotted {
		s = s[:1] + "." + s[1:] + "."
	}
	return applyCase(it.pattern, it.text, s)
}

// eraYear turns the year Go counts, where 1 BC is year 0 and 2 BC is year -1,
// into the year its era names.
func eraYear(year int) int {
	if year <= 0 {
		return 1 - year
	}
	return year
}

// isoWeekday numbers Monday 1 through Sunday 7, which is what ID prints and
// what IDDD counts within an ISO week.
func isoWeekday(tm time.Time) int {
	d := int(tm.Weekday())
	if d == 0 {
		return 7
	}
	return d
}

// julianDay is the Julian day number J prints, counted from the same epoch
// dialects.PostgreSQL counts from.
func julianDay(tm time.Time) int {
	y, m, d := tm.Date()
	a := (14 - int(m)) / 12
	yy := y + 4800 - a
	mm := int(m) + 12*a - 3
	return d + (153*mm+2)/5 + 365*yy + yy/4 - yy/100 + yy/400 - 32045
}

// --- the numeric half of TO_CHAR ---

// pgNumericSign is the sign spelling a numeric template asked for.
type pgNumericSign int

const (
	// pgSignDefault reserves one column in front of the number, holding a
	// minus for a negative value and a space for a positive one. It is what a
	// template with no sign element gets.
	pgSignDefault pgNumericSign = iota
	pgSignS                     // S: a plus or a minus, in the position S was written
	pgSignMI                    // MI: a minus or a space
	pgSignPL                    // PL: a plus or a space, beside the default column
	pgSignSG                    // SG: a plus or a minus, in place of the default column
	pgSignPR                    // PR: a negative value in angle brackets
)

// pgNumericTemplate is a scanned numeric template: the digit positions and
// separators of the integer part, how many decimals were asked for, where the
// sign goes, and the decorations around them.
type pgNumericTemplate struct {
	// intCells holds one entry per element of the integer part: "9" or "0" for
	// a digit position and the separator character for a group separator.
	intCells      []string
	fracCells     int
	hasPoint      bool
	sign          pgNumericSign
	signAhead     bool
	shift         int
	roman         bool
	scientific    bool
	romanSpelling string
	ordinal       string
	prefix        string
	suffix        string
}

// intDigits counts the digit positions of the integer part.
func (t *pgNumericTemplate) intDigits() int {
	n := 0
	for _, c := range t.intCells {
		if c == "9" || c == "0" {
			n++
		}
	}
	return n
}

// zeroFrom is the index into the padded digit string from which zeros rather
// than spaces fill, or -1 when the template asked for no zero fill. A "0"
// position makes every position at or after it a zero.
func (t *pgNumericTemplate) zeroFrom() int {
	seen := 0
	for _, c := range t.intCells {
		switch c {
		case "0":
			return seen
		case "9":
			seen++
		}
	}
	return -1
}

// numericTemplateFor reads a numeric template, from the cache when it has been
// read before. A template is nearly always a literal in the query, so the same
// string arrives once per row and reading it once is most of what TO_CHAR
// costs; the parsed form is never written after it is stored.
func numericTemplateFor(format string) (*pgNumericTemplate, bool) {
	if cached, ok := numericTemplates.Load(format); ok {
		parsed := cached.(parsedNumericTemplate) //nolint:forcetypeassert,errcheck // the map holds only this type
		return parsed.template, parsed.fillMode
	}
	items, fillMode := scanPGTemplate(format)
	t := parseNumericTemplate(items)
	if numericTemplateCount.Load() < maxScannedTemplates {
		if _, loaded := numericTemplates.LoadOrStore(format, parsedNumericTemplate{template: t, fillMode: fillMode}); !loaded {
			numericTemplateCount.Add(1)
		}
	}
	return t, fillMode
}

// parsedNumericTemplate is one cached numeric template.
type parsedNumericTemplate struct {
	template *pgNumericTemplate
	fillMode bool
}

var (
	numericTemplates     sync.Map     //nolint:gochecknoglobals // a process-wide cache of parsed numeric templates
	numericTemplateCount atomic.Int64 //nolint:gochecknoglobals // the bound on the cache above
)

// parseNumericTemplate reads the scanned items of a numeric template.
//
// digitCount is the number of digit positions the template names. A template
// with none reads nothing.
//
//nolint:cyclop,gocyclo // one arm per template element
func (t *pgNumericTemplate) digitCount() int {
	digits := t.fracCells
	for _, cell := range t.intCells {
		if cell != "," {
			digits++
		}
	}
	return digits
}

func parseNumericTemplate(items []pgTemplateItem) *pgNumericTemplate {
	t := &pgNumericTemplate{sign: pgSignDefault}
	afterV := false
	for _, it := range items {
		switch {
		case it.pattern == "" && (it.text == "9" || it.text == "0"):
			if afterV {
				t.shift++
			}
			if t.hasPoint {
				t.fracCells++
				continue
			}
			t.intCells = append(t.intCells, it.text)
		case it.pattern == "" && (it.text == "." || it.text == ","):
			if it.text == "." {
				t.hasPoint = true
				continue
			}
			t.intCells = append(t.intCells, ",")
		case it.pattern == "D":
			t.hasPoint = true
		case it.pattern == "G":
			t.intCells = append(t.intCells, ",")
		case it.pattern == "S", it.pattern == "MI", it.pattern == "PL",
			it.pattern == "SG", it.pattern == "PR":
			t.sign = pgSignOf(it.pattern)
			t.signAhead = len(t.intCells) == 0 && t.fracCells == 0
		case it.pattern == "V":
			afterV = true
		case it.pattern == "RN":
			t.roman, t.romanSpelling = true, it.text
		case it.pattern == "TH":
			t.ordinal = it.text
		case it.pattern == patEEEE:
			// The whole template is scientific notation; the digit positions
			// before it set the mantissa. Falling through to the default copied
			// the four letters into the result, where a caller could not tell
			// them from data.
			t.scientific = true
		case it.pattern == "L", it.pattern == "C":
			// The currency symbol and the currency code of the C locale, both
			// of which are empty. C used to reach the default and put its own
			// letter in front of the number.
		default:
			if len(t.intCells) == 0 && t.fracCells == 0 {
				t.prefix += it.text
			} else {
				t.suffix += it.text
			}
		}
	}
	return t
}

func pgSignOf(pattern string) pgNumericSign {
	switch pattern {
	case "S":
		return pgSignS
	case "MI":
		return pgSignMI
	case "PL":
		return pgSignPL
	case "SG":
		return pgSignSG
	default:
		return pgSignPR
	}
}

// pgFormatScientific renders a value the way an EEEE template does: one digit
// before the point, fracCells after it, and an exponent of at least two digits
// with its sign. The sign position in front is a space for a value that is not
// negative, which is what every dialects.PostgreSQL numeric template does.
func pgFormatScientific(value float64, fracCells int) string {
	sign := " "
	if math.Signbit(value) {
		sign = "-"
		value = -value
	}
	exponent := 0
	if value != 0 && !math.IsInf(value, 0) && !math.IsNaN(value) {
		exponent = int(math.Floor(math.Log10(value)))
		value /= math.Pow(10, float64(exponent))
		// Rounding the mantissa can carry it to ten, which belongs to the next
		// exponent: 9.99 at two places is 10.0, and dialects.PostgreSQL prints 1.0e+01.
		if rounded := roundHalfAway(value * math.Pow(10, float64(fracCells))); rounded >= math.Pow(10, float64(fracCells+1)) {
			value /= 10
			exponent++
		}
	}
	expSign := "+"
	if exponent < 0 {
		expSign = "-"
		exponent = -exponent
	}
	return fmt.Sprintf("%s%.*fe%s%02d", sign, fracCells, value, expSign, exponent)
}

// pgFormatNumber renders a value against a dialects.PostgreSQL numeric template.
func pgFormatNumber(value float64, format string) string {
	t, fillMode := numericTemplateFor(format)
	if t.roman {
		return applyCase("RN", t.romanSpelling, padRoman(int(roundHalfAway(value)), fillMode))
	}
	if t.scientific {
		return pgFormatScientific(value, t.fracCells)
	}
	for range t.shift {
		value *= 10
	}
	negative := math.Signbit(value)
	whole, frac := splitRounded(math.Abs(value), t.fracCells)
	// A leading "9" position prints nothing for the zero in front of a
	// decimal point, which is what makes TO_CHAR(0.5, '9.9') answer "  .5"
	// where TO_CHAR(0.5, '0.9') answers " 0.5".
	if t.hasPoint && whole == "0" && t.zeroFrom() < 0 {
		whole = ""
	}
	// A template with no digit position has nowhere to print a number, so only
	// its literal text comes out -- no field to pad and no column to hold a
	// sign. TO_CHAR(2024, 'YYYY') is "YYYY" in dialects.PostgreSQL for that reason.
	if t.intDigits() == 0 && t.fracCells == 0 && !t.hasPoint {
		return t.prefix + t.suffix
	}
	body, overflowed := pgNumericBody(t, whole, frac, fillMode)
	out := t.prefix + pgApplySign(t, body, negative, fillMode) + t.suffix
	// TH prints nothing for a negative value or for one that did not fit its
	// template, which is what dialects.PostgreSQL does: there is no ordinal for either.
	if t.ordinal != "" && !negative && !overflowed {
		out += applyCase("TH", t.ordinal, ordinalSuffix(int(roundHalfAway(value))))
	}
	return out
}

// splitRounded rounds a non-negative value to decimals places and returns its
// integer and fractional digits. SQL rounds a half away from zero, where Go's
// own formatting rounds it to even, so TO_CHAR(0.5, '999') is 1 rather than 0.
func splitRounded(abs float64, decimals int) (whole, frac string) {
	scale := math.Pow(10, float64(decimals))
	rounded := roundHalfAway(abs*scale) / scale
	digits := strconv.FormatFloat(rounded, 'f', decimals, 64)
	whole, frac, _ = strings.Cut(digits, ".")
	return whole, frac
}

func roundHalfAway(x float64) float64 { return math.Round(x) }

// padRoman right-aligns a roman numeral in the fifteen columns dialects.PostgreSQL
// reserves for RN, which is the width of the longest numeral it prints.
func padRoman(n int, fillMode bool) string {
	s := toRoman(n)
	if s == "" {
		// RN has no spelling for zero or for a negative number, and none past
		// 3999; dialects.PostgreSQL fills the field the way it does any other value that
		// does not fit its template.
		return strings.Repeat("#", 15)
	}
	if fillMode {
		return s
	}
	return strings.Repeat(" ", max(15-len(s), 0)) + s
}

// pgNumericBody lays the digits out over the template's positions. An integer
// part wider than the positions it was given becomes a run of "#", which is how
// dialects.PostgreSQL says a value does not fit its template rather than printing a
// number wider than the caller asked for; the separators stay where they are,
// so the shape of the field is still visible.
func pgNumericBody(t *pgNumericTemplate, whole, frac string, fillMode bool) (string, bool) {
	width := t.intDigits()
	overflowed := len(whole) > width
	var padded string
	switch {
	case overflowed:
		padded = strings.Repeat("#", width)
	case len(whole) >= width:
		padded = whole
	default:
		// The pad is written left to right into one buffer rather than
		// prepended a character at a time, which reallocated the whole string
		// per column.
		zeroFrom := t.zeroFrom()
		buf := make([]byte, 0, width)
		for i := range width - len(whole) {
			fill := byte(' ')
			if zeroFrom >= 0 && i >= zeroFrom {
				fill = '0'
			}
			buf = append(buf, fill)
		}
		padded = string(append(buf, whole...))
	}

	// A separator standing to the left of the first digit that was really
	// printed is part of the padding, so it becomes a space: dialects.PostgreSQL keeps
	// the width of the field but does not group a number that is not there.
	var b strings.Builder
	pos, seen := 0, false
	for _, c := range t.intCells {
		if c == "9" || c == "0" {
			if pos < len(padded) {
				b.WriteByte(padded[pos])
				seen = seen || padded[pos] != ' '
			}
			pos++
			continue
		}
		if seen {
			b.WriteString(c)
			continue
		}
		b.WriteString(strings.Repeat(" ", len(c)))
	}
	out := b.String()
	if fillMode {
		out = strings.TrimLeft(out, " ,")
	}
	if !t.hasPoint {
		return out, overflowed
	}
	switch {
	case overflowed:
		return out + "." + strings.Repeat("#", t.fracCells), overflowed
	case fillMode:
		// FM drops the trailing zeros of the fraction and keeps the point, so
		// TO_CHAR(12, 'FM999.99') is "12." rather than "12.00". A value with
		// nothing on either side of the point keeps its zero, which is the one
		// case where the leading zero suppressed above comes back.
		frac = strings.TrimRight(frac, "0")
		if out == "" && frac == "" {
			out = "0"
		}
		return out + "." + frac, overflowed
	default:
		return out + "." + frac, overflowed
	}
}

// pgApplySign places the sign the template asked for. Which columns it takes is
// part of the spelling: the default sign and S stand against the digits and the
// field is padded in front of them, MI and SG take a column of their own beside
// the digits, PL takes one beside the default column, and PR wraps a negative
// value in angle brackets.
//
//nolint:cyclop // one arm per sign spelling
func pgApplySign(t *pgNumericTemplate, body string, negative, fillMode bool) string {
	width := len([]rune(body))
	attach := func(mark string) string {
		if fillMode {
			return mark + body
		}
		return padLeftSpaces(mark+strings.TrimLeft(body, " "), width+1)
	}
	beside := func(mark string) string {
		if t.signAhead {
			return mark + body
		}
		return body + mark
	}
	switch t.sign {
	case pgSignS:
		mark := signMark(negative, "-", "+")
		if t.signAhead {
			return attach(mark)
		}
		return body + mark
	case pgSignMI:
		return beside(signMark(negative, "-", pgSignFill(fillMode)))
	case pgSignPL:
		inner := pgApplySign(&pgNumericTemplate{sign: pgSignDefault}, body, negative, fillMode)
		mark := signMark(negative, pgSignFill(fillMode), "+")
		if t.signAhead {
			return mark + inner
		}
		return inner + mark
	case pgSignSG:
		return beside(signMark(negative, "-", "+"))
	case pgSignPR:
		trimmed := strings.TrimLeft(body, " ")
		if negative {
			if fillMode {
				return "<" + trimmed + ">"
			}
			return padLeftSpaces("<"+trimmed+">", width+2)
		}
		if fillMode {
			return trimmed
		}
		return padLeftSpaces(trimmed+" ", width+2)
	default:
		if fillMode {
			return signMark(negative, "-", "") + strings.TrimLeft(body, " ")
		}
		return attach(signMark(negative, "-", " "))
	}
}

// signMark picks the character a sign element prints.
func signMark(negative bool, whenNegative, whenPositive string) string {
	if negative {
		return whenNegative
	}
	return whenPositive
}

// pgSignFill is the character a sign element prints where there is no sign to
// show: a space, which holds the column, or nothing under FM.
func pgSignFill(fillMode bool) string {
	if fillMode {
		return ""
	}
	return " "
}

func padLeftSpaces(s string, width int) string {
	if n := width - len([]rune(s)); n > 0 {
		return strings.Repeat(" ", n) + s
	}
	return s
}

// pgDateFields collects what a template read out of a value. The fields stay
// separate until the end because they are not independent: a Julian day names a
// whole date on its own, an ISO year needs its week and weekday, and a
// twelve-hour clock needs its meridiem.
type pgDateFields struct {
	year, month, day             int
	isoYear, isoWeek, isoDay     int
	isoDayOfYear                 int
	dayOfYear, julian            int
	hour, hour12, minute, second int
	secondOfDay                  int
	nanosecond                   int
	offsetHours, offsetMinutes   int
	haveOffset                   bool
	pm, hasPM                    bool
	bc                           bool
	haveYear, haveISOYear        bool
	haveJulian, haveDayOfYear    bool
	haveISODayOfYear             bool
	haveTime                     bool
}

// pgTemplateError is what a template reader answers for a value it cannot read.
// PostgreSQL raises there rather than answering NULL, and NULL is also its
// answer for a row whose value is simply absent, so the two were
// indistinguishable: a query written to reject a bad date reported success on
// exactly the rows it was written to reject.
func pgTemplateError(pattern, value string) error {
	return fmt.Errorf("dialect: invalid value %q for %q", value, pattern)
}

// pgReadTemplate reads a value against a to_char template and answers the
// instant it names.
//
// The reader walks the template and the value together rather than translating
// the template into a Go layout, because a Go layout cannot spell a day of the
// year beside a year, an ISO week date, a Julian day or a number written
// without its padding -- and PostgreSQL reads all four. Translating dropped
// them and the value came back NULL, which is also the answer for data that
// does not match, so a caller could not tell an unimplemented pattern from a
// bad row.
func pgReadTemplate(format, value string) (time.Time, error) {
	items, _ := scanPGTemplate(format)
	var (
		fields pgDateFields
		at     int
	)
	for _, it := range items {
		if it.pattern == "" {
			at = pgSkipLiteral(value, at, it.text)
			continue
		}
		next, err := pgReadPattern(&fields, it.pattern, value, at)
		if err != nil {
			return time.Time{}, err
		}
		at = next
	}
	return pgBuildTime(&fields)
}

// pgSkipLiteral steps over the literal text between two patterns. PostgreSQL
// does not insist that the separators match -- to_date('2024/03/05',
// 'YYYY-MM-DD') is the fifth of March there -- so a separator in the template
// stands for one in the value whatever the two are. Text the template quoted is
// there to be found, though, so an exact match is taken first: that is how
// 'IYYY "W"IW' reads the W in 2024 W10 without spending the I of a following
// pattern on it.
func pgSkipLiteral(value string, at int, text string) int {
	for at < len(value) && pgIsSpace(value[at]) {
		at++
	}
	if text != "" && len(value)-at >= len(text) && strings.EqualFold(value[at:at+len(text)], text) {
		return at + len(text)
	}
	for i := range len(text) {
		if pgIsSpace(text[i]) {
			// A blank in the template stands for the blanks in the value,
			// which were already stepped over. Letting it stand for any
			// separator ate the sign of a "-05" zone offset behind it.
			for at < len(value) && pgIsSpace(value[at]) {
				at++
			}
			continue
		}
		if at < len(value) && !pgIsDigit(value[at]) && !pgIsLetter(value[at]) {
			at++
		}
	}
	return at
}

// pgIsSpace reports whether a byte is one of the blanks a template steps over.
func pgIsSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

// pgIsDigit reports whether a byte is an ASCII digit.
func pgIsDigit(c byte) bool { return c >= '0' && c <= '9' }

// pgReadNumber reads up to width digits, which is how PostgreSQL reads a number
// written without its padding: it stops at the separator rather than insisting
// on the full width.
func pgReadNumber(value string, at, width int) (n, next int, ok bool) {
	for at < len(value) && pgIsSpace(value[at]) {
		at++
	}
	negative := false
	if at < len(value) && (value[at] == '-' || value[at] == '+') {
		negative, at = value[at] == '-', at+1
	}
	start := at
	for at < len(value) && at-start < width && pgIsDigit(value[at]) {
		n = n*10 + int(value[at]-'0')
		at++
	}
	if at == start {
		return 0, at, false
	}
	if negative {
		n = -n
	}
	return n, at, true
}

// pgReadWord reads the run of letters a name pattern stands for.
func pgReadWord(value string, at int) (word string, next int) {
	for at < len(value) && pgIsSpace(value[at]) {
		at++
	}
	start := at
	for at < len(value) && pgIsLetter(value[at]) {
		at++
	}
	return value[start:at], at
}

// pgIsLetter reports whether a byte is an ASCII letter.
func pgIsLetter(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }

// pgReadPattern reads one template pattern out of the value.
func pgReadPattern(f *pgDateFields, pattern, value string, at int) (int, error) {
	if width, ok := pgNumericPatternWidth(pattern); ok {
		n, next, ok := pgReadNumber(value, at, width)
		if !ok {
			return 0, pgTemplateError(pattern, pgRemainder(value, at))
		}
		pgAssignNumber(f, pattern, n)
		return next, nil
	}
	return pgReadNamePattern(f, pattern, value, at)
}

// pgRemainder is what is left of the value at a position, for an error message.
func pgRemainder(value string, at int) string {
	if at >= len(value) {
		return ""
	}
	return value[at:]
}

// pgNumericPatternWidth is the number of digits a pattern reads at most, for
// the patterns that read digits.
func pgNumericPatternWidth(pattern string) (int, bool) {
	switch pattern {
	case "Y", "I", "D", "W", "Q", "ID":
		return 1, true
	case "YY", "IY", "MM", "DD", "HH", patHH12, patHH24, "MI", "SS", "WW", "CC", "IW":
		return 2, true
	case patYYY, patIYY, patDDD, patIDDD, "MS":
		return 3, true
	case patYYYY, patIYYY:
		return 4, true
	case patSSSS, patSSSSS:
		return 5, true
	case "US":
		return 6, true
	case "J":
		return 7, true
	}
	if len(pattern) == 3 && strings.HasPrefix(pattern, patFF) && pgIsDigit(pattern[2]) {
		return int(pattern[2] - '0'), true
	}
	return 0, false
}

// pgAssignNumber files a number the template read under the field it names.
func pgAssignNumber(f *pgDateFields, pattern string, n int) {
	switch pattern {
	case patYYYY, patYYY, "YY", "Y":
		f.year, f.haveYear = pgExpandYear(pattern, n), true
	case patIYYY, patIYY, "IY", "I":
		f.isoYear, f.haveISOYear = pgExpandYear(pattern, n), true
	case "MM":
		f.month = n
	case "DD":
		f.day = n
	case patDDD:
		f.dayOfYear, f.haveDayOfYear = n, true
	case patIDDD:
		// The day of the ISO year, counted from the Monday its first week
		// begins on rather than from its first of January.
		f.isoDayOfYear, f.haveISODayOfYear = n, true
	case "IW":
		f.isoWeek = n
	case "ID":
		f.isoDay = n
	case "J":
		f.julian, f.haveJulian = n, true
	case patHH24:
		f.hour, f.haveTime = n, true
	case patHH12, "HH":
		f.hour12, f.haveTime = n, true
	case "MI":
		f.minute, f.haveTime = n, true
	case "SS":
		f.second, f.haveTime = n, true
	case patSSSS, patSSSSS:
		f.secondOfDay, f.haveTime = n, true
	case "MS":
		f.nanosecond, f.haveTime = n*1e6, true
	case "US":
		f.nanosecond, f.haveTime = n*1e3, true
	default:
		if len(pattern) == 3 && strings.HasPrefix(pattern, patFF) && pgIsDigit(pattern[2]) {
			scale := 1
			for range 9 - int(pattern[2]-'0') {
				scale *= 10
			}
			f.nanosecond, f.haveTime = n*scale, true
		}
		// The remaining numeric patterns -- the century and the week of the
		// month -- say nothing a date is built from.
	}
}

// pgExpandYear fills in the digits a short year leaves out. PostgreSQL reads
// two digits below seventy as the two thousands and the rest as the nineteen
// hundreds, and fills a one- or three-digit year out from two thousand.
func pgExpandYear(pattern string, n int) int {
	// The two families spell the same widths, so the pattern's own length is
	// the number of digits: Y and I are one, YYYY and IYYY are four.
	switch len(pattern) {
	case 1, 3:
		return 2000 + n
	case 2:
		if n < 70 {
			return 2000 + n
		}
		return 1900 + n
	default:
		return n
	}
}

// pgReadNamePattern reads the patterns written as words.
func pgReadNamePattern(f *pgDateFields, pattern, value string, at int) (int, error) {
	switch pattern {
	case patMonth, patMon:
		word, next := pgReadWord(value, at)
		month, ok := pgMonthNamed(word)
		if !ok {
			return 0, pgTemplateError(pattern, word)
		}
		f.month = month
		return next, nil
	case patDay, "DY":
		// The weekday name says nothing the date is built from, and
		// PostgreSQL ignores it too.
		_, next := pgReadWord(value, at)
		return next, nil
	case "RM":
		word, next := pgReadWord(value, at)
		month, ok := pgRomanMonth(word)
		if !ok {
			return 0, pgTemplateError(pattern, word)
		}
		f.month = month
		return next, nil
	case "AM", "PM", patAMDot, patPMDot:
		return pgReadMeridiem(f, pattern, value, at)
	case "BC", "AD", patBC, patAD:
		word, next := pgReadWord(value, at)
		f.bc = strings.EqualFold(strings.ReplaceAll(word, ".", ""), "BC")
		return next, nil
	case "TH", "PR", "SG", "PL", "RN", "V", "S", "L", "G", "C", patEEEE:
		// The patterns that decorate a number rather than name a field.
		// PostgreSQL steps over them on input and so does this.
		_, next := pgReadWord(value, at)
		return next, nil
	case "TZ":
		// A zone name, which PostgreSQL reads and ignores: the value it
		// answers is the one the clock fields spell.
		_, next := pgReadWord(value, at)
		return next, nil
	case patTZH, patTZM, "OF":
		return pgReadZoneOffset(f, pattern, value, at)
	}
	return 0, fmt.Errorf("dialect: the template pattern %q is not supported", pattern)
}

// pgReadZoneOffset reads the offset from UTC a template names, which
// PostgreSQL applies to the fields it read: 13:45 at +05 is 08:45 UTC.
func pgReadZoneOffset(f *pgDateFields, pattern, value string, at int) (int, error) {
	if pattern == patTZM {
		minutes, next, ok := pgReadNumber(value, at, 2)
		if !ok {
			return 0, pgTemplateError(pattern, pgRemainder(value, at))
		}
		f.offsetMinutes, f.haveOffset = minutes, true
		return next, nil
	}
	hours, next, ok := pgReadNumber(value, at, 3)
	if !ok {
		return 0, pgTemplateError(pattern, pgRemainder(value, at))
	}
	f.offsetHours, f.haveOffset = hours, true
	if pattern != "OF" || next >= len(value) || value[next] != ':' {
		return next, nil
	}
	// OF writes the minutes after a colon when the offset has them.
	minutes, after, ok := pgReadNumber(value, next+1, 2)
	if !ok {
		return next, nil
	}
	f.offsetMinutes = minutes
	return after, nil
}

// pgReadMeridiem reads AM or PM, in either of the two spellings.
func pgReadMeridiem(f *pgDateFields, pattern, value string, at int) (int, error) {
	for at < len(value) && pgIsSpace(value[at]) {
		at++
	}
	width := 2
	if strings.Contains(pattern, ".") {
		width = 4
	}
	if at+width > len(value) {
		return 0, pgTemplateError(pattern, pgRemainder(value, at))
	}
	word := strings.ToUpper(strings.ReplaceAll(value[at:at+width], ".", ""))
	switch word {
	case "AM":
		f.pm, f.hasPM = false, true
	case "PM":
		f.pm, f.hasPM = true, true
	default:
		return 0, pgTemplateError(pattern, value[at:at+width])
	}
	return at + width, nil
}

// pgMonthNamed reads a month written out or abbreviated.
func pgMonthNamed(word string) (int, bool) {
	if word == "" {
		return 0, false
	}
	for m := time.January; m <= time.December; m++ {
		name := m.String()
		if strings.EqualFold(word, name) || strings.EqualFold(word, name[:3]) {
			return int(m), true
		}
	}
	return 0, false
}

// pgRomanMonth reads a month written in Roman numerals, which is what RM writes.
func pgRomanMonth(word string) (int, bool) {
	numerals := []string{"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII"}
	for i, numeral := range numerals {
		if strings.EqualFold(word, numeral) {
			return i + 1, true
		}
	}
	return 0, false
}

// pgBuildTime turns the fields a template read into the instant they name,
// refusing a date that does not exist. PostgreSQL raises for the thirtieth of
// February and for a thirteenth month, and answering the first of March for the
// former is the silent wrong answer a validating query is written to catch.
func pgBuildTime(f *pgDateFields) (time.Time, error) {
	hour, err := pgClockHour(f)
	if err != nil {
		return time.Time{}, err
	}
	minute, second := f.minute, f.second
	if f.secondOfDay > 0 {
		hour, minute, second = f.secondOfDay/3600, f.secondOfDay/60%60, f.secondOfDay%60
	}
	if minute > 59 || second > 59 {
		return time.Time{}, fmt.Errorf("dialect: date/time field value out of range: %02d:%02d:%02d", hour, minute, second)
	}
	clock := time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute +
		time.Duration(second)*time.Second + time.Duration(f.nanosecond)*time.Nanosecond
	day, err := pgCalendarDay(f)
	if err != nil {
		return time.Time{}, err
	}
	built := day.Add(clock)
	if f.haveOffset {
		// The fields were written at that offset, so the instant is that many
		// hours earlier in UTC, which is the zone this package reads every
		// value in.
		minutes := f.offsetMinutes
		if f.offsetHours < 0 {
			minutes = -minutes
		}
		built = built.Add(-(time.Duration(f.offsetHours)*time.Hour + time.Duration(minutes)*time.Minute))
	}
	return built, nil
}

// pgClockHour resolves the two clocks a template can name into one.
func pgClockHour(f *pgDateFields) (int, error) {
	hour := f.hour
	if f.hour12 != 0 || f.hasPM {
		if f.hour12 < 1 || f.hour12 > 12 {
			return 0, fmt.Errorf("dialect: hour %d is out of range for a twelve-hour clock", f.hour12)
		}
		hour = f.hour12 % 12
		if f.pm {
			hour += 12
		}
	}
	if hour > 23 {
		return 0, fmt.Errorf("dialect: date/time field value out of range: hour %d", hour)
	}
	return hour, nil
}

// pgCalendarDay is the day the fields name, by whichever of the four ways the
// template spelled it.
func pgCalendarDay(f *pgDateFields) (time.Time, error) {
	switch {
	case f.haveJulian:
		// Day zero of the Julian period, from which PostgreSQL counts.
		return time.Date(-4713, time.November, 24, 0, 0, 0, 0, time.UTC).
			AddDate(0, 0, f.julian), nil
	case f.haveISOYear && f.haveISODayOfYear:
		if f.isoDayOfYear < 1 || f.isoDayOfYear > 371 {
			return time.Time{}, fmt.Errorf("dialect: date/time field value out of range: ISO day %d",
				f.isoDayOfYear)
		}
		return pgISOWeekDate(f.isoYear, 1, 1).AddDate(0, 0, f.isoDayOfYear-1), nil
	case f.haveISOYear:
		if f.isoWeek < 1 || f.isoWeek > 53 {
			return time.Time{}, fmt.Errorf("dialect: date/time field value out of range: ISO week %d", f.isoWeek)
		}
		isoDay := f.isoDay
		if isoDay == 0 {
			isoDay = 1
		}
		if isoDay > 7 {
			return time.Time{}, fmt.Errorf("dialect: date/time field value out of range: ISO day %d", isoDay)
		}
		return pgISOWeekDate(f.isoYear, f.isoWeek, isoDay), nil
	case f.haveDayOfYear:
		year := f.year
		if !f.haveYear {
			year = 1
		}
		if f.dayOfYear < 1 || f.dayOfYear > pgDaysInYear(year) {
			return time.Time{}, fmt.Errorf("dialect: date/time field value out of range: day %d of %d",
				f.dayOfYear, year)
		}
		return time.Date(pgSignedYear(f, year), time.January, f.dayOfYear, 0, 0, 0, 0, time.UTC), nil
	default:
		return pgCalendarDate(f)
	}
}

// pgCalendarDate is the day a year, a month and a day of the month name.
func pgCalendarDate(f *pgDateFields) (time.Time, error) {
	year, month, day := f.year, f.month, f.day
	if !f.haveYear {
		year = 1
	}
	if month == 0 {
		month = 1
	}
	if day == 0 {
		day = 1
	}
	if month > 12 {
		return time.Time{}, fmt.Errorf("dialect: date/time field value out of range: month %d", month)
	}
	built := time.Date(pgSignedYear(f, year), time.Month(month), day, 0, 0, 0, 0, time.UTC)
	if int(built.Month()) != month || built.Day() != day {
		return time.Time{}, fmt.Errorf("dialect: date/time field value out of range: %04d-%02d-%02d",
			year, month, day)
	}
	return built, nil
}

// pgSignedYear turns a year read with BC into the negative year Go counts in.
func pgSignedYear(f *pgDateFields, year int) int {
	if f.bc {
		return -year + 1
	}
	return year
}

// pgDaysInYear is 365, or 366 when the year is a leap year.
func pgDaysInYear(year int) int {
	if time.Date(year, time.December, 31, 0, 0, 0, 0, time.UTC).YearDay() == 366 {
		return 366
	}
	return 365
}

// pgISOWeekDate is the day an ISO year, week and weekday name. The fourth of
// January is in the first ISO week of every year, which is what fixes the
// week's Monday.
func pgISOWeekDate(isoYear, isoWeek, isoDay int) time.Time {
	fourth := time.Date(isoYear, time.January, 4, 0, 0, 0, 0, time.UTC)
	weekday := int(fourth.Weekday())
	if weekday == 0 {
		weekday = 7
	}
	monday := fourth.AddDate(0, 0, 1-weekday)
	return monday.AddDate(0, 0, (isoWeek-1)*7+isoDay-1)
}
