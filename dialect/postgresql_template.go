package dialect

import (
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// This file implements PostgreSQL's TO_CHAR template language, for date/time
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
)

// pgTemplateItem is one element of a scanned template: either a pattern, whose
// spelling decides the case of a name it prints, or literal text.
type pgTemplateItem struct {
	// pattern is the canonical upper-case pattern, empty for literal text.
	pattern string
	// text is the pattern as the template spelled it, or the literal text.
	text string
}

// pgTemplatePatterns are the template patterns, longest first so the scanner
// prefers the longest match: DDD has to be tried before DD and before D, and
// IYYY before IY and I, or the tail of a pattern is copied out as literal text.
var pgTemplatePatterns = []string{ //nolint:gochecknoglobals // a fixed table read by the scanner
	"Y,YYY", "A.M.", "P.M.", "B.C.", "A.D.",
	"IDDD", "IYYY", patHH24, patHH12, "SSSSS", "SSSS", "EEEE",
	patMonth, patIYY, "DDD", "FF1", "FF2", "FF3", "FF4", "FF5", "FF6",
	patMon, patDay, "RM",
	patYYYY, "YYY", "YY",
	"HH", "MI", "SS", "MS", "US", "MM", "DD", "DY", "ID", "IW", "WW", "CC", "TH",
	"AM", "PM", "BC", "AD", "IY", "PR", "SG", "PL", "RN", "TZ",
	"Y", "I", "D", "W", "Q", "J", "V", "S", "L", "G",
}

// scanPGTemplate splits a template into patterns and literal text, and reports
// whether it carried the FM prefix. Text in double quotes is literal with the
// quotes removed, which is how a template asks for a letter that would
// otherwise be read as a pattern.
func scanPGTemplate(format string) (items []pgTemplateItem, fillMode bool) {
	for i := 0; i < len(format); {
		if strings.HasPrefix(format[i:], "FM") || strings.HasPrefix(format[i:], "fm") {
			fillMode = true
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
			items = append(items, pgTemplateItem{pattern: pat, text: format[i : i+len(pat)]})
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
// for the capitalized form. PostgreSQL reads the pattern's own case this way,
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

// padName pads a name to width with spaces, as PostgreSQL pads a month or a
// weekday name to the width of the longest one. FM suppresses the padding.
func padName(s string, width int, fillMode bool) string {
	if fillMode || len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

// padNumber writes n zero-padded to width, or without the padding under FM.
func padNumber(n, width int, fillMode bool) string {
	s := strconv.Itoa(n)
	if n < 0 {
		s = "-" + strconv.Itoa(-n)
	}
	if fillMode {
		return s
	}
	for len(s) < width {
		s = "0" + s
	}
	return s
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

// pgFormatTime renders a time against a PostgreSQL date/time template.
func pgFormatTime(format string, tm time.Time) string {
	items, fillMode := scanPGTemplate(format)
	var b strings.Builder
	last := 0 // the number the previous pattern printed, for TH
	for _, it := range items {
		if it.pattern == "" {
			b.WriteString(it.text)
			continue
		}
		out, n := pgTimePattern(it, tm, fillMode)
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
	year, month, day := tm.Date()
	isoYear, isoWeek := tm.ISOWeek()
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
	case "SSSSS", "SSSS":
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
	case "AM", "PM", "A.M.", "P.M.":
		return pgMeridiem(it, tm), 0
	case "Y,YYY":
		return strconv.Itoa(year/1000) + "," + padNumber(year%1000, 3, false), year
	case patYYYY:
		return padNumber(year, 4, fillMode), year
	case "YYY":
		return padNumber(year%1000, 3, fillMode), year % 1000
	case "YY":
		return padNumber(year%100, 2, fillMode), year % 100
	case "Y":
		return strconv.Itoa(year % 10), year % 10
	case "IYYY":
		return padNumber(isoYear, 4, fillMode), isoYear
	case patIYY:
		return padNumber(isoYear%1000, 3, fillMode), isoYear % 1000
	case "IY":
		return padNumber(isoYear%100, 2, fillMode), isoYear % 100
	case "I":
		return strconv.Itoa(isoYear % 10), isoYear % 10
	case "BC", "AD", "B.C.", "A.D.":
		return pgEra(it, year), 0
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
	case "DDD":
		return padNumber(tm.YearDay(), 3, fillMode), tm.YearDay()
	case "IDDD":
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
		return padNumber(centuryOf(year), 2, fillMode), centuryOf(year)
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
		// zone SQLite does not carry -- prints as the template wrote it.
		return it.text, 0
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
// PostgreSQL counts from.
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

// parseNumericTemplate reads the scanned items of a numeric template.
//
//nolint:cyclop,gocyclo // one arm per template element
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
		case it.pattern == "L":
			// The currency symbol of the C locale, which is empty.
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

// pgFormatNumber renders a value against a PostgreSQL numeric template.
func pgFormatNumber(value float64, format string) string {
	items, fillMode := scanPGTemplate(format)
	t := parseNumericTemplate(items)
	if t.roman {
		return applyCase("RN", t.romanSpelling, padRoman(int(roundHalfAway(value)), fillMode))
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
	body, overflowed := pgNumericBody(t, whole, frac, fillMode)
	out := t.prefix + pgApplySign(t, body, negative, fillMode) + t.suffix
	// TH prints nothing for a negative value or for one that did not fit its
	// template, which is what PostgreSQL does: there is no ordinal for either.
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

// padRoman right-aligns a roman numeral in the fifteen columns PostgreSQL
// reserves for RN, which is the width of the longest numeral it prints.
func padRoman(n int, fillMode bool) string {
	s := toRoman(n)
	if s == "" {
		// RN has no spelling for zero or for a negative number, and none past
		// 3999; PostgreSQL fills the field the way it does any other value that
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
// PostgreSQL says a value does not fit its template rather than printing a
// number wider than the caller asked for; the separators stay where they are,
// so the shape of the field is still visible.
func pgNumericBody(t *pgNumericTemplate, whole, frac string, fillMode bool) (string, bool) {
	width := t.intDigits()
	overflowed := len(whole) > width
	padded := whole
	if overflowed {
		padded = strings.Repeat("#", width)
	} else {
		zeroFrom := t.zeroFrom()
		for len(padded) < width {
			fill := " "
			if zeroFrom >= 0 && width-len(padded)-1 >= zeroFrom {
				fill = "0"
			}
			padded = fill + padded
		}
	}

	// A separator standing to the left of the first digit that was really
	// printed is part of the padding, so it becomes a space: PostgreSQL keeps
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

// pgParseLayout turns a template into a Go layout, for TO_DATE and
// TO_TIMESTAMP, which read a value rather than write one. Only the patterns Go
// has a layout fragment for can be parsed; the rest are matched as the literal
// text they are, which is what a template holding one asks for anyway.
func pgParseLayout(format string) string {
	items, _ := scanPGTemplate(format)
	var b strings.Builder
	for _, it := range items {
		if it.pattern == "" {
			b.WriteString(it.text)
			continue
		}
		if fragment, ok := pgParseFragment(it); ok {
			b.WriteString(fragment)
			continue
		}
		b.WriteString(it.text)
	}
	return b.String()
}

// pgParseFragment is the Go layout fragment that reads one template pattern.
func pgParseFragment(it pgTemplateItem) (string, bool) {
	switch it.pattern {
	case patYYYY:
		return "2006", true
	case "YY":
		return "06", true
	case patMonth:
		return layoutMonthLong, true
	case patMon:
		return layoutMonthShort, true
	case "MM":
		return "01", true
	case patDay:
		return layoutWeekdayLong, true
	case "DY":
		return layoutWeekdayShort, true
	case "DD":
		return "02", true
	case patHH24:
		return "15", true
	case patHH12, "HH":
		return "03", true
	case "MI":
		return "04", true
	case "SS":
		return "05", true
	case "AM", "PM":
		if strings.ToUpper(it.text) == it.text {
			return "PM", true
		}
		return "pm", true
	default:
		return "", false
	}
}
