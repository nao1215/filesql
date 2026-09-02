package runtime

import (
	"crypto/md5" //nolint:gosec // MD5 backs dialects.PostgreSQL's MD5() function, not a security control
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// This file holds the dialects.PostgreSQL scalar functions that grew up in
// functions.go, beside the shared registry, while that file passed four
// thousand lines.
//
// Which of the two PostgreSQL files a function is in says nothing about it:
// both hold functions this package registers, some named by the shared registry
// in functions.go and some by postgresqlScalarFunctions, and some are helpers
// those are built from. registerAll is the index of what is registered; these
// files are a place to read the code.

// fnToChar implements dialects.PostgreSQL TO_CHAR(value, format) for date/time values and
// for numbers. The two are told apart by the template: a numeric one is built
// from digit positions, which no date template contains.
func fnToChar(args []driver.Value) (driver.Value, error) {
	format, ok := toString(args[1])
	if !ok {
		return nil, nil
	}
	// The argument decides which template language this is, because it is the
	// only thing that carries the answer: a digit in a date template is
	// literal text and a letter in a numeric one is too, so dialects.PostgreSQL reads
	// the argument's type and never the template. A number is a number; text
	// that parses as a date is a date. Text that is neither has no type to
	// read, and there the template is the only signal left.
	switch value := args[0].(type) {
	case int64:
		return pgFormatNumber(float64(value), format), nil
	case float64:
		return pgFormatNumber(value, format), nil
	}
	if tm, ok := toStringTime(args[0]); ok {
		return pgFormatTime(format, tm), nil
	}
	if !isDateTemplate(format) {
		value, ok := toFloat(args[0])
		if !ok {
			return nil, nil
		}
		return pgFormatNumber(value, format), nil
	}
	return nil, nil
}

// groupThousands inserts a comma every three digits from the right.
func groupThousands(digits string) string {
	var b strings.Builder
	for i, r := range digits {
		if i > 0 && (len(digits)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// fnToDate implements dialects.PostgreSQL TO_DATE(str, format).
func fnToDate(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	format, ok2 := toString(args[1])
	if !ok || !ok2 {
		return nil, nil
	}
	tm, err := pgReadTemplate(format, s)
	if err != nil {
		return nil, err
	}
	return tm.Format(layoutDateOnly), nil
}

// fnDateTrunc implements dialects.PostgreSQL DATE_TRUNC(unit, timestamp).
func fnDateTrunc(args []driver.Value) (driver.Value, error) {
	unit, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	tm, ok := toStringTime(args[1])
	if !ok {
		return nil, nil
	}
	y, mo, d := tm.Date()
	loc := tm.Location()
	switch strings.ToLower(strings.TrimSpace(unit)) {
	case unitYear:
		return time.Date(y, 1, 1, 0, 0, 0, 0, loc).Format(layoutDateTime), nil
	case unitQuarter:
		q := (int(mo)-1)/3*3 + 1
		return time.Date(y, time.Month(q), 1, 0, 0, 0, 0, loc).Format(layoutDateTime), nil
	case unitMonth:
		return time.Date(y, mo, 1, 0, 0, 0, 0, loc).Format(layoutDateTime), nil
	case unitWeek:
		offset := (int(tm.Weekday()) + 6) % 7 // days since Monday
		monday := time.Date(y, mo, d, 0, 0, 0, 0, loc).AddDate(0, 0, -offset)
		return monday.Format(layoutDateTime), nil
	case unitDay:
		return time.Date(y, mo, d, 0, 0, 0, 0, loc).Format(layoutDateTime), nil
	case unitHour:
		return time.Date(y, mo, d, tm.Hour(), 0, 0, 0, loc).Format(layoutDateTime), nil
	case unitMinute:
		return time.Date(y, mo, d, tm.Hour(), tm.Minute(), 0, 0, loc).Format(layoutDateTime), nil
	case unitSecond:
		return time.Date(y, mo, d, tm.Hour(), tm.Minute(), tm.Second(), 0, loc).Format(layoutDateTime), nil
	case unitDecade:
		return time.Date(decadeOf(y)*10, 1, 1, 0, 0, 0, 0, loc).Format(layoutDateTime), nil
	case unitCentury:
		// A century starts in its first year, which is the year ending in 01:
		// truncating 2024 to a century gives 2001 rather than 2000.
		return time.Date((centuryOf(y)-1)*100+1, 1, 1, 0, 0, 0, 0, loc).Format(layoutDateTime), nil
	case unitMillennium:
		return time.Date((millenniumOf(y)-1)*1000+1, 1, 1, 0, 0, 0, 0, loc).Format(layoutDateTime), nil
	case unitMillisecondsPlural, unitMillisecond:
		return truncatedFraction(tm, time.Millisecond), nil
	case unitMicrosecondsPlural, unitMicrosecond:
		return truncatedFraction(tm, time.Microsecond), nil
	case unitISOYear:
		// The ISO year begins on the Monday of the week holding its first
		// Thursday, which is not January 1 in most years.
		isoYear, _ := tm.ISOWeek()
		jan4 := time.Date(isoYear, time.January, 4, 0, 0, 0, 0, loc)
		offset := (int(jan4.Weekday()) + 6) % 7
		return jan4.AddDate(0, 0, -offset).Format(layoutDateTime), nil
	default:
		return nil, fmt.Errorf("dialect: unsupported DATE_TRUNC unit %q", unit)
	}
}

// truncatedFraction rounds a time down to a multiple of unit and spells it with
// however much of the fraction survives, trailing zeros removed, which is how
// dialects.PostgreSQL prints a timestamp: DATE_TRUNC('millisecond', '10:11:12.123456')
// is 10:11:12.123 and a whole second keeps no decimal point at all.
func truncatedFraction(tm time.Time, unit time.Duration) string {
	out := tm.Truncate(unit)
	if out.Nanosecond() == 0 {
		return out.Format(layoutDateTime)
	}
	return strings.TrimRight(out.Format(layoutDateTime+".000000000"), "0")
}

// fnSplitPart implements dialects.PostgreSQL SPLIT_PART(string, delimiter, n) with a
// 1-based field index.
func fnSplitPart(args []driver.Value) (driver.Value, error) {
	s, ok1 := toString(args[0])
	delim, ok2 := toString(args[1])
	n, ok3 := toInt(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	// dialects.PostgreSQL refuses a zero field position rather than answering with an
	// empty string, which is what makes an off-by-one in a computed position
	// visible instead of reading as an empty field.
	if n == 0 {
		return nil, errors.New("dialect: SPLIT_PART: field position must not be zero")
	}
	if delim == "" {
		if n == 1 || n == -1 {
			return s, nil
		}
		return "", nil
	}
	parts := strings.Split(s, delim)
	idx := int(n)
	if idx < 0 {
		idx = len(parts) + idx + 1
	}
	if idx < 1 || idx > len(parts) {
		return "", nil
	}
	return parts[idx-1], nil
}

// fnInitcap implements dialects.PostgreSQL INITCAP: uppercase the first letter of each
// alphanumeric run, lowercase the rest.
func fnInitcap(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	var b strings.Builder
	prevAlnum := false
	for _, r := range s {
		alnum := isAlnumRune(r)
		switch {
		case alnum && !prevAlnum:
			b.WriteString(strings.ToUpper(string(r)))
		case alnum:
			b.WriteString(strings.ToLower(string(r)))
		default:
			b.WriteRune(r)
		}
		prevAlnum = alnum
	}
	return b.String(), nil
}

// isAlnumRune reports whether r is part of a word, which is what decides where
// INITCAP capitalizes. A letter is a letter whatever its script: testing the
// ASCII range read an accented letter as a separator, so "école" came back
// "éCole".
// fnUnicodeUpper and fnUnicodeLower fold case over the whole of Unicode, which
// is what dialects.MySQL, dialects.PostgreSQL and dialects.GoogleSQL do and what dialects.SQLite's own upper() and
// lower() do not: theirs stop at ASCII, so UPPER('école') came back 'éCOLE'.
func fnUnicodeUpper(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	return strings.ToUpper(s), nil
}

func fnUnicodeLower(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	return strings.ToLower(s), nil
}

func isAlnumRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

// fnStrpos implements dialects.PostgreSQL STRPOS(string, substring): 1-based index or 0.
func fnStrpos(args []driver.Value) (driver.Value, error) {
	s, ok1 := toString(args[0])
	sub, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	return int64(characterIndex(s, sub, 0)), nil
}

// characterIndex is the 1-based position of sub in s counted in characters, or
// 0 when it is not there. from is a character offset to start at, which is what
// LOCATE's third argument means. Every dialect here counts a position in
// characters; strings.Index answers in bytes, and returning that answered a
// number that indexes nothing in text outside ASCII.
func characterIndex(s, sub string, from int) int {
	runes := []rune(s)
	if from < 0 || from > len(runes) {
		return 0
	}
	tail := string(runes[from:])
	idx := strings.Index(tail, sub)
	if idx < 0 {
		return 0
	}
	return from + utf8.RuneCountInString(tail[:idx]) + 1
}

func fnLeft(args []driver.Value) (driver.Value, error)  { return leftRight(args, true) }
func fnRight(args []driver.Value) (driver.Value, error) { return leftRight(args, false) }

func fnMySQLLeft(args []driver.Value) (driver.Value, error)  { return mysqlLeftRight(args, true) }
func fnMySQLRight(args []driver.Value) (driver.Value, error) { return mysqlLeftRight(args, false) }

func fnGoogleSQLLeft(args []driver.Value) (driver.Value, error) {
	return googlesqlLeftRight(args, true)
}

func fnGoogleSQLRight(args []driver.Value) (driver.Value, error) {
	return googlesqlLeftRight(args, false)
}

// leftRight implements LEFT/RIGHT with dialects.PostgreSQL's negative-count semantics: a
// negative n removes |n| characters from the far end.
func leftRight(args []driver.Value, left bool) (driver.Value, error) {
	s, n, ok := leftRightArgs(args)
	if !ok {
		return nil, nil
	}
	count := int(n)
	if count < 0 {
		count = len([]rune(s)) + count
	}
	return takeRunes(s, count, left), nil
}

// mysqlLeftRight implements LEFT/RIGHT with dialects.MySQL's negative-count semantics: a
// negative n answers the empty string rather than trimming the far end.
func mysqlLeftRight(args []driver.Value, left bool) (driver.Value, error) {
	s, n, ok := leftRightArgs(args)
	if !ok {
		return nil, nil
	}
	return takeRunes(s, int(n), left), nil
}

// googlesqlLeftRight implements LEFT/RIGHT with dialects.GoogleSQL's rule for a negative
// count, which is to raise: BigQuery has no meaning for a negative length and
// answering dialects.PostgreSQL's trimmed string for one would hide the mistake.
func googlesqlLeftRight(args []driver.Value, left bool) (driver.Value, error) {
	s, n, ok := leftRightArgs(args)
	if !ok {
		return nil, nil
	}
	if n < 0 {
		return nil, fmt.Errorf("dialect: LEFT/RIGHT length must not be negative, got %d", n)
	}
	return takeRunes(s, int(n), left), nil
}

// leftRightArgs coerces the shared (string, count) arguments of LEFT and RIGHT.
func leftRightArgs(args []driver.Value) (string, int64, bool) {
	s, ok1 := toString(args[0])
	n, ok2 := toCount(args[1])
	return s, n, ok1 && ok2
}

// takeRunes returns the first or last count characters of s, the whole of s when
// it is shorter than that, and the empty string when count is not positive.
func takeRunes(s string, count int, fromLeft bool) string {
	if count <= 0 {
		return ""
	}
	runes := []rune(s)
	if count > len(runes) {
		count = len(runes)
	}
	if fromLeft {
		return string(runes[:count])
	}
	return string(runes[len(runes)-count:])
}

// fnRegexpReplace implements dialects.GoogleSQL REGEXP_REPLACE(source, pattern,
// replacement), which replaces every match, and is also what a query written in
// the dialects.SQLite dialect reaches. dialects.PostgreSQL back-references (\1) are translated to
// Go's ${1} expansion form. A flags argument is accepted here for the callers
// that already pass one: "g" replaces every match and its absence replaces only
// the first, and "i" matches case insensitively.
func fnRegexpReplace(args []driver.Value) (driver.Value, error) {
	return regexpReplace(args, true)
}

// fnPostgresRegexpReplace implements dialects.PostgreSQL regexp_replace(source, pattern,
// replacement[, flags]), whose three-argument form replaces the first match
// alone; the rest need the "g" flag.
func fnPostgresRegexpReplace(args []driver.Value) (driver.Value, error) {
	return regexpReplace(args, false)
}

// regexpReplace is the shared body of the flag-taking REGEXP_REPLACE forms.
// defaultGlobal is what the three-argument form means, which is every match for
// dialects.GoogleSQL and the first alone for dialects.PostgreSQL.
func regexpReplace(args []driver.Value, defaultGlobal bool) (driver.Value, error) {
	if len(args) < 3 || len(args) > 4 {
		return nil, fmt.Errorf("dialect: REGEXP_REPLACE expects 3 or 4 arguments, got %d", len(args))
	}
	src, ok1 := toString(args[0])
	pattern, ok2 := toString(args[1])
	repl, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	global := defaultGlobal
	if len(args) == 4 {
		flags, ok := toString(args[3])
		if !ok {
			return nil, nil
		}
		global = strings.Contains(flags, "g")
		if strings.Contains(flags, "i") {
			pattern = "(?i)" + pattern
		}
	}
	re, err := compileRegexp(pattern)
	if err != nil {
		return nil, err
	}
	expansion := pgReplacement(repl)
	if global {
		return re.ReplaceAllString(src, expansion), nil
	}
	// Expand against the source rather than the matched text on its own: a
	// boundary-dependent pattern such as `\Bb` matches inside "ab" but not in
	// the isolated "b", which would leave ExpandString without submatch indices.
	loc := re.FindStringSubmatchIndex(src)
	if loc == nil {
		return src, nil
	}
	out := re.ExpandString([]byte(src[:loc[0]]), expansion, src, loc)
	return string(out) + src[loc[1]:], nil
}

// fnMySQLRegexpReplace implements dialects.MySQL REGEXP_REPLACE(subject, pattern,
// replacement[, pos[, occurrence[, match_type]]]). dialects.MySQL's fourth argument is a
// 1-based character position to start at rather than dialects.PostgreSQL's flag string,
// and its fifth selects one match to replace, with 0 meaning every match from
// the position onward.
func fnMySQLRegexpReplace(args []driver.Value) (driver.Value, error) {
	if len(args) < 3 || len(args) > 6 {
		return nil, fmt.Errorf("dialect: REGEXP_REPLACE expects 3 to 6 arguments, got %d", len(args))
	}
	src, ok1 := toString(args[0])
	pattern, ok2 := toString(args[1])
	repl, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	pos, occurrence := int64(1), int64(0)
	if len(args) >= 4 {
		n, ok := toInt(args[3])
		if !ok {
			return nil, nil
		}
		pos = n
	}
	if len(args) >= 5 {
		n, ok := toInt(args[4])
		if !ok {
			return nil, nil
		}
		occurrence = n
	}
	matchType := ""
	if len(args) == 6 {
		m, ok := toString(args[5])
		if !ok {
			return nil, nil
		}
		matchType = m
	}
	// The pattern goes through the match-type mapping even when no match type
	// was given, because dialects.MySQL's default is not Go's: its collation folds case.
	pattern, err := mysqlRegexpPattern(pattern, matchType)
	if err != nil {
		return nil, err
	}
	runes := []rune(src)
	if pos < 1 || int(pos) > len(runes)+1 {
		return nil, fmt.Errorf("dialect: REGEXP_REPLACE position %d is out of bounds", pos)
	}
	re, err := compileRegexp(pattern)
	if err != nil {
		return nil, err
	}
	head, tail := string(runes[:pos-1]), string(runes[pos-1:])
	expansion := mysqlReplacement(repl)
	if occurrence == 0 {
		return head + re.ReplaceAllString(tail, expansion), nil
	}
	// A negative occurrence is not a form dialects.MySQL documents; it answers the first
	// match there, so the count below starts at one for anything under it.
	wanted := int(occurrence)
	if wanted < 1 {
		wanted = 1
	}
	matches := re.FindAllStringSubmatchIndex(tail, wanted)
	if len(matches) < wanted {
		return src, nil
	}
	loc := matches[wanted-1]
	out := re.ExpandString([]byte(tail[:loc[0]]), expansion, tail, loc)
	return head + string(out) + tail[loc[1]:], nil
}

// applyMySQLMatchType folds a dialects.MySQL match_type string into the pattern as Go
// regexp flags. dialects.MySQL spells them c (case sensitive), i (case insensitive), m
// (multi-line) and n (a dot matches a newline); u, which selects Unix line
// endings, has no Go equivalent and is refused rather than ignored.
func mysqlRegexpPattern(pattern, matchType string) (string, error) {
	// MySQL refuses an empty pattern rather than matching everywhere with it,
	// which is what Go's regexp does and what this answered: REGEXP_LIKE(x, '')
	// was true for every row, including the rows a caller wrote the query to
	// find.
	if pattern == "" {
		return "", errors.New("dialect: the regular expression is empty, which MySQL refuses")
	}
	fold := true
	var flags string
	for _, c := range matchType {
		switch c {
		case 'c':
			fold = false
		case 'i':
			fold = true
		case 'm':
			flags += "m"
		case 'n':
			flags += "s"
		default:
			return "", fmt.Errorf("dialect: regular expression match type %q is not supported", matchType)
		}
	}
	if fold {
		flags = "i" + flags
	} else {
		flags += "-i"
	}
	// The flag group leads the pattern, so a group the caller wrote themselves
	// stands after it and wins for the rest of the pattern.
	return "(?" + flags + ")" + pattern, nil
}

// mysqlReplacement translates dialects.MySQL replacement references ($1..$9) to the ${n}
// form Go's regexp expansion understands. dialects.MySQL writes a literal "$" as "\$",
// and a backslash before anything else stands for that character.
func mysqlReplacement(repl string) string {
	var b strings.Builder
	for i := 0; i < len(repl); i++ {
		switch {
		case repl[i] == '\\' && i+1 < len(repl):
			if repl[i+1] == '$' {
				b.WriteString("$$")
			} else {
				b.WriteByte(repl[i+1])
			}
			i++
		case repl[i] == '$' && i+1 < len(repl) && repl[i+1] >= '0' && repl[i+1] <= '9':
			b.WriteString("${")
			b.WriteByte(repl[i+1])
			b.WriteByte('}')
			i++
		case repl[i] == '$':
			b.WriteString("$$")
		default:
			b.WriteByte(repl[i])
		}
	}
	return b.String()
}

// fnMD5 implements dialects.PostgreSQL MD5(text). MD5 is used here as a content
// fingerprint compatible with the source dialect, never for security.
func fnMD5(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	sum := md5.Sum([]byte(s)) //nolint:gosec // required for dialects.PostgreSQL MD5() compatibility, not a security control
	return hex.EncodeToString(sum[:]), nil
}

// fnASCII implements ASCII(text): the code point of the first character, or 0
// for an empty string.
func fnASCII(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	for _, r := range s {
		return int64(r), nil
	}
	return int64(0), nil
}

// fnChr implements CHR(code): the character for a code point.
func fnChr(args []driver.Value) (driver.Value, error) {
	n, ok := toInt(args[0])
	if !ok {
		return nil, nil
	}
	if n <= 0 || n > utf8.MaxRune {
		// dialects.PostgreSQL refuses zero as well as a negative code point: a zero byte
		// cannot be stored in a text value there. Answering the space that
		// dialects.SQLite's char() leaves behind for it is the one result a caller
		// cannot tell from a real character.
		return nil, fmt.Errorf("dialect: CHR: code point %d is out of range", n)
	}
	return string(rune(n)), nil
}

// fnTranslate implements dialects.PostgreSQL TRANSLATE(string, from, to): each character
// of from is replaced by the character at the same position in to, and a
// character whose position has no counterpart in to is dropped.
func fnTranslate(args []driver.Value) (driver.Value, error) {
	s, ok1 := toString(args[0])
	from, ok2 := toString(args[1])
	to, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	fromRunes := []rune(from)
	toRunes := []rune(to)
	var b strings.Builder
	for _, r := range s {
		idx := -1
		for i, f := range fromRunes {
			if f == r {
				idx = i
				break
			}
		}
		switch {
		case idx < 0:
			b.WriteRune(r)
		case idx < len(toRunes):
			b.WriteRune(toRunes[idx])
		}
	}
	return b.String(), nil
}

// pgReplacement translates dialects.PostgreSQL replacement back-references (\1..\9, \&) to
// the ${n} form Go's regexp expansion understands.
func pgReplacement(repl string) string {
	var b strings.Builder
	for i := 0; i < len(repl); i++ {
		if repl[i] == '\\' && i+1 < len(repl) {
			c := repl[i+1]
			switch {
			case c >= '0' && c <= '9':
				b.WriteString("${")
				b.WriteByte(c)
				b.WriteByte('}')
			case c == '&':
				b.WriteString("${0}")
			default:
				b.WriteByte(c)
			}
			i++
			continue
		}
		if repl[i] == '$' {
			// Escape a literal '$' so Go does not treat it as an expansion.
			b.WriteString("$$")
			continue
		}
		b.WriteByte(repl[i])
	}
	return b.String()
}
