package dialect

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// tokenKind classifies a lexical token. Rendering and rewrite rules branch on
// the kind.
type tokenKind int

const (
	tokWhitespace   tokenKind = iota // runs of spaces/tabs/newlines; rendered as a single space
	tokLineComment                   // -- or # comment; text holds the content after the leader
	tokBlockComment                  // /* ... */ comment; text holds the content between the delimiters
	tokWord                          // unquoted identifier, keyword, or function name
	tokNumber                        // numeric literal (rendered verbatim)
	tokQuotedIdent                   // quoted identifier; text holds the decoded name
	tokString                        // string literal; text holds the decoded content
	tokBlob                          // blob/bytes literal; text holds lowercase hex
	tokOp                            // operator or punctuation (rendered verbatim)
	tokPlaceholder                   // bind placeholder such as ?, ?1, $1, @name, :name
)

// token is one lexical unit. offset is the byte offset of the token in the
// source query and is used for error messages. The meaning of text depends on
// kind: for tokString/tokQuotedIdent it is the decoded value (escapes resolved,
// doubled quotes collapsed); for tokBlob it is lowercase hex; for the remaining
// kinds it is the verbatim source text (tokWhitespace and the comment kinds hold
// the content the renderer needs, not the surrounding delimiters).
type token struct {
	kind   tokenKind
	text   string
	offset int
}

// lexConfig describes the lexical rules that differ between dialects. The rest
// of the grammar (single-quoted strings, blob literals, numbers, operators,
// placeholders, /* */ and -- comments) is common to every dialect.
type lexConfig struct {
	identBacktick     bool // ` opens a quoted identifier (MySQL, GoogleSQL)
	identDoubleQuote  bool // " opens a quoted identifier (PostgreSQL)
	stringDoubleQuote bool // " opens a string literal (MySQL, GoogleSQL)
	hashComment       bool // # starts a line comment (MySQL, GoogleSQL)
	backslashEscapes  bool // backslash is an escape inside single-quoted strings (MySQL, GoogleSQL)
	ePrefixString     bool // E'...' escape-string literals (PostgreSQL)
	dollarQuote       bool // $tag$...$tag$ dollar-quoted strings (PostgreSQL)
	rawStringPrefix   bool // r'...'/r"..." raw strings (GoogleSQL)
	byteStringPrefix  bool // b'...'/b"..." byte strings (GoogleSQL)
	tripleQuoteString bool // '''...''' and """...""" strings (GoogleSQL)
	nestedComment     bool // /* */ block comments nest (PostgreSQL)
	numericEscapes    bool // \xHH, \ooo, \uXXXX and \UXXXXXXXX name a character (GoogleSQL)
}

// escapeRules says how a backslash behaves inside a quoted literal. The two
// halves are separate because they are configured separately: MySQL honors a
// backslash but has no numeric escapes, while a PostgreSQL E'...' string has
// both even though an ordinary PostgreSQL string has neither.
type escapeRules struct {
	backslash bool
	numeric   bool
}

// lexConfigFor returns the lexical configuration for a built-in non-SQLite
// dialect. It reports false for SQLite (handled as identity before tokenizing)
// and for any unknown dialect.
func lexConfigFor(d Dialect) (lexConfig, bool) {
	switch d {
	case MySQL:
		return lexConfig{
			identBacktick:     true,
			stringDoubleQuote: true,
			hashComment:       true,
			backslashEscapes:  true,
		}, true
	case PostgreSQL:
		return lexConfig{
			identDoubleQuote: true,
			ePrefixString:    true,
			dollarQuote:      true,
			nestedComment:    true,
		}, true
	case GoogleSQL:
		return lexConfig{
			identBacktick:     true,
			stringDoubleQuote: true,
			hashComment:       true,
			backslashEscapes:  true,
			rawStringPrefix:   true,
			byteStringPrefix:  true,
			tripleQuoteString: true,
			numericEscapes:    true,
		}, true
	default:
		return lexConfig{}, false
	}
}

// stringEscapes is how a backslash behaves inside an ordinary quoted string of
// the dialect cfg describes.
func (cfg lexConfig) stringEscapes() escapeRules {
	return escapeRules{backslash: cfg.backslashEscapes, numeric: cfg.numericEscapes}
}

// tokenize splits query into tokens using cfg. It returns ErrInvalidSyntax
// (wrapped, with the byte offset) when a quoted literal or identifier is not
// terminated.
func tokenize(query string, cfg lexConfig) ([]token, error) {
	tokens := make([]token, 0, len(query)/4+1)
	i := 0
	for i < len(query) {
		c := query[i]
		start := i

		switch {
		case isSpace(c):
			j := i + 1
			for j < len(query) && isSpace(query[j]) {
				j++
			}
			tokens = append(tokens, token{kind: tokWhitespace, text: query[i:j], offset: start})
			i = j

		case c == '-' && next(query, i) == '-':
			j := i + 2
			for j < len(query) && query[j] != '\n' {
				j++
			}
			tokens = append(tokens, token{kind: tokLineComment, text: query[i+2 : j], offset: start})
			i = j

		case c == '#' && cfg.hashComment:
			j := i + 1
			for j < len(query) && query[j] != '\n' {
				j++
			}
			tokens = append(tokens, token{kind: tokLineComment, text: query[i+1 : j], offset: start})
			i = j

		case c == '/' && next(query, i) == '*':
			j, ok := scanBlockComment(query, i, cfg.nestedComment)
			if !ok {
				return nil, fmt.Errorf("%w: unterminated block comment at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokBlockComment, text: query[i+2 : j-2], offset: start})
			i = j

		case cfg.tripleQuoteString && isTripleQuoteStart(query, i):
			content, ni, ok := scanTripleQuoted(query, i, c, cfg.stringEscapes())
			if !ok {
				return nil, fmt.Errorf("%w: unterminated string literal at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokString, text: content, offset: start})
			i = ni

		case c == '\'':
			content, ni, ok := scanQuoted(query, i, '\'', cfg.stringEscapes())
			if !ok {
				return nil, fmt.Errorf("%w: unterminated string literal at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokString, text: content, offset: start})
			i = ni

		case c == '"' && cfg.identDoubleQuote:
			content, ni, ok := scanQuoted(query, i, '"', escapeRules{})
			if !ok {
				return nil, fmt.Errorf("%w: unterminated quoted identifier at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokQuotedIdent, text: content, offset: start})
			i = ni

		case c == '"' && cfg.stringDoubleQuote:
			content, ni, ok := scanQuoted(query, i, '"', cfg.stringEscapes())
			if !ok {
				return nil, fmt.Errorf("%w: unterminated string literal at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokString, text: content, offset: start})
			i = ni

		case c == '`' && cfg.identBacktick:
			content, ni, ok := scanQuoted(query, i, '`', escapeRules{})
			if !ok {
				return nil, fmt.Errorf("%w: unterminated quoted identifier at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokQuotedIdent, text: content, offset: start})
			i = ni

		case (c == 'x' || c == 'X') && next(query, i) == '\'':
			content, ni, ok := scanQuoted(query, i+1, '\'', escapeRules{})
			if !ok {
				return nil, fmt.Errorf("%w: unterminated blob literal at offset %d", ErrInvalidSyntax, start)
			}
			// A blob literal holds hexadecimal digits and nothing else. Accepting
			// anything else rendered it back as x'<content>', which for content
			// carrying a quote is SQL that no longer parses: the caller got a
			// syntax error about text they had not written.
			if !isHexDigits(content) {
				return nil, fmt.Errorf("%w: blob literal is not hexadecimal at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokBlob, text: strings.ToLower(content), offset: start})
			i = ni

		case cfg.ePrefixString && (c == 'e' || c == 'E') && next(query, i) == '\'':
			// A PostgreSQL escape string decodes both the letter escapes and the
			// numeric ones, whatever an ordinary string in the same dialect does.
			content, ni, ok := scanQuoted(query, i+1, '\'', escapeRules{backslash: true, numeric: true})
			if !ok {
				return nil, fmt.Errorf("%w: unterminated string literal at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokString, text: content, offset: start})
			i = ni

		case prefixLen(query, i, cfg) > 0:
			n := prefixLen(query, i, cfg)
			raw, isBytes := prefixMeaning(query[i : i+n])
			esc := cfg.stringEscapes()
			if raw {
				// A raw string keeps its backslashes: that is what it is for.
				esc = escapeRules{}
			}
			quote := query[i+n]
			content, ni, ok := scanStringLiteral(query, i+n, quote, esc, cfg.tripleQuoteString)
			if !ok {
				return nil, fmt.Errorf("%w: unterminated string literal at offset %d", ErrInvalidSyntax, start)
			}
			kind, text := tokString, content
			if isBytes {
				kind, text = tokBlob, hex.EncodeToString([]byte(content))
			}
			tokens = append(tokens, token{kind: kind, text: text, offset: start})
			i = ni

		case cfg.dollarQuote && c == '$' && isDollarQuoteStart(query, i):
			content, ni, ok := scanDollarQuoted(query, i)
			if !ok {
				return nil, fmt.Errorf("%w: unterminated dollar-quoted string at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokString, text: content, offset: start})
			i = ni

		case c == '?':
			j := i + 1
			for j < len(query) && isDigit(query[j]) {
				j++
			}
			tokens = append(tokens, token{kind: tokPlaceholder, text: query[i:j], offset: start})
			i = j

		case c == '$' && isDigit(next(query, i)):
			j := i + 1
			for j < len(query) && isDigit(query[j]) {
				j++
			}
			tokens = append(tokens, token{kind: tokPlaceholder, text: query[i:j], offset: start})
			i = j

		case (c == '@' || c == ':') && isIdentStart(next(query, i)):
			j := i + 1
			for j < len(query) && isIdentPart(query[j]) {
				j++
			}
			tokens = append(tokens, token{kind: tokPlaceholder, text: query[i:j], offset: start})
			i = j

		case isDigit(c) || (c == '.' && isDigit(next(query, i))):
			j := scanNumber(query, i)
			tokens = append(tokens, token{kind: tokNumber, text: query[i:j], offset: start})
			i = j

		case isIdentStart(c):
			j := i + 1
			for j < len(query) && isIdentPart(query[j]) {
				j++
			}
			tokens = append(tokens, token{kind: tokWord, text: query[i:j], offset: start})
			i = j

		default:
			op := matchOperator(query, i)
			tokens = append(tokens, token{kind: tokOp, text: op, offset: start})
			i += len(op)
		}
	}
	return tokens, nil
}

// scanBlockComment returns the index just past the block comment opening at
// s[i], and whether it was closed. Where comments nest, an inner /* opens a
// comment of its own and the outer one ends at the matching */.
//
// Ending at the first */ regardless made the text between the inner close and
// the outer one part of the statement: a query that commented out a clause with
// a nested comment ran with that clause back in, without error and without
// anything in the result to say so.
func scanBlockComment(s string, i int, nested bool) (int, bool) {
	depth := 1
	j := i + 2
	for j+1 < len(s) {
		switch {
		case s[j] == '*' && s[j+1] == '/':
			depth--
			j += 2
			if depth == 0 {
				return j, true
			}
		case nested && s[j] == '/' && s[j+1] == '*':
			depth++
			j += 2
		default:
			j++
		}
	}
	return j, false
}

// prefixLen returns the length of the string prefix at s[i], or 0 when what is
// there does not open a prefixed string. GoogleSQL writes a raw string as r'..',
// a byte string as b'..', and both at once as rb'..' or br'..', in either case.
func prefixLen(s string, i int, cfg lexConfig) int {
	if !cfg.rawStringPrefix && !cfg.byteStringPrefix {
		return 0
	}
	n := 0
	var sawRaw, sawBytes bool
	for n < 2 && i+n < len(s) {
		switch s[i+n] {
		case 'r', 'R':
			if sawRaw || !cfg.rawStringPrefix {
				return 0
			}
			sawRaw = true
		case 'b', 'B':
			if sawBytes || !cfg.byteStringPrefix {
				return 0
			}
			sawBytes = true
		default:
			// Not a prefix letter: what follows has to be the quote.
		}
		if !sawRaw && !sawBytes {
			return 0
		}
		n++
		if i+n < len(s) && (s[i+n] == '\'' || s[i+n] == '"') {
			return n
		}
	}
	return 0
}

// prefixMeaning reads a string prefix: whether it makes the literal raw, and
// whether it makes it bytes.
func prefixMeaning(prefix string) (raw, isBytes bool) {
	for i := range len(prefix) {
		switch prefix[i] {
		case 'r', 'R':
			raw = true
		case 'b', 'B':
			isBytes = true
		}
	}
	return raw, isBytes
}

// scanStringLiteral reads the string opening at s[i], which is a quote, taking
// the triple-quoted form when the dialect has one and three quotes are there.
func scanStringLiteral(s string, i int, quote byte, esc escapeRules, triple bool) (content string, ni int, ok bool) {
	if triple && isTripleQuoteStart(s, i) {
		return scanTripleQuoted(s, i, quote, esc)
	}
	return scanQuoted(s, i, quote, esc)
}

// scanTripleQuoted returns the content of the triple-quoted string opening at
// s[i] and the index just past its closing delimiter. GoogleSQL writes a string
// that may hold quotes and line breaks this way; read as an ordinary string,
// the doubled quotes look like SQL-escaped quotes and the literal keeps one
// quote on each end.
func scanTripleQuoted(s string, i int, quote byte, esc escapeRules) (content string, ni int, ok bool) {
	var b strings.Builder
	i += 3 // skip the opening delimiter
	for i < len(s) {
		switch {
		case s[i] == quote && i+2 < len(s) && s[i+1] == quote && s[i+2] == quote:
			return b.String(), i + 3, true
		case esc.backslash && s[i] == '\\' && i+1 < len(s):
			decoded, adv := decodeBackslash(s, i, esc)
			b.WriteString(decoded)
			i += adv
		default:
			b.WriteByte(s[i])
			i++
		}
	}
	return "", i, false
}

// isTripleQuoteStart reports whether a triple-quoted string opens at s[i].
func isTripleQuoteStart(s string, i int) bool {
	c := s[i]
	if c != '\'' && c != '"' {
		return false
	}
	return i+2 < len(s) && s[i+1] == c && s[i+2] == c
}

// next returns the byte after position i, or 0 if i is the last byte.
func next(s string, i int) byte {
	if i+1 < len(s) {
		return s[i+1]
	}
	return 0
}

// scanQuoted reads a quoted literal whose opening quote is at s[i]. The closing
// quote is a matching quote that is not doubled; a doubled quote ("" or ”)
// yields one literal quote. When honorBackslash is true a backslash escapes the
// following byte. It returns the decoded content, the index just past the
// closing quote, and whether a closing quote was found.
func scanQuoted(s string, i int, quote byte, esc escapeRules) (content string, ni int, ok bool) {
	var b strings.Builder
	i++ // skip opening quote
	for i < len(s) {
		c := s[i]
		switch {
		case c == quote:
			if i+1 < len(s) && s[i+1] == quote {
				b.WriteByte(quote)
				i += 2
				continue
			}
			return b.String(), i + 1, true
		case esc.backslash && c == '\\' && i+1 < len(s):
			decoded, adv := decodeBackslash(s, i, esc)
			b.WriteString(decoded)
			i += adv
		default:
			b.WriteByte(c)
			i++
		}
	}
	return "", i, false
}

// decodeBackslash decodes the escape sequence starting at s[i] (which is a
// backslash). It returns the decoded text and the number of bytes consumed. An
// unrecognized escape drops the backslash and keeps the following byte, matching
// the lenient behavior of MySQL and GoogleSQL for unknown escapes.
func decodeBackslash(s string, i int, esc escapeRules) (string, int) {
	if i+1 >= len(s) {
		return "\\", 1
	}
	if esc.numeric {
		if decoded, adv, ok := decodeNumericEscape(s, i); ok {
			return decoded, adv
		}
	}
	switch s[i+1] {
	case 'n':
		return "\n", 2
	case 't':
		return "\t", 2
	case 'r':
		return "\r", 2
	case '0':
		return "\x00", 2
	case 'b':
		return "\b", 2
	case 'f':
		return "\f", 2
	case 'v':
		return "\v", 2
	case 'Z':
		// MySQL's \Z is ASCII 26. Falling through to the default dropped the
		// backslash and left a literal "Z".
		return "\x1a", 2
	case '%', '_':
		// The two sequences that keep their backslash. MySQL documents them that
		// way precisely so a LIKE pattern survives being written as a string
		// literal: "\%" is the two characters, and the LIKE that reads them
		// turns them into a literal percent. Dropping the backslash here left an
		// ordinary wildcard behind, so a pattern asking for one row matched
		// every row.
		return s[i : i+2], 2
	case '\\':
		return "\\", 2
	case '\'':
		return "'", 2
	case '"':
		return "\"", 2
	case '`':
		return "`", 2
	default:
		return string(s[i+1]), 2
	}
}

// decodeNumericEscape decodes an escape that names a character by its number:
// \xh[h] hexadecimal, \o[oo] octal, \uXXXX and \UXXXXXXXX by code point. It
// reports false when the bytes after the backslash are not one of those, which
// leaves the letter escapes and the lenient default to decodeBackslash.
//
// Dropping the backslash and keeping the digits, which is what the lenient
// default does, turned E'\x41' into the three characters x41 rather than the
// one character A — a literal that compares equal to different rows than the
// one the caller wrote.
func decodeNumericEscape(s string, i int) (string, int, bool) {
	switch s[i+1] {
	case 'x', 'X':
		digits := hexRun(s, i+2, 2)
		if digits == 0 {
			return "", 0, false
		}
		v, err := strconv.ParseUint(s[i+2:i+2+digits], 16, 8)
		if err != nil {
			return "", 0, false
		}
		return string(rune(v)), 2 + digits, true
	case 'u', 'U':
		width := 4
		if s[i+1] == 'U' {
			width = 8
		}
		if hexRun(s, i+2, width) != width {
			return "", 0, false
		}
		v, err := strconv.ParseUint(s[i+2:i+2+width], 16, 32)
		if err != nil || v > unicode.MaxRune {
			return "", 0, false
		}
		return string(rune(v)), 2 + width, true
	case '0', '1', '2', '3', '4', '5', '6', '7':
		digits := octalRun(s, i+1, 3)
		v, err := strconv.ParseUint(s[i+1:i+1+digits], 8, 16)
		if err != nil {
			return "", 0, false
		}
		return string(rune(v)), 1 + digits, true
	default:
		return "", 0, false
	}
}

// isHexDigits reports whether s is made only of hexadecimal digits, which is
// what a blob literal may hold. The empty string qualifies: x” is the empty
// blob.
func isHexDigits(s string) bool {
	for i := range len(s) {
		if !isHex(s[i]) {
			return false
		}
	}
	return true
}

// hexRun counts the hexadecimal digits at s[i], up to max.
func hexRun(s string, i, maxDigits int) int {
	n := 0
	for n < maxDigits && i+n < len(s) && isHex(s[i+n]) {
		n++
	}
	return n
}

// octalRun counts the octal digits at s[i], up to max.
func octalRun(s string, i, maxDigits int) int {
	n := 0
	for n < maxDigits && i+n < len(s) && s[i+n] >= '0' && s[i+n] <= '7' {
		n++
	}
	return n
}

// isDollarQuoteStart reports whether a dollar-quoted string opens at s[i] (which
// is a '$'). The tag between the dollars must be empty or an identifier.
func isDollarQuoteStart(s string, i int) bool {
	j := i + 1
	for j < len(s) && isIdentPart(s[j]) {
		j++
	}
	return j < len(s) && s[j] == '$'
}

// scanDollarQuoted reads a PostgreSQL dollar-quoted string ($tag$...$tag$)
// starting at s[i]. The content is taken verbatim (no escape processing).
func scanDollarQuoted(s string, i int) (content string, ni int, ok bool) {
	j := i + 1
	for j < len(s) && isIdentPart(s[j]) {
		j++
	}
	// s[i:j+1] is the opening delimiter "$tag$".
	delim := s[i : j+1]
	rest := s[j+1:]
	idx := strings.Index(rest, delim)
	if idx < 0 {
		return "", len(s), false
	}
	return rest[:idx], j + 1 + idx + len(delim), true
}

// scanNumber returns the index just past the numeric literal starting at s[i].
// It handles hexadecimal (0x...), a fractional part, and a signed exponent.
func scanNumber(s string, i int) int {
	if s[i] == '0' && i+1 < len(s) && (s[i+1] == 'x' || s[i+1] == 'X') {
		j := i + 2
		for j < len(s) && isHex(s[j]) {
			j++
		}
		return j
	}
	// MySQL's binary literal. Without this the "0" ended the number and "b1010"
	// began a word, so "SELECT 0b1010" reached SQLite as a zero with an alias
	// and answered 0.
	if s[i] == '0' && i+1 < len(s) && (s[i+1] == 'b' || s[i+1] == 'B') {
		j := i + 2
		for j < len(s) && (s[j] == '0' || s[j] == '1') {
			j++
		}
		if j > i+2 {
			return j
		}
	}
	j := i
	for j < len(s) && isDigit(s[j]) {
		j++
	}
	if j < len(s) && s[j] == '.' {
		j++
		for j < len(s) && isDigit(s[j]) {
			j++
		}
	}
	if j < len(s) && (s[j] == 'e' || s[j] == 'E') {
		k := j + 1
		if k < len(s) && (s[k] == '+' || s[k] == '-') {
			k++
		}
		if k < len(s) && isDigit(s[k]) {
			j = k
			for j < len(s) && isDigit(s[j]) {
				j++
			}
		}
	}
	return j
}

// multiCharOperators lists the multi-byte operators to recognize as one token,
// longest first so matchOperator prefers the longest match.
var multiCharOperators = []string{
	// The LIKE aliases are listed before the regex operators they start with,
	// so "~~" is one token rather than two "~" that each become a REGEXP.
	"!~~*", "~~*", "!~~", "~~",
	"<=>", "!~*", "->>", "!~", "~*", "->", "<=", ">=", "<>", "!=", "||", "&&", "<<", ">>", "::", ":=", "=>",
}

// matchOperator returns the operator or punctuation token starting at s[i],
// preferring the longest known multi-byte operator and otherwise the single
// byte at s[i].
func matchOperator(s string, i int) string {
	for _, op := range multiCharOperators {
		if strings.HasPrefix(s[i:], op) {
			return op
		}
	}
	return s[i : i+1]
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func isHex(c byte) bool {
	return isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c >= 0x80
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || isDigit(c)
}
