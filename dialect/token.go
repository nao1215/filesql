package dialect

import (
	"encoding/hex"
	"fmt"
	"strings"
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
		}, true
	case GoogleSQL:
		return lexConfig{
			identBacktick:     true,
			stringDoubleQuote: true,
			hashComment:       true,
			backslashEscapes:  true,
			rawStringPrefix:   true,
			byteStringPrefix:  true,
		}, true
	default:
		return lexConfig{}, false
	}
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
			j := i + 2
			for j+1 < len(query) && (query[j] != '*' || query[j+1] != '/') {
				j++
			}
			if j+1 >= len(query) {
				return nil, fmt.Errorf("%w: unterminated block comment at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokBlockComment, text: query[i+2 : j], offset: start})
			i = j + 2

		case c == '\'':
			content, ni, ok := scanQuoted(query, i, '\'', cfg.backslashEscapes)
			if !ok {
				return nil, fmt.Errorf("%w: unterminated string literal at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokString, text: content, offset: start})
			i = ni

		case c == '"' && cfg.identDoubleQuote:
			content, ni, ok := scanQuoted(query, i, '"', false)
			if !ok {
				return nil, fmt.Errorf("%w: unterminated quoted identifier at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokQuotedIdent, text: content, offset: start})
			i = ni

		case c == '"' && cfg.stringDoubleQuote:
			content, ni, ok := scanQuoted(query, i, '"', cfg.backslashEscapes)
			if !ok {
				return nil, fmt.Errorf("%w: unterminated string literal at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokString, text: content, offset: start})
			i = ni

		case c == '`' && cfg.identBacktick:
			content, ni, ok := scanQuoted(query, i, '`', false)
			if !ok {
				return nil, fmt.Errorf("%w: unterminated quoted identifier at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokQuotedIdent, text: content, offset: start})
			i = ni

		case (c == 'x' || c == 'X') && next(query, i) == '\'':
			content, ni, ok := scanQuoted(query, i+1, '\'', false)
			if !ok {
				return nil, fmt.Errorf("%w: unterminated blob literal at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokBlob, text: strings.ToLower(content), offset: start})
			i = ni

		case cfg.ePrefixString && (c == 'e' || c == 'E') && next(query, i) == '\'':
			content, ni, ok := scanQuoted(query, i+1, '\'', true)
			if !ok {
				return nil, fmt.Errorf("%w: unterminated string literal at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokString, text: content, offset: start})
			i = ni

		case cfg.rawStringPrefix && (c == 'r' || c == 'R') && (next(query, i) == '\'' || next(query, i) == '"'):
			content, ni, ok := scanQuoted(query, i+1, query[i+1], false)
			if !ok {
				return nil, fmt.Errorf("%w: unterminated raw string literal at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokString, text: content, offset: start})
			i = ni

		case cfg.byteStringPrefix && (c == 'b' || c == 'B') && (next(query, i) == '\'' || next(query, i) == '"'):
			content, ni, ok := scanQuoted(query, i+1, query[i+1], true)
			if !ok {
				return nil, fmt.Errorf("%w: unterminated byte string literal at offset %d", ErrInvalidSyntax, start)
			}
			tokens = append(tokens, token{kind: tokBlob, text: hex.EncodeToString([]byte(content)), offset: start})
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
func scanQuoted(s string, i int, quote byte, honorBackslash bool) (content string, ni int, ok bool) {
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
		case honorBackslash && c == '\\' && i+1 < len(s):
			esc, adv := decodeBackslash(s, i)
			b.WriteString(esc)
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
func decodeBackslash(s string, i int) (string, int) {
	if i+1 >= len(s) {
		return "\\", 1
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
	"<=>", "!~*", "->>", "!~", "~*", "->", "<=", ">=", "<>", "!=", "||", "&&", "<<", ">>", "::", ":=",
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
