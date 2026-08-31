// Package token turns SQL source text into tokens. It knows the lexical rules
// that differ between dialects -- identifier quoting, string quoting and
// escapes, comment leaders, dollar quoting, raw and byte strings -- and nothing
// about grammar: no token here is classified by what it means in a statement.
package token

import (
	"encoding/hex"
	"strconv"
	"strings"
	"unicode"

	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/sqlerr"
)

// Kind classifies a lexical token. Rendering and rewrite rules branch on
// the kind.
type Kind int

// The token kinds. Rendering and the grammar both branch on them.
const (
	Whitespace   Kind = iota // runs of spaces/tabs/newlines; rendered as a single space
	LineComment              // -- or # comment; text holds the content after the leader
	BlockComment             // /* ... */ comment; text holds the content between the delimiters
	Word                     // unquoted identifier, keyword, or function name
	Number                   // numeric literal (rendered verbatim)
	QuotedIdent              // quoted identifier; text holds the decoded name
	String                   // string literal; text holds the decoded content
	Blob                     // blob/bytes literal; text holds lowercase hex
	Op                       // operator or punctuation (rendered verbatim)
	Placeholder              // bind placeholder such as ?, ?1, $1, @name, :name
)

// Token is one lexical unit. offset is the byte offset of the token in the
// source query and is used for error messages. The meaning of text depends on
// kind: for String/QuotedIdent it is the decoded value (escapes resolved,
// doubled quotes collapsed); for Blob it is lowercase hex; for the remaining
// kinds it is the verbatim source text (Whitespace and the comment kinds hold
// the content the renderer needs, not the surrounding delimiters).
type Token struct {
	Kind   Kind
	Text   string
	Offset int
	// End is the byte offset just past the token in the source, so a caller can
	// take the text exactly as it was written: for a literal, Text holds the
	// decoded value and the source spelling is only recoverable this way.
	End int
	// Line and Col are the 1-based position of the token's first byte, counted
	// in bytes on the line. They are here rather than derived later because a
	// diagnostic about a construct is written where the construct is found, and
	// by then the source string is several layers away.
	Line int
	Col  int
}

// Config describes the lexical rules that differ between dialects. The rest
// of the grammar (single-quoted strings, blob literals, numbers, operators,
// placeholders, /* */ and -- comments) is common to every dialect.
type Config struct {
	IdentBacktick     bool // ` opens a quoted identifier (MySQL, GoogleSQL)
	IdentBacktickEsc  bool // a backslash escapes inside a backtick-quoted identifier (GoogleSQL)
	DashNeedsBlank    bool // -- opens a line comment only when a blank follows it (MySQL)
	IdentDoubleQuote  bool // " opens a quoted identifier (PostgreSQL)
	StringDoubleQuote bool // " opens a string literal (MySQL, GoogleSQL)
	HashComment       bool // # starts a line comment (MySQL, GoogleSQL)
	BackslashEscapes  bool // backslash is an escape inside single-quoted strings (MySQL, GoogleSQL)
	EPrefixString     bool // E'...' escape-string literals (PostgreSQL)
	DollarQuote       bool // $tag$...$tag$ dollar-quoted strings (PostgreSQL)
	RawStringPrefix   bool // r'...'/r"..." raw strings (GoogleSQL)
	ByteStringPrefix  bool // b'...'/b"..." byte strings (GoogleSQL)
	TripleQuoteString bool // '''...''' and """...""" strings (GoogleSQL)
	NestedComment     bool // /* */ block comments nest (PostgreSQL)
	NumericEscapes    bool // \xHH, \ooo, \uXXXX and \UXXXXXXXX name a character (GoogleSQL)
	AtOperator        bool // @ is the absolute-value operator rather than a parameter prefix (PostgreSQL)
}

// escapeRules says how a backslash behaves inside a quoted literal. The two
// halves are separate because they are configured separately: MySQL honors a
// backslash but has no numeric escapes, while a PostgreSQL E'...' string has
// both even though an ordinary PostgreSQL string has neither.
type escapeRules struct {
	backslash bool
	numeric   bool
}

// ConfigFor returns the lexical configuration for a built-in non-SQLite
// dialect. It reports false for SQLite (handled as identity before tokenizing)
// and for any unknown dialect.
func ConfigFor(d dialects.Dialect) (Config, bool) {
	switch d {
	case dialects.MySQL:
		return Config{
			IdentBacktick:     true,
			StringDoubleQuote: true,
			HashComment:       true,
			BackslashEscapes:  true,
			DashNeedsBlank:    true,
		}, true
	case dialects.PostgreSQL:
		return Config{
			IdentDoubleQuote: true,
			EPrefixString:    true,
			DollarQuote:      true,
			NestedComment:    true,
			AtOperator:       true,
		}, true
	case dialects.GoogleSQL:
		return Config{
			IdentBacktick:     true,
			IdentBacktickEsc:  true,
			StringDoubleQuote: true,
			HashComment:       true,
			BackslashEscapes:  true,
			RawStringPrefix:   true,
			ByteStringPrefix:  true,
			TripleQuoteString: true,
			NumericEscapes:    true,
		}, true
	default:
		return Config{}, false
	}
}

// identEscapes is how a backslash behaves inside a backtick-quoted identifier.
// BigQuery lists the string escapes among what one accepts, an escaped backtick
// with them, so a name written with one closed early when they were not read.
// MySQL has no escapes there: a backtick is doubled and a backslash is a
// backslash.
func (cfg Config) identEscapes() escapeRules {
	if !cfg.IdentBacktickEsc {
		return escapeRules{}
	}
	return escapeRules{backslash: true, numeric: cfg.NumericEscapes}
}

// dashOpensComment reports whether the double dash at s[i] opens a line
// comment. It does in every dialect but MySQL, which asks for a blank or a
// control character after the dashes: "SELECT 1--1" there is one minus negative
// one and not a statement with its tail commented out. Reading it as a comment
// dropped the rest of the line and answered the left operand, which is the kind
// of answer that reads as correct.
func dashOpensComment(s string, i int, cfg Config) bool {
	if !cfg.DashNeedsBlank || i+2 >= len(s) {
		return true
	}
	c := s[i+2]
	return c <= ' ' || c == 0x7f
}

// stringEscapes is how a backslash behaves inside an ordinary quoted string of
// the dialect cfg describes.
func (cfg Config) stringEscapes() escapeRules {
	return escapeRules{backslash: cfg.BackslashEscapes, numeric: cfg.NumericEscapes}
}

// Lex splits query into tokens using cfg. It returns sqlerr.ErrInvalidSyntax
// (wrapped, with the byte offset) when a quoted literal or identifier is not
// terminated.
func Lex(query string, cfg Config) ([]Token, error) {
	tokens := make([]Token, 0, len(query)/4+1)
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
			tokens = append(tokens, Token{Kind: Whitespace, Text: query[i:j], Offset: start})
			i = j

		case c == '-' && next(query, i) == '-' && dashOpensComment(query, i, cfg):
			j := i + 2
			for j < len(query) && query[j] != '\n' {
				j++
			}
			tokens = append(tokens, Token{Kind: LineComment, Text: query[i+2 : j], Offset: start})
			i = j

		case c == '#' && cfg.HashComment:
			j := i + 1
			for j < len(query) && query[j] != '\n' {
				j++
			}
			tokens = append(tokens, Token{Kind: LineComment, Text: query[i+1 : j], Offset: start})
			i = j

		case c == '/' && next(query, i) == '*':
			j, ok := scanBlockComment(query, i, cfg.NestedComment)
			if !ok {
				return nil, lexError(query, start, "unterminated block comment")
			}
			tokens = append(tokens, Token{Kind: BlockComment, Text: query[i+2 : j-2], Offset: start})
			i = j

		case cfg.TripleQuoteString && isTripleQuoteStart(query, i):
			content, ni, ok := scanTripleQuoted(query, i, c, cfg.stringEscapes())
			if !ok {
				return nil, lexError(query, start, "unterminated string literal")
			}
			tokens = append(tokens, Token{Kind: String, Text: content, Offset: start})
			i = ni

		case c == '\'':
			content, ni, ok := scanQuoted(query, i, '\'', cfg.stringEscapes())
			if !ok {
				return nil, lexError(query, start, "unterminated string literal")
			}
			tokens = append(tokens, Token{Kind: String, Text: content, Offset: start})
			i = ni

		case c == '"' && cfg.IdentDoubleQuote:
			content, ni, ok := scanQuoted(query, i, '"', escapeRules{})
			if !ok {
				return nil, lexError(query, start, "unterminated quoted identifier")
			}
			tokens = append(tokens, Token{Kind: QuotedIdent, Text: content, Offset: start})
			i = ni

		case c == '"' && cfg.StringDoubleQuote:
			content, ni, ok := scanQuoted(query, i, '"', cfg.stringEscapes())
			if !ok {
				return nil, lexError(query, start, "unterminated string literal")
			}
			tokens = append(tokens, Token{Kind: String, Text: content, Offset: start})
			i = ni

		case c == '`' && cfg.IdentBacktick:
			content, ni, ok := scanQuoted(query, i, '`', cfg.identEscapes())
			if !ok {
				return nil, lexError(query, start, "unterminated quoted identifier")
			}
			tokens = append(tokens, Token{Kind: QuotedIdent, Text: content, Offset: start})
			i = ni

		case (c == 'x' || c == 'X') && next(query, i) == '\'':
			content, ni, ok := scanQuoted(query, i+1, '\'', escapeRules{})
			if !ok {
				return nil, lexError(query, start, "unterminated blob literal")
			}
			// A blob literal holds hexadecimal digits and nothing else. Accepting
			// anything else rendered it back as x'<content>', which for content
			// carrying a quote is SQL that no longer parses: the caller got a
			// syntax error about text they had not written.
			if !isHexDigits(content) {
				return nil, lexError(query, start, "blob literal is not hexadecimal")
			}
			tokens = append(tokens, Token{Kind: Blob, Text: strings.ToLower(content), Offset: start})
			i = ni

		case cfg.EPrefixString && (c == 'e' || c == 'E') && next(query, i) == '\'':
			// A PostgreSQL escape string decodes both the letter escapes and the
			// numeric ones, whatever an ordinary string in the same dialect does.
			content, ni, ok := scanQuoted(query, i+1, '\'', escapeRules{backslash: true, numeric: true})
			if !ok {
				return nil, lexError(query, start, "unterminated string literal")
			}
			tokens = append(tokens, Token{Kind: String, Text: content, Offset: start})
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
			content, ni, ok := scanStringLiteral(query, i+n, quote, esc, cfg.TripleQuoteString)
			if !ok {
				return nil, lexError(query, start, "unterminated string literal")
			}
			kind, text := String, content
			if isBytes {
				kind, text = Blob, hex.EncodeToString([]byte(content))
			}
			tokens = append(tokens, Token{Kind: kind, Text: text, Offset: start})
			i = ni

		case cfg.DollarQuote && c == '$' && isDollarQuoteStart(query, i):
			content, ni, ok := scanDollarQuoted(query, i)
			if !ok {
				return nil, lexError(query, start, "unterminated dollar-quoted string")
			}
			tokens = append(tokens, Token{Kind: String, Text: content, Offset: start})
			i = ni

		case c == '?':
			j := i + 1
			for j < len(query) && isDigit(query[j]) {
				j++
			}
			tokens = append(tokens, Token{Kind: Placeholder, Text: query[i:j], Offset: start})
			i = j

		case (c == ':' || c == '$' || (c == '@' && !cfg.AtOperator)) && isPlaceholderName(next(query, i)):
			// A bound parameter's name holds the characters SQLite reads as
			// name characters, which include a digit in the first position and
			// a dollar sign anywhere. Stopping short of either split the name
			// the caller wrote into two tokens. PostgreSQL is the exception:
			// it writes a parameter as $1 and spells the absolute value with
			// "@", so "@0" there is the operator on a number.
			j := i + 1
			for j < len(query) && isPlaceholderName(query[j]) {
				j++
			}
			tokens = append(tokens, Token{Kind: Placeholder, Text: query[i:j], Offset: start})
			i = j

		case isDigit(c) || (c == '.' && isDigit(next(query, i))):
			j := scanNumber(query, i)
			tokens = append(tokens, Token{Kind: Number, Text: query[i:j], Offset: start})
			i = j

		case isIdentStart(c):
			j := i + 1
			for j < len(query) && isIdentPart(query[j]) {
				j++
			}
			tokens = append(tokens, Token{Kind: Word, Text: query[i:j], Offset: start})
			i = j

		default:
			op := matchOperator(query, i)
			tokens = append(tokens, Token{Kind: Op, Text: op, Offset: start})
			i += len(op)
		}
		if n := len(tokens); n > 0 && tokens[n-1].End == 0 {
			tokens[n-1].End = i
		}
	}
	assignPositions(query, tokens)
	return tokens, nil
}

// assignPositions fills in the line and column of every token from its byte
// offset. It is a second walk over the source rather than a counter carried
// through the scanner, because the scanner jumps over quoted content in one
// step and a counter there would have to be updated in a dozen places.
func assignPositions(query string, tokens []Token) {
	line, lineStart, at := 1, 0, 0
	for i := range tokens {
		for at < tokens[i].Offset {
			if query[at] == '\n' {
				line++
				lineStart = at + 1
			}
			at++
		}
		tokens[i].Line = line
		tokens[i].Col = tokens[i].Offset - lineStart + 1
	}
}

// PositionOf returns the 1-based line and column of a byte offset in query.
func PositionOf(query string, offset int) (line, col int) {
	line, lineStart := 1, 0
	if offset > len(query) {
		offset = len(query)
	}
	for i := range offset {
		if query[i] == '\n' {
			line++
			lineStart = i + 1
		}
	}
	return line, offset - lineStart + 1
}

// lexError reports a lexical error at a byte offset, with the line and column a
// person needs to find it.
func lexError(query string, offset int, message string) error {
	line, col := PositionOf(query, offset)
	return sqlerr.At(sqlerr.ErrInvalidSyntax, line, col, "%s", message)
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
func prefixLen(s string, i int, cfg Config) int {
	if !cfg.RawStringPrefix && !cfg.ByteStringPrefix {
		return 0
	}
	n := 0
	var sawRaw, sawBytes bool
	for n < 2 && i+n < len(s) {
		switch s[i+n] {
		case 'r', 'R':
			if sawRaw || !cfg.RawStringPrefix {
				return 0
			}
			sawRaw = true
		case 'b', 'B':
			if sawBytes || !cfg.ByteStringPrefix {
				return 0
			}
			sawBytes = true
		default:
			// Not a prefix letter, so the prefix ended before it. A letter
			// already read is an identifier rather than a prefix, since a
			// prefix runs into the quote: "b,'x'" is a column, a comma and a
			// string, not one bytes literal.
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
var multiCharOperators = []string{ //nolint:gochecknoglobals // a fixed table
	// The LIKE aliases are listed before the regex operators they start with,
	// so "~~" is one token rather than two "~" that each become a REGEXP.
	"!~~*", "~~*", "!~~", "~~",
	// PostgreSQL's JSON path operators, listed before "#" so the bitwise XOR
	// does not take the "#" and leave the rest behind.
	"#>>", "#>", "#-",
	// Its cube and square roots, listed before "||" and "|" for the same
	// reason: split, "|/" is a bitwise OR beside a division.
	"||/", "|/",
	// Its JSON containment operators, which are single tokens rather than a
	// comparison beside an "@".
	"@>", "<@", "@?", "@@",
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

// isPlaceholderName reports whether a byte may appear in a bound parameter's
// name. SQLite takes the identifier characters and the dollar sign, and takes
// them in the first position too, so ":1abc" and "@a$b" are each one name.
func isPlaceholderName(c byte) bool {
	return isIdentPart(c) || c == '$'
}

// Significant reports whether the token carries meaning for the grammar.
// Whitespace and comments do not, and the parser skips them.
func (t Token) Significant() bool {
	return t.Kind != Whitespace && t.Kind != LineComment && t.Kind != BlockComment
}

// IsWord reports whether the token is the unquoted word kw, compared without
// regard to case. A quoted identifier is never a keyword, which is what the
// quoting is for.
func (t Token) IsWord(kw string) bool {
	return t.Kind == Word && strings.EqualFold(t.Text, kw)
}

// IsOp reports whether the token is the operator or punctuation op.
func (t Token) IsOp(op string) bool {
	return t.Kind == Op && t.Text == op
}

// String spells the token for a diagnostic: the source text for a word, an
// operator or a number, and a description for a literal whose text has already
// been decoded.
func (t Token) String() string {
	switch t.Kind {
	case Whitespace:
		return "whitespace"
	case LineComment, BlockComment:
		return "comment"
	case String:
		return "string literal"
	case Blob:
		return "blob literal"
	case QuotedIdent:
		return strconv.Quote(t.Text)
	case Word, Number, Op, Placeholder:
		return t.Text
	default:
		return t.Text
	}
}
