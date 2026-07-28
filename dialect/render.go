package dialect

import "strings"

// render serializes tokens back into SQLite SQL. Quoted identifiers, strings,
// and blobs are re-encoded in SQLite form (double-quoted identifiers,
// single-quoted strings, x'..' blobs); every other token is written verbatim.
// Whitespace runs collapse to a single space, and line comments are emitted with
// a trailing newline so a following token can never be swallowed by the comment.
func render(tokens []token) string {
	var b strings.Builder
	for _, t := range tokens {
		switch t.kind {
		case tokWhitespace:
			b.WriteByte(' ')
		case tokLineComment:
			b.WriteString("--")
			b.WriteString(t.text)
			b.WriteByte('\n')
		case tokBlockComment:
			b.WriteString("/*")
			b.WriteString(t.text)
			b.WriteString("*/")
		case tokQuotedIdent:
			b.WriteString(quoteIdent(t.text))
		case tokString:
			b.WriteString(quoteString(t.text))
		case tokBlob:
			b.WriteString("x'")
			b.WriteString(t.text)
			b.WriteByte('\'')
		case tokWord, tokNumber, tokOp, tokPlaceholder:
			b.WriteString(t.text)
		}
	}
	return b.String()
}

// quoteIdent renders name as a SQLite double-quoted identifier, doubling any
// embedded double quote.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// quoteString renders value as a SQLite single-quoted string literal, doubling
// any embedded single quote.
func quoteString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
