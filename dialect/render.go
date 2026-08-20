package dialect

import "strings"

// render serializes tokens back into SQLite SQL. Quoted identifiers, strings,
// and blobs are re-encoded in SQLite form (double-quoted identifiers,
// single-quoted strings, x'..' blobs); every other token is written verbatim.
// Whitespace runs collapse to a single space, and line comments are emitted with
// a trailing newline so a following token can never be swallowed by the comment.
func render(tokens []token) string {
	var b strings.Builder
	prev := tokWhitespace
	for _, t := range tokens {
		// Two tokens the input kept apart have to stay apart. A quote is what
		// separated them there and is no longer what separates them here: MySQL
		// reads `X"41"` as the word X and the string 41, and written back with
		// nothing between them the two read as x'41', a blob literal the caller
		// never wrote. A space between them costs nothing and cannot fuse.
		if isAtom(prev) && isAtom(t.kind) {
			b.WriteByte(' ')
		}
		prev = t.kind
		switch t.kind {
		case tokWhitespace:
			b.WriteByte(' ')
		case tokLineComment:
			b.WriteString("--")
			b.WriteString(t.text)
			b.WriteByte('\n')
		case tokBlockComment:
			b.WriteString("/*")
			b.WriteString(commentBody(t.text))
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

// isAtom reports whether a token of this kind is a value or a name: the kinds
// that two of, written next to each other, can be read as one.
func isAtom(kind tokenKind) bool {
	switch kind {
	case tokWord, tokNumber, tokString, tokBlob, tokQuotedIdent, tokPlaceholder:
		return true
	default:
		return false
	}
}

// commentBody renders the text of a block comment so it cannot end the comment
// early. SQLite's comments do not nest, so a comment body carrying a "*/" —
// which a PostgreSQL nested comment has — would close the rendered comment
// there and leave the rest of the body as statement text. A space between the
// two characters keeps the body readable and inert.
func commentBody(text string) string {
	if !strings.Contains(text, "*/") && !strings.Contains(text, "/*") {
		return text
	}
	// Decided against what has been written rather than against the input: a
	// replacement can put a slash next to a star that was not next to one
	// before, and a rule that reads the input misses the pair it just created.
	var b strings.Builder
	b.Grow(len(text) + 4)
	var last byte
	for i := range len(text) {
		c := text[i]
		if (last == '*' && c == '/') || (last == '/' && c == '*') {
			b.WriteByte(' ')
		}
		b.WriteByte(c)
		last = c
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
