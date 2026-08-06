package dialect

import "strings"

// SQLite names an unaliased result column after the text of the expression that
// produced it, so rewriting an expression renames the caller's column. A
// PostgreSQL "amt::text" became "postgresql_cast(amt, 'text')" and reached the
// caller under that name — as a CSV header, as a JSON key, as whatever the
// result was rendered into. The helper is this package's business; the label is
// the caller's.
//
// So a select item the rewrite changed carries its original text as an alias.
// The label is the expression as it was written rather than the name the source
// database would have derived, because that rule is one rule for every dialect:
// PostgreSQL, MySQL, and GoogleSQL each name a bare cast differently, and a
// translation that guessed per dialect would be three rules that still all
// disagree with SQLite. What sqly promises is SQLite semantics over the caller's
// syntax, and the caller's syntax is what the label now shows.

// tokenSpan is a half-open range of token indices.
type tokenSpan struct {
	start int
	end   int
}

// preserveSelectLabels re-labels the select items that rewriting changed, so a
// result column keeps the name the original query gave it. original and
// rewritten are the same statement before and after rewriteTokens.
//
// The two streams are matched by position: a rewrite replaces expressions but
// never adds or removes a select list or a top-level comma, so the n-th item of
// the rewritten stream is the n-th item of the original. Anything that breaks
// that assumption — a differing number of lists or items — returns the rewritten
// stream untouched, because a wrong alias is worse than a rewritten label.
func preserveSelectLabels(original, rewritten []token) []token {
	before := selectListItems(original)
	after := selectListItems(rewritten)
	if len(before) != len(after) || len(before) == 0 {
		return rewritten
	}

	// Walk backwards so an insertion never shifts an index not yet used.
	out := rewritten
	for i := len(before) - 1; i >= 0; i-- {
		label, ok := labelFor(original, before[i], rewritten, after[i])
		if !ok {
			continue
		}
		alias := []token{
			spaceToken(), wordToken("AS"), spaceToken(),
			{kind: tokQuotedIdent, text: label},
		}
		end := lastSignificant(rewritten, after[i]) + 1
		out = insertTokens(out, end, alias)
	}
	return out
}

// labelFor reports the label a select item should carry, and whether it needs
// one at all. An item needs a label when the rewrite changed its text and the
// query did not already name it.
func labelFor(original []token, before tokenSpan, rewritten []token, after tokenSpan) (string, bool) {
	origText := strings.TrimSpace(render(original[before.start:before.end]))
	newText := strings.TrimSpace(render(rewritten[after.start:after.end]))
	if origText == "" || origText == newText {
		return "", false
	}
	if hasAlias(original, before) || isBareColumnRef(original, before) || endsWithStar(original, before) {
		return "", false
	}
	return origText, true
}

// hasAlias reports whether a select item already names its column.
//
// An item ending in a word or a quoted identifier is ambiguous: "amt::text" ends
// in a word that belongs to the expression, while "amt::text label" ends in an
// alias. The token before it decides — an operator means the word is part of the
// expression — and every other shape is read as an alias. That is the safe way
// round: a missed alias leaves a label unimproved, while a wrongly added one
// makes the statement a syntax error.
func hasAlias(tokens []token, span tokenSpan) bool {
	last := lastSignificant(tokens, span)
	if last < 0 {
		return false
	}
	if tokens[last].kind != tokWord && tokens[last].kind != tokQuotedIdent {
		return false
	}
	prev := lastSignificant(tokens, tokenSpan{start: span.start, end: last})
	if prev < 0 {
		return false
	}
	return tokens[prev].kind != tokOp
}

// isBareColumnRef reports whether a select item is nothing but a column
// reference such as "a" or "t"."b". SQLite already labels those with the column
// name, and an alias holding the qualified text would rename them.
func isBareColumnRef(tokens []token, span tokenSpan) bool {
	wantName := true
	seen := false
	for i := span.start; i < span.end; i++ {
		t := tokens[i]
		if !isSignificant(t) {
			continue
		}
		if wantName {
			if t.kind != tokWord && t.kind != tokQuotedIdent {
				return false
			}
			seen = true
		} else if !isOpEq(t, ".") {
			return false
		}
		wantName = !wantName
	}
	return seen && !wantName
}

// endsWithStar reports whether a select item is "*" or "t.*", neither of which
// can take an alias.
func endsWithStar(tokens []token, span tokenSpan) bool {
	last := lastSignificant(tokens, span)
	return last >= 0 && isOpEq(tokens[last], "*")
}

// lastSignificant returns the index of the last non-whitespace, non-comment
// token in span, or -1 when there is none.
func lastSignificant(tokens []token, span tokenSpan) int {
	for i := span.end - 1; i >= span.start; i-- {
		if isSignificant(tokens[i]) {
			return i
		}
	}
	return -1
}

// insertTokens returns tokens with extra spliced in at index at.
func insertTokens(tokens []token, at int, extra []token) []token {
	out := make([]token, 0, len(tokens)+len(extra))
	out = append(out, tokens[:at]...)
	out = append(out, extra...)
	out = append(out, tokens[at:]...)
	return out
}

// selectListItems returns the span of every top-level item of every select list
// in the statement, in the order the items appear. A subquery contributes its
// own items, which is what makes the result of a derived table keep its labels
// too.
func selectListItems(tokens []token) []tokenSpan {
	var items []tokenSpan
	depth := 0
	for i := range tokens {
		switch {
		case isOpEq(tokens[i], "("):
			depth++
		case isOpEq(tokens[i], ")"):
			depth--
		case tokens[i].kind == tokWord && strings.EqualFold(tokens[i].text, "SELECT"):
			items = append(items, parseSelectList(tokens, i+1, depth)...)
		}
	}
	return items
}

// selectListEnd names the keywords that end a select list. WHERE, GROUP,
// HAVING, ORDER, and LIMIT cannot legally precede FROM, but ending on them costs
// nothing and keeps a malformed statement from swallowing the rest of the query
// as one select item.
var selectListEnd = map[string]bool{
	"FROM": true, "INTO": true, "WHERE": true, "GROUP": true, "HAVING": true,
	"ORDER": true, "LIMIT": true, "WINDOW": true,
	"UNION": true, "INTERSECT": true, "EXCEPT": true,
}

// parseSelectList splits the select list starting at from into its top-level
// items. baseDepth is the parenthesis depth the SELECT itself sits at; the list
// ends where the depth drops below it, at a keyword that closes the list, or at
// the end of the statement.
func parseSelectList(tokens []token, from, baseDepth int) []tokenSpan {
	// A leading DISTINCT or ALL is a modifier, not the first item.
	if i := nextSig(tokens, from); i >= 0 && tokens[i].kind == tokWord {
		if strings.EqualFold(tokens[i].text, "DISTINCT") || strings.EqualFold(tokens[i].text, "ALL") {
			from = i + 1
		}
	}

	var items []tokenSpan
	depth := baseDepth
	start := from
	for i := from; i < len(tokens); i++ {
		t := tokens[i]
		switch {
		case isOpEq(t, "("):
			depth++
			continue
		case isOpEq(t, ")"):
			depth--
			if depth < baseDepth {
				return appendSpan(items, start, i)
			}
			continue
		}
		if depth != baseDepth {
			continue
		}
		if isOpEq(t, ";") {
			return appendSpan(items, start, i)
		}
		if t.kind == tokWord && selectListEnd[strings.ToUpper(t.text)] {
			return appendSpan(items, start, i)
		}
		if isOpEq(t, ",") {
			items = appendSpan(items, start, i)
			start = i + 1
		}
	}
	return appendSpan(items, start, len(tokens))
}

// appendSpan adds a span unless it is empty, which is what a select list with
// no items leaves behind.
func appendSpan(items []tokenSpan, start, end int) []tokenSpan {
	if start >= end {
		return items
	}
	return append(items, tokenSpan{start: start, end: end})
}
