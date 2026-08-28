package parser

import (
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"unicode"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/token"
)

// typedLiteralTypes are the type words that may introduce a string literal:
// DATE '2024-01-01' and its relatives. A word here followed by a string is a
// typed literal; followed by anything else it is an ordinary name.
var typedLiteralTypes = map[string]bool{ //nolint:gochecknoglobals // a fixed table
	"DATE": true, kwTime: true, kwTimestamp: true, "DATETIME": true,
	"INTERVAL": true, "NUMERIC": true, "BIGNUMERIC": true, "DECIMAL": true,
	"JSON": true, "BOOL": true, "BOOLEAN": true, "BYTES": true,
}

// parsePrimary reads a literal, a name, a call, a parenthesized expression or a
// subquery.
func (p *Parser) parsePrimary() (ast.Expr, error) {
	t := p.cur()
	span := ast.SpanOf(t)

	switch t.Kind {
	case token.Number:
		p.pos++
		return &ast.Literal{Kind: numberKind(t.Text), Value: t.Text, Span: span}, nil

	case token.String:
		p.pos++
		return &ast.Literal{Kind: ast.LitString, Value: t.Text, Span: span}, nil

	case token.Blob:
		p.pos++
		return &ast.Literal{Kind: ast.LitBlob, Value: t.Text, Span: span}, nil

	case token.Placeholder:
		p.pos++
		return &ast.Placeholder{Text: t.Text, Span: span}, nil

	case token.QuotedIdent:
		return p.parseNameOrCall()

	case token.Op:
		switch t.Text {
		case "(":
			return p.parseParenExpr()
		case "*":
			p.pos++
			if p.dialect == dialects.GoogleSQL && p.atAnyWord("EXCEPT", "REPLACE") {
				return nil, p.unsupportedf(
					"the %s clause of SELECT * is not supported; SQLite has no way to name the columns a star stands for",
					upper(p.cur().Text))
			}
			return &ast.Star{Span: span}, nil
		}

	case token.Word:
		return p.parseWordPrimary()

	case token.Whitespace, token.LineComment, token.BlockComment:
		// Not reachable: trivia is dropped before parsing.
	}
	return nil, p.unexpected("an expression")
}

// numberKind tells a hexadecimal or binary literal from a decimal one.
func numberKind(text string) ast.LiteralKind {
	if len(text) > 2 && text[0] == '0' {
		switch text[1] {
		case 'x', 'X':
			return ast.LitHex
		case 'b', 'B':
			return ast.LitBit
		}
	}
	return ast.LitNumber
}

// parseWordPrimary reads a primary that starts with a bare word: a keyword
// literal, a typed literal, a charset introducer, a name or a call.
func (p *Parser) parseWordPrimary() (ast.Expr, error) {
	t := p.cur()
	span := ast.SpanOf(t)
	word := upper(t.Text)

	switch word {
	case "NULL":
		p.pos++
		return &ast.Literal{Kind: ast.LitNull, Span: span}, nil
	case "TRUE", "FALSE":
		p.pos++
		return &ast.Literal{Kind: ast.LitBool, Value: word, Span: span}, nil
	case "UNKNOWN":
		// The SQL truth value, which is NULL. It is a literal only where a
		// value is wanted; as a name it would have been quoted.
		p.pos++
		return &ast.Literal{Kind: ast.LitNull, Span: span}, nil
	case kwDefault:
		p.pos++
		return &ast.Ident{Name: kwDefault, Span: span}, nil
	}

	// A clause keyword cannot begin a value. Reading one as a column name made
	// "SELECT b + FROM t" a sum of b and a column called FROM, with t as its
	// alias: a query that stops in the middle parsed, and what reached SQLite
	// was a different query.
	if clauseOnlyWords[word] {
		return nil, p.unexpected("an expression")
	}

	// An operator spelled as a word is not a name. MySQL reserves these, so a
	// query using one where a value belongs is malformed rather than a
	// reference to a column of that name.
	if p.dialect == dialects.MySQL && operatorWords[word] {
		return nil, p.unexpected("an expression")
	}

	// A type word in front of a string is a typed literal.
	if typedLiteralTypes[word] && p.peek(1).Kind == token.String {
		p.pos += 2
		return &ast.TypedLiteral{Type: word, Value: p.toks[p.pos-1].Text, Span: span}, nil
	}

	// MySQL's charset introducer and PostgreSQL's bit-string literal, both of
	// which are a word standing in front of a literal.
	if lit, ok, err := p.parsePrefixedLiteral(); ok || err != nil {
		return lit, err
	}

	return p.parseNameOrCall()
}

// clauseOnlyWords are the words that open a clause and can never begin a value.
// A name among them has to be quoted, which SQLite requires of them too.
var clauseOnlyWords = map[string]bool{ //nolint:gochecknoglobals // a fixed table
	"FROM": true, "WHERE": true, "GROUP": true, "HAVING": true, "WINDOW": true,
	"ORDER": true, "LIMIT": true, "OFFSET": true, "FETCH": true, "UNION": true,
	"INTERSECT": true, "EXCEPT": true, "ON": true, "USING": true, "JOIN": true,
	"INNER": true, "CROSS": true, "NATURAL": true, "SET": true, "VALUES": true,
	"INTO": true, "THEN": true, "ELSE": true, kwEnd: true, "WHEN": true,
	kwAnd: true, "OR": true, "AS": true, "ASC": true, "DESC": true,
	"RETURNING": true, "QUALIFY": true, "DO": true, "NOTHING": true,
}

// operatorWords are the words MySQL reads as operators, which therefore cannot
// stand where a value belongs.
var operatorWords = map[string]bool{ //nolint:gochecknoglobals // a fixed table
	"DIV": true, "XOR": true,
}

// parsePrefixedLiteral reads the literal forms written as a word in front of a
// string: MySQL's _utf8'x' and N'x', and PostgreSQL's B'1010' and X'41'.
func (p *Parser) parsePrefixedLiteral() (ast.Expr, bool, error) {
	if lit, ok, err := p.parseUnicodeEscapeLiteral(); ok || err != nil {
		return lit, ok, err
	}
	t := p.cur()
	span := ast.SpanOf(t)
	if p.peek(1).Kind != token.String {
		return nil, false, nil
	}
	word := upper(t.Text)
	value := p.peek(1).Text

	switch p.dialect {
	case dialects.MySQL, dialects.GoogleSQL:
		switch {
		case word == "B":
			// MySQL's b'1010' is a bit literal, which it reads as a number in
			// an arithmetic context and as bytes elsewhere.
			p.pos += 2
			return &ast.Literal{Kind: ast.LitBit, Value: value, Span: span}, true, nil
		case word == "N":
			p.pos += 2
			return &ast.Literal{Kind: ast.LitString, Value: value, Span: span}, true, nil
		case word == "_BINARY":
			// The introducer that makes a literal its own bytes.
			p.pos += 2
			return &ast.Literal{
				Kind: ast.LitBlob, Value: hex.EncodeToString([]byte(value)), Span: span,
			}, true, nil
		case strings.HasPrefix(word, "_"):
			p.pos += 2
			return &ast.Literal{
				Kind: ast.LitString, Value: value, Span: span,
			}, true, charsetIntroducer(word[1:], t)
		}
	case dialects.PostgreSQL:
		switch word {
		case "B", "X":
			// PostgreSQL's B'1010' and X'41' are bit strings, one written in
			// binary and the other in hexadecimal: both compare and
			// concatenate as text.
			p.pos += 2
			return &ast.Literal{Kind: ast.LitBit, Value: value, Span: span}, true, nil
		case "U&":
			// Not reachable: the lexer does not produce this word.
		}
	case dialects.SQLite:
		// Not reachable: SQLite is translated as the identity.
	}
	return nil, false, nil
}

// parseUnicodeEscapeLiteral reads PostgreSQL's U&'...' literal, whose backslash
// escapes name characters by code point, together with the UESCAPE clause that
// may replace the backslash. Read as a name beside a string, the whole literal
// became a bitwise AND of a column called U with an ordinary string.
func (p *Parser) parseUnicodeEscapeLiteral() (ast.Expr, bool, error) {
	if p.dialect != dialects.PostgreSQL {
		return nil, false, nil
	}
	t := p.cur()
	if !t.IsWord("U") || !p.peek(1).IsOp("&") || p.peek(2).Kind != token.String {
		return nil, false, nil
	}
	if p.peek(1).Offset != t.End || p.peek(2).Offset != p.peek(1).End {
		// The three have to be written against each other; with a space
		// between them this is a bitwise AND.
		return nil, false, nil
	}
	span := ast.SpanOf(t)
	raw := p.peek(2).Text
	p.pos += 3
	escape := byte('\\')
	if p.atWord("UESCAPE") {
		p.pos++
		e := p.cur()
		if e.Kind != token.String || len(e.Text) != 1 {
			return nil, false, p.unexpected("a one-character escape")
		}
		p.pos++
		escape = e.Text[0]
	}
	decoded, err := decodeUnicodeEscapes(raw, escape)
	if err != nil {
		return nil, false, unsupportedAt(t, "%s", err.Error())
	}
	return &ast.Literal{Kind: ast.LitString, Value: decoded, Span: span}, true, nil
}

// decodeUnicodeEscapes replaces the escape sequences of a U&'...' literal with
// the characters they name: XXXX for a code point in the basic plane, +XXXXXX
// for one outside it, and a doubled escape character for the character itself.
func decodeUnicodeEscapes(s string, escape byte) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] != escape {
			b.WriteByte(s[i])
			i++
			continue
		}
		switch {
		case i+1 < len(s) && s[i+1] == escape:
			b.WriteByte(escape)
			i += 2
		case i+6 < len(s) && s[i+1] == '+':
			r, err := strconv.ParseUint(s[i+2:i+8], 16, 32)
			if err != nil || r > unicode.MaxRune {
				return "", errors.New("a Unicode escape names no character")
			}
			b.WriteRune(rune(r))
			i += 8
		case i+4 < len(s):
			r, err := strconv.ParseUint(s[i+1:i+5], 16, 32)
			if err != nil || r > unicode.MaxRune {
				return "", errors.New("a Unicode escape names no character")
			}
			b.WriteRune(rune(r))
			i += 5
		default:
			return "", errors.New("a Unicode escape is cut short")
		}
	}
	return b.String(), nil
}

// charsetIntroducer reports whether a charset introducer names an encoding the
// literal already is. SQLite holds text as UTF-8 and has no second encoding to
// convert to, so an introducer naming anything else changes the bytes in a way
// that cannot be reproduced.
func charsetIntroducer(name string, t token.Token) error {
	switch strings.ToLower(name) {
	case "utf8", "utf8mb3", "utf8mb4", "ucs2", "utf16", "utf32", "binary":
		return nil
	default:
		return unsupportedAt(t, "the character set introducer _%s is not supported; SQLite holds text as UTF-8", name)
	}
}

// parseNameOrCall reads a qualified name, which may turn out to be a function
// call or a star.
func (p *Parser) parseNameOrCall() (ast.Expr, error) {
	span := p.span()
	parts, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	// "t.*" in a select list.
	if p.atOp("*") && len(parts) > 0 && p.prevWasDot() {
		p.pos++
		return &ast.Star{Qualifier: parts, Span: span}, nil
	}
	if p.atOp("(") {
		return p.parseCall(parts, span)
	}
	if len(parts) == 1 {
		return &ast.ColumnRef{Parts: parts, Span: span}, nil
	}
	return &ast.ColumnRef{Parts: parts, Span: span}, nil
}

// prevWasDot reports whether the token before the cursor is a dot, which is
// what makes a following star a qualified star rather than multiplication.
func (p *Parser) prevWasDot() bool {
	return p.pos > 0 && p.toks[p.pos-1].IsOp(".")
}

// parseQualifiedName reads a dotted name. A star after a dot ends the name and
// is left for the caller.
func (p *Parser) parseQualifiedName() ([]ast.Ident, error) {
	var parts []ast.Ident
	for {
		t := p.cur()
		switch t.Kind {
		case token.Word:
			parts = append(parts, ast.Ident{Name: t.Text, Span: ast.SpanOf(t)})
		case token.QuotedIdent:
			parts = append(parts, ast.Ident{Name: t.Text, Quoted: true, Span: ast.SpanOf(t)})
		default:
			return nil, p.unexpected("a name")
		}
		p.pos++
		if !p.atOp(".") {
			return parts, nil
		}
		p.pos++
		if p.atOp("*") {
			return parts, nil
		}
	}
}

// parseCall reads a function or aggregate call whose name has been read.
func (p *Parser) parseCall(name []ast.Ident, span ast.Span) (ast.Expr, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()

	p.pos++ // (
	call := &ast.FuncCall{Name: name, Span: span}

	switch {
	case p.atOp("*"):
		p.pos++
		call.Star = true
	case p.atOp(")"):
		// A call with no arguments.
	default:
		if p.eatWord("DISTINCT") {
			call.Distinct = true
		} else {
			p.eatWord("ALL")
		}
		if err := p.parseCallArgs(call); err != nil {
			return nil, err
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	if err := p.parseCallSuffixes(call); err != nil {
		return nil, err
	}
	return call, nil
}

// parseCallArgs reads the argument list and the clauses that may sit inside the
// parentheses of an aggregate.
func (p *Parser) parseCallArgs(call *ast.FuncCall) error {
	if done, err := p.parseKeywordCall(call); done || err != nil {
		return err
	}
	for {
		// A named argument, which GoogleSQL writes as "name => value". The name
		// selects an optional parameter; lowering decides what each means, so
		// the name is kept beside the value rather than dropped.
		if p.cur().Kind == token.Word && p.peek(1).IsOp("=>") {
			name := p.advance()
			p.pos++
			value, err := p.parseExpr(precLowest)
			if err != nil {
				return err
			}
			call.ArgNames = append(call.ArgNames, argNameAt(len(call.Args), upper(name.Text)))
			call.Args = append(call.Args, value)
			if !p.eatOp(",") {
				break
			}
			continue
		}
		arg, err := p.parseExpr(precLowest)
		if err != nil {
			return err
		}
		call.Args = append(call.Args, arg)
		if !p.eatOp(",") {
			break
		}
	}
	if p.eatWords("ORDER", "BY") {
		terms, err := p.parseOrderTerms()
		if err != nil {
			return err
		}
		call.OrderBy = terms
	}
	if p.eatWord("SEPARATOR") {
		sep, err := p.parseExpr(precLowest)
		if err != nil {
			return err
		}
		call.Separator = sep
	}
	if p.eatWord("LIMIT") {
		limit, err := p.parseExpr(precLowest)
		if err != nil {
			return err
		}
		call.Limit = limit
	}
	if p.eatWord("USING") {
		// CONVERT(x USING charset) and CHAR(n USING charset), whose last
		// argument names an encoding rather than a value.
		charset := p.cur()
		if charset.Kind != token.Word && charset.Kind != token.String {
			return p.unexpected("a character set name")
		}
		p.pos++
		call.Args = append(call.Args, &ast.Literal{
			Kind: ast.LitString, Value: upper(charset.Text), Span: ast.SpanOf(charset),
		})
		if callName(call) == "CHAR" {
			call.Syntax = ast.CallCharUsing
		} else {
			call.Syntax = ast.CallConvertUsing
		}
	}
	return nil
}

// argNameAt records the name written on the argument at an index.
func argNameAt(index int, name string) ast.ArgName {
	return ast.ArgName{Index: index, Name: name}
}

// parseKeywordCall reads the calls whose arguments keywords separate. It
// reports whether it read one; the rest are ordinary comma-separated calls.
//
// These forms are SQL's own, not a dialect's invention, and reading them here
// is what keeps the FROM of an EXTRACT from being mistaken for the FROM of a
// query.
func (p *Parser) parseKeywordCall(call *ast.FuncCall) (bool, error) {
	switch callName(call) {
	case "EXTRACT":
		return true, p.parseExtractCall(call)
	case "TRIM":
		return p.parseTrimCall(call)
	case "OVERLAY":
		return p.parseOverlayCall(call)
	case "POSITION":
		return p.parsePositionCall(call)
	case "SUBSTRING", "SUBSTR":
		return p.parseSubstringCall(call)
	default:
		return false, nil
	}
}

// parseExtractCall reads EXTRACT(unit FROM x), whose unit is a bare word.
func (p *Parser) parseExtractCall(call *ast.FuncCall) error {
	t := p.cur()
	if t.Kind != token.Word {
		return p.unexpected("a date part")
	}
	p.pos++
	unit := upper(t.Text)
	// GoogleSQL writes the day a week starts on inside the part:
	// EXTRACT(WEEK(MONDAY) FROM d).
	if p.atOp("(") && (unit == "WEEK" || unit == "ISOWEEK") {
		p.pos++
		day := p.cur()
		if day.Kind != token.Word {
			return p.unexpected("a weekday")
		}
		p.pos++
		unit += "(" + upper(day.Text) + ")"
		if err := p.expectOp(")"); err != nil {
			return err
		}
	}
	call.Syntax = ast.CallExtract
	call.Args = append(call.Args, &ast.Literal{Kind: ast.LitString, Value: unit, Span: ast.SpanOf(t)})
	if err := p.expectWord("FROM"); err != nil {
		return err
	}
	value, err := p.parseExpr(precLowest)
	if err != nil {
		return err
	}
	call.Args = append(call.Args, value)
	return nil
}

// parseTrimCall reads the SQL-standard TRIM, whose side is a keyword and whose
// characters are separated from the subject by FROM.
func (p *Parser) parseTrimCall(call *ast.FuncCall) (bool, error) {
	syntax := ast.CallTrimBoth
	sided := false
	switch {
	case p.eatWord("LEADING"):
		syntax, sided = ast.CallTrimLeading, true
	case p.eatWord("TRAILING"):
		syntax, sided = ast.CallTrimTrailing, true
	case p.eatWord("BOTH"):
		syntax, sided = ast.CallTrimBoth, true
	}
	if !sided && !p.atWord("FROM") {
		// Look ahead for the FROM that makes this the standard form rather
		// than MySQL's TRIM(x) or TRIM(chars, x).
		save := p.pos
		first, err := p.parseExpr(precLowest)
		if err != nil {
			return false, err
		}
		if !p.atWord("FROM") {
			p.pos = save
			return false, nil
		}
		p.pos++
		subject, err := p.parseExpr(precLowest)
		if err != nil {
			return false, err
		}
		call.Syntax = ast.CallTrimBoth
		call.Args = append(call.Args, subject, first)
		return true, nil
	}
	call.Syntax = syntax
	var chars ast.Expr
	if !p.atWord("FROM") {
		e, err := p.parseExpr(precLowest)
		if err != nil {
			return false, err
		}
		chars = e
	}
	if err := p.expectWord("FROM"); err != nil {
		return false, err
	}
	subject, err := p.parseExpr(precLowest)
	if err != nil {
		return false, err
	}
	call.Args = append(call.Args, subject)
	if chars != nil {
		call.Args = append(call.Args, chars)
	}
	return true, nil
}

// parseOverlayCall reads OVERLAY(x PLACING y FROM n [FOR m]).
func (p *Parser) parseOverlayCall(call *ast.FuncCall) (bool, error) {
	save := p.pos
	subject, err := p.parseExpr(precLowest)
	if err != nil {
		return false, err
	}
	if !p.eatWord("PLACING") {
		p.pos = save
		return false, nil
	}
	replacement, err := p.parseExpr(precLowest)
	if err != nil {
		return false, err
	}
	if err := p.expectWord("FROM"); err != nil {
		return false, err
	}
	from, err := p.parseExpr(precLowest)
	if err != nil {
		return false, err
	}
	call.Syntax = ast.CallOverlay
	call.Args = append(call.Args, subject, replacement, from)
	if p.eatWord("FOR") {
		count, err := p.parseExpr(precLowest)
		if err != nil {
			return false, err
		}
		call.Args = append(call.Args, count)
	}
	return true, nil
}

// parsePositionCall reads POSITION(a IN b), whose comma-separated spelling is
// an ordinary call.
func (p *Parser) parsePositionCall(call *ast.FuncCall) (bool, error) {
	save := p.pos
	needle, err := p.parseExpr(precCompare + 1)
	if err != nil {
		return false, err
	}
	if !p.eatWord("IN") {
		p.pos = save
		return false, nil
	}
	haystack, err := p.parseExpr(precLowest)
	if err != nil {
		return false, err
	}
	call.Syntax = ast.CallPositionIn
	call.Args = append(call.Args, needle, haystack)
	return true, nil
}

// parseSubstringCall reads SUBSTRING(x FROM a [FOR b]), whose comma-separated
// spelling is an ordinary call.
func (p *Parser) parseSubstringCall(call *ast.FuncCall) (bool, error) {
	save := p.pos
	subject, err := p.parseExpr(precLowest)
	if err != nil {
		return false, err
	}
	if p.eatWord("SIMILAR") {
		// The SQL-standard SUBSTRING ... SIMILAR, which returns the part of the
		// subject that the pattern's escaped double quotes mark.
		pattern, err := p.parseExpr(precLowest)
		if err != nil {
			return false, err
		}
		if err := p.expectWord("ESCAPE"); err != nil {
			return false, err
		}
		escape, err := p.parseExpr(precLowest)
		if err != nil {
			return false, err
		}
		call.Syntax = ast.CallSubstringSimilar
		call.Args = append(call.Args, subject, pattern, escape)
		return true, nil
	}
	if !p.atWord("FROM") && !p.atWord("FOR") {
		p.pos = save
		return false, nil
	}
	call.Syntax = ast.CallSubstringFrom
	call.Args = append(call.Args, subject)
	if p.eatWord("FROM") {
		from, err := p.parseExpr(precLowest)
		if err != nil {
			return false, err
		}
		call.Args = append(call.Args, from)
	} else {
		// SUBSTRING(x FOR n) starts at the beginning.
		call.Args = append(call.Args, &ast.Literal{Kind: ast.LitNumber, Value: "1", Span: call.Span})
	}
	if p.eatWord("FOR") {
		count, err := p.parseExpr(precLowest)
		if err != nil {
			return false, err
		}
		call.Args = append(call.Args, count)
	}
	return true, nil
}

// parseCallSuffixes reads the clauses that follow a call's parentheses.
func (p *Parser) parseCallSuffixes(call *ast.FuncCall) error {
	if p.eatWords("WITHIN", "GROUP") {
		if err := p.expectOp("("); err != nil {
			return err
		}
		if !p.eatWords("ORDER", "BY") {
			return p.unexpected("ORDER BY")
		}
		terms, err := p.parseOrderTerms()
		if err != nil {
			return err
		}
		call.WithinGroup = terms
		if err := p.expectOp(")"); err != nil {
			return err
		}
	}
	if p.eatWord("FILTER") {
		if err := p.expectOp("("); err != nil {
			return err
		}
		if err := p.expectWord("WHERE"); err != nil {
			return err
		}
		cond, err := p.parseExpr(precLowest)
		if err != nil {
			return err
		}
		call.Filter = cond
		if err := p.expectOp(")"); err != nil {
			return err
		}
	}
	if p.eatWord("OVER") {
		spec, err := p.parseWindowSpec()
		if err != nil {
			return err
		}
		call.Over = spec
	}
	return nil
}

// callName is the function's own name, without its qualification, upper-cased.
func callName(call *ast.FuncCall) string {
	if len(call.Name) == 0 {
		return ""
	}
	return upper(call.Name[len(call.Name)-1].Name)
}

// parseParenExpr reads what follows an opening parenthesis: a subquery, a row
// constructor, or a parenthesized expression.
func (p *Parser) parseParenExpr() (ast.Expr, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()

	span := p.span()
	p.pos++ // (
	if p.startsSelect() {
		sub, err := p.parseSelectStmt()
		if err != nil {
			return nil, err
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		return &ast.SubqueryExpr{Sub: sub, Span: span}, nil
	}
	first, err := p.parseExpr(precLowest)
	if err != nil {
		return nil, err
	}
	if !p.atOp(",") {
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		return &ast.ParenExpr{Expr: first, Span: span}, nil
	}
	exprs := []ast.Expr{first}
	for p.eatOp(",") {
		e, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, e)
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return &ast.RowExpr{Exprs: exprs, Span: span}, nil
}

// parseParenSelect reads a parenthesized SELECT, which is what EXISTS and a
// scalar subquery take.
func (p *Parser) parseParenSelect() (*ast.SelectStmt, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	sub, err := p.parseSelectStmt()
	if err != nil {
		return nil, err
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return sub, nil
}

// unnestAt refuses GoogleSQL's UNNEST, which turns an array into rows.
func (p *Parser) unnestAt() bool {
	return p.dialect == dialects.GoogleSQL && p.atWord("UNNEST")
}

// startsSelect reports whether a query expression begins at the cursor.
func (p *Parser) startsSelect() bool {
	if p.atAnyWord("SELECT", "WITH", "VALUES", "TABLE") {
		return true
	}
	// A parenthesized query expression, as in "((SELECT 1) UNION ...)".
	return p.atOp("(") && p.peek(1).IsWord("SELECT")
}
