// Package parser builds a typed syntax tree from SQL text. Expression parsing
// is a Pratt parser over one precedence table per dialect, so precedence and
// associativity are decided in a single place; statement parsing is recursive
// descent.
//
// The parser implements a stated subset of SQL rather than any dialect in full.
// A construct it does not model is refused with ErrUnsupportedFeature, never
// forwarded to the caller untranslated: what this package accepts is what it
// has been taught, not what SQLite happens to tolerate.
package parser

import (
	"strings"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/sqlerr"
	"github.com/nao1215/filesql/dialect/internal/token"
)

// The SQL keywords this package compares against more than twice, spelled once.
const (
	kwTime      = "TIME"
	kwTimestamp = "TIMESTAMP"
	kwZone      = "ZONE"
	kwAnd       = "AND"
	kwEnd       = "END"
	kwDefault   = "DEFAULT"
	kwWith      = "WITH"
)

// maxDepth bounds how deeply expressions and subqueries may nest. SQL text can
// arrive from anywhere, and a recursive-descent parser on a query that is ten
// thousand parentheses deep would exhaust the goroutine stack rather than
// answer. The limit is far above any query a person writes and far below the
// stack.
const maxDepth = 200

// Parser reads a token stream into a syntax tree.
type Parser struct {
	dialect dialects.Dialect
	toks    []token.Token
	pos     int
	src     string
	depth   int
}

// Parse reads one statement, which must be the whole of the query apart from a
// trailing semicolon.
func Parse(d dialects.Dialect, query string) (ast.Stmt, error) {
	cfg, ok := token.ConfigFor(d)
	if !ok {
		return nil, sqlerr.At(dialects.ErrUnknownDialect, 0, 0, "%q", string(d))
	}
	raw, err := token.Lex(query, cfg)
	if err != nil {
		return nil, err
	}
	p := &Parser{dialect: d, toks: significant(raw), src: query}
	if len(p.toks) == 0 {
		return nil, sqlerr.At(sqlerr.ErrInvalidSyntax, 1, 1, "the query holds no executable statement")
	}
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	p.skipSemicolons()
	if !p.atEnd() {
		return nil, p.unexpected("end of statement")
	}
	return stmt, nil
}

// ParseExpr reads one expression and nothing else. It exists for tests and for
// callers that translate a fragment rather than a statement.
func ParseExpr(d dialects.Dialect, text string) (ast.Expr, error) {
	cfg, ok := token.ConfigFor(d)
	if !ok {
		return nil, sqlerr.At(dialects.ErrUnknownDialect, 0, 0, "%q", string(d))
	}
	raw, err := token.Lex(text, cfg)
	if err != nil {
		return nil, err
	}
	p := &Parser{dialect: d, toks: significant(raw), src: text}
	expr, err := p.parseExpr(precLowest)
	if err != nil {
		return nil, err
	}
	if !p.atEnd() {
		return nil, p.unexpected("end of expression")
	}
	return expr, nil
}

// significant drops the tokens the grammar does not see. Comments and
// whitespace carry no meaning for a translation whose output is generated
// rather than edited, and keeping them would put a "skip trivia" step in front
// of every lookahead in the parser.
func significant(toks []token.Token) []token.Token {
	out := make([]token.Token, 0, len(toks))
	for _, t := range toks {
		if t.Significant() {
			out = append(out, t)
		}
	}
	return out
}

// cur is the token under the cursor. Past the end it is a zero token, whose
// kind matches nothing, so every caller can look without checking first.
func (p *Parser) cur() token.Token {
	if p.pos < len(p.toks) {
		return p.toks[p.pos]
	}
	return token.Token{Kind: token.Whitespace, Line: p.endLine(), Col: p.endCol()}
}

// peek is the token n places ahead of the cursor.
func (p *Parser) peek(n int) token.Token {
	if p.pos+n < len(p.toks) {
		return p.toks[p.pos+n]
	}
	return token.Token{Kind: token.Whitespace}
}

// endLine and endCol name the place just past the last token, which is where a
// diagnostic about a query that stops too early belongs.
func (p *Parser) endLine() int {
	if len(p.toks) == 0 {
		return 1
	}
	return p.toks[len(p.toks)-1].Line
}

func (p *Parser) endCol() int {
	if len(p.toks) == 0 {
		return 1
	}
	last := p.toks[len(p.toks)-1]
	return last.Col + len(last.Text)
}

func (p *Parser) atEnd() bool { return p.pos >= len(p.toks) }

func (p *Parser) advance() token.Token {
	t := p.cur()
	p.pos++
	return t
}

// atWord reports whether the cursor is on the unquoted keyword kw.
func (p *Parser) atWord(kw string) bool { return p.cur().IsWord(kw) }

// atAnyWord reports whether the cursor is on any of the keywords.
func (p *Parser) atAnyWord(kws ...string) bool {
	for _, kw := range kws {
		if p.cur().IsWord(kw) {
			return true
		}
	}
	return false
}

// eatWord consumes the keyword when it is there.
func (p *Parser) eatWord(kw string) bool {
	if p.atWord(kw) {
		p.pos++
		return true
	}
	return false
}

// eatWords consumes a run of keywords, all or nothing.
func (p *Parser) eatWords(kws ...string) bool {
	for i, kw := range kws {
		if !p.peek(i).IsWord(kw) {
			return false
		}
	}
	p.pos += len(kws)
	return true
}

// adjacent reports whether the token n places ahead begins exactly where the
// token under the cursor ends, with nothing between them. MySQL tells a call
// from an operator that way: "MOD(a, b)" is the function and "a MOD (b)" is the
// operator, and the only difference is the space.
func (p *Parser) adjacent(n int) bool {
	cur := p.cur()
	return p.peek(n).Offset == cur.Offset+len(cur.Text)
}

// callParenFollows reports whether an opening parenthesis stands against the
// word at the cursor with nothing between them, which is how MySQL tells the
// function "MOD(a, b)" from the operator in "a MOD (b + 1)", and "INTERVAL(...)"
// from the "INTERVAL 1 DAY" unit.
func (p *Parser) callParenFollows() bool {
	return p.peek(1).IsOp("(") && p.adjacent(1)
}

// atOp reports whether the cursor is on the operator op.
func (p *Parser) atOp(op string) bool { return p.cur().IsOp(op) }

// eatOp consumes the operator when it is there.
func (p *Parser) eatOp(op string) bool {
	if p.atOp(op) {
		p.pos++
		return true
	}
	return false
}

// expectOp consumes the operator or reports what was found instead.
func (p *Parser) expectOp(op string) error {
	if p.eatOp(op) {
		return nil
	}
	return p.unexpected(op)
}

// expectWord consumes the keyword or reports what was found instead.
func (p *Parser) expectWord(kw string) error {
	if p.eatWord(kw) {
		return nil
	}
	return p.unexpected(kw)
}

// skipSemicolons steps over trailing statement separators.
func (p *Parser) skipSemicolons() {
	for p.atOp(";") {
		p.pos++
	}
}

// unexpected reports the token under the cursor against what was wanted.
func (p *Parser) unexpected(want string) error {
	t := p.cur()
	got := t.String()
	if p.atEnd() {
		got = "end of input"
	}
	return sqlerr.At(sqlerr.ErrInvalidSyntax, t.Line, t.Col, "expected %s, found %s", want, got)
}

// unsupportedf reports a construct SQLite cannot express, at the cursor.
func (p *Parser) unsupportedf(format string, args ...any) error {
	t := p.cur()
	return sqlerr.At(sqlerr.ErrUnsupportedSyntax, t.Line, t.Col, format, args...)
}

// unimplementedf reports a construct this package does not model, at the
// cursor. The distinction from unsupportedf matters to a caller: one says
// SQLite cannot do this, the other says filesql has not been taught it.
func (p *Parser) unimplementedf(format string, args ...any) error {
	t := p.cur()
	return sqlerr.At(sqlerr.ErrUnsupportedFeature, t.Line, t.Col, format, args...)
}

// unsupportedAt reports a construct SQLite cannot express, at a token that is
// no longer under the cursor.
func unsupportedAt(t token.Token, format string, args ...any) error {
	return sqlerr.At(sqlerr.ErrUnsupportedSyntax, t.Line, t.Col, format, args...)
}

// unimplementedAt reports a construct this package does not model, at a token
// that is no longer under the cursor.
func unimplementedAt(t token.Token, format string, args ...any) error {
	return sqlerr.At(sqlerr.ErrUnsupportedFeature, t.Line, t.Col, format, args...)
}

// enter and leave bound recursion. Every rule that can nest calls enter first.
func (p *Parser) enter() error {
	p.depth++
	if p.depth > maxDepth {
		t := p.cur()
		return sqlerr.At(sqlerr.ErrInvalidSyntax, t.Line, t.Col,
			"expression nests more than %d levels deep", maxDepth)
	}
	return nil
}

func (p *Parser) leave() { p.depth-- }

// span reads the position of the token under the cursor.
func (p *Parser) span() ast.Span {
	t := p.cur()
	return ast.Span{Line: t.Line, Col: t.Col}
}

// upper is the keyword spelling used for comparisons that are not case
// sensitive.
func upper(s string) string { return strings.ToUpper(s) }

// sourceText spells a token range the way the item was written, with the
// literals in SQLite's own spelling. It becomes the name of a result column
// whose expression translation changed, so it has to be comparable with what
// the renderer writes: a literal that only changed its quoting must come out
// the same both ways, or every string literal would be labeled.
//
// The layout follows the source: two tokens the caller wrote apart stay apart
// and two written together stay together, which is what keeps
// "EXTRACT(YEAR FROM d)" from becoming "EXTRACT (YEAR FROM d)" and "B'1010'"
// from becoming "B '1010'".
func (p *Parser) sourceText(from, to int) string {
	if from >= to || to > len(p.toks) {
		return ""
	}
	var b strings.Builder
	for i := from; i < to; i++ {
		t := p.toks[i]
		if i > from && separated(p.toks[i-1], t) {
			b.WriteByte(' ')
		}
		b.WriteString(spell(t))
	}
	return b.String()
}

// separated reports whether two adjacent tokens need a space between them: the
// caller's own layout decides, since this text becomes a name rather than SQL
// and nothing re-reads it.
func separated(prev, next token.Token) bool {
	return next.Offset > prev.End
}

// spell writes one token in SQLite's spelling.
func spell(t token.Token) string {
	switch t.Kind {
	case token.QuotedIdent:
		return `"` + strings.ReplaceAll(t.Text, `"`, `""`) + `"`
	case token.String:
		return "'" + strings.ReplaceAll(t.Text, "'", "''") + "'"
	case token.Blob:
		return "x'" + t.Text + "'"
	default:
		return t.Text
	}
}

// newForTest builds a parser over a query, for the tests that look at the token
// stream rather than at the tree.
func newForTest(d dialects.Dialect, query string) (*Parser, error) {
	cfg, ok := token.ConfigFor(d)
	if !ok {
		return nil, sqlerr.At(dialects.ErrUnknownDialect, 0, 0, "%q", string(d))
	}
	raw, err := token.Lex(query, cfg)
	if err != nil {
		return nil, err
	}
	return &Parser{dialect: d, toks: significant(raw), src: query}, nil
}
