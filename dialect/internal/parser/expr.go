package parser

import (
	"strings"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/token"
)

// parseExpr reads an expression whose operators bind at least as tightly as
// minPrec. This is the whole of the precedence logic: an operator weaker than
// minPrec ends the expression and is left for the caller.
func (p *Parser) parseExpr(minPrec int) (ast.Expr, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()

	left, err := p.parsePrefix(minPrec)
	if err != nil {
		return nil, err
	}
	return p.parseInfix(left, minPrec)
}

// parsePrefix reads a unary operator or a primary expression.
func (p *Parser) parsePrefix(minPrec int) (ast.Expr, error) {
	t := p.cur()
	span := ast.SpanOf(t)

	switch {
	case t.IsWord("NOT"):
		if precNot < minPrec {
			return nil, p.unexpected("an expression")
		}
		p.pos++
		operand, err := p.parseExpr(precNot)
		if err != nil {
			return nil, err
		}
		return &ast.UnaryExpr{Op: ast.UnaryNot, Expr: operand, Span: span}, nil

	case t.IsOp("!") && p.dialect == dialects.MySQL:
		// MySQL's "!" is NOT, and it binds tighter than every arithmetic
		// operator rather than at NOT's level.
		p.pos++
		operand, err := p.parseExpr(precUnary)
		if err != nil {
			return nil, err
		}
		return &ast.UnaryExpr{Op: ast.UnaryNot, Expr: operand, Span: span}, nil

	case t.IsOp("-"):
		p.pos++
		operand, err := p.parseExpr(precUnary)
		if err != nil {
			return nil, err
		}
		return &ast.UnaryExpr{Op: ast.UnaryMinus, Expr: operand, Span: span}, nil

	case t.IsOp("+"):
		p.pos++
		operand, err := p.parseExpr(precUnary)
		if err != nil {
			return nil, err
		}
		return &ast.UnaryExpr{Op: ast.UnaryPlus, Expr: operand, Span: span}, nil

	case t.IsOp("~"):
		p.pos++
		operand, err := p.parseExpr(precUnary)
		if err != nil {
			return nil, err
		}
		// A "~" with nothing on its left is the bitwise complement in every
		// dialect that has one, which SQLite spells the same way. The regular
		// expression operator of the same name is infix and never reaches here.
		return &ast.UnaryExpr{Op: ast.UnaryBitNot, Expr: operand, Span: span}, nil

	case t.IsWord("BINARY") && p.dialect == dialects.MySQL && !p.peek(1).IsOp("("):
		// MySQL's BINARY is a cast written as a prefix operator.
		p.pos++
		operand, err := p.parseExpr(precUnary)
		if err != nil {
			return nil, err
		}
		return &ast.CastExpr{
			Expr: operand,
			Type: ast.TypeName{Name: "BINARY", Span: span},
			Span: span,
		}, nil

	case t.IsWord("INTERVAL") && p.dialect != dialects.PostgreSQL &&
		!(p.peek(1).IsOp("(") && p.adjacent(1)):
		return p.parseUnitInterval()

	case t.IsWord("EXISTS") && p.peek(1).IsOp("("):
		p.pos++
		sub, err := p.parseParenSelect()
		if err != nil {
			return nil, err
		}
		return &ast.ExistsExpr{Sub: sub, Span: span}, nil

	case t.IsWord("CASE"):
		return p.parseCase()

	case t.IsWord("CAST") && p.peek(1).IsOp("("):
		return p.parseCastCall(false)

	case t.IsWord("SAFE_CAST") && p.peek(1).IsOp("(") && p.dialect == dialects.GoogleSQL:
		return p.parseCastCall(true)

	case t.IsWord("SAFE") && p.peek(1).IsOp(".") && p.peek(2).IsWord("CAST") && p.dialect == dialects.GoogleSQL:
		// The SAFE prefix applies to a function, and CAST is not one: BigQuery
		// spells the cast that answers NULL rather than raising SAFE_CAST.
		return nil, p.unsupportedf("SAFE.CAST is not supported; write SAFE_CAST")

	case t.IsWord("ARRAY") && (p.peek(1).IsOp("[") || p.peek(1).IsOp("<") || p.peek(1).IsOp("(")):
		// ARRAY[...], ARRAY<t>[...] and the ARRAY(subquery) that gathers a
		// column into one value: all three answer an array.
		return p.parseArrayConstructor()

	case t.IsOp("[") && p.dialect == dialects.GoogleSQL:
		return p.parseArrayLiteral()

	case t.IsWord("STRUCT") && p.dialect == dialects.GoogleSQL:
		return nil, p.unsupportedf("STRUCT is not supported; SQLite has no struct type")
	}

	return p.parsePrimary()
}

// parseInfix reads the operators that follow an operand, stopping at the first
// one weaker than minPrec.
func (p *Parser) parseInfix(left ast.Expr, minPrec int) (ast.Expr, error) {
	for {
		next, err := p.parseInfixOnce(left, minPrec)
		if err != nil {
			return nil, err
		}
		if next == nil {
			return left, nil
		}
		left = next
	}
}

// parseInfixOnce applies one infix or postfix operator, or reports nil when the
// next token does not continue the expression.
func (p *Parser) parseInfixOnce(left ast.Expr, minPrec int) (ast.Expr, error) {
	switch {
	case p.atOp("::") && p.dialect == dialects.PostgreSQL:
		if precCast < minPrec {
			return nil, nil
		}
		span := p.span()
		p.pos++
		typ, err := p.parseTypeName()
		if err != nil {
			return nil, err
		}
		return &ast.CastExpr{Expr: left, Type: typ, Span: span}, nil

	case p.atOp("["):
		if precPostfix < minPrec {
			return nil, nil
		}
		// A subscript indexes an array, and SQLite has no array to index. The
		// refusal is here rather than after parsing the index, because
		// GoogleSQL writes the index with keywords -- OFFSET(0), ORDINAL(1) --
		// that are not expressions.
		return nil, p.unsupportedf("a subscript is not supported; SQLite has no array type")

	case p.atWord("COLLATE"):
		if precCollate < minPrec {
			return nil, nil
		}
		span := p.span()
		p.pos++
		name, err := p.parseCollationName()
		if err != nil {
			return nil, err
		}
		return &ast.CollateExpr{Expr: left, Collation: name, Span: span}, nil

	case p.atWord("IS"):
		if precIs < minPrec {
			return nil, nil
		}
		return p.parseIs(left)

	case p.atWord("ISNULL") && p.dialect == dialects.PostgreSQL:
		if precIs < minPrec {
			return nil, nil
		}
		span := p.span()
		p.pos++
		return &ast.IsExpr{Expr: left, Right: &ast.Literal{Kind: ast.LitNull, Span: span}, Span: span}, nil

	case p.atWord("NOTNULL") && p.dialect == dialects.PostgreSQL:
		if precIs < minPrec {
			return nil, nil
		}
		span := p.span()
		p.pos++
		return &ast.IsExpr{Expr: left, Right: &ast.Literal{Kind: ast.LitNull, Span: span}, Negated: true, Span: span}, nil

	case p.atWord("BETWEEN"):
		if precBetween < minPrec {
			return nil, nil
		}
		return p.parseBetween(left, false)

	case p.atWord("IN"):
		if precCompare < minPrec {
			return nil, nil
		}
		return p.parseIn(left, false)

	case p.atWord("NOT"):
		// NOT after an operand is the negated spelling of a comparison rather
		// than the prefix operator: NOT LIKE, NOT IN, NOT BETWEEN, and the
		// dialect-specific pattern operators.
		return p.parseNegatedComparison(left, minPrec)
	}

	op, prec, width, ok := p.binaryOpFor()
	if !ok || prec < minPrec {
		return nil, nil
	}
	span := p.span()
	spelling := p.cur().Text
	p.pos += width

	if quant, isQuant := p.quantifierAt(op); isQuant {
		return p.parseQuantified(left, op, quant, span)
	}

	// Every operator here associates to the left, which is why the right side
	// is parsed at one level tighter.
	right, err := p.parseExpr(prec + 1)
	if err != nil {
		return nil, err
	}
	node := &ast.BinaryExpr{Left: left, Op: op, Right: right, Spelling: spelling, Span: span}
	if escapes(op) && p.atWord("ESCAPE") {
		p.pos++
		esc, err := p.parseExpr(prec + 1)
		if err != nil {
			return nil, err
		}
		node.Escape = esc
	}
	return node, nil
}

// escapes reports whether the operator takes an ESCAPE clause.
func escapes(op ast.BinaryOp) bool {
	switch op {
	case ast.Like, ast.NotLike, ast.ILike, ast.NotILike, ast.SimilarTo, ast.NotSimilarTo:
		return true
	default:
		return false
	}
}

// quantifierAt reads ANY, SOME or ALL following a comparison operator.
func (p *Parser) quantifierAt(op ast.BinaryOp) (ast.Quantifier, bool) {
	if !comparison(op) {
		return 0, false
	}
	switch {
	case p.atWord("ANY"):
		return ast.QuantAny, true
	case p.atWord("SOME"):
		return ast.QuantSome, true
	case p.atWord("ALL"):
		return ast.QuantAll, true
	default:
		return 0, false
	}
}

// comparison reports whether the operator is one a quantifier may follow.
func comparison(op ast.BinaryOp) bool {
	switch op {
	case ast.Eq, ast.NotEq, ast.Lt, ast.Lte, ast.Gt, ast.Gte, ast.Like, ast.NotLike, ast.ILike, ast.NotILike:
		return true
	default:
		return false
	}
}

// parseQuantified reads "op ANY (...)" after the operator has been consumed.
func (p *Parser) parseQuantified(left ast.Expr, op ast.BinaryOp, quant ast.Quantifier, span ast.Span) (ast.Expr, error) {
	p.pos++ // the quantifier word
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	node := &ast.QuantifiedExpr{Left: left, Op: op, Quant: quant, Span: span}
	if p.startsSelect() {
		sub, err := p.parseSelectStmt()
		if err != nil {
			return nil, err
		}
		node.Sub = sub
	} else {
		list, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		node.List = list
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return node, nil
}

// parseNegatedComparison reads the NOT that stands between an operand and a
// pattern or membership operator.
func (p *Parser) parseNegatedComparison(left ast.Expr, minPrec int) (ast.Expr, error) {
	span := p.span()
	switch {
	case p.peek(1).IsWord("BETWEEN"):
		if precBetween < minPrec {
			return nil, nil
		}
		p.pos++
		return p.parseBetween(left, true)
	case p.peek(1).IsWord("IN"):
		if precCompare < minPrec {
			return nil, nil
		}
		p.pos++
		return p.parseIn(left, true)
	case p.peek(1).IsWord("LIKE"):
		return p.parseNegatedOperator(left, minPrec, ast.NotLike, 2, span)
	case p.peek(1).IsWord("ILIKE") && p.dialect == dialects.PostgreSQL:
		return p.parseNegatedOperator(left, minPrec, ast.NotILike, 2, span)
	case p.peek(1).IsWord("REGEXP") || p.peek(1).IsWord("RLIKE"):
		return p.parseNegatedOperator(left, minPrec, ast.NotRegexp, 2, span)
	case p.peek(1).IsWord("SIMILAR") && p.peek(2).IsWord("TO"):
		return p.parseNegatedOperator(left, minPrec, ast.NotSimilarTo, 3, span)
	default:
		return nil, nil
	}
}

// parseNegatedOperator reads a negated pattern operator whose words span width
// tokens starting at the NOT.
func (p *Parser) parseNegatedOperator(left ast.Expr, minPrec int, op ast.BinaryOp, width int, span ast.Span) (ast.Expr, error) {
	if precCompare < minPrec {
		return nil, nil
	}
	p.pos += width
	right, err := p.parseExpr(precCompare + 1)
	if err != nil {
		return nil, err
	}
	node := &ast.BinaryExpr{Left: left, Op: op, Right: right, Span: span}
	if escapes(op) && p.eatWord("ESCAPE") {
		esc, err := p.parseExpr(precCompare + 1)
		if err != nil {
			return nil, err
		}
		node.Escape = esc
	}
	return node, nil
}

// parseIs reads IS, IS NOT, IS DISTINCT FROM and the truth-value predicates.
func (p *Parser) parseIs(left ast.Expr) (ast.Expr, error) {
	span := p.span()
	p.pos++ // IS
	negated := p.eatWord("NOT")
	if p.eatWords("DISTINCT", "FROM") {
		right, err := p.parseExpr(precIs + 1)
		if err != nil {
			return nil, err
		}
		return &ast.IsExpr{Expr: left, Right: right, Negated: negated, Distinct: true, Span: span}, nil
	}
	right, err := p.parseExpr(precIs + 1)
	if err != nil {
		return nil, err
	}
	return &ast.IsExpr{Expr: left, Right: right, Negated: negated, Span: span}, nil
}

// parseBetween reads "BETWEEN lo AND hi" after any NOT has been consumed.
func (p *Parser) parseBetween(left ast.Expr, negated bool) (ast.Expr, error) {
	span := p.span()
	p.pos++ // BETWEEN
	symmetric := false
	switch {
	case p.eatWord("SYMMETRIC"):
		symmetric = true
	case p.eatWord("ASYMMETRIC"):
		// The default, named explicitly.
	}
	// The bounds bind tighter than AND, which is what keeps the AND of the
	// BETWEEN from being read as a logical conjunction.
	low, err := p.parseExpr(precBetween + 1)
	if err != nil {
		return nil, err
	}
	if err := p.expectWord(kwAnd); err != nil {
		return nil, err
	}
	high, err := p.parseExpr(precBetween + 1)
	if err != nil {
		return nil, err
	}
	return &ast.BetweenExpr{
		Expr: left, Low: low, High: high, Negated: negated, Symmetric: symmetric, Span: span,
	}, nil
}

// parseIn reads "IN (...)" after any NOT has been consumed.
func (p *Parser) parseIn(left ast.Expr, negated bool) (ast.Expr, error) {
	span := p.span()
	p.pos++ // IN
	if p.unnestAt() {
		return nil, p.unsupportedf("UNNEST is not supported; SQLite has no array type")
	}
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	node := &ast.InExpr{Expr: left, Negated: negated, Span: span}
	if p.startsSelect() {
		sub, err := p.parseSelectStmt()
		if err != nil {
			return nil, err
		}
		node.Sub = sub
	} else if !p.atOp(")") {
		list, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		node.List = list
	} else {
		node.List = []ast.Expr{}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return node, nil
}

// parseCase reads a CASE expression in both its forms.
func (p *Parser) parseCase() (ast.Expr, error) {
	span := p.span()
	p.pos++ // CASE
	node := &ast.CaseExpr{Span: span}
	if !p.atWord("WHEN") {
		operand, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		node.Operand = operand
	}
	for p.atWord("WHEN") {
		whenSpan := p.span()
		p.pos++
		cond, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		if err := p.expectWord("THEN"); err != nil {
			return nil, err
		}
		result, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		node.Whens = append(node.Whens, ast.WhenClause{Cond: cond, Result: result, Span: whenSpan})
	}
	if len(node.Whens) == 0 {
		return nil, p.unexpected("WHEN")
	}
	if p.eatWord("ELSE") {
		otherwise, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		node.Else = otherwise
	}
	if err := p.expectWord(kwEnd); err != nil {
		return nil, err
	}
	return node, nil
}

// parseCastCall reads CAST(x AS t) and GoogleSQL's SAFE_CAST.
func (p *Parser) parseCastCall(try bool) (ast.Expr, error) {
	span := p.span()
	p.pos++ // CAST
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	value, err := p.parseExpr(precLowest)
	if err != nil {
		return nil, err
	}
	if err := p.expectWord("AS"); err != nil {
		return nil, err
	}
	typ, err := p.parseTypeName()
	if err != nil {
		return nil, err
	}
	// GoogleSQL's FORMAT clause of a cast names a template the conversion
	// follows, which has no SQLite form.
	if p.atWord("FORMAT") {
		return nil, p.unsupportedf("the FORMAT clause of CAST is not supported")
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return &ast.CastExpr{Expr: value, Type: typ, TryCast: try, Span: span}, nil
}

// parseUnitInterval reads "INTERVAL n unit", the operator spelling MySQL and
// GoogleSQL use.
func (p *Parser) parseUnitInterval() (ast.Expr, error) {
	span := p.span()
	p.pos++ // INTERVAL
	// The amount is a whole expression: MySQL reads "INTERVAL 1 + 1 DAY" as two
	// days. The unit that follows is a bare word, which no operator table makes
	// infix, so the expression ends there on its own.
	value, err := p.parseExpr(precLowest)
	if err != nil {
		return nil, err
	}
	t := p.cur()
	if t.Kind != token.Word {
		return nil, p.unexpected("an interval unit")
	}
	p.pos++
	return &ast.IntervalExpr{Value: value, Unit: upper(t.Text), Span: span}, nil
}

// parseArrayConstructor reads PostgreSQL's ARRAY[...] and GoogleSQL's
// ARRAY<t>[...]. Both are refused, and both are parsed first so the refusal
// names the construct rather than the bracket.
func (p *Parser) parseArrayConstructor() (ast.Expr, error) {
	return nil, p.unsupportedf("arrays are not supported; SQLite has no array type")
}

// parseArrayLiteral reads GoogleSQL's [a, b] array literal.
func (p *Parser) parseArrayLiteral() (ast.Expr, error) {
	return nil, p.unsupportedf("arrays are not supported; SQLite has no array type")
}

// parseCollationName reads the name after COLLATE, which may be written bare or
// quoted.
func (p *Parser) parseCollationName() (string, error) {
	t := p.cur()
	switch t.Kind {
	case token.Word, token.QuotedIdent:
		p.pos++
		return t.Text, nil
	case token.String:
		p.pos++
		return t.Text, nil
	default:
		return "", p.unexpected("a collation name")
	}
}

// parseExprList reads one or more comma-separated expressions.
func (p *Parser) parseExprList() ([]ast.Expr, error) {
	var list []ast.Expr
	for {
		e, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		list = append(list, e)
		if !p.eatOp(",") {
			return list, nil
		}
	}
}

// typeWords are the multi-word type names, longest first, so "DOUBLE
// PRECISION" is read as one name rather than as DOUBLE followed by a column
// called PRECISION.
var typeWords = [][]string{ //nolint:gochecknoglobals // a fixed table
	{kwTimestamp, kwWith, kwTime, kwZone},
	{kwTimestamp, "WITHOUT", kwTime, kwZone},
	{kwTime, kwWith, kwTime, kwZone},
	{kwTime, "WITHOUT", kwTime, kwZone},
	{"CHARACTER", "VARYING"},
	{"DOUBLE", "PRECISION"},
	{"BIT", "VARYING"},
	{"NATIONAL", "CHAR"},
	{"NATIONAL", "VARCHAR"},
	{"UNSIGNED", "INTEGER"},
	{"SIGNED", "INTEGER"},
}

// parseTypeName reads a type as written in a cast or a column definition.
func (p *Parser) parseTypeName() (ast.TypeName, error) {
	span := p.span()
	typeStart := p.pos
	t := p.cur()
	if t.Kind != token.Word {
		return ast.TypeName{}, p.unexpected("a type name")
	}
	name := upper(t.Text)
	p.pos++
	for _, words := range typeWords {
		if !strings.EqualFold(words[0], name) {
			continue
		}
		if p.matchesWords(words[1:]) {
			name = strings.Join(words, " ")
			p.pos += len(words) - 1
			break
		}
	}
	// A trailing word that qualifies the type rather than naming a new one.
	var qualified strings.Builder
	qualified.WriteString(name)
	for p.atAnyWord("UNSIGNED", "SIGNED", "ZEROFILL", "PRECISION") {
		qualified.WriteByte(' ')
		qualified.WriteString(upper(p.advance().Text))
	}
	name = qualified.String()
	typ := ast.TypeName{Name: name, Written: p.sourceText(typeStart, p.pos), Span: span}
	if p.eatOp("(") {
		for {
			arg := p.cur()
			if arg.Kind != token.Number {
				return ast.TypeName{}, p.unexpected("a type parameter")
			}
			p.pos++
			typ.Params = append(typ.Params, arg.Text)
			if !p.eatOp(",") {
				break
			}
		}
		if err := p.expectOp(")"); err != nil {
			return ast.TypeName{}, err
		}
	}
	// A character set or collation written on the type, which names an encoding
	// SQLite does not have a second of.
	if p.eatWords("CHARACTER", "SET") || p.eatWord("CHARSET") {
		if p.cur().Kind != token.Word && p.cur().Kind != token.String {
			return ast.TypeName{}, p.unexpected("a character set name")
		}
		p.pos++
	}
	for p.atOp("[") && p.peek(1).IsOp("]") {
		typ.Array = true
		p.pos += 2
	}
	typ.Written = p.sourceText(typeStart, p.pos)
	return typ, nil
}

// matchesWords reports whether the cursor is on the given keyword run.
func (p *Parser) matchesWords(words []string) bool {
	for i, w := range words {
		if !p.peek(i).IsWord(w) {
			return false
		}
	}
	return true
}
