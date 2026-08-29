package parser

import (
	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
)

// parseFrom reads a FROM clause: one or more table expressions, joined by
// commas or by explicit joins.
func (p *Parser) parseFrom() ([]ast.TableExpr, error) {
	var tables []ast.TableExpr
	for {
		table, err := p.parseTableExpr()
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
		if !p.eatOp(",") {
			return tables, nil
		}
	}
}

// parseTableExpr reads one table reference together with the joins that follow
// it. A join binds to the left, which is what makes "a JOIN b JOIN c" a join of
// (a JOIN b) with c.
func (p *Parser) parseTableExpr() (ast.TableExpr, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()

	left, err := p.parseTablePrimary()
	if err != nil {
		return nil, err
	}
	for {
		join, ok, err := p.parseJoin(left)
		if err != nil {
			return nil, err
		}
		if !ok {
			return left, nil
		}
		left = join
	}
}

// parseJoin reads one join clause, if there is one at the cursor.
func (p *Parser) parseJoin(left ast.TableExpr) (ast.TableExpr, bool, error) {
	span := p.span()
	natural := false
	if p.atWord("NATURAL") {
		natural = true
		p.pos++
	}
	joinType, ok := p.joinTypeAt()
	if !ok {
		if natural {
			return nil, false, p.unexpected("JOIN")
		}
		return nil, false, nil
	}
	right, err := p.parseTablePrimary()
	if err != nil {
		return nil, false, err
	}
	join := &ast.JoinTable{Type: joinType, Left: left, Right: right, Natural: natural, Span: span}
	switch {
	case p.eatWord("ON"):
		cond, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, false, err
		}
		join.On = cond
	case p.eatWord("USING"):
		names, err := p.parseNameList()
		if err != nil {
			return nil, false, err
		}
		join.Using = names
	}
	return join, true, nil
}

// joinTypeAt reads the words that name a join, consuming them.
func (p *Parser) joinTypeAt() (ast.JoinType, bool) {
	switch {
	case p.atWord("JOIN"):
		p.pos++
		return ast.JoinInner, true
	case p.atWord("INNER") && p.peek(1).IsWord("JOIN"):
		p.pos += 2
		return ast.JoinInner, true
	case p.atWord("CROSS") && p.peek(1).IsWord("JOIN"):
		p.pos += 2
		return ast.JoinCross, true
	case p.atWord("STRAIGHT_JOIN") && p.dialect == dialects.MySQL:
		// MySQL's STRAIGHT_JOIN is an inner join with the join order forced.
		// The order is a planner instruction with no SQLite equivalent, and the
		// rows are the same, so it lowers to an inner join.
		p.pos++
		return ast.JoinInner, true
	case p.atWord("LEFT"):
		return p.outerJoinAt(ast.JoinLeft)
	case p.atWord("RIGHT"):
		return p.outerJoinAt(ast.JoinRight)
	case p.atWord("FULL"):
		return p.outerJoinAt(ast.JoinFull)
	default:
		return 0, false
	}
}

// outerJoinAt reads "LEFT [OUTER] JOIN" and its relatives.
func (p *Parser) outerJoinAt(kind ast.JoinType) (ast.JoinType, bool) {
	n := 1
	if p.peek(n).IsWord("OUTER") {
		n++
	}
	if !p.peek(n).IsWord("JOIN") {
		return 0, false
	}
	p.pos += n + 1
	return kind, true
}

// parseTablePrimary reads one table reference: a name, a subquery, a function
// call, or a parenthesized table expression.
func (p *Parser) parseTablePrimary() (ast.TableExpr, error) {
	span := p.span()
	lateral := p.eatWord("LATERAL")

	if p.atOp("(") {
		return p.parseParenTable(span, lateral)
	}
	if lateral {
		return nil, p.unexpected("a subquery")
	}

	parts, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	if p.atOp("(") {
		call, err := p.parseCall(parts, span)
		if err != nil {
			return nil, err
		}
		fn, ok := call.(*ast.FuncCall)
		if !ok {
			return nil, p.unexpected("a table")
		}
		table := &ast.FuncTable{Call: fn, Span: span}
		if err := p.parseTableAlias(&table.Alias, &table.Columns); err != nil {
			return nil, err
		}
		return table, nil
	}
	table := &ast.TableName{Parts: parts, Span: span}
	// The suffixes come before the alias in MySQL's grammar for a hint and
	// after it for the rest, so both sides of the alias are read.
	if err := p.parseTableSuffixes(table); err != nil {
		return nil, err
	}
	if err := p.parseTableAlias(&table.Alias, &table.Columns); err != nil {
		return nil, err
	}
	if err := p.parseTableSuffixes(table); err != nil {
		return nil, err
	}
	return table, nil
}

// parseParenTable reads what follows an opening parenthesis in a FROM clause:
// a subquery or a nested table expression.
func (p *Parser) parseParenTable(span ast.Span, lateral bool) (ast.TableExpr, error) {
	p.pos++ // (
	if p.startsSelect() {
		sub, err := p.parseSelectStmt()
		if err != nil {
			return nil, err
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		table := &ast.SubqueryTable{Sub: sub, Lateral: lateral, Span: span}
		if err := p.parseTableAlias(&table.Alias, &table.Columns); err != nil {
			return nil, err
		}
		return table, nil
	}
	inner, err := p.parseTableExpr()
	if err != nil {
		return nil, err
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return &ast.ParenTable{Inner: inner, Span: span}, nil
}

// parseTableAlias reads the alias and column list of a table reference.
func (p *Parser) parseTableAlias(alias *string, columns *[]string) error {
	name, _, ok, err := p.parseAlias()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	*alias = name
	if p.atOp("(") {
		names, err := p.parseNameList()
		if err != nil {
			return err
		}
		*columns = names
	}
	return nil
}

// parseTableSuffixes reads the clauses a dialect allows after a table name.
func (p *Parser) parseTableSuffixes(table *ast.TableName) error {
	for {
		switch {
		case p.dialect == dialects.MySQL && p.atAnyWord("USE", "FORCE", "IGNORE") &&
			(p.peek(1).IsWord("INDEX") || p.peek(1).IsWord("KEY")):
			// An index hint tells the planner which index to consider. SQLite
			// has its own INDEXED BY and no way to say the same thing, and the
			// rows are the same either way, so the hint is read and dropped.
			if err := p.skipIndexHint(table); err != nil {
				return err
			}
		case p.atWord("TABLESAMPLE"):
			return p.unsupportedf("TABLESAMPLE is not supported; SQLite has no sampling clause")
		case p.dialect == dialects.MySQL && p.atWord("PARTITION") && p.peek(1).IsOp("("):
			return p.unsupportedf("the PARTITION clause of a table reference is not supported; SQLite has no partitioned tables")
		default:
			return nil
		}
	}
}

// skipIndexHint reads a MySQL index hint and records that one was written.
func (p *Parser) skipIndexHint(table *ast.TableName) error {
	hint := upper(p.advance().Text)
	p.pos++ // INDEX or KEY
	if p.eatWord("FOR") {
		switch {
		case p.eatWord("JOIN"):
		case p.eatWords("ORDER", "BY"):
		case p.eatWords("GROUP", "BY"):
		default:
			return p.unexpected("JOIN, ORDER BY or GROUP BY")
		}
	}
	if err := p.expectOp("("); err != nil {
		return err
	}
	for !p.atOp(")") {
		if p.atEnd() {
			return p.unexpected(")")
		}
		p.pos++
	}
	p.pos++
	table.Hints = append(table.Hints, hint)
	return nil
}

// rowLockKeywords are the words that open a row-locking clause. SQLite takes a
// lock on the whole database rather than on rows, so there is nothing for the
// clause to ask for.
func (p *Parser) parseRowLock() error {
	if !p.atWords("FOR", "UPDATE") && !p.atWords("FOR", "SHARE") &&
		!p.atWords("FOR", "NO", "KEY") && !p.atWords("LOCK", "IN") {
		return nil
	}
	return p.unsupportedf("a row-locking clause is not supported; SQLite locks the database rather than rows")
}
