package parser

import (
	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/token"
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
	case p.atWord(kwStraight) && p.dialect == dialects.MySQL:
		// MySQL's STRAIGHT_JOIN is an inner join with the join order forced.
		// The order is a planner instruction with no SQLite equivalent, and the
		// rows are the same, so it lowers to an inner join.
		p.pos++
		return ast.JoinInner, true
	case p.atWord(kwLeft):
		return p.outerJoinAt(ast.JoinLeft)
	case p.atWord(kwRight):
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
	lateral := p.eatWord(kwLateral)

	if p.atOp("(") {
		return p.parseParenTable(span, lateral)
	}
	if lateral {
		return nil, p.unexpected("a subquery")
	}

	if p.atInheritanceOnly() {
		return p.parseOnlyTable(span)
	}

	parts, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	if p.atOp("(") {
		name := p.cur()
		call, err := p.parseCall(parts, span)
		if err != nil {
			return nil, err
		}
		fn, ok := call.(*ast.FuncCall)
		if !ok {
			return nil, p.unexpected("a table")
		}
		if err := refuseAggregateTable(fn, name); err != nil {
			return nil, err
		}
		table := &ast.FuncTable{Call: fn, Span: span}
		if err := p.parseTableAlias(&table.Alias, &table.Columns); err != nil {
			return nil, err
		}
		return table, nil
	}
	return p.finishTableName(parts, span)
}

// refuseAggregateTable refuses the clauses that make a call an aggregate or a
// window function on a call that stands as a table. SQLite reads a table-valued
// call as a name and its arguments and nothing else, and none of these reads in
// the engines here either, where a call in FROM returns a set of rows rather
// than one value: keeping them rendered "FROM f() OVER ()", which gave SQLite a
// syntax error at a word this package had written back out.
func refuseAggregateTable(fn *ast.FuncCall, at token.Token) error {
	var clause string
	switch {
	case fn.Star:
		clause = "a star argument"
	case fn.Distinct:
		clause = "DISTINCT"
	case len(fn.OrderBy) > 0:
		clause = "an ORDER BY among its arguments"
	case fn.Separator != nil:
		clause = kwSeparator
	case fn.Limit != nil:
		clause = "a LIMIT among its arguments"
	case fn.Filter != nil:
		clause = "FILTER"
	case fn.Over != nil:
		clause = "OVER"
	case len(fn.WithinGroup) > 0:
		clause = "WITHIN GROUP"
	default:
		return nil
	}
	return invalidAt(at, "a call standing as a table cannot take %s; it answers rows rather than one value", clause)
}

// finishTableName reads what a table name carries after it: the star of
// PostgreSQL's inheritance spelling, the suffixes a dialect allows, and the
// alias.
func (p *Parser) finishTableName(parts []ast.Ident, span ast.Span) (ast.TableExpr, error) {
	table := &ast.TableName{Parts: parts, Span: span}
	p.eatInheritanceStar()
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

// eatInheritanceStar reads the star PostgreSQL writes after a table name, which
// is the opposite of ONLY: it reaches the tables that inherit from this one as
// well. Nothing inherits here, so it names the same table and is dropped. A
// star straight after a dot is not that star -- "s.*" is a qualified name that
// stops before its last part, and reading the star here would answer from the
// schema instead of refusing the name.
func (p *Parser) eatInheritanceStar() {
	if p.dialect != dialects.PostgreSQL || !p.atOp("*") {
		return
	}
	if p.pos > 0 && p.toks[p.pos-1].IsOp(".") {
		return
	}
	p.pos++
}

// atInheritanceOnly reports whether the cursor is on PostgreSQL's ONLY in front
// of a table name. ONLY says a statement reaches this table and not the tables
// inheriting from it, and it is a reserved word there, so what follows it is
// the name. The other dialects have no such keyword: MySQL answers "Table
// 'db.ONLY' doesn't exist" for "FROM ONLY t", which is ONLY aliased t, and
// reading it as a keyword would take a table away from them.
func (p *Parser) atInheritanceOnly() bool {
	if p.dialect != dialects.PostgreSQL || !p.atWord("ONLY") {
		return false
	}
	next := p.peek(1)
	if next.IsOp("(") {
		return p.namesSomething(2)
	}
	// A quoted identifier is a name whatever it spells, so only a bare word is
	// weighed against the words that open a clause.
	if next.Kind == token.QuotedIdent {
		return true
	}
	return next.Kind == token.Word && !clauseKeywords[upper(next.Text)]
}

// parseOnlyTable reads a table reference behind PostgreSQL's ONLY, with or
// without the parentheses that spelling also allows.
func (p *Parser) parseOnlyTable(span ast.Span) (ast.TableExpr, error) {
	p.pos++ // ONLY
	parenthesized := p.eatOp("(")
	parts, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	if parenthesized {
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
	}
	return p.finishTableName(parts, span)
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
	// LOCK opens MySQL's row-locking clause when IN follows it and is an alias
	// anywhere else, since MySQL does not reserve the word. Reading it as the
	// alias left the parser on IN, and the clause a query writes right after a
	// bare table name was reported as unreadable SQL rather than refused as the
	// row lock it is.
	if p.atWord("LOCK") && p.peek(1).IsWord("IN") {
		return nil
	}
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
		case p.atWord(kwSample):
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
	if p.eatWord(kwFor) {
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
		!p.atWords("FOR", "NO", "KEY") && !p.atWords("FOR", "KEY", "SHARE") &&
		!p.atWords("LOCK", "IN") {
		return nil
	}
	return p.unsupportedf("a row-locking clause is not supported; SQLite locks the database rather than rows")
}
