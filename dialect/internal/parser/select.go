package parser

import (
	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/token"
)

// parseSelectStmt reads a query expression: a WITH, a query body built from
// SELECTs and set operations, and the ORDER BY and LIMIT that belong to the
// whole of it.
func (p *Parser) parseSelectStmt() (*ast.SelectStmt, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()

	stmt := &ast.SelectStmt{Span: p.span()}
	if p.atWord(kwWith) {
		with, err := p.parseWith()
		if err != nil {
			return nil, err
		}
		stmt.With = with
	}
	body, err := p.parseQueryBody(precLowest)
	if err != nil {
		return nil, err
	}
	stmt.Body = body

	order, limit, err := p.parseStatementTail()
	if err != nil {
		return nil, err
	}
	stmt.OrderBy, stmt.Limit = order, limit
	return stmt, nil
}

// setOpPrec gives INTERSECT a tighter binding than UNION and EXCEPT, which is
// the standard's rule and SQLite's.
func setOpPrec(op ast.SetOperator) int {
	if op == ast.Intersect {
		return 2
	}
	return 1
}

// parseQueryBody reads SELECTs joined by set operations.
func (p *Parser) parseQueryBody(minPrec int) (ast.QueryBody, error) {
	left, err := p.parseQueryPrimary()
	if err != nil {
		return nil, err
	}
	for {
		op, ok := p.setOperatorAt()
		if !ok || setOpPrec(op) < minPrec {
			return left, nil
		}
		span := p.span()
		p.pos++
		all := false
		switch {
		case p.eatWord("ALL"):
			all = true
		case p.eatWord("DISTINCT"):
			// The default, named explicitly.
		}
		right, err := p.parseQueryBody(setOpPrec(op) + 1)
		if err != nil {
			return nil, err
		}
		left = &ast.SetOp{Op: op, All: all, Left: left, Right: right, Span: span}
	}
}

// setOperatorAt reads the set operator at the cursor.
func (p *Parser) setOperatorAt() (ast.SetOperator, bool) {
	switch {
	case p.atWord("UNION"):
		return ast.Union, true
	case p.atWord("INTERSECT"):
		return ast.Intersect, true
	case p.atWord("EXCEPT"):
		return ast.Except, true
	case p.atWord("MINUS") && p.dialect != dialects.PostgreSQL:
		return ast.Except, true
	default:
		return 0, false
	}
}

// parseQueryPrimary reads one SELECT, a VALUES, or a parenthesized query.
func (p *Parser) parseQueryPrimary() (ast.QueryBody, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()

	switch {
	case p.atOp("("):
		p.pos++
		inner, err := p.parseQueryBody(precLowest)
		if err != nil {
			return nil, err
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		return inner, nil
	case p.atWord("VALUES"):
		return p.parseValuesBody()
	case p.atWord("SELECT"):
		return p.parseSelectCore()
	case p.atWord("TABLE"):
		// "TABLE t" is shorthand for "SELECT * FROM t" in PostgreSQL. It is
		// named rather than translated because the shorthand is rare and
		// silently rewriting it would hide that the parser guessed.
		return nil, p.unimplementedf("the TABLE shorthand for a query is not implemented; write SELECT * FROM instead")
	default:
		return nil, p.unexpected("SELECT")
	}
}

// parseValuesBody reads a VALUES clause standing as a query.
func (p *Parser) parseValuesBody() (ast.QueryBody, error) {
	span := p.span()
	p.pos++ // VALUES
	rows, err := p.parseValuesRows()
	if err != nil {
		return nil, err
	}
	return &ast.ValuesBody{Rows: rows, Span: span}, nil
}

// parseValuesRows reads the parenthesized row lists of a VALUES.
func (p *Parser) parseValuesRows() ([][]ast.Expr, error) {
	var rows [][]ast.Expr
	for {
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		var row []ast.Expr
		if p.atOp(")") {
			// MySQL writes VALUES () for a row of the columns' own defaults,
			// and SQLite has no way to write one: a row there is a list of
			// values and the list may not be empty. Rendering it gave SQLite
			// "VALUES ()", which it reads as a syntax error near a bracket
			// this package wrote.
			return nil, p.unsupportedf(
				"a values row cannot be empty; SQLite has no way to write a row of column defaults")
		}
		list, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		row = list
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		rows = append(rows, row)
		if !p.eatOp(",") {
			return rows, nil
		}
	}
}

// selectModifiers are the words MySQL allows between SELECT and the select
// list. They were read as column names before, so "SELECT SQL_CALC_FOUND_ROWS
// a FROM t" asked for a column by that name.
var selectModifiers = map[string]bool{ //nolint:gochecknoglobals // a fixed table
	"HIGH_PRIORITY": true, "STRAIGHT_JOIN": true, "SQL_SMALL_RESULT": true,
	"SQL_BIG_RESULT": true, "SQL_BUFFER_RESULT": true, "SQL_NO_CACHE": true,
	"SQL_CALC_FOUND_ROWS": true, "SQL_CACHE": true,
}

// parseSelectCore reads one SELECT with its clauses.
func (p *Parser) parseSelectCore() (ast.QueryBody, error) {
	core := &ast.SelectCore{Span: p.span()}
	p.pos++ // SELECT

	// Only the statement's first select list can be followed by an INTO, so the
	// mark is spent here: every core read after this one is inside something.
	intoAllowed := p.intoAllowed
	p.intoAllowed = false

	if p.dialect == dialects.MySQL {
		for p.cur().Kind == token.Word && selectModifiers[upper(p.cur().Text)] {
			core.Modifiers = append(core.Modifiers, upper(p.advance().Text))
		}
	}
	switch {
	case p.eatWord("DISTINCT"):
		core.Distinct = true
		if p.dialect == dialects.PostgreSQL && p.eatWord("ON") {
			if err := p.expectOp("("); err != nil {
				return nil, err
			}
			list, err := p.parseExprList()
			if err != nil {
				return nil, err
			}
			core.DistinctOn = list
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
		}
	case p.eatWord("ALL"):
		core.All = true
	}
	if p.dialect == dialects.GoogleSQL && p.atWord("AS") {
		// SELECT AS STRUCT and SELECT AS VALUE, which change the row's type.
		return nil, p.unsupportedf("SELECT AS is not supported; SQLite has no struct type")
	}

	items, err := p.parseSelectItems()
	if err != nil {
		return nil, err
	}
	core.Items = items

	if err := p.parseSelectInto(core, intoAllowed); err != nil {
		return nil, err
	}
	if p.eatWord("FROM") {
		from, err := p.parseFrom()
		if err != nil {
			return nil, err
		}
		core.From = from
	}
	if p.eatWord("WHERE") {
		where, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		core.Where = where
	}
	if err := p.parseGroupBy(core); err != nil {
		return nil, err
	}
	if p.eatWord("HAVING") {
		having, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		core.Having = having
	}
	if p.eatWord("WINDOW") {
		windows, err := p.parseNamedWindows()
		if err != nil {
			return nil, err
		}
		core.Windows = windows
	}
	if p.dialect == dialects.GoogleSQL && p.eatWord("QUALIFY") {
		qualify, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		core.Qualify = qualify
	}
	return core, nil
}

// parseGroupBy reads GROUP BY together with the grouping-set spellings.
func (p *Parser) parseGroupBy(core *ast.SelectCore) error {
	if !p.eatWords("GROUP", "BY") {
		return nil
	}
	if p.dialect == dialects.GoogleSQL && p.eatWord("ALL") {
		core.GroupByAll = true
		return nil
	}
	// PostgreSQL writes ALL or DISTINCT in front of the grouping elements to
	// say whether the grouping sets they produce are deduplicated. A plain list
	// of expressions produces one set either way, and the lists that produce
	// more -- GROUPING SETS, ROLLUP, CUBE -- are refused below, so the word is
	// read and dropped.
	if p.dialect == dialects.PostgreSQL && p.atAnyWord("ALL", "DISTINCT") {
		p.pos++
	}
	switch {
	case p.atWords("GROUPING", "SETS"):
		return p.parseGroupingClause(core, ast.GroupingSets, 2)
	case p.atWord("ROLLUP"):
		return p.parseGroupingClause(core, ast.GroupingRollup, 1)
	case p.atWord("CUBE"):
		return p.parseGroupingClause(core, ast.GroupingCube, 1)
	}
	list, err := p.parseExprList()
	if err != nil {
		return err
	}
	core.GroupBy = list
	if p.eatWords(kwWith, "ROLLUP") {
		core.Grouping = &ast.GroupingClause{Kind: ast.GroupingRollup, Sets: [][]ast.Expr{list}, Span: core.Span}
	}
	return nil
}

// atWords reports whether the cursor is on the given keyword run.
func (p *Parser) atWords(words ...string) bool { return p.matchesWords(words) }

// parseGroupingClause reads GROUPING SETS, ROLLUP or CUBE, whose lists SQLite
// has no form for. Reading them rather than stopping at the word is what lets
// the refusal name the clause.
func (p *Parser) parseGroupingClause(core *ast.SelectCore, kind ast.GroupingKind, words int) error {
	span := p.span()
	p.pos += words
	if err := p.expectOp("("); err != nil {
		return err
	}
	var sets [][]ast.Expr
	for {
		if p.eatOp("(") {
			var set []ast.Expr
			if !p.atOp(")") {
				list, err := p.parseExprList()
				if err != nil {
					return err
				}
				set = list
			}
			if err := p.expectOp(")"); err != nil {
				return err
			}
			sets = append(sets, set)
		} else {
			e, err := p.parseExpr(precLowest)
			if err != nil {
				return err
			}
			sets = append(sets, []ast.Expr{e})
		}
		if !p.eatOp(",") {
			break
		}
	}
	if err := p.expectOp(")"); err != nil {
		return err
	}
	core.Grouping = &ast.GroupingClause{Kind: kind, Sets: sets, Span: span}
	return nil
}

// parseSelectItems reads the select list.
func (p *Parser) parseSelectItems() ([]ast.SelectItem, error) {
	var items []ast.SelectItem
	for {
		span := p.span()
		from := p.pos
		e, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		item := ast.SelectItem{Expr: e, Span: span, Source: p.sourceText(from, p.pos)}
		// The token the alias begins at, taken before it is consumed, so the
		// refusal below points at what the caller wrote rather than at
		// whatever follows it.
		aliasStart := p.cur()
		if alias, quoted, ok, err := p.parseAlias(); err != nil {
			return nil, err
		} else if ok {
			// A star stands for every column of a row, so there is nothing for
			// one name to name, and no engine this package reads takes an alias
			// on one. Taking it here rendered "SELECT * AS a", which SQLite
			// refuses as a syntax error near an AS the caller may not even have
			// written -- an answer about this package's own text.
			if _, isStar := e.(*ast.Star); isStar {
				return nil, unsupportedAt(aliasStart,
					"a star cannot be given a name; it stands for every column of the row")
			}
			item.Alias, item.AliasQuoted = alias, quoted
		}
		items = append(items, item)
		if !p.eatOp(",") {
			return items, nil
		}
	}
}

// parseSelectInto reads the INTO that can follow a select list. PostgreSQL
// writes one to create a table from the query, which SQLite spells CREATE TABLE
// ... AS SELECT, so the target is carried up to the statement parser; it is
// read only where the query is the whole statement, since that is the only
// place the rewrite can happen and a silently dropped INTO would answer without
// creating anything. MySQL writes one to fill a file or a session variable,
// neither of which SQLite has.
func (p *Parser) parseSelectInto(core *ast.SelectCore, allowed bool) error {
	if !p.atWord("INTO") {
		return nil
	}
	if p.dialect == dialects.MySQL {
		return p.refuseSelectIntoTarget()
	}
	if p.dialect != dialects.PostgreSQL || !allowed {
		return nil
	}
	p.pos++ // INTO
	core.IntoTemporary = p.eatWord("TEMPORARY") || p.eatWord("TEMP")
	p.eatWord("UNLOGGED")
	p.eatWord("TABLE")
	name, err := p.parseTableNameOnly()
	if err != nil {
		return err
	}
	core.Into = name
	return nil
}

// refuseSelectIntoTarget refuses MySQL's INTO by name.
func (p *Parser) refuseSelectIntoTarget() error {
	return p.unsupportedf(
		"SELECT ... INTO is not supported; SQLite writes no file and holds no session variable")
}

// clauseKeywords are the words that end a select item or a table reference, so
// a bare word among them is a clause rather than an alias.
var clauseKeywords = map[string]bool{ //nolint:gochecknoglobals // a fixed table
	"FROM": true, "WHERE": true, "GROUP": true, "HAVING": true, "WINDOW": true,
	"ORDER": true, "LIMIT": true, "OFFSET": true, "FETCH": true, "UNION": true,
	"INTERSECT": true, "EXCEPT": true, "MINUS": true, "ON": true, "USING": true,
	"JOIN": true, "INNER": true, "LEFT": true, "RIGHT": true, "FULL": true,
	"CROSS": true, "NATURAL": true, "STRAIGHT_JOIN": true, "QUALIFY": true,
	"RETURNING": true, "SET": true, "VALUES": true, kwWith: true, "INTO": true,
	"FOR": true, "LATERAL": true, "TABLESAMPLE": true, "PARTITION": true,
	kwAnd: true, "OR": true, "AS": true, "WHEN": true, "THEN": true, "ELSE": true,
	kwEnd: true, "DO": true, "NOTHING": true, "CONFLICT": true, "IGNORE": true,
	"ASC": true, "DESC": true, "NULLS": true, "SEPARATOR": true, "ESCAPE": true,
	"IS": true, "NOT": true, "IN": true, "LIKE": true, "BETWEEN": true,
	// A query cannot be an alias, and one of these after a table name opens the
	// statement's own body.
	"SELECT": true, "TABLE": true, "USE": true, "FORCE": true,
	kwDefault: true, "DUPLICATE": true, "KEY": true,
}

// parseAlias reads an alias, with or without AS.
func (p *Parser) parseAlias() (name string, quoted, ok bool, err error) {
	if p.eatWord("AS") {
		t := p.cur()
		switch t.Kind {
		case token.Word, token.QuotedIdent:
			p.pos++
			return t.Text, t.Kind == token.QuotedIdent, true, nil
		case token.String:
			// MySQL and SQLite accept a string as an alias. It names a column,
			// so it is read as a name.
			p.pos++
			return t.Text, true, true, nil
		default:
			return "", false, false, p.unexpected("an alias")
		}
	}
	t := p.cur()
	switch t.Kind {
	case token.QuotedIdent:
		p.pos++
		return t.Text, true, true, nil
	case token.String:
		// MySQL names a column with a bare string: "SELECT 1 'b'" is a column
		// called b. PostgreSQL and GoogleSQL do not, and reading one there
		// would take a literal for a name.
		if p.dialect != dialects.MySQL {
			return "", false, false, nil
		}
		p.pos++
		return t.Text, true, true, nil
	case token.Word:
		if clauseKeywords[upper(t.Text)] {
			return "", false, false, nil
		}
		p.pos++
		return t.Text, false, true, nil
	default:
		return "", false, false, nil
	}
}

// parseOrderBy reads an ORDER BY when one is there, and answers nothing when it
// is not. Six places may be followed by one and each wrote the words, the call
// and the error check out for itself.
func (p *Parser) parseOrderBy() ([]ast.OrderTerm, error) {
	if !p.eatWords("ORDER", "BY") {
		return nil, nil
	}
	return p.parseOrderTerms()
}

// parseStatementTail reads the ORDER BY and the LIMIT a statement may end with.
// Four of them may -- the two spellings of SELECT, UPDATE and DELETE -- and
// they end the same way because SQL says so, not because they happen to.
func (p *Parser) parseStatementTail() ([]ast.OrderTerm, *ast.LimitClause, error) {
	order, err := p.parseOrderBy()
	if err != nil {
		return nil, nil, err
	}
	limit, err := p.parseLimit()
	if err != nil {
		return nil, nil, err
	}
	return order, limit, nil
}

// parseOrderTerms reads the terms of an ORDER BY.
func (p *Parser) parseOrderTerms() ([]ast.OrderTerm, error) {
	var terms []ast.OrderTerm
	for {
		span := p.span()
		e, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		term := ast.OrderTerm{Expr: e, Span: span}
		if p.eatWord("COLLATE") {
			name, err := p.parseCollationName()
			if err != nil {
				return nil, err
			}
			term.Collation = name
		}
		switch {
		case p.eatWord("DESC"):
			term.Desc = true
		case p.eatWord("ASC"):
			// The default, named explicitly.
		}
		if p.eatWord("USING") {
			t := p.cur()
			if t.Kind != token.Op {
				return nil, p.unexpected("an operator")
			}
			p.pos++
			term.Using = t.Text
		}
		if p.eatWord("NULLS") {
			switch {
			case p.eatWord("FIRST"):
				term.Nulls = ast.NullsFirst
			case p.eatWord("LAST"):
				term.Nulls = ast.NullsLast
			default:
				return nil, p.unexpected("FIRST or LAST")
			}
		}
		terms = append(terms, term)
		if !p.eatOp(",") {
			return terms, nil
		}
	}
}

// parseLimit reads LIMIT, OFFSET and the FETCH spelling of the same thing.
func (p *Parser) parseLimit() (*ast.LimitClause, error) {
	switch {
	case p.atWord("LIMIT"):
		span := p.span()
		p.pos++
		clause := &ast.LimitClause{Span: span}
		if !p.eatWord("ALL") {
			count, err := p.parseExpr(precLowest)
			if err != nil {
				return nil, err
			}
			clause.Count = count
		}
		// MySQL's "LIMIT offset, count", where the first number is the offset.
		if p.eatOp(",") {
			offset := clause.Count
			count, err := p.parseExpr(precLowest)
			if err != nil {
				return nil, err
			}
			clause.Offset, clause.Count = offset, count
		}
		if p.eatWord("OFFSET") {
			offset, err := p.parseExpr(precLowest)
			if err != nil {
				return nil, err
			}
			clause.Offset = offset
		}
		return clause, nil

	case p.atWord("OFFSET"):
		span := p.span()
		p.pos++
		offset, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		// PostgreSQL writes the unit after the number, and means nothing by it.
		p.eatWord("ROW")
		p.eatWord("ROWS")
		clause := &ast.LimitClause{Offset: offset, Span: span}
		if p.atWord("FETCH") {
			fetch, err := p.parseFetch()
			if err != nil {
				return nil, err
			}
			clause.Count, clause.WithTies = fetch.Count, fetch.WithTies
		}
		return clause, nil

	case p.atWord("FETCH"):
		return p.parseFetch()

	default:
		return nil, nil
	}
}

// parseFetch reads the FETCH FIRST spelling of a row limit.
func (p *Parser) parseFetch() (*ast.LimitClause, error) {
	span := p.span()
	p.pos++ // FETCH
	if !p.eatWord("FIRST") && !p.eatWord("NEXT") {
		return nil, p.unexpected("FIRST or NEXT")
	}
	clause := &ast.LimitClause{Span: span}
	if p.atWord("ROW") || p.atWord("ROWS") {
		// "FETCH FIRST ROW ONLY" asks for one row; the number is what may be
		// left out, not the limit.
		clause.Count = &ast.Literal{Kind: ast.LitNumber, Value: "1", Span: span}
	} else {
		count, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		clause.Count = count
	}
	if !p.eatWord("ROW") && !p.eatWord("ROWS") {
		return nil, p.unexpected("ROW or ROWS")
	}
	switch {
	case p.eatWord("ONLY"):
		// The default.
	case p.eatWords(kwWith, "TIES"):
		clause.WithTies = true
	default:
		return nil, p.unexpected("ONLY or WITH TIES")
	}
	return clause, nil
}

// parseWith reads a WITH clause.
func (p *Parser) parseWith() (*ast.WithClause, error) {
	span := p.span()
	p.pos++ // WITH
	with := &ast.WithClause{Recursive: p.eatWord("RECURSIVE"), Span: span}
	for {
		cte, err := p.parseCTE()
		if err != nil {
			return nil, err
		}
		with.CTEs = append(with.CTEs, cte)
		if !p.eatOp(",") {
			return with, nil
		}
	}
}

// parseCTE reads one common table expression.
func (p *Parser) parseCTE() (ast.CTE, error) {
	span := p.span()
	t := p.cur()
	if t.Kind != token.Word && t.Kind != token.QuotedIdent {
		return ast.CTE{}, p.unexpected("a common table expression name")
	}
	p.pos++
	cte := ast.CTE{Name: t.Text, Span: span}
	if p.atOp("(") {
		cols, err := p.parseNameList()
		if err != nil {
			return ast.CTE{}, err
		}
		cte.Columns = cols
	}
	if err := p.expectWord("AS"); err != nil {
		return ast.CTE{}, err
	}
	// PostgreSQL's MATERIALIZED and NOT MATERIALIZED hints, which say how the
	// planner should treat the CTE and which SQLite has no equivalent of.
	if p.eatWord("MATERIALIZED") || p.eatWords("NOT", "MATERIALIZED") {
		return ast.CTE{}, unsupportedAt(t, "the MATERIALIZED hint on a common table expression is not supported")
	}
	if err := p.expectOp("("); err != nil {
		return ast.CTE{}, err
	}
	if p.atAnyWord("INSERT", "UPDATE", "DELETE") {
		return ast.CTE{}, p.unsupportedf(
			"a data-modifying common table expression is not supported; SQLite runs one statement at a time")
	}
	stmt, err := p.parseSelectStmt()
	if err != nil {
		return ast.CTE{}, err
	}
	cte.Stmt = stmt
	if err := p.expectOp(")"); err != nil {
		return ast.CTE{}, err
	}
	return cte, nil
}

// parseNameList reads a parenthesized list of names.
func (p *Parser) parseNameList() ([]string, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	var names []string
	for {
		t := p.cur()
		if t.Kind != token.Word && t.Kind != token.QuotedIdent {
			return nil, p.unexpected("a name")
		}
		p.pos++
		names = append(names, t.Text)
		if !p.eatOp(",") {
			break
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return names, nil
}

// parseNamedWindows reads the definitions of a WINDOW clause.
func (p *Parser) parseNamedWindows() ([]ast.NamedWindow, error) {
	var windows []ast.NamedWindow
	for {
		span := p.span()
		t := p.cur()
		if t.Kind != token.Word && t.Kind != token.QuotedIdent {
			return nil, p.unexpected("a window name")
		}
		p.pos++
		if err := p.expectWord("AS"); err != nil {
			return nil, err
		}
		spec, err := p.parseWindowSpec()
		if err != nil {
			return nil, err
		}
		windows = append(windows, ast.NamedWindow{Name: t.Text, Spec: spec, Span: span})
		if !p.eatOp(",") {
			return windows, nil
		}
	}
}

// parseWindowSpec reads a window: a name, or a parenthesized definition.
func (p *Parser) parseWindowSpec() (*ast.WindowSpec, error) {
	span := p.span()
	if !p.atOp("(") {
		t := p.cur()
		if t.Kind != token.Word && t.Kind != token.QuotedIdent {
			return nil, p.unexpected("a window name or (")
		}
		p.pos++
		return &ast.WindowSpec{Name: t.Text, Span: span}, nil
	}
	p.pos++ // (
	spec := &ast.WindowSpec{Span: span}
	// A window may open by naming the window it extends.
	if t := p.cur(); (t.Kind == token.Word || t.Kind == token.QuotedIdent) &&
		!t.IsWord("PARTITION") && !t.IsWord("ORDER") && !t.IsWord("ROWS") &&
		!t.IsWord("RANGE") && !t.IsWord("GROUPS") {
		p.pos++
		spec.Base = t.Text
	}
	if p.eatWords("PARTITION", "BY") {
		list, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		spec.PartitionBy = list
	}
	order, err := p.parseOrderBy()
	if err != nil {
		return nil, err
	}
	spec.OrderBy = order
	if p.atAnyWord("ROWS", "RANGE", "GROUPS") {
		frame, err := p.parseWindowFrame()
		if err != nil {
			return nil, err
		}
		spec.Frame = frame
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return spec, nil
}

// parseWindowFrame reads the frame clause of a window.
func (p *Parser) parseWindowFrame() (*ast.WindowFrame, error) {
	span := p.span()
	frame := &ast.WindowFrame{Span: span}
	switch {
	case p.eatWord("ROWS"):
		frame.Unit = ast.FrameRows
	case p.eatWord("RANGE"):
		frame.Unit = ast.FrameRange
	case p.eatWord("GROUPS"):
		frame.Unit = ast.FrameGroups
	}
	if p.eatWord("BETWEEN") {
		start, err := p.parseFrameBound()
		if err != nil {
			return nil, err
		}
		if err := p.expectWord(kwAnd); err != nil {
			return nil, err
		}
		end, err := p.parseFrameBound()
		if err != nil {
			return nil, err
		}
		frame.Start, frame.End = start, &end
	} else {
		start, err := p.parseFrameBound()
		if err != nil {
			return nil, err
		}
		frame.Start = start
	}
	if p.eatWord("EXCLUDE") {
		switch {
		case p.eatWords("CURRENT", "ROW"):
			frame.Exclude = ast.ExcludeCurrentRow
		case p.eatWord("GROUP"):
			frame.Exclude = ast.ExcludeGroup
		case p.eatWord("TIES"):
			frame.Exclude = ast.ExcludeTies
		case p.eatWords("NO", "OTHERS"):
			frame.Exclude = ast.ExcludeNoOthers
		default:
			return nil, p.unexpected("CURRENT ROW, GROUP, TIES or NO OTHERS")
		}
	}
	return frame, nil
}

// parseFrameBound reads one end of a window frame.
func (p *Parser) parseFrameBound() (ast.FrameBound, error) {
	span := p.span()
	switch {
	case p.eatWords("UNBOUNDED", "PRECEDING"):
		return ast.FrameBound{Kind: ast.BoundUnboundedPreceding, Span: span}, nil
	case p.eatWords("UNBOUNDED", "FOLLOWING"):
		return ast.FrameBound{Kind: ast.BoundUnboundedFollowing, Span: span}, nil
	case p.eatWords("CURRENT", "ROW"):
		return ast.FrameBound{Kind: ast.BoundCurrentRow, Span: span}, nil
	}
	offset, err := p.parseExpr(precCompare)
	if err != nil {
		return ast.FrameBound{}, err
	}
	switch {
	case p.eatWord("PRECEDING"):
		return ast.FrameBound{Kind: ast.BoundPreceding, Offset: offset, Span: span}, nil
	case p.eatWord("FOLLOWING"):
		return ast.FrameBound{Kind: ast.BoundFollowing, Offset: offset, Span: span}, nil
	default:
		return ast.FrameBound{}, p.unexpected("PRECEDING or FOLLOWING")
	}
}
