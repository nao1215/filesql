package parser

import (
	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/token"
)

// parseStatement reads one statement. Every statement kind this package models
// is listed here; a word that opens none of them is refused by name, which is
// what keeps an unmodeled statement from reaching SQLite.
func (p *Parser) parseStatement() (ast.Stmt, error) {
	p.skipSemicolons()
	t := p.cur()
	if t.Kind != token.Word && !p.atOp("(") {
		return nil, p.unexpected("a statement")
	}
	if p.atWord("WITH") {
		return p.parseWithStatement()
	}
	switch {
	case p.startsSelect():
		stmt, err := p.parseSelectStmt()
		if err != nil {
			return nil, err
		}
		if err := p.parseRowLock(); err != nil {
			return nil, err
		}
		return stmt, nil
	case p.atWord("INSERT"), p.atWord("REPLACE"):
		return p.parseInsert(nil)
	case p.atWord("UPDATE"):
		return p.parseUpdate(nil)
	case p.atWord("DELETE"):
		return p.parseDelete(nil)
	case p.atWord("CREATE"):
		return p.parseCreate()
	case p.atWord("DROP"):
		return p.parseDrop()
	case p.atWord("ALTER"):
		return p.parseAlter()
	case p.atWord("EXPLAIN"):
		return p.parseExplain()
	case p.atWord("PRAGMA"):
		return p.parsePragma()
	case p.atWord("ANALYZE"):
		return p.parseAnalyze()
	case p.atAnyWord("BEGIN", "START", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE"):
		return p.parseTransaction()
	default:
		return nil, p.unimplementedf("the %s statement is not implemented", upper(t.Text))
	}
}

// parseWithStatement reads a WITH and the statement it stands in front of,
// which may be a query or a data-modifying statement.
func (p *Parser) parseWithStatement() (ast.Stmt, error) {
	span := p.span()
	with, err := p.parseWith()
	if err != nil {
		return nil, err
	}
	switch {
	case p.atWord("INSERT"), p.atWord("REPLACE"):
		return p.parseInsert(with)
	case p.atWord("UPDATE"):
		return p.parseUpdate(with)
	case p.atWord("DELETE"):
		return p.parseDelete(with)
	}
	body, err := p.parseQueryBody(precLowest)
	if err != nil {
		return nil, err
	}
	stmt := &ast.SelectStmt{With: with, Body: body, Span: span}
	if p.eatWords("ORDER", "BY") {
		terms, err := p.parseOrderTerms()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = terms
	}
	limit, err := p.parseLimit()
	if err != nil {
		return nil, err
	}
	stmt.Limit = limit
	if err := p.parseRowLock(); err != nil {
		return nil, err
	}
	return stmt, nil
}

// parseInsert reads an INSERT.
func (p *Parser) parseInsert(with *ast.WithClause) (ast.Stmt, error) {
	span := p.span()
	stmt := &ast.InsertStmt{With: with, Span: span}
	if p.eatWord("REPLACE") {
		stmt.Or = "REPLACE"
	} else {
		p.pos++ // INSERT
		if p.eatWord("OR") {
			t := p.cur()
			if t.Kind != token.Word {
				return nil, p.unexpected("a conflict resolution")
			}
			p.pos++
			stmt.Or = upper(t.Text)
		}
		if p.dialect == dialects.MySQL {
			for p.atAnyWord("LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED") {
				// A priority modifier tells MySQL when to run the statement
				// relative to others. SQLite runs one statement at a time, so
				// there is nothing for it to say.
				p.pos++
			}
			if p.eatWord("IGNORE") {
				// INSERT IGNORE skips a row that would violate a constraint,
				// which is what SQLite's OR IGNORE does.
				stmt.Or = "IGNORE"
			}
		}
	}
	p.eatWord("INTO")

	name, err := p.parseTargetName()
	if err != nil {
		return nil, err
	}
	stmt.Table = name
	if p.atOp("(") && !p.peek(1).IsWord("SELECT") {
		cols, err := p.parseNameList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = cols
	}
	if err := p.parseInsertSource(stmt); err != nil {
		return nil, err
	}
	if err := p.parseInsertConflict(stmt); err != nil {
		return nil, err
	}
	if p.eatWord("RETURNING") {
		items, err := p.parseSelectItems()
		if err != nil {
			return nil, err
		}
		stmt.Returning = items
	}
	return stmt, nil
}

// parseInsertSource reads the rows an INSERT adds.
func (p *Parser) parseInsertSource(stmt *ast.InsertStmt) error {
	switch {
	case p.eatWords(kwDefault, "VALUES"):
		stmt.DefaultValues = true
		return nil
	case p.atWord("VALUES"), p.atWord("VALUE"):
		p.pos++
		rows, err := p.parseValuesRows()
		if err != nil {
			return err
		}
		stmt.Rows = rows
		return nil
	case p.eatWord("SET"):
		// MySQL's SET form, which names each column beside its value. It is the
		// same insert written differently, so it is read into the column list
		// and the row that every other spelling produces.
		assigns, err := p.parseAssignments()
		if err != nil {
			return err
		}
		row := make([]ast.Expr, 0, len(assigns))
		for _, a := range assigns {
			if len(a.Columns) != 1 {
				return p.unexpected("a column name")
			}
			stmt.Columns = append(stmt.Columns, a.Columns[0])
			row = append(row, a.Value)
		}
		stmt.Rows = [][]ast.Expr{row}
		return nil
	case p.startsSelect():
		query, err := p.parseSelectStmt()
		if err != nil {
			return err
		}
		stmt.Query = query
		return nil
	default:
		return p.unexpected("VALUES or a query")
	}
}

// parseInsertConflict reads the upsert clause in either spelling.
func (p *Parser) parseInsertConflict(stmt *ast.InsertStmt) error {
	switch {
	case p.atWords("ON", "CONFLICT"):
		span := p.span()
		p.pos += 2
		clause := &ast.OnConflictClause{Span: span}
		if p.atOp("(") {
			cols, err := p.parseNameList()
			if err != nil {
				return err
			}
			clause.Target = cols
			if p.eatWord("WHERE") {
				// The predicate selects which partial unique index the upsert
				// applies to, so dropping it would resolve the conflict against
				// a different index.
				where, err := p.parseExpr(precLowest)
				if err != nil {
					return err
				}
				clause.TargetWhere = where
			}
		}
		if err := p.expectWord("DO"); err != nil {
			return err
		}
		switch {
		case p.eatWord("NOTHING"):
			clause.DoNothing = true
		case p.eatWord("UPDATE"):
			if err := p.expectWord("SET"); err != nil {
				return err
			}
			assigns, err := p.parseAssignments()
			if err != nil {
				return err
			}
			clause.Set = assigns
			if p.eatWord("WHERE") {
				cond, err := p.parseExpr(precLowest)
				if err != nil {
					return err
				}
				clause.Where = cond
			}
		default:
			return p.unexpected("NOTHING or UPDATE")
		}
		stmt.OnConflict = clause
		return nil

	case p.dialect == dialects.MySQL && p.atWords("ON", "DUPLICATE", "KEY", "UPDATE"):
		span := p.span()
		p.pos += 4
		assigns, err := p.parseAssignments()
		if err != nil {
			return err
		}
		stmt.OnConflict = &ast.OnConflictClause{Set: assigns, Span: span}
		return nil

	default:
		return nil
	}
}

// parseUpdate reads an UPDATE.
func (p *Parser) parseUpdate(with *ast.WithClause) (ast.Stmt, error) {
	span := p.span()
	p.pos++ // UPDATE
	if p.dialect == dialects.MySQL {
		p.eatWord("LOW_PRIORITY")
		p.eatWord("IGNORE")
	}
	name, err := p.parseTargetName()
	if err != nil {
		return nil, err
	}
	if p.atOp(",") {
		return nil, p.unsupportedf("an UPDATE of more than one table is not supported; SQLite updates one table at a time")
	}
	if err := p.expectWord("SET"); err != nil {
		return nil, err
	}
	assigns, err := p.parseAssignments()
	if err != nil {
		return nil, err
	}
	stmt := &ast.UpdateStmt{With: with, Table: name, Set: assigns, Span: span}
	if p.eatWord("FROM") {
		from, err := p.parseFrom()
		if err != nil {
			return nil, err
		}
		stmt.From = from
	}
	if p.eatWord("WHERE") {
		cond, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		stmt.Where = cond
	}
	if p.eatWords("ORDER", "BY") {
		terms, err := p.parseOrderTerms()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = terms
	}
	limit, err := p.parseLimit()
	if err != nil {
		return nil, err
	}
	stmt.Limit = limit
	if p.eatWord("RETURNING") {
		items, err := p.parseSelectItems()
		if err != nil {
			return nil, err
		}
		stmt.Returning = items
	}
	return stmt, nil
}

// parseDelete reads a DELETE.
func (p *Parser) parseDelete(with *ast.WithClause) (ast.Stmt, error) {
	span := p.span()
	p.pos++ // DELETE
	if p.dialect == dialects.MySQL {
		p.eatWord("LOW_PRIORITY")
		p.eatWord("QUICK")
		p.eatWord("IGNORE")
	}
	if !p.atWord("FROM") {
		return nil, p.unsupportedf("a DELETE naming more than one table is not supported; SQLite deletes from one table")
	}
	p.pos++ // FROM
	name, err := p.parseTargetName()
	if err != nil {
		return nil, err
	}
	stmt := &ast.DeleteStmt{With: with, Table: name, Span: span}
	if p.eatWord("USING") {
		using, err := p.parseFrom()
		if err != nil {
			return nil, err
		}
		stmt.Using = using
	}
	if p.eatWord("WHERE") {
		cond, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		stmt.Where = cond
	}
	if p.eatWords("ORDER", "BY") {
		terms, err := p.parseOrderTerms()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = terms
	}
	limit, err := p.parseLimit()
	if err != nil {
		return nil, err
	}
	stmt.Limit = limit
	if p.eatWord("RETURNING") {
		items, err := p.parseSelectItems()
		if err != nil {
			return nil, err
		}
		stmt.Returning = items
	}
	return stmt, nil
}

// parseAssignments reads a SET list.
func (p *Parser) parseAssignments() ([]ast.Assignment, error) {
	var assigns []ast.Assignment
	for {
		span := p.span()
		var columns []string
		if p.atOp("(") {
			names, err := p.parseNameList()
			if err != nil {
				return nil, err
			}
			columns = names
		} else {
			parts, err := p.parseQualifiedName()
			if err != nil {
				return nil, err
			}
			columns = []string{parts[len(parts)-1].Name}
		}
		if err := p.expectOp("="); err != nil {
			return nil, err
		}
		value, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		assigns = append(assigns, ast.Assignment{Columns: columns, Value: value, Span: span})
		if !p.eatOp(",") {
			return assigns, nil
		}
	}
}

// parseTableNameOnly reads a table name where no alias may follow: the name of
// an object being created or dropped, which is not a row source.
func (p *Parser) parseTableNameOnly() (*ast.TableName, error) {
	span := p.span()
	parts, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	return &ast.TableName{Parts: parts, Span: span}, nil
}

// parseTableNameRef reads a table name with an optional alias.
func (p *Parser) parseTableNameRef() (*ast.TableName, error) {
	span := p.span()
	parts, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	name := &ast.TableName{Parts: parts, Span: span}
	if err := p.parseTableAlias(&name.Alias, &name.Columns); err != nil {
		return nil, err
	}
	return name, nil
}

// parseTargetName reads the table a data statement writes to: a name and an
// alias, and no column list. The parentheses after it belong to the statement --
// they are the columns an INSERT names -- rather than to the table reference.
func (p *Parser) parseTargetName() (*ast.TableName, error) {
	span := p.span()
	parts, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	name := &ast.TableName{Parts: parts, Span: span}
	alias, _, ok, err := p.parseAlias()
	if err != nil {
		return nil, err
	}
	if ok {
		name.Alias = alias
	}
	return name, nil
}

// parseTransaction reads a transaction-control statement.
func (p *Parser) parseTransaction() (ast.Stmt, error) {
	span := p.span()
	t := p.advance()
	stmt := &ast.TransactionStmt{Span: span}
	switch upper(t.Text) {
	case "BEGIN":
		stmt.Kind = ast.TxBegin
		p.eatWord("DEFERRED")
		p.eatWord("IMMEDIATE")
		p.eatWord("EXCLUSIVE")
		p.eatWord("WORK")
		p.eatWord("TRANSACTION")
	case "START":
		if !p.eatWord("TRANSACTION") {
			return nil, p.unexpected("TRANSACTION")
		}
		stmt.Kind = ast.TxBegin
		// The characteristics MySQL and PostgreSQL write after START
		// TRANSACTION name an isolation level SQLite does not choose between.
		if p.atAnyWord("READ", "ISOLATION", "WITH", "DEFERRABLE", "NOT") {
			return nil, p.unsupportedf("a transaction characteristic is not supported; SQLite has one isolation level")
		}
	case "COMMIT", "END":
		stmt.Kind = ast.TxCommit
		p.eatWord("WORK")
		p.eatWord("TRANSACTION")
	case "ROLLBACK":
		stmt.Kind = ast.TxRollback
		p.eatWord("WORK")
		p.eatWord("TRANSACTION")
		if p.eatWord("TO") {
			p.eatWord("SAVEPOINT")
			name, err := p.parseSimpleName()
			if err != nil {
				return nil, err
			}
			stmt.Kind, stmt.Name = ast.TxRollback, name
		}
	case "SAVEPOINT":
		name, err := p.parseSimpleName()
		if err != nil {
			return nil, err
		}
		stmt.Kind, stmt.Name = ast.TxSavepoint, name
	case "RELEASE":
		p.eatWord("SAVEPOINT")
		name, err := p.parseSimpleName()
		if err != nil {
			return nil, err
		}
		stmt.Kind, stmt.Name = ast.TxRelease, name
	}
	return stmt, nil
}

// parseSimpleName reads one bare or quoted name.
func (p *Parser) parseSimpleName() (string, error) {
	t := p.cur()
	if t.Kind != token.Word && t.Kind != token.QuotedIdent {
		return "", p.unexpected("a name")
	}
	p.pos++
	return t.Text, nil
}

// parseExplain reads EXPLAIN, with or without QUERY PLAN.
func (p *Parser) parseExplain() (ast.Stmt, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()

	span := p.span()
	p.pos++ // EXPLAIN
	stmt := &ast.ExplainStmt{Span: span}
	if p.eatWords("QUERY", "PLAN") {
		stmt.QueryPlan = true
	}
	// PostgreSQL's parenthesized options and MySQL's FORMAT clause both ask for
	// a report SQLite does not produce.
	if p.atOp("(") || p.atWord("ANALYZE") || p.atWord("FORMAT") || p.atWord("VERBOSE") {
		return nil, p.unsupportedf("the options of EXPLAIN are not supported; SQLite explains one way")
	}
	inner, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	stmt.Stmt = inner
	return stmt, nil
}

// parsePragma reads a PRAGMA, which addresses SQLite itself.
func (p *Parser) parsePragma() (ast.Stmt, error) {
	span := p.span()
	p.pos++ // PRAGMA
	parts, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	stmt := &ast.PragmaStmt{Name: parts, Span: span}
	switch {
	case p.eatOp("="):
		value, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		stmt.Value = value
	case p.eatOp("("):
		value, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		stmt.Value, stmt.Call = value, true
	}
	return stmt, nil
}

// parseAnalyze reads ANALYZE.
func (p *Parser) parseAnalyze() (ast.Stmt, error) {
	span := p.span()
	p.pos++ // ANALYZE
	stmt := &ast.AnalyzeStmt{Span: span}
	if p.cur().Kind == token.Word || p.cur().Kind == token.QuotedIdent {
		name, err := p.parseTableNameRef()
		if err != nil {
			return nil, err
		}
		stmt.Name = name
	}
	return stmt, nil
}
