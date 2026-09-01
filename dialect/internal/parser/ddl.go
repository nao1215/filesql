package parser

import (
	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/token"
)

// parseCreate reads a CREATE. Only the three objects SQLite has are modeled;
// everything else is refused by name rather than handed to SQLite, which used
// to answer with a syntax error about a word the caller had written.
func (p *Parser) parseCreate() (ast.Stmt, error) {
	start := p.cur()
	p.pos++ // CREATE

	if p.eatWords("OR", "REPLACE") {
		return nil, unsupportedAt(start,
			"CREATE OR REPLACE is not supported; SQLite has no single statement that replaces an object")
	}
	temporary := p.eatWord("TEMPORARY") || p.eatWord("TEMP")
	if p.eatWord("UNLOGGED") {
		// An unlogged table is a table without write-ahead logging, which is
		// the only kind SQLite has when journalling is off. The word asks for
		// nothing SQLite can refuse, so it is dropped.
		_ = temporary
	}
	unique := p.eatWord("UNIQUE")

	switch {
	case p.atWord("TABLE"):
		if unique {
			return nil, p.unexpected("INDEX")
		}
		return p.parseCreateTable(start, temporary)
	case p.atWord("VIEW"):
		if unique {
			return nil, p.unexpected("INDEX")
		}
		return p.parseCreateView(start, temporary)
	case p.atWord("INDEX"):
		return p.parseCreateIndex(start, unique)
	case p.atWord("MATERIALIZED"):
		return nil, unsupportedAt(start, "CREATE MATERIALIZED VIEW is not supported; SQLite has no materialized view")
	case p.atWord("SEQUENCE"):
		return nil, unsupportedAt(start, "CREATE SEQUENCE is not supported; SQLite numbers rows with AUTOINCREMENT instead")
	case p.atWord("DATABASE"), p.atWord("SCHEMA"):
		return nil, unsupportedAt(start, "CREATE %s is not supported; a filesql database holds the files it was opened with",
			upper(p.cur().Text))
	default:
		return nil, unimplementedAt(start, "CREATE %s is not implemented", upper(p.cur().Text))
	}
}

// parseCreateTable reads a CREATE TABLE.
func (p *Parser) parseCreateTable(start token.Token, temporary bool) (ast.Stmt, error) {
	p.pos++ // TABLE
	stmt := &ast.CreateTableStmt{Temporary: temporary, Span: ast.SpanOf(start)}
	stmt.IfNotExists = p.eatWords("IF", "NOT", "EXISTS")
	name, err := p.parseTableNameOnly()
	if err != nil {
		return nil, err
	}
	stmt.Name = name

	switch {
	case p.atWord("LIKE"):
		// MySQL copies a table's structure this way. SQLite has no statement
		// for it, and its column-definition grammar is loose enough to read
		// "LIKE t" as a column named LIKE of type t, so the statement used to
		// succeed and build a table with no columns.
		return nil, unsupportedAt(start,
			"CREATE TABLE ... LIKE is not supported; SQLite has no statement that copies a table's structure")
	case p.eatWord("AS"):
		query, err := p.parseSelectStmt()
		if err != nil {
			return nil, err
		}
		stmt.AsSelect = query
		return stmt, p.parseTableOptions(stmt)
	case p.atOp("("):
		if err := p.parseTableBody(stmt); err != nil {
			return nil, err
		}
		if err := p.parseTrailingPrimaryKey(stmt); err != nil {
			return nil, err
		}
		return stmt, p.parseTableOptions(stmt)
	default:
		return nil, p.unexpected("a column list")
	}
}

// parseTrailingPrimaryKey reads the primary key Cloud Spanner writes after the
// table body rather than inside it. It is the key SQLite writes as a table
// constraint, so it is moved there.
func (p *Parser) parseTrailingPrimaryKey(stmt *ast.CreateTableStmt) error {
	if p.dialect != dialects.GoogleSQL || !p.atWords("PRIMARY", "KEY") {
		return nil
	}
	span := p.span()
	p.pos += 2
	columns, err := p.parseIndexedColumnNames()
	if err != nil {
		return err
	}
	stmt.Constraints = append(stmt.Constraints, ast.TableConstraint{
		Kind: ast.TableConstraintPrimaryKey, Columns: columns, Span: span,
	})
	return nil
}

// parseTableBody reads the parenthesized column and constraint list.
func (p *Parser) parseTableBody(stmt *ast.CreateTableStmt) error {
	if err := p.expectOp("("); err != nil {
		return err
	}
	for {
		if err := p.parseTableElement(stmt); err != nil {
			return err
		}
		if !p.eatOp(",") {
			break
		}
	}
	return p.expectOp(")")
}

// parseTableElement reads one column or table constraint.
func (p *Parser) parseTableElement(stmt *ast.CreateTableStmt) error {
	switch {
	case p.atWord("CONSTRAINT"), p.atWords("PRIMARY", "KEY"), p.atWord("UNIQUE"),
		p.atWord("CHECK"), p.atWords("FOREIGN", "KEY"):
		constraint, err := p.parseTableConstraint()
		if err != nil {
			return err
		}
		stmt.Constraints = append(stmt.Constraints, constraint)
		return nil
	case p.atWord("LIKE") && p.namesSomething(1):
		// PostgreSQL copies a table's structure from inside the body, where
		// MySQL writes LIKE after the name. The column-definition grammar is
		// loose enough to read "LIKE u" as a column named LIKE of type u, and
		// the refusal then reported a column type the caller never wrote.
		return p.unsupportedf(
			"CREATE TABLE ... LIKE is not supported; SQLite has no statement that copies a table's structure")
	case p.dialect == dialects.MySQL && p.atAnyWord("INDEX", "KEY", "FULLTEXT", "SPATIAL"):
		// A secondary index written inside the table. SQLite creates an index
		// with its own statement, and there is nothing here to attach it to
		// yet, so it is refused rather than dropped: dropping it would lose the
		// index the caller asked for without saying so.
		return p.unsupportedf(
			"an index declared inside CREATE TABLE is not supported; write a separate CREATE INDEX")
	default:
		column, err := p.parseColumnDef()
		if err != nil {
			return err
		}
		stmt.Columns = append(stmt.Columns, column)
		return nil
	}
}

// parseColumnDef reads one column definition.
func (p *Parser) parseColumnDef() (ast.ColumnDef, error) {
	span := p.span()
	t := p.cur()
	if t.Kind != token.Word && t.Kind != token.QuotedIdent {
		return ast.ColumnDef{}, p.unexpected("a column name")
	}
	p.pos++
	column := ast.ColumnDef{Name: t.Text, Span: span}
	if p.cur().Kind == token.Word && !p.startsColumnConstraint() {
		typ, err := p.parseTypeName()
		if err != nil {
			return ast.ColumnDef{}, err
		}
		column.Type = &typ
	}
	for {
		constraint, ok, err := p.parseColumnConstraint()
		if err != nil {
			return ast.ColumnDef{}, err
		}
		if !ok {
			return column, nil
		}
		column.Constraints = append(column.Constraints, constraint)
	}
}

// startsColumnConstraint reports whether the word at the cursor opens a column
// constraint rather than naming a type.
func (p *Parser) startsColumnConstraint() bool {
	return p.atAnyWord("PRIMARY", "NOT", "NULL", "UNIQUE", "CHECK", "DEFAULT",
		"COLLATE", "REFERENCES", "CONSTRAINT", "GENERATED", "AS", "AUTOINCREMENT",
		"AUTO_INCREMENT", "COMMENT")
}

// parseColumnConstraint reads one constraint on a column.
func (p *Parser) parseColumnConstraint() (ast.ColumnConstraint, bool, error) {
	span := p.span()
	name := ""
	if p.eatWord("CONSTRAINT") {
		n, err := p.parseSimpleName()
		if err != nil {
			return ast.ColumnConstraint{}, false, err
		}
		name = n
	}
	switch {
	case p.eatWords("PRIMARY", "KEY"):
		c := ast.ColumnConstraint{Kind: ast.ConstraintPrimaryKey, Name: name, Span: span}
		if p.eatWord("DESC") {
			c.Desc = true
		} else {
			p.eatWord("ASC")
		}
		if p.eatWord("AUTOINCREMENT") || p.eatWord("AUTO_INCREMENT") {
			c.AutoIncrement = true
		}
		p.skipConflictClause()
		return c, true, nil
	case p.eatWords("NOT", "NULL"):
		p.skipConflictClause()
		return ast.ColumnConstraint{Kind: ast.ConstraintNotNull, Name: name, Span: span}, true, nil
	case p.eatWord("NULL"):
		return ast.ColumnConstraint{Kind: ast.ConstraintNull, Name: name, Span: span}, true, nil
	case p.eatWord("UNIQUE"):
		p.skipConflictClause()
		return ast.ColumnConstraint{Kind: ast.ConstraintUnique, Name: name, Span: span}, true, nil
	case p.eatWord("CHECK"):
		if err := p.expectOp("("); err != nil {
			return ast.ColumnConstraint{}, false, err
		}
		cond, err := p.parseExpr(precLowest)
		if err != nil {
			return ast.ColumnConstraint{}, false, err
		}
		if err := p.expectOp(")"); err != nil {
			return ast.ColumnConstraint{}, false, err
		}
		return ast.ColumnConstraint{Kind: ast.ConstraintCheck, Name: name, Expr: cond, Span: span}, true, nil
	case p.eatWord(kwDefault):
		value, err := p.parseDefaultValue()
		if err != nil {
			return ast.ColumnConstraint{}, false, err
		}
		return ast.ColumnConstraint{Kind: ast.ConstraintDefault, Name: name, Expr: value, Span: span}, true, nil
	case p.eatWord("COLLATE"):
		collation, err := p.parseCollationName()
		if err != nil {
			return ast.ColumnConstraint{}, false, err
		}
		return ast.ColumnConstraint{Kind: ast.ConstraintCollate, Name: name, Text: collation, Span: span}, true, nil
	case p.atWord("REFERENCES"):
		text, err := p.parseReferencesClause()
		if err != nil {
			return ast.ColumnConstraint{}, false, err
		}
		return ast.ColumnConstraint{Kind: ast.ConstraintReferences, Name: name, Text: text, Span: span}, true, nil
	case p.eatWord("AUTOINCREMENT"), p.eatWord("AUTO_INCREMENT"):
		return ast.ColumnConstraint{Kind: ast.ConstraintAutoIncrement, Name: name, Span: span}, true, nil
	case p.atWord("GENERATED"), p.atWord("AS"):
		return p.parseGeneratedColumn(name, span)
	case p.eatWord("COMMENT"):
		// MySQL's column comment. SQLite has nowhere to keep it, and leaving it
		// in the statement made it part of the declared type.
		if p.cur().Kind != token.String {
			return ast.ColumnConstraint{}, false, p.unexpected("a comment string")
		}
		p.pos++
		return ast.ColumnConstraint{}, true, nil
	default:
		if name != "" {
			return ast.ColumnConstraint{}, false, p.unexpected("a constraint")
		}
		return ast.ColumnConstraint{}, false, nil
	}
}

// parseGeneratedColumn reads a generated column, and the identity spelling that
// looks like one.
func (p *Parser) parseGeneratedColumn(name string, span ast.Span) (ast.ColumnConstraint, bool, error) {
	start := p.cur()
	if p.eatWord("GENERATED") {
		if p.eatWords("ALWAYS", "AS", "IDENTITY") || p.eatWords("BY", "DEFAULT", "AS", "IDENTITY") {
			// An identity column numbers its rows from a sequence. SQLite
			// numbers them with AUTOINCREMENT on an INTEGER PRIMARY KEY, which
			// only the primary key can be; lowering decides whether this column
			// is one, so the constraint is recorded rather than refused here.
			p.skipIdentityOptions()
			return ast.ColumnConstraint{Kind: ast.ConstraintAutoIncrement, Name: name, Span: span}, true, nil
		}
		if !p.eatWord("ALWAYS") {
			return ast.ColumnConstraint{}, false, p.unexpected("ALWAYS")
		}
	}
	if !p.eatWord("AS") {
		return ast.ColumnConstraint{}, false, unimplementedAt(start, "this column definition is not implemented")
	}
	if err := p.expectOp("("); err != nil {
		return ast.ColumnConstraint{}, false, err
	}
	expr, err := p.parseExpr(precLowest)
	if err != nil {
		return ast.ColumnConstraint{}, false, err
	}
	if err := p.expectOp(")"); err != nil {
		return ast.ColumnConstraint{}, false, err
	}
	c := ast.ColumnConstraint{Kind: ast.ConstraintGenerated, Name: name, Expr: expr, Span: span}
	switch {
	case p.eatWord("STORED"):
		c.Stored = true
	case p.eatWord("VIRTUAL"):
		// The default.
	}
	return c, true, nil
}

// skipIdentityOptions steps over the sequence options of an identity column.
func (p *Parser) skipIdentityOptions() {
	if !p.atOp("(") {
		return
	}
	depth := 0
	for !p.atEnd() {
		switch {
		case p.atOp("("):
			depth++
		case p.atOp(")"):
			depth--
		}
		p.pos++
		if depth == 0 {
			return
		}
	}
}

// skipConflictClause steps over SQLite's ON CONFLICT resolution on a
// constraint.
func (p *Parser) skipConflictClause() {
	if p.eatWords("ON", "CONFLICT") {
		p.pos++
	}
}

// parseDefaultValue reads the value of a DEFAULT clause, which may be
// parenthesized.
func (p *Parser) parseDefaultValue() (ast.Expr, error) {
	if p.atOp("(") {
		return p.parseParenExpr()
	}
	return p.parseExpr(precUnary)
}

// parseReferencesClause reads a REFERENCES clause and returns it as written,
// because SQLite spells it the same way and nothing in it needs translating.
func (p *Parser) parseReferencesClause() (string, error) {
	start := p.pos
	p.pos++ // REFERENCES
	if _, err := p.parseQualifiedName(); err != nil {
		return "", err
	}
	if p.atOp("(") {
		if _, err := p.parseNameList(); err != nil {
			return "", err
		}
	}
	for {
		switch {
		case p.eatWord("ON"):
			// The referential action. SET NULL and SET DEFAULT end in words
			// that also open a column constraint, so the action is read by
			// name rather than by scanning to the first word that could be
			// one: reading it that way stopped at the NULL and left it to be
			// taken for a constraint the caller never wrote.
			if !p.eatWord("DELETE") && !p.eatWord("UPDATE") {
				return "", p.unexpected("DELETE or UPDATE")
			}
			switch {
			case p.eatWords("SET", "NULL"), p.eatWords("SET", kwDefault),
				p.eatWord("CASCADE"), p.eatWord("RESTRICT"), p.eatWords("NO", "ACTION"):
			default:
				return "", p.unexpected("a referential action")
			}
		case p.eatWord("MATCH"):
			if !p.eatWord("SIMPLE") && !p.eatWord("FULL") && !p.eatWord("PARTIAL") {
				return "", p.unexpected("SIMPLE, FULL or PARTIAL")
			}
		case p.eatWords("NOT", "DEFERRABLE"), p.eatWord("DEFERRABLE"):
			if p.eatWord("INITIALLY") {
				if !p.eatWord("DEFERRED") && !p.eatWord("IMMEDIATE") {
					return "", p.unexpected("DEFERRED or IMMEDIATE")
				}
			}
		default:
			return p.sourceText(start, p.pos), nil
		}
	}
}

// parseTableConstraint reads one table-level constraint.
func (p *Parser) parseTableConstraint() (ast.TableConstraint, error) {
	span := p.span()
	name := ""
	if p.eatWord("CONSTRAINT") {
		n, err := p.parseSimpleName()
		if err != nil {
			return ast.TableConstraint{}, err
		}
		name = n
	}
	switch {
	case p.eatWords("PRIMARY", "KEY"):
		cols, err := p.parseIndexedColumnNames()
		if err != nil {
			return ast.TableConstraint{}, err
		}
		p.skipConflictClause()
		return ast.TableConstraint{Kind: ast.TableConstraintPrimaryKey, Name: name, Columns: cols, Span: span}, nil
	case p.eatWord("UNIQUE"):
		cols, err := p.parseIndexedColumnNames()
		if err != nil {
			return ast.TableConstraint{}, err
		}
		p.skipConflictClause()
		return ast.TableConstraint{Kind: ast.TableConstraintUnique, Name: name, Columns: cols, Span: span}, nil
	case p.eatWord("CHECK"):
		if err := p.expectOp("("); err != nil {
			return ast.TableConstraint{}, err
		}
		cond, err := p.parseExpr(precLowest)
		if err != nil {
			return ast.TableConstraint{}, err
		}
		if err := p.expectOp(")"); err != nil {
			return ast.TableConstraint{}, err
		}
		return ast.TableConstraint{Kind: ast.TableConstraintCheck, Name: name, Expr: cond, Span: span}, nil
	case p.eatWords("FOREIGN", "KEY"):
		cols, err := p.parseNameList()
		if err != nil {
			return ast.TableConstraint{}, err
		}
		text, err := p.parseReferencesClause()
		if err != nil {
			return ast.TableConstraint{}, err
		}
		return ast.TableConstraint{
			Kind: ast.TableConstraintForeignKey, Name: name, Columns: cols, Text: text, Span: span,
		}, nil
	default:
		return ast.TableConstraint{}, p.unexpected("a table constraint")
	}
}

// parseIndexedColumnNames reads a parenthesized column list that may carry a
// sort direction and an index method on each name.
func (p *Parser) parseIndexedColumnNames() ([]string, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	var names []string
	for {
		t := p.cur()
		if t.Kind != token.Word && t.Kind != token.QuotedIdent {
			return nil, p.unexpected("a column name")
		}
		p.pos++
		names = append(names, t.Text)
		if p.eatWord("COLLATE") {
			if _, err := p.parseCollationName(); err != nil {
				return nil, err
			}
		}
		p.eatWord("ASC")
		p.eatWord("DESC")
		if !p.eatOp(",") {
			break
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	// MySQL writes the index method after the column list.
	if p.eatWord("USING") {
		p.pos++
	}
	return names, nil
}

// parseTableOptions reads what follows the column list. SQLite has two options
// and every other dialect has a storage vocabulary that means nothing here.
func (p *Parser) parseTableOptions(stmt *ast.CreateTableStmt) error {
	for {
		switch {
		case p.eatWords("WITHOUT", "ROWID"):
			stmt.WithoutRowid = true
		case p.eatWord("STRICT"):
			stmt.Strict = true
		case p.atOp(","):
			p.pos++
		case p.atWord("INHERITS"):
			// The table takes its parent's columns and its rows answer the
			// parent's queries. SQLite has no table hierarchy, and dropping the
			// word would build a table missing the columns it inherits.
			return p.unsupportedf(
				"INHERITS is not supported; SQLite has no table that inherits from another")
		case p.cur().Kind == token.Word && p.dialect != dialects.SQLite:
			// A storage option: ENGINE=InnoDB, DEFAULT CHARSET=utf8mb4,
			// COMMENT='...', AUTO_INCREMENT=1, and the GoogleSQL OPTIONS and
			// CLUSTER BY clauses. None of them changes the rows, so they are
			// read and dropped rather than left to fail on the "=" that follows.
			if err := p.skipTableOption(); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

// skipTableOption steps over one storage option.
func (p *Parser) skipTableOption() error {
	switch {
	case p.atWords("PARTITION", "BY"), p.atWords("CLUSTER", "BY"):
		p.pos += 2
		if _, err := p.parseExprList(); err != nil {
			return err
		}
		return nil
	case p.atWord("OPTIONS"):
		p.pos++
		return p.skipBalancedParens()
	}
	p.pos++ // the option word
	for p.cur().Kind == token.Word && !p.atOp("=") {
		p.pos++
	}
	p.eatOp("=")
	switch p.cur().Kind {
	case token.Word, token.Number, token.String, token.QuotedIdent:
		p.pos++
		return nil
	case token.Op, token.Blob, token.Placeholder, token.Whitespace, token.LineComment, token.BlockComment:
		return nil
	default:
		return nil
	}
}

// skipBalancedParens steps over a parenthesized run whose content the parser
// does not model.
func (p *Parser) skipBalancedParens() error {
	if !p.atOp("(") {
		return p.unexpected("(")
	}
	depth := 0
	for !p.atEnd() {
		switch {
		case p.atOp("("):
			depth++
		case p.atOp(")"):
			depth--
		}
		p.pos++
		if depth == 0 {
			return nil
		}
	}
	return p.unexpected(")")
}

// parseCreateView reads a CREATE VIEW.
func (p *Parser) parseCreateView(start token.Token, temporary bool) (ast.Stmt, error) {
	p.pos++ // VIEW
	stmt := &ast.CreateViewStmt{Temporary: temporary, Span: ast.SpanOf(start)}
	stmt.IfNotExists = p.eatWords("IF", "NOT", "EXISTS")
	name, err := p.parseTableNameOnly()
	if err != nil {
		return nil, err
	}
	stmt.Name = name
	if p.atOp("(") {
		cols, err := p.parseNameList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = cols
	}
	if err := p.expectWord("AS"); err != nil {
		return nil, err
	}
	query, err := p.parseSelectStmt()
	if err != nil {
		return nil, err
	}
	stmt.Select = query
	// WITH CHECK OPTION constrains the writes made through a view. SQLite
	// refuses every write through a view that carries no INSTEAD OF trigger, so
	// the clause can never bind and is read and dropped.
	if p.atWord("WITH") {
		p.eatWords("WITH", "CASCADED", "CHECK", "OPTION")
		p.eatWords("WITH", "LOCAL", "CHECK", "OPTION")
		p.eatWords("WITH", "CHECK", "OPTION")
	}
	return stmt, nil
}

// parseCreateIndex reads a CREATE INDEX.
func (p *Parser) parseCreateIndex(start token.Token, unique bool) (ast.Stmt, error) {
	p.pos++ // INDEX
	// PostgreSQL's CONCURRENTLY builds the index without locking writers.
	// SQLite builds it one way, and the index is the same index, so the word is
	// dropped.
	p.eatWord("CONCURRENTLY")
	stmt := &ast.CreateIndexStmt{Unique: unique, Span: ast.SpanOf(start)}
	stmt.IfNotExists = p.eatWords("IF", "NOT", "EXISTS")
	name, err := p.parseTableNameOnly()
	if err != nil {
		return nil, err
	}
	stmt.Name = name
	if err := p.expectWord("ON"); err != nil {
		return nil, err
	}
	table, err := p.parseTableNameOnly()
	if err != nil {
		return nil, err
	}
	stmt.Table = table
	if p.eatWord("USING") {
		p.pos++ // the index method, which SQLite chooses for itself
	}
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	terms, err := p.parseOrderTerms()
	if err != nil {
		return nil, err
	}
	stmt.Columns = terms
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	// INCLUDE names columns the index carries without keying on them, which
	// changes how the index is stored and not what a query answers, so it is
	// read and dropped as CONCURRENTLY and the index method already are.
	if p.atWord("INCLUDE") && p.peek(1).IsOp("(") {
		p.pos++
		if err := p.skipBalancedParens(); err != nil {
			return nil, err
		}
	}
	if p.eatWord("WHERE") {
		cond, err := p.parseExpr(precLowest)
		if err != nil {
			return nil, err
		}
		stmt.Where = cond
	}
	return stmt, nil
}

// parseDrop reads a DROP.
func (p *Parser) parseDrop() (ast.Stmt, error) {
	start := p.cur()
	p.pos++ // DROP
	var kind ast.DropKind
	switch {
	case p.eatWord("TABLE"):
		kind = ast.DropTable
	case p.eatWord("VIEW"):
		kind = ast.DropView
	case p.eatWord("INDEX"):
		kind = ast.DropIndex
	case p.eatWord("TRIGGER"):
		kind = ast.DropTrigger
	default:
		return nil, unimplementedAt(start, "DROP %s is not implemented", upper(p.cur().Text))
	}
	p.eatWord("CONCURRENTLY")
	stmt := &ast.DropStmt{Kind: kind, Span: ast.SpanOf(start)}
	stmt.IfExists = p.eatWords("IF", "EXISTS")
	for {
		name, err := p.parseTableNameOnly()
		if err != nil {
			return nil, err
		}
		stmt.Names = append(stmt.Names, name)
		if !p.eatOp(",") {
			break
		}
	}
	p.eatDropBehavior()
	return stmt, nil
}

// eatDropBehavior reads the word that says what to do with what depends on a
// dropped object. CASCADE drops those objects and RESTRICT refuses when there
// are any; SQLite has neither word and behaves as RESTRICT does not, since a
// view over a dropped table simply fails when it is next used. A dialect writes
// one of them or neither, so one is read: "CASCADE RESTRICT" is a statement no
// engine takes, and reading both would translate it rather than refuse it.
func (p *Parser) eatDropBehavior() {
	if !p.eatWord("CASCADE") {
		p.eatWord("RESTRICT")
	}
}

// parseAlter reads an ALTER TABLE, restricted to the four changes SQLite can
// make. Anything else is refused by name.
func (p *Parser) parseAlter() (ast.Stmt, error) {
	start := p.cur()
	p.pos++ // ALTER
	if !p.eatWord("TABLE") {
		return nil, unimplementedAt(start, "ALTER %s is not implemented", upper(p.cur().Text))
	}
	// IF EXISTS asks for the statement to be skipped when the table is not
	// there. SQLite takes it on DROP TABLE and nowhere else, and running the
	// change anyway is the opposite of what was asked, so it is refused by
	// name; read first, so the refusal is about the words the caller wrote
	// rather than about a table named IF.
	if p.atWords("IF", "EXISTS") {
		return nil, p.unsupportedf(
			"ALTER TABLE IF EXISTS is not supported; SQLite has no IF EXISTS on ALTER TABLE")
	}
	// PostgreSQL's ONLY, and the star that is its opposite, say whether the
	// tables inheriting from this one are altered too. Nothing inherits here.
	inheritance := p.dialect == dialects.PostgreSQL && p.atWord("ONLY") && p.namesSomething(1)
	if inheritance {
		p.pos++
	}
	table, err := p.parseTableNameOnly()
	if err != nil {
		return nil, err
	}
	if p.dialect == dialects.PostgreSQL {
		p.eatOp("*")
	}
	stmt := &ast.AlterTableStmt{Table: table, Span: ast.SpanOf(start)}
	switch {
	case p.eatWords("RENAME", "TO"):
		name, err := p.parseSimpleName()
		if err != nil {
			return nil, err
		}
		stmt.Kind, stmt.NewName = ast.AlterRenameTable, name
		return p.finishAlter(stmt)
	case p.eatWord("RENAME"):
		p.eatWord("COLUMN")
		from, err := p.parseSimpleName()
		if err != nil {
			return nil, err
		}
		if err := p.expectWord("TO"); err != nil {
			return nil, err
		}
		to, err := p.parseSimpleName()
		if err != nil {
			return nil, err
		}
		stmt.Kind, stmt.Name, stmt.NewName = ast.AlterRenameColumn, from, to
		return p.finishAlter(stmt)
	case p.eatWord("ADD"):
		if err := p.refuseAlterTableElement(true); err != nil {
			return nil, err
		}
		p.eatWord("COLUMN")
		if err := p.refuseColumnExistenceCheck(true); err != nil {
			return nil, err
		}
		column, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		// FIRST and AFTER place the new column among the others. SQLite adds a
		// column after the last one and has no way to say otherwise.
		if p.atAnyWord("FIRST", "AFTER") {
			return nil, p.unsupportedf(
				"placing a column with FIRST or AFTER is not supported; SQLite adds a column after the last one")
		}
		stmt.Kind, stmt.Column = ast.AlterAddColumn, &column
		return p.finishAlter(stmt)
	case p.eatWord("DROP"):
		if err := p.refuseAlterTableElement(false); err != nil {
			return nil, err
		}
		p.eatWord("COLUMN")
		if err := p.refuseColumnExistenceCheck(false); err != nil {
			return nil, err
		}
		name, err := p.parseSimpleName()
		if err != nil {
			return nil, err
		}
		// CASCADE and RESTRICT are read and dropped here for the reason DROP
		// TABLE reads them: SQLite has neither word.
		p.eatDropBehavior()
		stmt.Kind, stmt.Name = ast.AlterDropColumn, name
		return p.finishAlter(stmt)
	case p.atAnyWord("MODIFY", "CHANGE", "ALTER"):
		return nil, p.unsupportedf(
			"changing a column's type is not supported; SQLite can only add, drop and rename columns")
	case p.atWord("SET"):
		return nil, p.unsupportedf("ALTER TABLE ... SET is not supported; SQLite has no table options to set")
	default:
		return nil, p.unimplementedf("this ALTER TABLE is not implemented")
	}
}

// finishAlter refuses a second change in the same ALTER TABLE. A dialect writes
// them in a list -- "ADD COLUMN a INT, ADD COLUMN b INT" -- and SQLite makes one
// change per statement, so the list is a statement it cannot express rather
// than one this package failed to read.
func (p *Parser) finishAlter(stmt *ast.AlterTableStmt) (ast.Stmt, error) {
	if !p.atOp(",") {
		return stmt, nil
	}
	p.pos++
	// A comma with nothing behind it is a statement that stops in the middle
	// rather than a second change, and is reported as one.
	if p.atEnd() {
		return nil, p.unexpected("another change")
	}
	return nil, p.unsupportedf(
		"an ALTER TABLE with more than one change is not supported; SQLite makes one change per statement")
}

// refuseColumnExistenceCheck refuses the IF NOT EXISTS of an added column and
// the IF EXISTS of a dropped one. SQLite has neither, and adding or dropping
// the column anyway would run the statement the caller asked to skip.
func (p *Parser) refuseColumnExistenceCheck(adding bool) error {
	if adding && p.atWords("IF", "NOT", "EXISTS") {
		return p.unsupportedf(
			"IF NOT EXISTS on an added column is not supported; SQLite has no such check")
	}
	if !adding && p.atWords("IF", "EXISTS") {
		return p.unsupportedf(
			"IF EXISTS on a dropped column is not supported; SQLite has no such check")
	}
	return nil
}

// refuseAlterTableElement refuses what follows ADD or DROP in an ALTER TABLE
// when it declares something other than a column. SQLite can only add, drop and
// rename columns, so a constraint or an index has to be refused; without this
// the words that open one are read as a column name and the refusal describes a
// column definition the caller did not write -- "ADD CONSTRAINT ck CHECK (a >
// 0)" reported the constraint's name as a column type.
//
// The keywords a column could also be called are told apart by what follows
// them. A constraint or an index takes a parenthesized column list where a
// column takes a type, and the one being dropped is named where a column being
// dropped is not, so "ADD COLUMN check INT" and "DROP unique" stay columns. A
// quoted name is not a keyword at all, which is how a column named INDEX or KEY
// is written in the dialects that reserve those words -- and every keyword read
// here is reserved in at least one of them, so a column carrying one as a bare
// name is not valid in the query's own dialect either.
func (p *Parser) refuseAlterTableElement(adding bool) error {
	constraint := func() error {
		verb := "adding"
		if !adding {
			verb = "dropping"
		}
		return p.unsupportedf(
			"%s a constraint is not supported; SQLite can only add, drop and rename columns", verb)
	}
	index := func() error {
		verb, statement := "adding", "CREATE INDEX"
		if !adding {
			verb, statement = "dropping", "DROP INDEX"
		}
		return p.unsupportedf(
			"%s an index inside ALTER TABLE is not supported; write a separate %s", verb, statement)
	}
	switch {
	case p.atAnyWord("CONSTRAINT", "PRIMARY", "FOREIGN", "EXCLUDE"):
		return constraint()
	case p.atAnyWord("UNIQUE", "CHECK") && p.declaresAnObject(adding):
		return constraint()
	case p.atAnyWord("INDEX", "KEY") && p.declaresAnObject(adding):
		return index()
	case p.atAnyWord("FULLTEXT", "SPATIAL") && (p.peek(1).IsWord("INDEX") || p.peek(1).IsWord("KEY")):
		return index()
	}
	return nil
}

// declaresAnObject reports whether the word the cursor is on opens a constraint
// or an index rather than naming a column. One being added carries a column
// list, behind an optional INDEX or KEY of MySQL's spelling and an optional
// name of its own -- "ADD UNIQUE KEY uq (a)" is the same constraint as "ADD
// UNIQUE (a)". One being dropped carries the name of what to drop, where a
// column of the same name carries nothing.
func (p *Parser) declaresAnObject(adding bool) bool {
	if !adding {
		return p.namesSomething(1)
	}
	at := 1
	if p.peek(at).IsWord("INDEX") || p.peek(at).IsWord("KEY") {
		at++
	}
	if p.namesSomething(at) {
		at++
	}
	return p.peek(at).IsOp("(")
}

// namesSomething reports whether the token n places ahead is a name.
func (p *Parser) namesSomething(n int) bool {
	kind := p.peek(n).Kind
	return kind == token.Word || kind == token.QuotedIdent
}
