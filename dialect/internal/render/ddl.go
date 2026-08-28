package render

import (
	"github.com/nao1215/filesql/dialect/internal/ast"
)

func (w *writer) insert(n *ast.InsertStmt) error {
	if n.With != nil {
		if err := w.with(n.With); err != nil {
			return err
		}
	}
	w.word("INSERT")
	if n.Or != "" {
		w.word("OR")
		w.word(n.Or)
	}
	w.word("INTO")
	if err := w.tableName(n.Table); err != nil {
		return err
	}
	if n.Table.Alias != "" {
		w.word("AS")
		w.word(quoteIfNeeded(n.Table.Alias))
	}
	if len(n.Columns) > 0 {
		w.word("(")
		for i, col := range n.Columns {
			if i > 0 {
				w.word(",")
			}
			w.word(quoteIfNeeded(col))
		}
		w.word(")")
	}
	switch {
	case n.DefaultValues:
		w.word("DEFAULT VALUES")
	case n.Query != nil:
		if err := w.selectStmt(n.Query); err != nil {
			return err
		}
	default:
		if err := w.values(n.Rows); err != nil {
			return err
		}
	}
	if n.OnConflict != nil {
		if err := w.onConflict(n.OnConflict); err != nil {
			return err
		}
	}
	return w.returning(n.Returning)
}

func (w *writer) onConflict(n *ast.OnConflictClause) error {
	w.word("ON CONFLICT")
	if len(n.Target) > 0 {
		w.word("(")
		for i, col := range n.Target {
			if i > 0 {
				w.word(",")
			}
			w.word(quoteIfNeeded(col))
		}
		w.word(")")
	}
	w.word("DO")
	if n.DoNothing {
		w.word("NOTHING")
		return nil
	}
	w.word("UPDATE SET")
	if err := w.assignments(n.Set); err != nil {
		return err
	}
	if n.Where != nil {
		w.word("WHERE")
		return w.expr(n.Where, precLowest)
	}
	return nil
}

func (w *writer) assignments(assigns []ast.Assignment) error {
	for i, a := range assigns {
		if i > 0 {
			w.word(",")
		}
		if len(a.Columns) == 1 {
			w.word(quoteIfNeeded(a.Columns[0]))
		} else {
			w.word("(")
			for j, col := range a.Columns {
				if j > 0 {
					w.word(",")
				}
				w.word(quoteIfNeeded(col))
			}
			w.word(")")
		}
		w.word("=")
		if err := w.expr(a.Value, precLowest); err != nil {
			return err
		}
	}
	return nil
}

func (w *writer) returning(items []ast.SelectItem) error {
	if len(items) == 0 {
		return nil
	}
	w.word("RETURNING")
	for i, item := range items {
		if i > 0 {
			w.word(",")
		}
		if err := w.expr(item.Expr, precLowest); err != nil {
			return err
		}
		if item.Alias != "" {
			w.word("AS")
			w.word(QuoteIdent(item.Alias))
		}
	}
	return nil
}

func (w *writer) update(n *ast.UpdateStmt) error {
	if n.With != nil {
		if err := w.with(n.With); err != nil {
			return err
		}
	}
	w.word("UPDATE")
	if err := w.tableName(n.Table); err != nil {
		return err
	}
	if n.Table.Alias != "" {
		w.word("AS")
		w.word(quoteIfNeeded(n.Table.Alias))
	}
	w.word("SET")
	if err := w.assignments(n.Set); err != nil {
		return err
	}
	if len(n.From) > 0 {
		w.word("FROM")
		for i, table := range n.From {
			if i > 0 {
				w.word(",")
			}
			if err := w.tableExpr(table); err != nil {
				return err
			}
		}
	}
	if n.Where != nil {
		w.word("WHERE")
		if err := w.expr(n.Where, precLowest); err != nil {
			return err
		}
	}
	if len(n.OrderBy) > 0 {
		w.word("ORDER BY")
		if err := w.orderTerms(n.OrderBy); err != nil {
			return err
		}
	}
	if err := w.limit(n.Limit); err != nil {
		return err
	}
	return w.returning(n.Returning)
}

func (w *writer) deleteStmt(n *ast.DeleteStmt) error {
	if n.With != nil {
		if err := w.with(n.With); err != nil {
			return err
		}
	}
	if len(n.Using) > 0 {
		return unsupported(n.Span, "DELETE ... USING")
	}
	w.word("DELETE FROM")
	if err := w.tableName(n.Table); err != nil {
		return err
	}
	if n.Table.Alias != "" {
		w.word("AS")
		w.word(quoteIfNeeded(n.Table.Alias))
	}
	if n.Where != nil {
		w.word("WHERE")
		if err := w.expr(n.Where, precLowest); err != nil {
			return err
		}
	}
	if len(n.OrderBy) > 0 {
		w.word("ORDER BY")
		if err := w.orderTerms(n.OrderBy); err != nil {
			return err
		}
	}
	if err := w.limit(n.Limit); err != nil {
		return err
	}
	return w.returning(n.Returning)
}

func (w *writer) createTable(n *ast.CreateTableStmt) error {
	w.word("CREATE")
	if n.Temporary {
		w.word("TEMPORARY")
	}
	w.word("TABLE")
	if n.IfNotExists {
		w.word("IF NOT EXISTS")
	}
	if err := w.tableName(n.Name); err != nil {
		return err
	}
	if n.AsSelect != nil {
		w.word("AS")
		return w.selectStmt(n.AsSelect)
	}
	w.word("(")
	for i, col := range n.Columns {
		if i > 0 {
			w.word(",")
		}
		if err := w.columnDef(col); err != nil {
			return err
		}
	}
	for i, constraint := range n.Constraints {
		if i > 0 || len(n.Columns) > 0 {
			w.word(",")
		}
		if err := w.tableConstraint(constraint); err != nil {
			return err
		}
	}
	w.word(")")
	switch {
	case n.WithoutRowid && n.Strict:
		w.word("STRICT, WITHOUT ROWID")
	case n.WithoutRowid:
		w.word("WITHOUT ROWID")
	case n.Strict:
		w.word("STRICT")
	}
	return nil
}

func (w *writer) columnDef(n ast.ColumnDef) error {
	w.word(quoteIfNeeded(n.Name))
	if n.Type != nil {
		w.word(n.Type.Name)
		if len(n.Type.Params) > 0 {
			w.b.WriteByte('(')
			for i, param := range n.Type.Params {
				if i > 0 {
					w.b.WriteByte(',')
				}
				w.b.WriteString(param)
			}
			w.b.WriteByte(')')
		}
	}
	for _, c := range n.Constraints {
		if err := w.columnConstraint(c); err != nil {
			return err
		}
	}
	return nil
}

func (w *writer) columnConstraint(n ast.ColumnConstraint) error {
	if n.Name != "" {
		w.word("CONSTRAINT")
		w.word(quoteIfNeeded(n.Name))
	}
	switch n.Kind {
	case ast.ConstraintPrimaryKey:
		w.word("PRIMARY KEY")
		if n.Desc {
			w.word("DESC")
		}
		if n.AutoIncrement {
			w.word("AUTOINCREMENT")
		}
	case ast.ConstraintNotNull:
		w.word("NOT NULL")
	case ast.ConstraintNull:
		w.word("NULL")
	case ast.ConstraintUnique:
		w.word("UNIQUE")
	case ast.ConstraintCheck:
		w.word("CHECK")
		w.word("(")
		if err := w.expr(n.Expr, precLowest); err != nil {
			return err
		}
		w.word(")")
	case ast.ConstraintDefault:
		w.word("DEFAULT")
		w.word("(")
		if err := w.expr(n.Expr, precLowest); err != nil {
			return err
		}
		w.word(")")
	case ast.ConstraintCollate:
		w.word("COLLATE")
		w.word(n.Text)
	case ast.ConstraintReferences:
		w.word(n.Text)
	case ast.ConstraintAutoIncrement:
		return unsupported(n.Span, "an auto-numbered column that is not the primary key")
	case ast.ConstraintGenerated:
		w.word("AS")
		w.word("(")
		if err := w.expr(n.Expr, precLowest); err != nil {
			return err
		}
		w.word(")")
		if n.Stored {
			w.word("STORED")
		}
	}
	return nil
}

func (w *writer) tableConstraint(n ast.TableConstraint) error {
	if n.Name != "" {
		w.word("CONSTRAINT")
		w.word(quoteIfNeeded(n.Name))
	}
	switch n.Kind {
	case ast.TableConstraintPrimaryKey, ast.TableConstraintUnique:
		if n.Kind == ast.TableConstraintPrimaryKey {
			w.word("PRIMARY KEY")
		} else {
			w.word("UNIQUE")
		}
		w.word("(")
		for i, col := range n.Columns {
			if i > 0 {
				w.word(",")
			}
			w.word(quoteIfNeeded(col))
		}
		w.word(")")
	case ast.TableConstraintCheck:
		w.word("CHECK")
		w.word("(")
		if err := w.expr(n.Expr, precLowest); err != nil {
			return err
		}
		w.word(")")
	case ast.TableConstraintForeignKey:
		w.word("FOREIGN KEY")
		w.word("(")
		for i, col := range n.Columns {
			if i > 0 {
				w.word(",")
			}
			w.word(quoteIfNeeded(col))
		}
		w.word(")")
		w.word(n.Text)
	}
	return nil
}

func (w *writer) createView(n *ast.CreateViewStmt) error {
	w.word("CREATE")
	if n.Temporary {
		w.word("TEMPORARY")
	}
	w.word("VIEW")
	if n.IfNotExists {
		w.word("IF NOT EXISTS")
	}
	if err := w.tableName(n.Name); err != nil {
		return err
	}
	if len(n.Columns) > 0 {
		w.word("(")
		for i, col := range n.Columns {
			if i > 0 {
				w.word(",")
			}
			w.word(quoteIfNeeded(col))
		}
		w.word(")")
	}
	w.word("AS")
	return w.selectStmt(n.Select)
}

func (w *writer) createIndex(n *ast.CreateIndexStmt) error {
	w.word("CREATE")
	if n.Unique {
		w.word("UNIQUE")
	}
	w.word("INDEX")
	if n.IfNotExists {
		w.word("IF NOT EXISTS")
	}
	if err := w.tableName(n.Name); err != nil {
		return err
	}
	w.word("ON")
	if err := w.tableName(n.Table); err != nil {
		return err
	}
	w.word("(")
	if err := w.orderTerms(n.Columns); err != nil {
		return err
	}
	w.word(")")
	if n.Where != nil {
		w.word("WHERE")
		return w.expr(n.Where, precLowest)
	}
	return nil
}

func (w *writer) drop(n *ast.DropStmt) error {
	w.word("DROP")
	switch n.Kind {
	case ast.DropTable:
		w.word("TABLE")
	case ast.DropView:
		w.word("VIEW")
	case ast.DropIndex:
		w.word("INDEX")
	case ast.DropTrigger:
		w.word("TRIGGER")
	}
	if n.IfExists {
		w.word("IF EXISTS")
	}
	if len(n.Names) != 1 {
		// SQLite drops one object per statement, and splitting the statement
		// here would run half of it before a failure in the other half.
		return unsupported(n.Span, "a DROP naming more than one object")
	}
	return w.tableName(n.Names[0])
}

func (w *writer) alter(n *ast.AlterTableStmt) error {
	w.word("ALTER TABLE")
	if err := w.tableName(n.Table); err != nil {
		return err
	}
	switch n.Kind {
	case ast.AlterRenameTable:
		w.word("RENAME TO")
		w.word(quoteIfNeeded(n.NewName))
	case ast.AlterRenameColumn:
		w.word("RENAME COLUMN")
		w.word(quoteIfNeeded(n.Name))
		w.word("TO")
		w.word(quoteIfNeeded(n.NewName))
	case ast.AlterAddColumn:
		w.word("ADD COLUMN")
		return w.columnDef(*n.Column)
	case ast.AlterDropColumn:
		w.word("DROP COLUMN")
		w.word(quoteIfNeeded(n.Name))
	}
	return nil
}

func (w *writer) transaction(n *ast.TransactionStmt) error {
	switch n.Kind {
	case ast.TxBegin:
		w.word("BEGIN")
	case ast.TxCommit:
		w.word("COMMIT")
	case ast.TxRollback:
		w.word("ROLLBACK")
		if n.Name != "" {
			w.word("TO SAVEPOINT")
			w.word(quoteIfNeeded(n.Name))
		}
	case ast.TxSavepoint:
		w.word("SAVEPOINT")
		w.word(quoteIfNeeded(n.Name))
	case ast.TxRelease:
		w.word("RELEASE")
		w.word(quoteIfNeeded(n.Name))
	}
	return nil
}

func (w *writer) pragma(n *ast.PragmaStmt) error {
	w.word("PRAGMA")
	for i, part := range n.Name {
		if i > 0 {
			w.b.WriteByte('.')
			w.name(part)
			continue
		}
		w.name(part)
	}
	if n.Value == nil {
		return nil
	}
	if n.Call {
		w.b.WriteByte('(')
		if err := w.expr(n.Value, precLowest); err != nil {
			return err
		}
		w.word(")")
		return nil
	}
	w.word("=")
	return w.expr(n.Value, precLowest)
}
