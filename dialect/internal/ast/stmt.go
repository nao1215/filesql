package ast

// SelectStmt is a query expression: a SELECT, or a set operation over two of
// them, together with the WITH, ORDER BY and LIMIT that belong to the whole.
//
// The shape follows SQL's own: a set operation binds looser than the SELECT on
// either side, and ORDER BY and LIMIT attach to the result of the whole
// operation rather than to its last branch.
type SelectStmt struct {
	With *WithClause
	// Body is the query without its WITH, ORDER BY and LIMIT.
	Body    QueryBody
	OrderBy []OrderTerm
	Limit   *LimitClause
	Span    Span
}

// QueryBody is either a single SELECT or a set operation.
type QueryBody interface {
	Node
	queryBodyNode()
}

// SelectCore is one SELECT with its clauses.
type SelectCore struct {
	Distinct bool
	// DistinctOn is PostgreSQL's DISTINCT ON (...) list.
	DistinctOn []Expr
	// All marks an explicit SELECT ALL.
	All bool
	// Modifiers are the words MySQL allows between SELECT and the list, such as
	// SQL_CALC_FOUND_ROWS or STRAIGHT_JOIN. They are kept so lowering can decide
	// about each rather than so the renderer can print them.
	Modifiers []string
	Items     []SelectItem
	From      []TableExpr
	Where     Expr
	GroupBy   []Expr
	// GroupByAll marks GoogleSQL's GROUP BY ALL.
	GroupByAll bool
	// Into is the table PostgreSQL's SELECT ... INTO creates. It is read only
	// where a statement stands on its own, and the statement parser turns the
	// query into the CREATE TABLE ... AS SELECT that SQLite spells, so nothing
	// below ever renders it.
	Into *TableName
	// IntoTemporary marks the TEMP or TEMPORARY of that spelling.
	IntoTemporary bool
	// GroupingSets, Rollup and Cube carry the grouping-set spellings.
	Grouping *GroupingClause
	Having   Expr
	// Windows are the named windows of a WINDOW clause.
	Windows []NamedWindow
	// Qualify is GoogleSQL's QUALIFY clause.
	Qualify Expr
	Span    Span
}

// GroupingKind says which grouping-set spelling was written.
type GroupingKind int

const (
	// GroupingSets is GROUPING SETS (...).
	GroupingSets GroupingKind = iota
	// GroupingRollup is ROLLUP (...) or MySQL's WITH ROLLUP.
	GroupingRollup
	// GroupingCube is CUBE (...).
	GroupingCube
)

// GroupingClause is a grouping-set spelling in a GROUP BY.
type GroupingClause struct {
	Kind GroupingKind
	Sets [][]Expr
	Span Span
}

// NamedWindow is one definition of a WINDOW clause.
type NamedWindow struct {
	Name string
	Spec *WindowSpec
	Span Span
}

// SetOp is a set operation over two query bodies.
type SetOp struct {
	Op    SetOperator
	All   bool
	Left  QueryBody
	Right QueryBody
	Span  Span
}

// SetOperator names a set operation.
type SetOperator int

const (
	// Union is UNION.
	Union SetOperator = iota
	// Intersect is INTERSECT.
	Intersect
	// Except is EXCEPT, which MySQL and GoogleSQL also spell that way and which
	// some dialects spell MINUS.
	Except
)

// ValuesBody is a VALUES clause standing where a SELECT can stand.
type ValuesBody struct {
	Rows [][]Expr
	Span Span
}

// SelectItem is one entry of a select list.
type SelectItem struct {
	Expr Expr
	// Source is the item as the caller wrote it, normalized. SQLite names an
	// unaliased result column after the text of the expression that produced
	// it, so an item whose text translation changed needs this to keep the name
	// the caller's query gave it.
	Source string
	// Alias is the AS name, empty when there is none.
	Alias string
	// AliasQuoted records whether the alias was written quoted.
	AliasQuoted bool
	Span        Span
}

// LimitClause is LIMIT and OFFSET, in either spelling.
type LimitClause struct {
	// Count is the row limit, nil for LIMIT ALL and for OFFSET with no LIMIT.
	Count  Expr
	Offset Expr
	// WithTies marks FETCH ... WITH TIES.
	WithTies bool
	Span     Span
}

// WithClause is a WITH, with its common table expressions.
type WithClause struct {
	Recursive bool
	CTEs      []CTE
	Span      Span
}

// CTE is one common table expression.
type CTE struct {
	Name    string
	Columns []string
	// Stmt is the query the name stands for. A data-modifying CTE, which
	// PostgreSQL allows and SQLite has no form for, is refused while it is read
	// rather than modeled here.
	Stmt *SelectStmt
	Span Span
}

// TableExpr is something a FROM clause can name.
type TableExpr interface {
	Node
	tableExprNode()
}

// TableName is a possibly qualified table name with an optional alias.
type TableName struct {
	Parts   []Ident
	Alias   string
	Columns []string
	// Hints are MySQL's index hints, kept so lowering can drop or refuse them.
	Hints []string
	Span  Span
}

// SubqueryTable is a parenthesized SELECT in a FROM clause.
type SubqueryTable struct {
	Sub     *SelectStmt
	Lateral bool
	Alias   string
	Columns []string
	Span    Span
}

// FuncTable is a function call in a FROM clause, such as a table-valued
// function.
type FuncTable struct {
	Call    *FuncCall
	Lateral bool
	Alias   string
	Columns []string
	Span    Span
}

// JoinType names a join.
type JoinType int

const (
	// JoinInner is an inner join, including a comma join.
	JoinInner JoinType = iota
	// JoinLeft is a left outer join.
	JoinLeft
	// JoinRight is a right outer join.
	JoinRight
	// JoinFull is a full outer join.
	JoinFull
	// JoinCross is a cross join.
	JoinCross
)

// JoinTable is a join of two table expressions.
type JoinTable struct {
	Type  JoinType
	Left  TableExpr
	Right TableExpr
	On    Expr
	Using []string
	// Natural marks a NATURAL join.
	Natural bool
	Span    Span
}

// ParenTable is a parenthesized table expression.
type ParenTable struct {
	Inner TableExpr
	Span  Span
}

// InsertStmt is an INSERT.
type InsertStmt struct {
	With    *WithClause
	Table   *TableName
	Columns []string
	// Rows is the VALUES, nil when the source is a query.
	Rows [][]Expr
	// Query is the SELECT the rows come from, nil for VALUES.
	Query *SelectStmt
	// DefaultValues marks INSERT INTO t DEFAULT VALUES.
	DefaultValues bool
	// Or is the SQLite conflict clause word for INSERT OR REPLACE and friends,
	// which lowering also produces for MySQL's INSERT IGNORE.
	Or string
	// OnConflict is the upsert clause.
	OnConflict *OnConflictClause
	Returning  []SelectItem
	Span       Span
}

// OnConflictClause is an upsert.
type OnConflictClause struct {
	// Target is the conflicting column list, empty for a bare ON CONFLICT.
	Target []string
	// TargetWhere is the predicate of a partial unique index, which decides
	// which index the conflict is resolved against.
	TargetWhere Expr
	// DoNothing marks DO NOTHING; otherwise Set holds the assignments.
	DoNothing bool
	Set       []Assignment
	Where     Expr
	Span      Span
}

// Assignment is "column = expr" in an UPDATE or an upsert.
type Assignment struct {
	Columns []string
	Value   Expr
	Span    Span
}

// UpdateStmt is an UPDATE.
type UpdateStmt struct {
	With  *WithClause
	Table *TableName
	Set   []Assignment
	// From is PostgreSQL's UPDATE ... FROM.
	From      []TableExpr
	Where     Expr
	OrderBy   []OrderTerm
	Limit     *LimitClause
	Returning []SelectItem
	Span      Span
}

// DeleteStmt is a DELETE.
type DeleteStmt struct {
	With  *WithClause
	Table *TableName
	// Using is PostgreSQL's DELETE ... USING.
	Using     []TableExpr
	Where     Expr
	OrderBy   []OrderTerm
	Limit     *LimitClause
	Returning []SelectItem
	Span      Span
}

// CreateTableStmt is a CREATE TABLE.
type CreateTableStmt struct {
	Temporary   bool
	IfNotExists bool
	Name        *TableName
	Columns     []ColumnDef
	Constraints []TableConstraint
	// AsSelect is the query of CREATE TABLE ... AS SELECT.
	AsSelect *SelectStmt
	// WithoutRowid marks SQLite's WITHOUT ROWID; Strict marks STRICT.
	WithoutRowid bool
	Strict       bool
	Span         Span
}

// ColumnDef is one column of a CREATE TABLE.
type ColumnDef struct {
	Name        string
	Type        *TypeName
	Constraints []ColumnConstraint
	Span        Span
}

// ColumnConstraintKind names a column constraint.
type ColumnConstraintKind int

const (
	// ConstraintPrimaryKey is PRIMARY KEY.
	ConstraintPrimaryKey ColumnConstraintKind = iota
	// ConstraintNotNull is NOT NULL.
	ConstraintNotNull
	// ConstraintNull is an explicit NULL.
	ConstraintNull
	// ConstraintUnique is UNIQUE.
	ConstraintUnique
	// ConstraintCheck is CHECK (...).
	ConstraintCheck
	// ConstraintDefault is DEFAULT.
	ConstraintDefault
	// ConstraintCollate is COLLATE.
	ConstraintCollate
	// ConstraintReferences is REFERENCES.
	ConstraintReferences
	// ConstraintAutoIncrement is SQLite's AUTOINCREMENT, which lowering also
	// produces for MySQL's AUTO_INCREMENT and PostgreSQL's SERIAL.
	ConstraintAutoIncrement
	// ConstraintGenerated is a generated column.
	ConstraintGenerated
)

// ColumnConstraint is one constraint on a column.
type ColumnConstraint struct {
	Kind ColumnConstraintKind
	Name string
	// Expr is the CHECK condition, the DEFAULT value or the generated
	// expression.
	Expr Expr
	// Text carries a collation name or a referenced table clause verbatim.
	Text string
	// AutoIncrement marks a PRIMARY KEY written with AUTOINCREMENT.
	AutoIncrement bool
	Desc          bool
	Stored        bool
	Span          Span
}

// TableConstraintKind names a table constraint.
type TableConstraintKind int

const (
	// TableConstraintPrimaryKey is PRIMARY KEY (...).
	TableConstraintPrimaryKey TableConstraintKind = iota
	// TableConstraintUnique is UNIQUE (...).
	TableConstraintUnique
	// TableConstraintCheck is CHECK (...).
	TableConstraintCheck
	// TableConstraintForeignKey is FOREIGN KEY (...) REFERENCES ...
	TableConstraintForeignKey
)

// TableConstraint is one constraint on a table.
type TableConstraint struct {
	Kind    TableConstraintKind
	Name    string
	Columns []string
	Expr    Expr
	// Text carries the REFERENCES clause verbatim.
	Text string
	Span Span
}

// CreateViewStmt is a CREATE VIEW.
type CreateViewStmt struct {
	Temporary   bool
	IfNotExists bool
	Name        *TableName
	Columns     []string
	Select      *SelectStmt
	Span        Span
}

// CreateIndexStmt is a CREATE INDEX.
type CreateIndexStmt struct {
	Unique      bool
	IfNotExists bool
	Name        *TableName
	Table       *TableName
	Columns     []OrderTerm
	Where       Expr
	Span        Span
}

// DropKind names what a DROP drops.
type DropKind int

const (
	// DropTable is DROP TABLE.
	DropTable DropKind = iota
	// DropView is DROP VIEW.
	DropView
	// DropIndex is DROP INDEX.
	DropIndex
	// DropTrigger is DROP TRIGGER.
	DropTrigger
)

// DropStmt is a DROP.
type DropStmt struct {
	Kind     DropKind
	IfExists bool
	Names    []*TableName
	Span     Span
}

// AlterKind names what an ALTER TABLE does.
type AlterKind int

const (
	// AlterRenameTable is ALTER TABLE ... RENAME TO.
	AlterRenameTable AlterKind = iota
	// AlterRenameColumn is ALTER TABLE ... RENAME COLUMN ... TO.
	AlterRenameColumn
	// AlterAddColumn is ALTER TABLE ... ADD COLUMN.
	AlterAddColumn
	// AlterDropColumn is ALTER TABLE ... DROP COLUMN.
	AlterDropColumn
)

// AlterTableStmt is an ALTER TABLE, restricted to the four things SQLite can
// do. Anything else is refused by the parser rather than modeled here.
type AlterTableStmt struct {
	Kind    AlterKind
	Table   *TableName
	Name    string
	NewName string
	Column  *ColumnDef
	Span    Span
}

// TransactionKind names a transaction-control statement.
type TransactionKind int

const (
	// TxBegin is BEGIN or START TRANSACTION.
	TxBegin TransactionKind = iota
	// TxCommit is COMMIT or END.
	TxCommit
	// TxRollback is ROLLBACK.
	TxRollback
	// TxSavepoint is SAVEPOINT.
	TxSavepoint
	// TxRelease is RELEASE.
	TxRelease
)

// TransactionStmt is a transaction-control statement.
type TransactionStmt struct {
	Kind TransactionKind
	Name string
	Span Span
}

// ExplainStmt is EXPLAIN, with or without QUERY PLAN.
type ExplainStmt struct {
	QueryPlan bool
	Stmt      Stmt
	Span      Span
}

// PragmaStmt is a PRAGMA. It is passed through because it addresses SQLite
// itself rather than the source dialect.
type PragmaStmt struct {
	Name  []Ident
	Value Expr
	// Call marks the "PRAGMA name(value)" spelling.
	Call bool
	Span Span
}

// AnalyzeStmt is ANALYZE.
type AnalyzeStmt struct {
	Name *TableName
	Span Span
}

// The marker methods that make each node a Stmt, a QueryBody or a TableExpr.
func (*SelectStmt) stmtNode()      {}
func (*InsertStmt) stmtNode()      {}
func (*UpdateStmt) stmtNode()      {}
func (*DeleteStmt) stmtNode()      {}
func (*CreateTableStmt) stmtNode() {}
func (*CreateViewStmt) stmtNode()  {}
func (*CreateIndexStmt) stmtNode() {}
func (*DropStmt) stmtNode()        {}
func (*AlterTableStmt) stmtNode()  {}
func (*TransactionStmt) stmtNode() {}
func (*ExplainStmt) stmtNode()     {}
func (*PragmaStmt) stmtNode()      {}
func (*AnalyzeStmt) stmtNode()     {}

func (*SelectCore) queryBodyNode() {}
func (*SetOp) queryBodyNode()      {}
func (*ValuesBody) queryBodyNode() {}

func (*TableName) tableExprNode()     {}
func (*SubqueryTable) tableExprNode() {}
func (*FuncTable) tableExprNode()     {}
func (*JoinTable) tableExprNode()     {}
func (*ParenTable) tableExprNode()    {}

// At reports where each node starts.
func (n *SelectStmt) At() Span      { return n.Span }
func (n *SelectCore) At() Span      { return n.Span }
func (n *SetOp) At() Span           { return n.Span }
func (n *ValuesBody) At() Span      { return n.Span }
func (n *InsertStmt) At() Span      { return n.Span }
func (n *UpdateStmt) At() Span      { return n.Span }
func (n *DeleteStmt) At() Span      { return n.Span }
func (n *CreateTableStmt) At() Span { return n.Span }
func (n *CreateViewStmt) At() Span  { return n.Span }
func (n *CreateIndexStmt) At() Span { return n.Span }
func (n *DropStmt) At() Span        { return n.Span }
func (n *AlterTableStmt) At() Span  { return n.Span }
func (n *TransactionStmt) At() Span { return n.Span }
func (n *ExplainStmt) At() Span     { return n.Span }
func (n *PragmaStmt) At() Span      { return n.Span }
func (n *AnalyzeStmt) At() Span     { return n.Span }
func (n *TableName) At() Span       { return n.Span }
func (n *SubqueryTable) At() Span   { return n.Span }
func (n *FuncTable) At() Span       { return n.Span }
func (n *JoinTable) At() Span       { return n.Span }
func (n *ParenTable) At() Span      { return n.Span }
