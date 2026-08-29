package ast

// LiteralKind says what sort of constant a Literal holds. The kinds are the
// storage classes SQLite has plus the ones a source dialect writes and SQLite
// does not, which lowering has to decide about.
type LiteralKind int

const (
	// LitNumber is an integer or a real written in decimal.
	LitNumber LiteralKind = iota
	// LitString is a character string. Value holds the decoded content, with
	// escapes resolved and doubled quotes collapsed.
	LitString
	// LitBlob is a byte string. Value holds lowercase hexadecimal.
	LitBlob
	// LitNull is NULL.
	LitNull
	// LitBool is TRUE or FALSE.
	LitBool
	// LitHex is a hexadecimal integer literal such as 0x41.
	LitHex
	// LitBit is a bit-string literal: MySQL's 0b1010 or PostgreSQL's B'1010'.
	LitBit
)

// Literal is a constant.
type Literal struct {
	Kind  LiteralKind
	Value string
	Span  Span
}

// Ident is a name: a column, a table, an alias. Quoted records whether the name
// was written in quotes, which decides whether it may be read as a keyword.
type Ident struct {
	Name   string
	Quoted bool
	Span   Span
}

// ColumnRef is a possibly qualified column name: a, t.a, s.t.a.
type ColumnRef struct {
	// Parts is the qualification from the outside in, ending with the column.
	Parts []Ident
	Span  Span
}

// Star is "*" or "t.*" in a select list or in COUNT(*).
type Star struct {
	// Qualifier is the table part of "t.*", empty for a bare "*".
	Qualifier []Ident
	Span      Span
}

// Keyword is a value SQLite spells as a bare word rather than as a call:
// CURRENT_DATE and its two relatives. It is not an identifier, because writing
// it quoted -- which a name that collides with a keyword needs -- would make it
// a string.
type Keyword struct {
	Name string
	Span Span
}

// Placeholder is a bind parameter: ?, ?1, $1, @name, :name. Text holds it
// exactly as written, because the driver matches on that text.
type Placeholder struct {
	Text string
	Span Span
}

// UnaryOp is a prefix operator.
type UnaryOp int

const (
	// UnaryPlus is a leading "+".
	UnaryPlus UnaryOp = iota
	// UnaryMinus is arithmetic negation.
	UnaryMinus
	// UnaryNot is logical negation, written NOT or (in MySQL) "!".
	UnaryNot
	// UnaryBitNot is a bitwise complement, "~".
	UnaryBitNot
	// UnaryRegexpMatch is PostgreSQL's prefix "~" on no left operand, which is
	// not an operator at all; it is here so the parser can name it in a
	// diagnostic rather than mis-parse it.
	UnaryRegexpMatch
	// UnarySquareRoot is PostgreSQL's prefix "|/".
	UnarySquareRoot
	// UnaryCubeRoot is PostgreSQL's prefix "||/".
	UnaryCubeRoot
	// UnaryAbsolute is PostgreSQL's prefix "@".
	UnaryAbsolute
)

// UnaryExpr applies a prefix operator to one operand.
type UnaryExpr struct {
	Op   UnaryOp
	Expr Expr
	Span Span
}

// BinaryOp is an infix operator. The set covers every operator the supported
// dialects spell, including the ones SQLite has no form for; lowering decides
// what becomes of those.
type BinaryOp int

const (
	// Arithmetic.

	// Add is "+".
	Add BinaryOp = iota
	// Sub is "-".
	Sub
	// Mul is "*".
	Mul
	// Div is "/". Whether it is integer division depends on the dialect, which
	// is a lowering question rather than a parsing one.
	Div
	// Mod is "%" or MySQL's MOD operator.
	Mod
	// IntDiv is MySQL's DIV: division truncated to an integer.
	IntDiv
	// Power is PostgreSQL's "^", which raises to a power rather than taking a
	// bitwise XOR the way the same character does in the other two dialects.
	Power

	// Comparison.

	// Eq is "=" or "==".
	Eq
	// NotEq is "<>" or "!=".
	NotEq
	// Lt is "<".
	Lt
	// Lte is "<=".
	Lte
	// Gt is ">".
	Gt
	// Gte is ">=".
	Gte
	// NullSafeEq is MySQL's "<=>".
	NullSafeEq

	// Logical.

	// And is AND, or MySQL's "&&".
	And
	// Or is OR.
	Or
	// Xor is MySQL's XOR, whose precedence sits between OR and AND.
	Xor

	// Bitwise.

	// BitAnd is "&".
	BitAnd
	// BitOr is "|".
	BitOr
	// BitXor is MySQL's "^" and PostgreSQL's "#".
	BitXor
	// ShiftLeft is "<<".
	ShiftLeft
	// ShiftRight is ">>".
	ShiftRight

	// Strings and patterns.

	// Concat is "||" where the dialect means concatenation.
	Concat
	// Like is LIKE.
	Like
	// NotLike is NOT LIKE.
	NotLike
	// ILike is PostgreSQL's ILIKE.
	ILike
	// NotILike is PostgreSQL's NOT ILIKE.
	NotILike
	// SimilarTo is PostgreSQL's SIMILAR TO.
	SimilarTo
	// NotSimilarTo is PostgreSQL's NOT SIMILAR TO.
	NotSimilarTo
	// Regexp is MySQL's REGEXP and RLIKE and PostgreSQL's "~".
	Regexp
	// NotRegexp is NOT REGEXP and PostgreSQL's "!~".
	NotRegexp
	// RegexpCI is PostgreSQL's "~*".
	RegexpCI
	// NotRegexpCI is PostgreSQL's "!~*".
	NotRegexpCI
	// SoundsLike is MySQL's SOUNDS LIKE.
	SoundsLike
	// MemberOf is MySQL's MEMBER OF.
	MemberOf

	// JSON, all PostgreSQL.

	// JSONGet is "->".
	JSONGet
	// JSONGetText is "->>".
	JSONGetText
	// JSONPathGet is "#>".
	JSONPathGet
	// JSONPathGetText is "#>>".
	JSONPathGetText
	// JSONContains is "@>".
	JSONContains
	// JSONContainedBy is "<@".
	JSONContainedBy
	// JSONExists is "?" as an operator, which the lexer cannot tell from a
	// placeholder and which is therefore never produced; it is named so the
	// parser can refuse it by name.
	JSONExists
	// JSONPathExists is "@?".
	JSONPathExists
	// JSONPathMatch is "@@".
	JSONPathMatch
	// JSONPathDelete is "#-", which removes what a path names.
	JSONPathDelete
)

// BinaryExpr applies an infix operator to two operands. Escape is the ESCAPE
// clause of a LIKE or SIMILAR TO, nil when there is none.
type BinaryExpr struct {
	Left  Expr
	Op    BinaryOp
	Right Expr
	// Spelling is the operator as the caller wrote it, kept only where the
	// dialect has more than one spelling and SQLite accepts both: writing back
	// the other one would rename an unaliased result column for nothing.
	Spelling string
	Escape   Expr
	Span     Span
}

// IsExpr is "x IS y" and its relatives: IS NULL, IS NOT NULL, IS TRUE, IS
// UNKNOWN, IS DISTINCT FROM. Negated covers the NOT spelling of each.
type IsExpr struct {
	Expr    Expr
	Right   Expr
	Negated bool
	// Distinct marks IS DISTINCT FROM, which is not the same as IS.
	Distinct bool
	Span     Span
}

// BetweenExpr is "x BETWEEN lo AND hi". Symmetric marks the SQL-standard
// BETWEEN SYMMETRIC, which SQLite has no form for.
type BetweenExpr struct {
	Expr      Expr
	Low       Expr
	High      Expr
	Negated   bool
	Symmetric bool
	Span      Span
}

// InExpr is "x IN (...)". Exactly one of List and Sub is set.
type InExpr struct {
	Expr    Expr
	List    []Expr
	Sub     *SelectStmt
	Negated bool
	Span    Span
}

// Quantifier is ANY, SOME or ALL in a quantified comparison.
type Quantifier int

const (
	// QuantAny is ANY.
	QuantAny Quantifier = iota
	// QuantSome is SOME, which means ANY.
	QuantSome
	// QuantAll is ALL.
	QuantAll
)

// QuantifiedExpr is "x = ANY (...)" and its relatives. Exactly one of List and
// Sub is set.
type QuantifiedExpr struct {
	Left  Expr
	Op    BinaryOp
	Quant Quantifier
	List  []Expr
	Sub   *SelectStmt
	Span  Span
}

// ExistsExpr is "EXISTS (subquery)".
type ExistsExpr struct {
	Sub     *SelectStmt
	Negated bool
	Span    Span
}

// SubqueryExpr is a parenthesized SELECT used as a value.
type SubqueryExpr struct {
	Sub  *SelectStmt
	Span Span
}

// ParenExpr is a parenthesized expression. It is kept in the tree because
// dropping it and re-deriving it from precedence at render time is a second
// implementation of precedence; the renderer adds parentheses where the tree
// needs them and this node records the ones the caller wrote.
type ParenExpr struct {
	Expr Expr
	Span Span
}

// RowExpr is a parenthesized list of two or more expressions: "(a, b)".
type RowExpr struct {
	Exprs []Expr
	Span  Span
}

// CaseExpr is CASE. Operand is nil for the searched form.
type CaseExpr struct {
	Operand Expr
	Whens   []WhenClause
	Else    Expr
	Span    Span
}

// WhenClause is one WHEN ... THEN ... of a CASE.
type WhenClause struct {
	Cond   Expr
	Result Expr
	Span   Span
}

// TypeName is a type as written in a cast or a column definition.
type TypeName struct {
	// Name is the type word or words, upper-cased, with no parameters:
	// "VARCHAR", "DOUBLE PRECISION", "TIMESTAMP WITH TIME ZONE".
	Name string
	// Written is the type as the caller spelled it, parameters and all. The
	// cast helpers take it as written, so a name they do not know can be
	// reported back the way it was typed.
	Written string
	// Params are the numbers in parentheses after the name, if any.
	Params []string
	// Array marks a type written with trailing brackets or as ARRAY<...>.
	Array bool
	Span  Span
}

// CastExpr is CAST(x AS t), PostgreSQL's x::t, and MySQL's CONVERT(x, t).
type CastExpr struct {
	Expr Expr
	Type TypeName
	// TryCast marks GoogleSQL's SAFE_CAST and MySQL's CONVERT with no error, a
	// cast that answers NULL rather than raising.
	TryCast bool
	Span    Span
}

// CollateExpr is "x COLLATE name".
type CollateExpr struct {
	Expr      Expr
	Collation string
	Span      Span
}

// IntervalExpr is "INTERVAL n unit", the operator spelling of a duration.
type IntervalExpr struct {
	// Value is the amount, which need not be a literal.
	Value Expr
	// Unit is the unit word, upper-cased: DAY, MONTH, MICROSECOND, or one of
	// MySQL's compound units such as DAY_SECOND.
	Unit string
	Span Span
}

// TypedLiteral is a literal introduced by its type: DATE '2024-01-01',
// TIMESTAMP '...', GoogleSQL's DATETIME and PostgreSQL's INTERVAL '1 day'.
type TypedLiteral struct {
	Type  string
	Value string
	Span  Span
}

// CallSyntax records the written form of a call whose arguments are separated
// by keywords rather than by commas. The arguments are normalized into Args in
// written order, and this says which form they came from, so lowering can
// answer what the form means without looking at token text.
type CallSyntax int

const (
	// CallPlain is the ordinary comma-separated form.
	CallPlain CallSyntax = iota
	// CallExtract is EXTRACT(unit FROM x). Args are the unit as a string
	// literal and the value.
	CallExtract
	// CallSubstringFrom is SUBSTRING(x FROM a [FOR b]).
	CallSubstringFrom
	// CallPositionIn is POSITION(a IN b).
	CallPositionIn
	// CallTrimBoth is TRIM([BOTH] [chars] FROM x), the default form.
	CallTrimBoth
	// CallTrimLeading is TRIM(LEADING [chars] FROM x).
	CallTrimLeading
	// CallTrimTrailing is TRIM(TRAILING [chars] FROM x).
	CallTrimTrailing
	// CallOverlay is OVERLAY(x PLACING y FROM n [FOR m]).
	CallOverlay
	// CallConvertUsing is CONVERT(x USING charset). Args are the value and the
	// charset name as a string literal.
	CallConvertUsing
	// CallCharUsing is CHAR(n, ... USING charset).
	CallCharUsing
	// CallSubstringSimilar is SUBSTRING(x SIMILAR p ESCAPE e). Args are the
	// subject, the pattern and the escape character.
	CallSubstringSimilar
)

// ArgName is the name written on one argument of a call, for the dialects that
// let an optional parameter be named rather than positional.
type ArgName struct {
	Index int
	Name  string
}

// FuncCall is a function or aggregate call.
type FuncCall struct {
	// Syntax records a written form whose arguments keywords separate.
	Syntax CallSyntax
	// ArgNames are the names written on arguments, for a call whose optional
	// parameters may be named.
	ArgNames []ArgName
	// Name is the function name as written, with its qualification for a
	// namespaced call such as GoogleSQL's SAFE.PARSE_DATE.
	Name []Ident
	Args []Expr
	// Star marks COUNT(*).
	Star bool
	// Distinct marks an aggregate written with DISTINCT.
	Distinct bool
	// OrderBy is the ORDER BY inside an aggregate call, which PostgreSQL and
	// GoogleSQL both allow.
	OrderBy []OrderTerm
	// Separator is MySQL's SEPARATOR clause of GROUP_CONCAT.
	Separator Expr
	// Limit is GoogleSQL's LIMIT inside an aggregate call.
	Limit Expr
	// Filter is the FILTER (WHERE ...) clause.
	Filter Expr
	// Over is the window a windowed call runs over, nil when the call is not
	// windowed.
	Over *WindowSpec
	// WithinGroup is the WITHIN GROUP (ORDER BY ...) of an ordered-set
	// aggregate.
	WithinGroup []OrderTerm
	Span        Span
}

// WindowSpec is an OVER clause: either a named window or an inline definition.
type WindowSpec struct {
	// Name is the window name for "OVER w", empty for an inline definition.
	Name string
	// Base is the existing window an inline definition builds on.
	Base        string
	PartitionBy []Expr
	OrderBy     []OrderTerm
	Frame       *WindowFrame
	Span        Span
}

// FrameUnit is the unit a window frame counts in.
type FrameUnit int

const (
	// FrameRows counts rows.
	FrameRows FrameUnit = iota
	// FrameRange counts values of the ordering expression.
	FrameRange
	// FrameGroups counts peer groups.
	FrameGroups
)

// FrameBoundKind is one end of a window frame.
type FrameBoundKind int

const (
	// BoundUnboundedPreceding is UNBOUNDED PRECEDING.
	BoundUnboundedPreceding FrameBoundKind = iota
	// BoundPreceding is "n PRECEDING".
	BoundPreceding
	// BoundCurrentRow is CURRENT ROW.
	BoundCurrentRow
	// BoundFollowing is "n FOLLOWING".
	BoundFollowing
	// BoundUnboundedFollowing is UNBOUNDED FOLLOWING.
	BoundUnboundedFollowing
)

// FrameBound is one end of a window frame.
type FrameBound struct {
	Kind   FrameBoundKind
	Offset Expr
	Span   Span
}

// FrameExclusion is the EXCLUDE clause of a window frame.
type FrameExclusion int

const (
	// ExcludeNone is no EXCLUDE clause.
	ExcludeNone FrameExclusion = iota
	// ExcludeCurrentRow is EXCLUDE CURRENT ROW.
	ExcludeCurrentRow
	// ExcludeGroup is EXCLUDE GROUP.
	ExcludeGroup
	// ExcludeTies is EXCLUDE TIES.
	ExcludeTies
	// ExcludeNoOthers is EXCLUDE NO OTHERS.
	ExcludeNoOthers
)

// WindowFrame is the frame clause of a window definition.
type WindowFrame struct {
	Unit    FrameUnit
	Start   FrameBound
	End     *FrameBound
	Exclude FrameExclusion
	Span    Span
}

// OrderTerm is one term of an ORDER BY.
type OrderTerm struct {
	Expr Expr
	// Desc marks DESC.
	Desc bool
	// Nulls is the NULLS FIRST or NULLS LAST clause.
	Nulls NullsOrder
	// Collation is a COLLATE written on the term.
	Collation string
	// Using is PostgreSQL's "USING <operator>", which SQLite has no form for.
	Using string
	Span  Span
}

// NullsOrder is where NULLs sort in an ORDER BY term.
type NullsOrder int

const (
	// NullsDefault is no NULLS clause; the dialect's own rule applies.
	NullsDefault NullsOrder = iota
	// NullsFirst is NULLS FIRST.
	NullsFirst
	// NullsLast is NULLS LAST.
	NullsLast
)

// ArrayExpr is an array constructor: PostgreSQL's ARRAY[...] and GoogleSQL's
// [...]. SQLite has no array type, so lowering refuses it; it is a node so the
// refusal can name it.
type ArrayExpr struct {
	Elems []Expr
	Span  Span
}

// SubscriptExpr is "x[i]" or "x[a:b]", an array or string subscript.
type SubscriptExpr struct {
	Expr  Expr
	Index Expr
	High  Expr
	Slice bool
	Span  Span
}

// StructExpr is GoogleSQL's STRUCT(...) or a parenthesized struct literal.
type StructExpr struct {
	Fields []Expr
	Names  []string
	Span   Span
}

// The marker methods that make each node an Expr.
func (*Literal) exprNode()        {}
func (*Ident) exprNode()          {}
func (*ColumnRef) exprNode()      {}
func (*Star) exprNode()           {}
func (*Keyword) exprNode()        {}
func (*Placeholder) exprNode()    {}
func (*UnaryExpr) exprNode()      {}
func (*BinaryExpr) exprNode()     {}
func (*IsExpr) exprNode()         {}
func (*BetweenExpr) exprNode()    {}
func (*InExpr) exprNode()         {}
func (*QuantifiedExpr) exprNode() {}
func (*ExistsExpr) exprNode()     {}
func (*SubqueryExpr) exprNode()   {}
func (*ParenExpr) exprNode()      {}
func (*RowExpr) exprNode()        {}
func (*CaseExpr) exprNode()       {}
func (*CastExpr) exprNode()       {}
func (*CollateExpr) exprNode()    {}
func (*IntervalExpr) exprNode()   {}
func (*TypedLiteral) exprNode()   {}
func (*FuncCall) exprNode()       {}
func (*ArrayExpr) exprNode()      {}
func (*SubscriptExpr) exprNode()  {}
func (*StructExpr) exprNode()     {}

// At reports where each node starts. Every node carries the position of its
// first token, so a diagnostic can name the place a construct was written.
func (n *Literal) At() Span        { return n.Span }
func (n *Ident) At() Span          { return n.Span }
func (n *ColumnRef) At() Span      { return n.Span }
func (n *Star) At() Span           { return n.Span }
func (n *Keyword) At() Span        { return n.Span }
func (n *Placeholder) At() Span    { return n.Span }
func (n *UnaryExpr) At() Span      { return n.Span }
func (n *BinaryExpr) At() Span     { return n.Span }
func (n *IsExpr) At() Span         { return n.Span }
func (n *BetweenExpr) At() Span    { return n.Span }
func (n *InExpr) At() Span         { return n.Span }
func (n *QuantifiedExpr) At() Span { return n.Span }
func (n *ExistsExpr) At() Span     { return n.Span }
func (n *SubqueryExpr) At() Span   { return n.Span }
func (n *ParenExpr) At() Span      { return n.Span }
func (n *RowExpr) At() Span        { return n.Span }
func (n *CaseExpr) At() Span       { return n.Span }
func (n *CastExpr) At() Span       { return n.Span }
func (n *CollateExpr) At() Span    { return n.Span }
func (n *IntervalExpr) At() Span   { return n.Span }
func (n *TypedLiteral) At() Span   { return n.Span }
func (n *FuncCall) At() Span       { return n.Span }
func (n *ArrayExpr) At() Span      { return n.Span }
func (n *SubscriptExpr) At() Span  { return n.Span }
func (n *StructExpr) At() Span     { return n.Span }
