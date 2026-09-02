package lower

import (
	"strings"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
)

// This file holds the lowerings more than one dialect shares. A rule that only
// one dialect needs lives in that dialect's file.

// datePartArgument reads a date part written as a bare word where an argument
// belongs, which the parser leaves as a column reference because nothing at
// that point says it is a keyword. The week that names the day it starts on is
// written as a call, and the helpers spell it week_monday.
func datePartArgument(e ast.Expr) (string, bool) {
	switch n := e.(type) {
	case *ast.ColumnRef:
		if len(n.Parts) == 1 && !n.Parts[0].Quoted {
			return strings.ToLower(n.Parts[0].Name), true
		}
	case *ast.Literal:
		if n.Kind == ast.LitString {
			return strings.ToLower(n.Value), true
		}
	case *ast.FuncCall:
		if len(n.Name) != 1 || len(n.Args) != 1 || !strings.EqualFold(n.Name[0].Name, "WEEK") {
			return "", false
		}
		day, ok := datePartArgument(n.Args[0])
		if !ok {
			return "", false
		}
		return "week_" + day, true
	}
	return "", false
}

// datePartAt turns the argument at an index into the string the helpers take.
func datePartAt(call *ast.FuncCall, index int) error {
	if index >= len(call.Args) {
		return nil
	}
	part, ok := datePartArgument(call.Args[index])
	if !ok {
		return unsupported(call.Span, "%s needs a date part written as a word", callName(call))
	}
	call.Args[index] = text(normalizeDatePart(part), call.Args[index].At())
	return nil
}

// datePartCall lowers a call whose first argument is a date part, which the
// helpers take in lower case. The week that names the day it starts on is
// written as a call in the source and spelled week_monday here.
func datePartCall(call *ast.FuncCall, name string) ast.Expr {
	if len(call.Args) > 0 {
		if part, ok := literalText(call.Args[0]); ok {
			call.Args[0] = text(normalizeDatePart(part), call.Args[0].At())
		}
	}
	return rename(call, name)
}

// normalizeDatePart spells a date part the way the helpers take it.
func normalizeDatePart(part string) string {
	lower := strings.ToLower(part)
	if open := strings.IndexByte(lower, '('); open >= 0 && strings.HasSuffix(lower, ")") {
		return lower[:open] + "_" + lower[open+1:len(lower)-1]
	}
	return lower
}

// commonCall lowers the calls every dialect spells the same way and SQLite does
// not. The prefix names the dialect for the helpers whose answer differs
// between them. It reports whether it handled the call.
func commonCall(call *ast.FuncCall, prefix string) (ast.Expr, bool, error) {
	if len(call.Name) > 1 {
		return qualifiedCall(call, prefix)
	}
	switch callName(call) {
	case keywordCurrentDate, keywordCurrentTime, keywordCurrentTimestamp, keywordLocalTime, keywordLocalTimestamp:
		// The parenthesized spelling, which SQLite's parser does not accept for
		// these; the bare keyword means the same thing.
		if len(call.Args) == 0 {
			name := callName(call)
			switch name {
			case keywordLocalTime:
				name = "CURRENT_TIME"
			case keywordLocalTimestamp:
				name = "CURRENT_TIMESTAMP"
			}
			return &ast.Keyword{Name: name, Span: call.Span}, true, nil
		}
		// MySQL and PostgreSQL let these take a fractional-seconds precision.
		// The clock here reads whole seconds, so a precision cannot be
		// answered; saying so is the point, because the alternative was the
		// name reaching SQLite, which has no function by it, and the caller
		// being told their own spelling does not exist.
		return nil, true, unsupported(call.Span,
			"%s takes no arguments here; the clock this package reads has whole-second precision",
			callName(call))
	case "COALESCE":
		// SQLite's own needs two arguments, where MySQL and PostgreSQL answer
		// the value itself for one.
		if len(call.Args) == 1 {
			return paren(call.Args[0]), true, nil
		}
	}
	return nil, false, nil
}

// qualifiedCall lowers a call whose name is qualified. GoogleSQL's SAFE. prefix
// asks for the error to become NULL, which no helper can do from inside the
// call; every other qualification names a namespace SQLite does not have.
func qualifiedCall(call *ast.FuncCall, prefix string) (ast.Expr, bool, error) {
	first := strings.ToUpper(call.Name[0].Name)
	if first == "SAFE" && prefix == "googlesql" {
		return nil, false, unsupported(call.Span,
			"the SAFE prefix is not supported; it asks for an error to become NULL, "+
				"and SQLite has no way to catch one inside a query")
	}
	return nil, false, unsupported(call.Span,
		"a qualified function name is not supported; SQLite has one namespace of functions")
}

// castHelper turns a cast into the helper that converts the way the source
// dialect converts. SQLite's own CAST follows its own rules for what a string
// is worth, which differ from every source dialect's at the edges.
func castHelper(d dialects.Dialect, c *ast.CastExpr, name string) (ast.Expr, error) {
	if c.Type.Array {
		return nil, unsupported(c.Span, "a cast to an array type is not supported; SQLite has no array type")
	}
	c.Type = normalizeCastTarget(c.Type)
	if !knownCastTarget(d, c.Type.Name) {
		// SQLite's own CAST is not a conversion to a type it has never heard
		// of: it applies numeric affinity, so text became the number its
		// leading digits spell and the value was gone with nothing said. Every
		// engine here raises for a type it does not have, and so does this.
		return nil, unsupported(c.Span,
			"a cast to %s is not supported; SQLite has no type it converts to and would answer "+
				"the number the value begins with", c.Type.Written)
	}
	if c.TryCast {
		// A cast that answers NULL rather than raising has a helper of its own,
		// named the way the dialect names the cast.
		return helper(safeCastName(name), c.Span, c.Expr, text(typeText(c.Type), c.Span)), nil
	}

	return helper(name, c.Span, c.Expr, text(typeText(c.Type), c.Span)), nil
}

// normalizeCastTarget drops the noise word MySQL allows after SIGNED and
// UNSIGNED, which name the same conversion with or without it.
func normalizeCastTarget(t ast.TypeName) ast.TypeName {
	switch t.Name {
	case "SIGNED INTEGER":
		t.Name, t.Written = typeNameSigned, typeNameSigned
	case "UNSIGNED INTEGER":
		t.Name, t.Written = typeNameUnsigned, typeNameUnsigned
	}
	return t
}

// safeCastName is the helper that converts without raising: googlesql_cast
// becomes googlesql_safe_cast.
func safeCastName(name string) string {
	return strings.Replace(name, "_cast", "_safe_cast", 1)
}

// likeHelper turns a LIKE into the helper that matches the way the source
// dialect matches. SQLite's LIKE folds only ASCII and matches nothing at all
// for a pattern ending in the escape character.
func likeHelper(b *ast.BinaryExpr, name string) (ast.Expr, error) {
	// The helper takes the pattern first, the way SQLite's own like() does, and
	// the escape character last. Leaving an escaped pattern on SQLite's own
	// LIKE would answer with SQLite's rules instead of the dialect's: its
	// folding stops at ASCII, so under PostgreSQL, whose LIKE is case
	// sensitive, 'A' LIKE 'a' ESCAPE '!' would have been true.
	args := []ast.Expr{b.Right, b.Left}
	if b.Escape != nil {
		args = append(args, b.Escape)
	}
	call := helper(name, b.Span, args...)
	if b.Op == ast.NotLike || b.Op == ast.NotILike {
		return notExpr(call, b.Span), nil
	}
	return call, nil
}

// regexpHelper turns a regular-expression match into the helper that matches
// under the operands' own collation.
func regexpHelper(b *ast.BinaryExpr, name string) (ast.Expr, error) {
	call := helper(name, b.Span, b.Right, b.Left)
	if b.Op == ast.NotRegexp || b.Op == ast.NotRegexpCI {
		return notExpr(call, b.Span), nil
	}
	return call, nil
}

// bareValueNames are the words a dialect reads as a value rather than as a
// column. They reach here as column references, because nothing in the grammar
// says a bare word is a function.
//
// The three SQLite spells the same way are in here too, mapped to themselves,
// and leaving them out on the grounds that they needed no rewriting is what
// made a bare CURRENT_TIMESTAMP answer the eleven characters of its own name:
// a column reference whose name is a SQLite keyword is rendered quoted, and
// SQLite reads a quoted name that matches no column as a string rather than
// refusing it. Nothing reported that, so a query stamping rows with the time
// wrote the same text into every one of them.
var bareValueNames = map[string]string{ //nolint:gochecknoglobals // a fixed table
	keywordLocalTime:        keywordCurrentTime,
	keywordLocalTimestamp:   keywordCurrentTimestamp,
	keywordCurrentDate:      keywordCurrentDate,
	keywordCurrentTime:      keywordCurrentTime,
	keywordCurrentTimestamp: keywordCurrentTimestamp,
}

// bareValue lowers a name that is a value rather than a column.
func bareValue(ref *ast.ColumnRef) (ast.Expr, bool) {
	if len(ref.Parts) != 1 || ref.Parts[0].Quoted {
		return nil, false
	}
	name, ok := bareValueNames[strings.ToUpper(ref.Parts[0].Name)]
	if !ok {
		return nil, false
	}
	return &ast.Keyword{Name: name, Span: ref.Span}, true
}

// position normalizes the several spellings of "where does a substring start"
// onto SQLite's instr, which takes the subject first. POSITION(a IN b) writes
// them the other way round.
func position(call *ast.FuncCall) (ast.Expr, error) {
	if len(call.Args) != 2 {
		// Every engine that has POSITION takes two operands, one way round or
		// the other. Leaving the call alone sent the name to SQLite, which has
		// no POSITION, so a caller who wrote one operand was told their
		// spelling does not exist rather than that it takes two.
		return nil, unsupported(call.Span,
			"POSITION takes a substring and the string to look in, written POSITION(a IN b) or POSITION(a, b)")
	}
	if call.Syntax == ast.CallPositionIn {
		call.Args[0], call.Args[1] = call.Args[1], call.Args[0]
	}
	return rename(call, "INSTR"), nil
}

// dateArith lowers DATE_ADD, DATE_SUB, ADDDATE and SUBDATE, whose second
// argument is either an interval or, for the last two, a number of days.
func dateArith(name string, call *ast.FuncCall, sign string) (ast.Expr, error) {
	if len(call.Args) != 2 {
		return nil, unsupported(call.Span, "%s takes a value and an interval", callName(call))
	}
	if iv, ok := call.Args[1].(*ast.IntervalExpr); ok {
		return intervalAdd(name, call.Args[0], iv, sign, call.Span)
	}
	if name := callName(call); name == fnNameDateAdd || name == fnNameDateSub {
		return nil, unsupported(call.Span, "%s takes an INTERVAL", name)
	}
	// ADDDATE and SUBDATE also take a plain number, which counts days.
	amount := call.Args[1]
	if sign == "-" {
		amount = &ast.UnaryExpr{Op: ast.UnaryMinus, Expr: paren(amount), Span: call.Span}
	}
	return helper(name, call.Span, call.Args[0], amount, text("day", call.Span)), nil
}

// timestampAdd is the call spelling of adding a duration, with the unit first.
func timestampAdd(name string, call *ast.FuncCall) (ast.Expr, error) {
	if len(call.Args) != 3 {
		return nil, unsupported(call.Span, "TIMESTAMPADD takes a unit, an amount and a value")
	}
	written, ok := unitName(call.Args[0])
	if !ok {
		return nil, unsupported(call.Span, "TIMESTAMPADD needs a unit written as a word")
	}
	unit, known := intervalUnitName(written)
	if !known {
		return nil, unsupported(call.Span, "%s is not an interval unit this package knows", written)
	}
	return helper(name, call.Span, call.Args[2], call.Args[1], text(unit, call.Span)), nil
}

// timestampDiff is the difference of two datetimes in a named unit.
func timestampDiff(call *ast.FuncCall) (ast.Expr, error) {
	if len(call.Args) != 3 {
		return nil, unsupported(call.Span, "TIMESTAMPDIFF takes a unit and two values")
	}
	written, ok := unitName(call.Args[0])
	if !ok {
		return nil, unsupported(call.Span, "TIMESTAMPDIFF needs a unit written as a word")
	}
	unit, known := intervalUnitName(written)
	if !known {
		return nil, unsupported(call.Span, "%s is not an interval unit this package knows", written)
	}
	// MySQL subtracts the first from the second, which is the opposite of the
	// order the helper takes, and counts whole units rather than boundaries
	// crossed, which is its own helper.
	return helper("mysql_date_diff", call.Span, call.Args[2], call.Args[1], text(unit, call.Span)), nil
}

// unitName reads a unit written as a bare word, which the parser leaves as a
// column reference because nothing at that point says it is a keyword.
func unitName(e ast.Expr) (string, bool) {
	switch n := e.(type) {
	case *ast.ColumnRef:
		if len(n.Parts) == 1 && !n.Parts[0].Quoted {
			return strings.ToUpper(n.Parts[0].Name), true
		}
	case *ast.Literal:
		if n.Kind == ast.LitString {
			return strings.ToUpper(n.Value), true
		}
	case *ast.IntervalExpr:
		return n.Unit, true
	}
	return "", false
}

// groupConcat moves MySQL's SEPARATOR clause into the second argument SQLite's
// group_concat takes.
func groupConcat(call *ast.FuncCall) (ast.Expr, error) {
	name := callName(call)
	if call.Separator != nil {
		if call.Distinct {
			// SQLite's DISTINCT aggregates take exactly one argument, so the
			// separator has nowhere to go.
			if sep, ok := literalText(call.Separator); !ok || sep != "," {
				return nil, unsupported(call.Span,
					"GROUP_CONCAT cannot combine DISTINCT with a separator other than ',' on the SQLite backend; "+
						"drop DISTINCT, or use ','")
			}
		} else {
			call.Args = append(call.Args, call.Separator)
		}
		call.Separator = nil
	}
	// SQLite spells both GROUP_CONCAT and STRING_AGG, so only the separator
	// moves and the name the caller wrote stays.
	_ = name
	return call, nil
}

// roundEven turns ROUND into the helper that breaks a tie toward the even
// neighbor, which is what MySQL and PostgreSQL do for a floating-point
// argument and SQLite does not.
func roundEven(call *ast.FuncCall) (ast.Expr, error) {
	if len(call.Args) < 1 || len(call.Args) > 2 {
		return nil, unsupported(call.Span, "ROUND takes a value and an optional number of decimal places")
	}
	return rename(call, "dialect_round_even"), nil
}

// truncCall lowers TRUNC onto the helper that truncates at a scale, filling in
// the scale of zero the one-argument form means.
func truncScale(call *ast.FuncCall) (ast.Expr, error) {
	if len(call.Args) == 1 {
		call.Args = append(call.Args, number(0, call.Span))
	}
	return rename(call, "trunc_scale"), nil
}

// sqliteCollation maps a collation name onto the SQLite collation that means
// the same. SQLite has three, and a name that asks for a locale's ordering has
// no answer among them.
func sqliteCollation(name string) (string, bool) {
	lower := strings.ToLower(name)
	switch lower {
	case "c", "posix", "ucs_basic", "binary", "default":
		return typeNameBinary, true
	case "nocase", "rtrim":
		return strings.ToUpper(lower), true
	}
	switch {
	case strings.HasSuffix(lower, "_bin"):
		return typeNameBinary, true
	case strings.HasSuffix(lower, "_ci"), strings.HasSuffix(lower, "_ai_ci"), strings.HasSuffix(lower, "_as_ci"):
		return "NOCASE", true
	default:
		return "", false
	}
}

// collate lowers a COLLATE clause, which names an ordering rather than a value.
func (l *lowerer) collate(n *ast.CollateExpr) (ast.Expr, error) {
	inner, err := l.expr(n.Expr)
	if err != nil {
		return nil, err
	}
	n.Expr = inner
	name, ok := sqliteCollation(n.Collation)
	if !ok {
		return nil, unsupported(n.Span,
			"the collation %s is not supported; SQLite has BINARY, NOCASE and RTRIM", n.Collation)
	}
	n.Collation = name
	return n, nil
}

// quantified lowers "x = ANY (...)" and its relatives. Equality against ANY is
// IN and inequality against ALL is NOT IN; the rest have no short SQLite form
// and are refused by name rather than left to fail on the subquery's SELECT.
func (l *lowerer) quantified(n *ast.QuantifiedExpr) (ast.Expr, error) {
	left, err := l.expr(n.Left)
	if err != nil {
		return nil, err
	}
	n.Left = left
	if n.Sub != nil {
		sub, err := l.selectStmt(n.Sub)
		if err != nil {
			return nil, err
		}
		n.Sub = sub
	}
	list, err := l.exprList(n.List)
	if err != nil {
		return nil, err
	}
	n.List = list

	anyOf := n.Quant == ast.QuantAny || n.Quant == ast.QuantSome
	switch {
	case n.Op == ast.Eq && anyOf:
		return &ast.InExpr{Expr: n.Left, List: n.List, Sub: n.Sub, Span: n.Span}, nil
	case n.Op == ast.NotEq && n.Quant == ast.QuantAll:
		return &ast.InExpr{Expr: n.Left, List: n.List, Sub: n.Sub, Negated: true, Span: n.Span}, nil
	default:
		return nil, unsupported(n.Span,
			"a quantified comparison with %s is not supported; only = ANY and <> ALL have a SQLite form",
			quantifierName(n.Quant))
	}
}

// quantifierName spells a quantifier for a diagnostic.
func quantifierName(q ast.Quantifier) string {
	switch q {
	case ast.QuantAny:
		return "ANY"
	case ast.QuantSome:
		return "SOME"
	default:
		return "ALL"
	}
}

// intervalUnits are the units a duration may be written in, mapped to the
// spelling the runtime helpers take. A unit outside the table is refused rather
// than passed on: the helper would answer NULL for a unit it does not know, and
// a NULL is indistinguishable from a NULL in the data.
var intervalUnits = map[string]string{ //nolint:gochecknoglobals // a fixed table
	"MICROSECOND": "microsecond",
	"MILLISECOND": "millisecond",
	"SECOND":      "second",
	"MINUTE":      "minute",
	"HOUR":        "hour",
	"DAY":         "day",
	"WEEK":        "week",
	"MONTH":       "month",
	"QUARTER":     "quarter",
	unitYear:      "year",
}

// mysqlCompositeUnits are MySQL's compound units, which carry more than one
// field in one value. They reach a helper of their own rather than interval_add.
var mysqlCompositeUnits = map[string]bool{ //nolint:gochecknoglobals // a fixed table
	"SECOND_MICROSECOND": true, "MINUTE_MICROSECOND": true, "MINUTE_SECOND": true,
	"HOUR_MICROSECOND": true, "HOUR_SECOND": true, "HOUR_MINUTE": true,
	"DAY_MICROSECOND": true, "DAY_SECOND": true, "DAY_MINUTE": true,
	"DAY_HOUR": true, "YEAR_MONTH": true,
}

// intervalUnitName maps a written unit onto the spelling the helpers take, and
// reports whether the unit is one they know.
func intervalUnitName(unit string) (string, bool) {
	name, ok := intervalUnits[strings.ToUpper(unit)]
	return name, ok
}
