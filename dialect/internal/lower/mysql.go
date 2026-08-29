package lower

import (
	"strings"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
)

// mysqlRules lowers MySQL's meaning onto SQLite's. Where MySQL computes
// something SQLite computes differently -- division, the remainder, case
// folding, the shifts -- the operator becomes a call to a helper rather than
// SQLite's own operator, because the operator would answer a different number.
type mysqlRules struct{ baseRules }

func (*mysqlRules) Dialect() dialects.Dialect { return dialects.MySQL }

// Pre catches the calls that have to be read before their arguments are
// lowered. CONVERT writes a type where an argument goes, and lowering it as an
// expression turns CHAR(3) into a call to the CHAR helper.
func (r *mysqlRules) Pre(e ast.Expr) (ast.Expr, bool, error) {
	call, ok := e.(*ast.FuncCall)
	if !ok || callName(call) != "CONVERT" || call.Syntax == ast.CallConvertUsing {
		return e, false, nil
	}
	replaced, err := mysqlConvert(call)
	if err != nil {
		return nil, false, err
	}
	return replaced, replaced != ast.Expr(call), nil
}

func (r *mysqlRules) Binary(b *ast.BinaryExpr) (ast.Expr, error) {
	switch b.Op {
	case ast.Div:
		// MySQL's "/" is floating-point division, so 5/2 is 2.5 rather than the
		// 2 SQLite gives for two integers.
		return helper("mysql_divide", b.Span, b.Left, b.Right), nil
	case ast.Mod:
		// SQLite truncates both operands to integers before taking the
		// remainder, so "7 % 2.5" answered 1 where MySQL answers 2.0.
		return helper("mysql_mod", b.Span, b.Left, b.Right), nil
	case ast.IntDiv:
		// DIV divides the way MySQL's "/" divides and truncates the quotient,
		// which is not what SQLite's integer division does with two integers.
		return &ast.CastExpr{
			Expr: helper("mysql_divide", b.Span, b.Left, b.Right),
			Type: ast.TypeName{Name: typeNameInteger, Written: typeNameInteger, Span: b.Span},
			Span: b.Span,
		}, nil
	case ast.BitXor:
		// A bitwise XOR, which SQLite has no operator for. Writing it as
		// (a|b)&~(a&b) would evaluate each operand twice.
		return helper("mysql_bit_xor", b.Span, b.Left, b.Right), nil
	case ast.ShiftLeft:
		// MySQL shifts an unsigned 64-bit value where SQLite shifts a signed
		// one, so ">>" brought the sign bit down instead of zeros.
		return helper("mysql_shift_left", b.Span, b.Left, b.Right), nil
	case ast.ShiftRight:
		return helper("mysql_shift_right", b.Span, b.Left, b.Right), nil
	case ast.Xor:
		// XOR sits between OR and AND, so its operands are whole
		// AND-expressions rather than the primaries a rewrite can pick out.
		return nil, unsupported(b.Span,
			"XOR is not supported: SQLite has no operator with its precedence; write (a AND NOT b) OR (NOT a AND b)")
	case ast.NullSafeEq:
		return &ast.IsExpr{Expr: b.Left, Right: b.Right, Span: b.Span}, nil
	case ast.Like, ast.NotLike:
		// MySQL's LIKE escapes with a backslash unless an ESCAPE clause says
		// otherwise, and its default collation folds case beyond ASCII.
		return likeHelper(b, "like_insensitive")
	case ast.Regexp, ast.NotRegexp:
		return regexpHelper(b, "mysql_regexp")
	case ast.SoundsLike:
		// "a SOUNDS LIKE b" is SOUNDEX(a) = SOUNDEX(b), which is what MySQL
		// documents it as.
		return binary(
			helper("mysql_soundex", b.Span, b.Left),
			ast.Eq,
			helper("mysql_soundex", b.Span, b.Right),
			b.Span,
		), nil
	case ast.MemberOf:
		return nil, unsupported(b.Span,
			"MEMBER OF is not supported; test membership with json_each in a subquery")
	case ast.Add, ast.Sub:
		return mysqlDateArithmetic(b)
	}
	return b, nil
}

// mysqlDateArithmetic turns "x + INTERVAL n unit" into the helper that adds a
// duration, which is the operator spelling of DATE_ADD.
func mysqlDateArithmetic(b *ast.BinaryExpr) (ast.Expr, error) {
	sign := "+"
	if b.Op == ast.Sub {
		sign = "-"
	}
	if iv, ok := b.Right.(*ast.IntervalExpr); ok {
		return intervalAdd(b.Left, iv, sign, b.Span)
	}
	// "INTERVAL n unit + x" is the same sum written the other way round, which
	// MySQL accepts for addition only.
	if iv, ok := b.Left.(*ast.IntervalExpr); ok && b.Op == ast.Add {
		return intervalAdd(b.Right, iv, "+", b.Span)
	}
	return b, nil
}

// intervalAdd builds the call that adds a duration to a datetime. A compound
// MySQL unit carries several fields in one value and reaches a helper of its
// own; a unit no helper knows is refused rather than passed on, because the
// helper would answer NULL and a NULL cannot be told from one in the data.
func intervalAdd(value ast.Expr, iv *ast.IntervalExpr, sign string, span ast.Span) (ast.Expr, error) {
	amount := iv.Value
	if sign == "-" {
		// Negate the whole amount, which may be an expression rather than a
		// literal: MySQL accepts DATE_SUB(d, INTERVAL n DAY) with a column n.
		amount = &ast.UnaryExpr{Op: ast.UnaryMinus, Expr: paren(amount), Span: span}
	}
	if mysqlCompositeUnits[strings.ToUpper(iv.Unit)] {
		// A compound value carries its fields in one string, so the sign is a
		// separate argument rather than a negation of the amount.
		sign := int64(1)
		if negated(amount) {
			sign = -1
		}
		return helper("mysql_interval_compound", span, value, iv.Value,
			text(strings.ToLower(iv.Unit), span), number(sign, span)), nil
	}
	unit, ok := intervalUnitName(iv.Unit)
	if !ok {
		return nil, unsupported(span, "%s is not an interval unit this package knows", iv.Unit)
	}
	return helper("interval_add", span, value, amount, text(unit, span)), nil
}

func (r *mysqlRules) Unary(u *ast.UnaryExpr) (ast.Expr, error) {
	return u, nil
}

func (r *mysqlRules) Literal(lit *ast.Literal) (ast.Expr, error) {
	switch lit.Kind {
	case ast.LitHex:
		// A hexadecimal literal means a number in one place and a byte string
		// in another, and the translation cannot see which.
		return nil, unsupported(lit.Span,
			"a hexadecimal literal is not supported: MySQL reads it as a number in an arithmetic context and as bytes "+
				"elsewhere; write CAST(x'..' AS ...) or the decimal value")
	case ast.LitBit:
		return nil, unsupported(lit.Span,
			"a bit literal is not supported: MySQL reads it as a number in an arithmetic context and as bytes "+
				"elsewhere; write the decimal value")
	default:
		return lit, nil
	}
}

func (r *mysqlRules) TypedLiteral(lit *ast.TypedLiteral) (ast.Expr, error) {
	// MySQL's typed literals name the type of a string that is already written
	// the way SQLite stores it.
	return text(lit.Value, lit.Span), nil
}

func (r *mysqlRules) Cast(c *ast.CastExpr) (ast.Expr, error) {
	return castHelper(dialects.MySQL, c, "mysql_cast")
}

func (r *mysqlRules) Call(call *ast.FuncCall) (ast.Expr, error) {
	if lowered, ok, err := commonCall(call, "mysql"); ok || err != nil {
		return lowered, err
	}
	name := callName(call)
	switch name {
	case fnNameExtract, fnNameDatePart:
		return datePartCall(call, "mysql_date_part"), nil
	case fnNameDateAdd, "ADDDATE":
		return dateArith(call, "+")
	case fnNameDateSub, "SUBDATE":
		return dateArith(call, "-")
	case "TIMESTAMPADD":
		return timestampAdd(call)
	case "TIMESTAMPDIFF":
		return timestampDiff(call)
	case "GROUP_CONCAT":
		return groupConcat(call)
	case "TIMEDIFF":
		// SQLite has its own timediff() since 3.43, which answers in SQLite's
		// interval spelling; MySQL's answers a TIME.
		return rename(call, "mysql_timediff"), nil
	case "WEEK", "WEEKOFYEAR", "YEARWEEK":
		// MySQL numbers weeks its own way, by a mode that decides which day
		// starts a week and which week is week 1.
		return rename(call, "mysql_"+strings.ToLower(name)), nil
	case fnNamePosition, "LOCATE", "INSTR":
		return mysqlPosition(call)
	case fnNameSubstring, fnNameSubstr, "MID":
		return rename(call, "mysql_substr"), nil
	case "GREATEST", "LEAST", "LEFT", "RIGHT", fnNameLpad, fnNameRpad:
		return rename(call, "mysql_"+strings.ToLower(name)), nil
	case "JSON_TYPE":
		return rename(call, "mysql_json_type"), nil
	case typeNameTimestamp:
		return rename(call, "mysql_timestamp"), nil
	case typeNameTime:
		return rename(call, "mysql_time_of_day"), nil
	case "CEIL", "CEILING", "FLOOR", "SIGN", "SQRT", "EXP", "LN", "LOG2", "LOG10":
		return rename(call, mysqlMathHelper(name)), nil
	case typeNameInterval:
		return rename(call, "mysql_interval"), nil
	case fnNameReplace:
		// SQLite answers the subject for an empty search string without looking
		// at the replacement, so a NULL replacement did not reach the result.
		return rename(call, "dialect_replace"), nil
	case typeNameChar:
		return mysqlChar(call)
	case "CONVERT":
		return mysqlConvert(call)
	case "SOUNDEX":
		return rename(call, "mysql_soundex"), nil
	case fnNameRound:
		return roundEven(call)
	case fnNameMod:
		return rename(call, "mysql_mod"), nil
	case "LENGTH", "OCTET_LENGTH":
		return rename(call, "octet_length"), nil
	case fnNameCharLength, fnNameCharLen:
		return rename(call, "length"), nil
	case "LOG":
		if len(call.Args) == 1 {
			return rename(call, "ln"), nil
		}
		return call, nil
	case fnNameFormat:
		if len(call.Args) > 2 {
			return nil, unsupported(call.Span,
				"FORMAT takes a value and a number of decimal places; its locale argument is not supported")
		}
		return rename(call, "mysql_format"), nil
	case "REGEXP_REPLACE":
		return rename(call, "mysql_regexp_replace"), nil
	case "ORD", "HEX", "QUOTE", "ASCII", "UNHEX", "INSERT":
		return rename(call, "mysql_"+strings.ToLower(name)), nil
	case "ISNULL":
		if len(call.Args) != 1 {
			return call, nil
		}
		return paren(isNull(call.Args[0], false, call.Span)), nil
	case "ATAN":
		if len(call.Args) == 2 {
			return rename(call, "atan2"), nil
		}
		return call, nil
	case fnNameConcat:
		// MySQL's CONCAT is NULL when any argument is; SQLite's concat() skips
		// a NULL argument instead.
		return rename(call, "strict_concat"), nil
	case fnNameTrim:
		return mysqlTrim(call)
	case fnNameUpper, "UCASE":
		return rename(call, "unicode_upper"), nil
	case fnNameLower, "LCASE":
		return rename(call, "unicode_lower"), nil
	}
	return call, nil
}

// negated reports whether an amount was turned round, which is how a compound
// interval carries its direction: its fields are in one string, so the sign is
// a separate argument rather than a negation of the value.
func negated(e ast.Expr) bool {
	u, ok := e.(*ast.UnaryExpr)
	return ok && u.Op == ast.UnaryMinus
}

// mysqlMathHelper names the helper for a numeric function whose MySQL reading
// of its argument differs from SQLite's.
func mysqlMathHelper(name string) string {
	if name == "CEILING" {
		// The two spellings are one function.
		name = "CEIL"
	}
	return "mysql_" + strings.ToLower(name)
}

// mysqlPosition normalizes the three spellings of "where does a substring
// start": POSITION(a IN b), LOCATE(a, b[, n]) and INSTR(b, a).
func mysqlPosition(call *ast.FuncCall) (ast.Expr, error) {
	switch callName(call) {
	case fnNamePosition:
		return position(call)
	case "LOCATE":
		// LOCATE writes the needle first, which is the order POSITION writes;
		// with a third argument it starts from a position SQLite's instr has no
		// room for.
		if len(call.Args) == 2 {
			call.Args[0], call.Args[1] = call.Args[1], call.Args[0]
			return rename(call, "INSTR"), nil
		}
		return rename(call, "locate"), nil
	default:
		return call, nil
	}
}

// mysqlTrim reads the SQL-standard TRIM into the SQLite function that trims the
// same side.
func mysqlTrim(call *ast.FuncCall) (ast.Expr, error) {
	switch call.Syntax {
	case ast.CallTrimLeading:
		return rename(call, "ltrim"), nil
	case ast.CallTrimTrailing:
		return rename(call, "rtrim"), nil
	case ast.CallTrimBoth:
		return rename(call, "trim"), nil
	default:
		return call, nil
	}
}

// mysqlChar builds bytes from code points, and refuses a charset whose bytes
// would differ from UTF-8.
func mysqlChar(call *ast.FuncCall) (ast.Expr, error) {
	if call.Syntax == ast.CallCharUsing {
		charset, _ := literalText(call.Args[len(call.Args)-1])
		if !utf8CharsetName(charset) {
			return nil, unsupported(call.Span,
				"CHAR ... USING %s is not supported; SQLite holds text as UTF-8", charset)
		}
		call.Args = call.Args[:len(call.Args)-1]
	}
	return rename(call, "mysql_char"), nil
}

// mysqlConvert is MySQL's other spelling of a cast, and of a charset
// conversion.
func mysqlConvert(call *ast.FuncCall) (ast.Expr, error) {
	if call.Syntax == ast.CallConvertUsing {
		charset, _ := literalText(call.Args[len(call.Args)-1])
		if !utf8CharsetName(charset) {
			return nil, unsupported(call.Span,
				"CONVERT ... USING %s is not supported; SQLite holds text as UTF-8", charset)
		}
		return paren(call.Args[0]), nil
	}
	if len(call.Args) != 2 {
		return call, nil
	}
	// The target is a type written where an argument goes, so the parser read
	// it as a name, or as a call when it carries a length.
	target, ok := typeArgumentText(call.Args[1])
	if !ok {
		return call, nil
	}
	call.Args[1] = text(target, call.Span)
	return rename(call, "mysql_cast"), nil
}

// typeArgumentText reads a type written where an argument belongs: a bare name,
// or a name with a length in parentheses.
func typeArgumentText(e ast.Expr) (string, bool) {
	switch n := e.(type) {
	case *ast.ColumnRef:
		if len(n.Parts) == 1 {
			return strings.ToUpper(n.Parts[0].Name), true
		}
	case *ast.FuncCall:
		if len(n.Name) != 1 {
			return "", false
		}
		params := make([]string, 0, len(n.Args))
		for _, arg := range n.Args {
			lit, ok := arg.(*ast.Literal)
			if !ok || lit.Kind != ast.LitNumber {
				return "", false
			}
			params = append(params, lit.Value)
		}
		return strings.ToUpper(n.Name[0].Name) + "(" + strings.Join(params, ",") + ")", true
	}
	return "", false
}

// utf8CharsetName reports whether a charset names the encoding a string in
// SQLite already is. Only the UTF-8 spellings do: a conversion to ucs2, utf16
// or utf32 changes the bytes, and one to binary changes how the value compares
// and sorts, neither of which dropping the conversion would reproduce.
func utf8CharsetName(name string) bool {
	switch strings.ToLower(name) {
	case "utf8", "utf8mb3", "utf8mb4", "":
		return true
	default:
		return false
	}
}

func (r *mysqlRules) Core(core *ast.SelectCore) error {
	return coreCommon(core)
}
