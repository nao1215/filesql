package lower

import (
	"strings"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
)

// googleRules lowers GoogleSQL's meaning onto SQLite's. Its distinguishing
// habit is to raise where SQLite answers NULL -- for a zero divisor, a negative
// length, an overflowing sum -- so many of its functions reach a helper that
// raises rather than SQLite's own.
type googleRules struct{ baseRules }

func (*googleRules) Dialect() dialects.Dialect { return dialects.GoogleSQL }

func (r *googleRules) Binary(b *ast.BinaryExpr) (ast.Expr, error) {
	switch b.Op {
	case ast.Div:
		// BigQuery divides to a double and raises on a zero divisor, where
		// SQLite truncates two integers and answers NULL.
		return helper("googlesql_divide", b.Span, b.Left, b.Right), nil
	case ast.Mod:
		return helper("googlesql_mod", b.Span, b.Left, b.Right), nil
	case ast.BitXor:
		return helper("googlesql_bit_xor", b.Span, b.Left, b.Right), nil
	case ast.BitAnd:
		// GoogleSQL applies these bytewise to a BYTES operand, where SQLite
		// reads a BLOB in an arithmetic context as the integer 0.
		return helper("googlesql_bit_and", b.Span, b.Left, b.Right), nil
	case ast.BitOr:
		return helper("googlesql_bit_or", b.Span, b.Left, b.Right), nil
	case ast.ShiftLeft:
		// BigQuery shifts an unsigned value where SQLite shifts a signed one, so
		// ">>" brought the sign bit down instead of zeros, and it refuses a
		// negative count where SQLite reads one as a shift the other way.
		return helper("googlesql_shift_left", b.Span, b.Left, b.Right), nil
	case ast.ShiftRight:
		return helper("googlesql_shift_right", b.Span, b.Left, b.Right), nil
	case ast.Like, ast.NotLike:
		// BigQuery's LIKE is case sensitive, and SQLite's folds ASCII.
		return likeHelper(b, "like_sensitive")
	case ast.Add, ast.Sub:
		if iv, ok := b.Right.(*ast.IntervalExpr); ok {
			sign := "+"
			if b.Op == ast.Sub {
				sign = "-"
			}
			return intervalAdd(fnNameIntervalAdd, b.Left, iv, sign, b.Span)
		}
	}
	return b, nil
}

func (r *googleRules) Unary(u *ast.UnaryExpr) (ast.Expr, error) {
	if u.Op == ast.UnaryBitNot {
		// GoogleSQL complements a BYTES operand byte by byte, where SQLite
		// reads a BLOB as the integer 0 and answers the complement of that.
		return helper("googlesql_bit_not", u.Span, u.Expr), nil
	}
	return u, nil
}

func (r *googleRules) Literal(lit *ast.Literal) (ast.Expr, error) {
	if lit.Kind == ast.LitBit {
		// BigQuery has no binary literal, so a 0b... reached the lexer only
		// because the number scanner reads that form for every dialect.
		return nil, unsupported(lit.Span,
			"a binary literal is not supported; write the decimal value")
	}
	return lit, nil
}

func (r *googleRules) TypedLiteral(lit *ast.TypedLiteral) (ast.Expr, error) {
	return text(lit.Value, lit.Span), nil
}

func (r *googleRules) Cast(c *ast.CastExpr) (ast.Expr, error) {
	return castHelper(dialects.GoogleSQL, c, "googlesql_cast")
}

// arrayFunctions are the scalar functions whose result is an array. Reaching
// SQLite they failed as "no such function", telling the caller a name they did
// write does not exist rather than that the construct has no SQLite form.
var arrayFunctions = map[string]bool{ //nolint:gochecknoglobals // a fixed table
	"SPLIT": true, "TO_CODE_POINTS": true, "REGEXP_EXTRACT_ALL": true,
	"GENERATE_ARRAY": true, "GENERATE_DATE_ARRAY": true, "GENERATE_TIMESTAMP_ARRAY": true,
	"ARRAY_CONCAT": true, "ARRAY_REVERSE": true, "JSON_EXTRACT_ARRAY": true,
	"JSON_QUERY_ARRAY": true, "JSON_VALUE_ARRAY": true, "ARRAY_TRANSFORM": true,
	"ARRAY_FILTER": true, "ARRAY_ZIP": true, "ARRAY_SLICE": true,
	"ARRAY_AGG": true, "ARRAY_CONCAT_AGG": true, "APPROX_QUANTILES": true,
	"APPROX_TOP_COUNT": true, "APPROX_TOP_SUM": true, "CODE_POINTS_TO_STRING": true,
	"CODE_POINTS_TO_BYTES": true, "UNNEST": true,
}

// safeArithmetic are the five SAFE. calls BigQuery also spells with an
// underscore, and which this package answers to under that spelling.
var safeArithmetic = map[string]string{ //nolint:gochecknoglobals // a fixed table
	"ADD": "safe_add", "DIVIDE": "safe_divide", "MULTIPLY": "safe_multiply",
	"NEGATE": "safe_negate", "SUBTRACT": "safe_subtract",
}

// safePrefix lowers a SAFE. call, which asks for an error to become NULL. The
// helper safe_call runs the named function and answers NULL where it would have
// raised, which is what the prefix means.
func safePrefix(call *ast.FuncCall) (ast.Expr, error) {
	name := callName(call)
	if alias, ok := safeArithmetic[name]; ok {
		return rename(call, alias), nil
	}
	inner := &ast.FuncCall{Name: call.Name[len(call.Name)-1:], Args: call.Args, Span: call.Span}
	lowered, err := (&googleRules{}).Call(inner)
	if err != nil {
		return nil, err
	}
	target, ok := lowered.(*ast.FuncCall)
	if !ok || len(target.Name) != 1 {
		return nil, unsupported(call.Span,
			"the SAFE prefix is not supported on %s; it asks for an error to become NULL, "+
				"and this call does not become one function", name)
	}
	helperName := strings.ToLower(target.Name[0].Name)
	if !registeredHelper(helperName) {
		// SQLite's own function of that name answers or refuses for itself;
		// there is nothing here to catch an error in.
		return target, nil
	}
	args := append([]ast.Expr{text(helperName, call.Span)}, target.Args...)
	return helper("safe_call", call.Span, args...), nil
}

func (r *googleRules) Call(call *ast.FuncCall) (ast.Expr, error) {
	if len(call.Name) == 2 && strings.EqualFold(call.Name[0].Name, "SAFE") {
		return safePrefix(call)
	}
	if lowered, ok, err := commonCall(call, "googlesql"); ok || err != nil {
		return lowered, err
	}
	name := callName(call)
	if arrayFunctions[name] {
		return nil, unsupported(call.Span,
			"%s is not supported; its result is an array and SQLite has no array type", name)
	}
	if name == "FARM_FINGERPRINT" {
		// The exact bits of the hash are the point of calling it: a bucket
		// assignment computed here has to agree with one computed in BigQuery.
		return nil, unsupported(call.Span,
			"FARM_FINGERPRINT is not supported; its value is a FarmHash fingerprint, "+
				"and a different hash would not agree with the one BigQuery computes")
	}
	switch name {
	case fnNameExtract, fnNameDatePart:
		return datePartCall(call, "googlesql_date_part"), nil
	case fnNameUpper:
		return rename(call, "unicode_upper"), nil
	case fnNameLower:
		return rename(call, "unicode_lower"), nil
	case fnNameFormat:
		// The verbs GoogleSQL shares with printf are printed the same way, but
		// %t and %T are its own and SQLite's printf answers NULL for a format
		// string holding a verb it does not know.
		return rename(call, "googlesql_format"), nil
	case "LOG":
		switch len(call.Args) {
		case 1:
			return rename(call, "ln"), nil
		case 2:
			// SQLite's log takes the base first and BigQuery's takes it last.
			call.Args[0], call.Args[1] = call.Args[1], call.Args[0]
			return rename(call, "log"), nil
		default:
			return call, nil
		}
	case "LAST_DAY":
		if err := datePartAt(call, 1); err != nil {
			return nil, err
		}
		return rename(call, "googlesql_last_day"), nil
	case fnNameSubstr, fnNameSubstring:
		// The two spellings are one function, so they name one helper. Deriving
		// the name from the spelling below gave SUBSTRING a helper the runtime
		// does not register, and the call reached the driver as an unknown
		// function named after this package rather than after the query.
		return rename(call, "googlesql_substr"), nil
	case fnNamePosition:
		// BigQuery spells the question INSTR, which reads its arguments the
		// other way round; position() swaps them, and the helper is the one
		// INSTR itself lowers to.
		instr, err := position(call)
		if err != nil {
			return nil, err
		}
		if lowered, ok := instr.(*ast.FuncCall); ok {
			return rename(lowered, "googlesql_instr"), nil
		}
		return instr, nil
	case "LEFT", "RIGHT", "REPEAT", fnNameLpad, fnNameRpad,
		"MOD", "INSTR", "SOUNDEX", "MD5", "SHA1", typeNameDate, typeNameDatetime, typeNameTime,
		typeNameTimestamp:
		// Each raises where SQLite answers NULL, or builds a value from fields
		// SQLite's own function of that name does not take.
		return rename(call, "googlesql_"+strings.ToLower(name)), nil
	case typeNameString:
		return rename(call, "googlesql_string"), nil
	case fnNameConcat:
		// GoogleSQL's CONCAT is NULL when any argument is, where SQLite's
		// concat() treats a NULL as an empty string.
		return rename(call, "strict_concat"), nil
	case "TRUNC":
		return truncScale(call)
	case fnNameRound:
		// BigQuery rounds away from zero, which is what SQLite does. Only the
		// two-argument form needs a helper, because SQLite's own ROUND takes no
		// negative number of decimal places.
		if len(call.Args) == 2 {
			return rename(call, "dialect_round"), nil
		}
		return call, nil
	case fnNameReplace:
		return rename(call, "dialect_replace"), nil
	case fnNameDateAdd, "TIMESTAMP_ADD", "DATETIME_ADD":
		return dateArith(fnNameIntervalAdd, call, "+")
	case fnNameDateSub, "TIMESTAMP_SUB", "DATETIME_SUB":
		return dateArith(fnNameIntervalAdd, call, "-")
	case "TIME_ADD":
		return timeArith(call, "+")
	case "TIME_SUB":
		return timeArith(call, "-")
	case "DATE_DIFF", "TIMESTAMP_DIFF", "DATETIME_DIFF", "TIME_DIFF":
		return googleDateDiff(call)
	case "DATE_TRUNC", "TIMESTAMP_TRUNC", "DATETIME_TRUNC":
		// The helper takes the value first and the part second, which is the
		// order BigQuery writes and the opposite of PostgreSQL's date_trunc.
		return truncCall(call, "date_trunc_part")
	case "TIME_TRUNC":
		return truncCall(call, "time_trunc")
	case "NORMALIZE", "NORMALIZE_AND_CASEFOLD":
		return normalizeCall(call)
	case "EDIT_DISTANCE":
		return editDistance(call)
	case "JSON_VALUE", "JSON_EXTRACT_SCALAR":
		return rename(call, "json_extract"), nil
	case "JSON_QUERY":
		if len(call.Args) != 2 {
			return call, nil
		}
		// SQLite's -> answers the value as JSON text, which is what JSON_QUERY
		// answers: a string keeps its quotes and an object keeps its braces.
		return paren(binary(call.Args[0], ast.JSONGet, call.Args[1], call.Span)), nil
	case "BYTE_LENGTH":
		return rename(call, "octet_length"), nil
	case fnNameCharLength, fnNameCharLen:
		return rename(call, "length"), nil
	case fnNameStringAgg:
		return stringAgg(call)
	}
	return call, nil
}

// timeArith adds a duration to a time of day, whose arithmetic wraps at
// midnight rather than moving to another day.
func timeArith(call *ast.FuncCall, sign string) (ast.Expr, error) {
	if len(call.Args) != 2 {
		return nil, unsupported(call.Span, "%s takes a value and an interval", callName(call))
	}
	iv, ok := call.Args[1].(*ast.IntervalExpr)
	if !ok {
		return nil, unsupported(call.Span, "%s takes an INTERVAL", callName(call))
	}
	amount := iv.Value
	if sign == "-" {
		amount = &ast.UnaryExpr{Op: ast.UnaryMinus, Expr: paren(amount), Span: call.Span}
	}
	return helper("time_add", call.Span, call.Args[0], amount, text(iv.Unit, call.Span)), nil
}

// googleDateDiff reads the unit BigQuery writes as a bare word in the last
// argument.
func googleDateDiff(call *ast.FuncCall) (ast.Expr, error) {
	if len(call.Args) != 3 {
		return nil, unsupported(call.Span, "%s takes two values and a unit", callName(call))
	}
	// The difference of two dates counts more parts than a duration can be
	// written in: an ISO week, an ISO year, and the week that names the day it
	// starts on.
	if err := datePartAt(call, 2); err != nil {
		return nil, err
	}
	switch callName(call) {
	case "TIMESTAMP_DIFF":
		return rename(call, "timestamp_diff"), nil
	case "DATETIME_DIFF":
		return rename(call, "datetime_diff"), nil
	case "TIME_DIFF":
		return rename(call, "time_diff"), nil
	default:
		return rename(call, "date_diff"), nil
	}
}

// truncCall reads the date part BigQuery writes as a bare word.
func truncCall(call *ast.FuncCall, name string) (ast.Expr, error) {
	if err := datePartAt(call, 1); err != nil {
		return nil, err
	}
	return rename(call, name), nil
}

// normalizeCall reads the normalization form BigQuery writes as a bare word.
func normalizeCall(call *ast.FuncCall) (ast.Expr, error) {
	if len(call.Args) == 2 {
		form, ok := unitName(call.Args[1])
		if !ok {
			return nil, unsupported(call.Span, "%s needs a normalization form written as a word", callName(call))
		}
		call.Args[1] = text(form, call.Span)
	}
	return rename(call, strings.ToLower(callName(call))), nil
}

// editDistance takes its bound as a named argument, which the helper takes
// positionally.
func editDistance(call *ast.FuncCall) (ast.Expr, error) {
	if len(call.ArgNames) > 0 {
		for _, named := range call.ArgNames {
			if named.Name != "MAX_DISTANCE" {
				return nil, unsupported(call.Span,
					"the named argument %s of EDIT_DISTANCE is not supported", named.Name)
			}
		}
		call.ArgNames = nil
	}
	return rename(call, "edit_distance"), nil
}
