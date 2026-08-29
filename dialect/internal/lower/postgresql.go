package lower

import (
	"strconv"

	"strings"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
)

// postgresRules lowers PostgreSQL's meaning onto SQLite's.
type postgresRules struct{ baseRules }

func (*postgresRules) Dialect() dialects.Dialect { return dialects.PostgreSQL }

// Pre catches the arithmetic that has to be read before its operands are
// lowered: a date literal is a date here and an ordinary string afterwards, a
// cast to jsonb is a document here and a call afterwards, and PostgreSQL's own
// rule for what "x - 1" means depends on which of the two the left side is.
func (r *postgresRules) Pre(e ast.Expr) (ast.Expr, bool, error) {
	b, ok := e.(*ast.BinaryExpr)
	if !ok {
		return e, false, nil
	}
	if replaced, handled, err := pgJSONOperator(b); handled || err != nil {
		return replaced, handled, err
	}
	if b.Op != ast.Add && b.Op != ast.Sub {
		return e, false, nil
	}
	replaced := pgDateArithmetic(b)
	return replaced, replaced != ast.Expr(b), nil
}

// pgJSONOperator lowers the operators that mean one thing beside a document and
// another beside a number or a string. SQLite runs its own "||" and "-" over
// the same operands and answers from the wrong operation: two documents
// concatenated as text, and a key subtracted as a number.
//
// A document is recognized the way the date arithmetic recognizes a date: a
// cast to json or jsonb, or a literal written as one. That is the form
// PostgreSQL itself requires for these operators to resolve, so nothing is
// missed by asking for it.
func pgJSONOperator(b *ast.BinaryExpr) (ast.Expr, bool, error) {
	switch b.Op {
	case ast.JSONPathDelete:
		path, ok := jsonTextArrayPath(b.Right)
		if !ok {
			return nil, false, nil
		}
		return helper("json_remove", b.Span, b.Left, text(path, b.Span)), true, nil
	case ast.Concat:
		if !isJSONValued(b.Left) && !isJSONValued(b.Right) {
			return b, false, nil
		}
		return nil, false, unsupported(b.Span,
			"the || operator on a JSON document is not supported; it merges two objects, concatenates two "+
				"arrays and wraps a scalar, and SQLite's json_patch does none of the three")
	case ast.Sub:
		if !isJSONValued(b.Left) {
			return b, false, nil
		}
		path, ok := jsonMemberPath(b.Right)
		if !ok {
			return nil, false, unsupported(b.Span,
				"the - operator on a JSON document needs the key or index written as a literal; "+
					"SQLite removes by a $ path, which is built from it here")
		}
		return helper("json_remove", b.Span, b.Left, text(path, b.Span)), true, nil
	default:
		return b, false, nil
	}
}

// The types a cast or a typed literal names when its value is a document.
const (
	jsonTypeName       = "JSON"
	jsonBinaryTypeName = "JSONB"
)

// isJSONValued reports whether an expression can be seen to be a document
// without knowing the schema.
func isJSONValued(e ast.Expr) bool {
	switch n := e.(type) {
	case *ast.CastExpr:
		switch strings.ToUpper(n.Type.Name) {
		case jsonTypeName, jsonBinaryTypeName:
			return true
		}
	case *ast.TypedLiteral:
		return strings.EqualFold(n.Type, jsonTypeName)
	case *ast.ParenExpr:
		return isJSONValued(n.Expr)
	}
	return false
}

// jsonMemberPath turns the key or index a "-" removes into the $ path SQLite's
// json_remove takes.
func jsonMemberPath(e ast.Expr) (string, bool) {
	switch n := e.(type) {
	case *ast.Literal:
		switch n.Kind {
		case ast.LitString:
			return "$." + quoteJSONMember(n.Value), true
		case ast.LitNumber:
			if _, err := strconv.Atoi(n.Value); err == nil {
				return "$[" + n.Value + "]", true
			}
		}
	case *ast.UnaryExpr:
		// A negative index counts from the end, which SQLite spells "#-n".
		if n.Op != ast.UnaryMinus {
			return "", false
		}
		lit, ok := n.Expr.(*ast.Literal)
		if !ok || lit.Kind != ast.LitNumber {
			return "", false
		}
		if _, err := strconv.Atoi(lit.Value); err != nil {
			return "", false
		}
		return "$[#-" + lit.Value + "]", true
	}
	return "", false
}

// jsonTextArrayPath turns the text array a "#-" takes -- written {a,b} -- into
// the $ path SQLite removes by. A path element that is a number addresses an
// array, which is how PostgreSQL reads it too.
func jsonTextArrayPath(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.Literal)
	if !ok || lit.Kind != ast.LitString {
		return "", false
	}
	body := strings.TrimSpace(lit.Value)
	if !strings.HasPrefix(body, "{") || !strings.HasSuffix(body, "}") {
		return "", false
	}
	body = strings.TrimSuffix(strings.TrimPrefix(body, "{"), "}")
	if body == "" {
		return "", false
	}
	var path strings.Builder
	path.WriteByte('$')
	for _, element := range strings.Split(body, ",") {
		element = strings.TrimSpace(element)
		if element == "" {
			return "", false
		}
		if _, err := strconv.Atoi(element); err == nil {
			path.WriteString("[" + element + "]")
			continue
		}
		path.WriteString("." + quoteJSONMember(element))
	}
	return path.String(), true
}

// quoteJSONMember writes an object key into a $ path, quoting it when it holds
// a character the path syntax reads as punctuation.
func quoteJSONMember(name string) string {
	if name != "" && !strings.ContainsAny(name, `.[]"$ `) {
		return name
	}
	return `"` + strings.ReplaceAll(name, `"`, `\"`) + `"`
}

func (r *postgresRules) Binary(b *ast.BinaryExpr) (ast.Expr, error) {
	switch b.Op {
	case ast.Div:
		// PostgreSQL divides two integers to an integer and anything else to a
		// double, where SQLite truncates whenever both operands are integers.
		return helper("postgresql_divide", b.Span, b.Left, b.Right), nil
	case ast.Mod:
		return helper("postgresql_mod", b.Span, b.Left, b.Right), nil
	case ast.Power:
		return helper("power", b.Span, b.Left, b.Right), nil
	case ast.BitXor:
		return helper("postgresql_bit_xor", b.Span, b.Left, b.Right), nil
	case ast.ILike, ast.NotILike:
		return likeHelper(b, "like_insensitive")
	case ast.Like, ast.NotLike:
		// PostgreSQL's LIKE is case sensitive and matches beyond ASCII, and
		// SQLite's folds ASCII by default.
		return likeHelper(b, "like_sensitive")
	case ast.SimilarTo, ast.NotSimilarTo:
		return similarTo(b)
	case ast.Regexp, ast.NotRegexp:
		// SQLite spells the match REGEXP, and the function behind it is
		// registered by this package.
		return b, nil
	case ast.RegexpCI, ast.NotRegexpCI:
		return caseInsensitiveRegexp(b)
	case ast.Concat:
		// PostgreSQL's concatenation is NULL when either side is, which is what
		// SQLite's own "||" does; nothing to change.
		return b, nil
	case ast.JSONGet, ast.JSONGetText:
		// SQLite spells these two the same way and means the same thing by
		// them: -> answers JSON and ->> answers a SQL value.
		return b, nil
	case ast.JSONPathGet, ast.JSONPathGetText:
		return nil, unsupported(b.Span,
			"the JSON path operators #> and #>> are not supported; write json_extract with a $ path")
	case ast.JSONContains, ast.JSONContainedBy:
		return nil, unsupported(b.Span,
			"the JSON containment operators @> and <@ are not supported; SQLite has no containment test")
	case ast.JSONPathExists, ast.JSONPathMatch:
		return nil, unsupported(b.Span,
			"the JSON path predicates @? and @@ are not supported; SQLite has no jsonpath")
	case ast.JSONPathDelete:
		// Reached only when the path is not a literal; the Pre rule handles the
		// rest.
		return nil, unsupported(b.Span,
			"the #- operator needs a path written as a literal; SQLite removes by a $ path, "+
				"and the text array this takes is only readable here when it is written out")
	}
	return b, nil
}

// pgDateArithmetic turns arithmetic on a date into the datetime helper.
// PostgreSQL adds an integer to a date as a number of days, and subtracts one
// date from another as a number of days; SQLite would read the text of the date
// as a number and answer about the year.
func pgDateArithmetic(b *ast.BinaryExpr) ast.Expr {
	if iv, ok := b.Right.(*ast.TypedLiteral); ok && strings.EqualFold(iv.Type, "INTERVAL") {
		sign := "+"
		if b.Op == ast.Sub {
			sign = "-"
		}
		amount := int64(1)
		if sign == "-" {
			amount = -1
		}
		return helper("interval_text_add", b.Span, b.Left, text(iv.Value, b.Span), number(amount, b.Span))
	}
	leftDate, rightDate := isDateValued(b.Left), isDateValued(b.Right)
	switch {
	case leftDate && rightDate && b.Op == ast.Sub:
		return helper("postgresql_date_diff", b.Span, b.Left, b.Right)
	case leftDate && !rightDate:
		amount := b.Right
		if b.Op == ast.Sub {
			amount = &ast.UnaryExpr{Op: ast.UnaryMinus, Expr: paren(amount), Span: b.Span}
		}
		return helper("postgresql_date_add", b.Span, b.Left, amount)
	case rightDate && !leftDate && b.Op == ast.Add:
		// "1 + date" is the same sum written the other way round.
		return helper("postgresql_date_add", b.Span, b.Right, b.Left)
	default:
		return b
	}
}

// isDateValued reports whether an expression can be seen to be a date without
// knowing the schema: a cast to a date or timestamp type, or a literal written
// as one. This is the form PostgreSQL itself requires for the expression to
// compile, so nothing is missed by asking for it.
func isDateValued(e ast.Expr) bool {
	switch n := e.(type) {
	case *ast.CastExpr:
		switch strings.ToUpper(n.Type.Name) {
		case typeNameDate, typeNameTimestamp, typeNameTimestampTZ, "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITHOUT TIME ZONE":
			return true
		}
	case *ast.TypedLiteral:
		switch strings.ToUpper(n.Type) {
		case typeNameDate, typeNameTimestamp:
			return true
		}
	case *ast.FuncCall:
		// The helper a cast has already become, whose second argument names
		// the type it converted to.
		if callName(n) == "POSTGRESQL_CAST" && len(n.Args) == 2 {
			if name, ok := literalText(n.Args[1]); ok {
				switch strings.ToUpper(name) {
				case typeNameDate, typeNameTimestamp, typeNameTimestampTZ:
					return true
				}
			}
		}
	case *ast.ParenExpr:
		return isDateValued(n.Expr)
	}
	return false
}

// similarTo turns SIMILAR TO into the regular-expression match it is defined
// as: the SQL pattern language is a regular expression with its own spelling of
// the wildcards.
func similarTo(b *ast.BinaryExpr) (ast.Expr, error) {
	// The helper takes the pattern first, the way SQLite's own like() does.
	args := []ast.Expr{b.Right, b.Left}
	if b.Escape != nil {
		args = append(args, b.Escape)
	}
	call := helper("similar_to", b.Span, args...)
	if b.Op == ast.NotSimilarTo {
		return notExpr(call, b.Span), nil
	}
	return call, nil
}

func (r *postgresRules) Unary(u *ast.UnaryExpr) (ast.Expr, error) {
	// The three prefix arithmetic operators are functions SQLite already has.
	switch u.Op {
	case ast.UnarySquareRoot:
		return helper("sqrt", u.Span, u.Expr), nil
	case ast.UnaryCubeRoot:
		return helper("cbrt", u.Span, u.Expr), nil
	case ast.UnaryAbsolute:
		return helper("abs", u.Span, u.Expr), nil
	}
	return u, nil
}

func (r *postgresRules) Literal(lit *ast.Literal) (ast.Expr, error) {
	switch lit.Kind {
	case ast.LitBit:
		// PostgreSQL's B'1010' is a bit string, which it compares and
		// concatenates as text rather than as a number.
		return text(lit.Value, lit.Span), nil
	default:
		return lit, nil
	}
}

func (r *postgresRules) TypedLiteral(lit *ast.TypedLiteral) (ast.Expr, error) {
	if strings.EqualFold(lit.Type, "INTERVAL") {
		return nil, unsupported(lit.Span,
			"an INTERVAL value is only supported beside a date; SQLite has no interval type")
	}
	return text(lit.Value, lit.Span), nil
}

func (r *postgresRules) Cast(c *ast.CastExpr) (ast.Expr, error) {
	return castHelper(dialects.PostgreSQL, c, "postgresql_cast")
}

// caseInsensitiveRegexp folds the case into the pattern, which is the only
// place SQLite's REGEXP will read it: its match has no flags argument. The
// pattern has to be a literal for that, since a column's value is not visible
// here.
func caseInsensitiveRegexp(b *ast.BinaryExpr) (ast.Expr, error) {
	pattern, ok := literalText(b.Right)
	if !ok {
		return nil, unsupported(b.Span,
			"the case-insensitive match operator needs a pattern written as a literal; "+
				"SQLite's REGEXP takes no flags, so the case folding is written into the pattern")
	}
	op := ast.Regexp
	if b.Op == ast.NotRegexpCI {
		op = ast.NotRegexp
	}
	return binary(b.Left, op, text("(?i)"+pattern, b.Span), b.Span), nil
}

func (r *postgresRules) Order(term *ast.OrderTerm) error {
	if err := orderCommon(term); err != nil {
		return err
	}
	// PostgreSQL sorts NULLs at the opposite end from SQLite: last for an
	// ascending order and first for a descending one. Naming the end keeps the
	// rows in the order the query asked for.
	if term.Nulls == ast.NullsDefault {
		if term.Desc {
			term.Nulls = ast.NullsFirst
		} else {
			term.Nulls = ast.NullsLast
		}
	}
	return nil
}

func (r *postgresRules) Call(call *ast.FuncCall) (ast.Expr, error) {
	if lowered, ok, err := commonCall(call, "postgresql"); ok || err != nil {
		return lowered, err
	}
	name := callName(call)
	if setReturningFunctions[name] {
		return nil, unsupported(call.Span,
			"%s returns a set of rows, which SQLite has no form for outside a table", name)
	}
	switch name {
	case fnNameExtract, fnNameDatePart:
		return datePartCall(call, "DATE_PART"), nil
	case fnNamePosition, "STRPOS":
		return position(call)
	case fnNameSubstring, fnNameSubstr:
		return pgSubstring(call)
	case fnNameFormat:
		return rename(call, "postgresql_format"), nil
	case "RANDOM":
		// SQLite's random() answers a pseudo-random 64-bit integer where
		// PostgreSQL's answers a double in [0, 1).
		return rename(call, "postgresql_random"), nil
	case "JSONB_TYPEOF", "JSON_TYPEOF":
		return rename(call, "postgresql_json_typeof"), nil
	case fnNameReplace:
		return rename(call, "dialect_replace"), nil
	case fnNameRound:
		return roundEven(call)
	case fnNameTrim:
		return pgTrim(call)
	case "BTRIM":
		return rename(call, "trim"), nil
	case "OVERLAY":
		return rename(call, "overlay"), nil
	case fnNameUpper:
		return rename(call, "unicode_upper"), nil
	case fnNameLower:
		return rename(call, "unicode_lower"), nil
	case "TO_HEX":
		return rename(call, "postgresql_to_hex"), nil
	case "REGEXP_REPLACE":
		return rename(call, "postgresql_regexp_replace"), nil
	case "GREATEST", "LEAST", fnNameLpad, fnNameRpad, fnNameMod:
		return rename(call, "postgresql_"+strings.ToLower(name)), nil
	case "TRUNC":
		return truncScale(call)
	case "JSONB_ARRAY_LENGTH", "JSON_ARRAY_LENGTH":
		return rename(call, "json_array_length"), nil
	case fnNameCharLength, fnNameCharLen:
		return rename(call, "length"), nil
	case fnNameStringAgg:
		return stringAgg(call)
	}
	return call, nil
}

// setReturningFunctions are the PostgreSQL functions that answer rows rather
// than a value. SQLite has no such function and no LATERAL to put one in, so a
// call to one is refused rather than left to fail as a missing name.
var setReturningFunctions = map[string]bool{ //nolint:gochecknoglobals // a fixed table
	"GENERATE_SERIES": true, "GENERATE_SUBSCRIPTS": true, "REGEXP_SPLIT_TO_TABLE": true,
	"UNNEST": true, "JSON_EACH": true, "JSONB_EACH": true, "JSON_ARRAY_ELEMENTS": true,
	"JSONB_ARRAY_ELEMENTS": true, "JSON_OBJECT_KEYS": true, "JSONB_OBJECT_KEYS": true,
	"STRING_TO_TABLE": true,
}

// pgSubstring lowers the several things PostgreSQL spells SUBSTRING. The
// FROM/FOR form is a position and a length when its operands are numbers, a
// pattern and an escape character when they are strings, and undecidable in the
// query text when the operand is a column, where a helper reads it at run time.
func pgSubstring(call *ast.FuncCall) (ast.Expr, error) {
	if call.Syntax == ast.CallSubstringSimilar {
		return rename(call, "similar_substring"), nil
	}
	if call.Syntax != ast.CallSubstringFrom || len(call.Args) < 2 {
		return rename(call, "postgresql_substr"), nil
	}
	_, fromIsText := literalText(call.Args[1])
	if len(call.Args) == 3 {
		if _, forIsText := literalText(call.Args[2]); fromIsText && forIsText {
			// Both operands are strings, so the second is an escape character
			// and the first a pattern. A numeric FOR is a length however the
			// FROM operand is written.
			return rename(call, "similar_substring"), nil
		}
		return rename(call, "postgresql_substr"), nil
	}
	switch {
	case fromIsText:
		return rename(call, "regexp_extract"), nil
	case isNumberLiteral(call.Args[1]):
		return rename(call, "postgresql_substr"), nil
	default:
		// A column, a placeholder or an expression: the kind of the operand is
		// not in the query text, so the reading is chosen from the value at run
		// time. It is the reading PostgreSQL would have chosen whenever the
		// operand's type matches what its value looks like, which is every
		// integer column and every text column that does not hold digits.
		return rename(call, "postgresql_substring_from"), nil
	}
}

// isNumberLiteral reports whether an expression is a numeric constant.
func isNumberLiteral(e ast.Expr) bool {
	lit, ok := e.(*ast.Literal)
	return ok && lit.Kind == ast.LitNumber
}

// pgTrim reads the SQL-standard TRIM into the SQLite function that trims the
// same side.
func pgTrim(call *ast.FuncCall) (ast.Expr, error) {
	switch call.Syntax {
	case ast.CallTrimLeading:
		return rename(call, "ltrim"), nil
	case ast.CallTrimTrailing:
		return rename(call, "rtrim"), nil
	default:
		return rename(call, "trim"), nil
	}
}

// stringAgg joins with a separator, which SQLite's group_concat also does.
func stringAgg(call *ast.FuncCall) (ast.Expr, error) {
	if call.Distinct && len(call.Args) == 2 {
		// SQLite's DISTINCT aggregates take exactly one argument, so the
		// separator has nowhere to go. A separator of "," is what group_concat
		// already joins with, so dropping it leaves the answer unchanged; any
		// other separator would answer a question nobody asked.
		sep, ok := literalText(call.Args[1])
		if !ok || sep != "," {
			return nil, unsupported(call.Span,
				"STRING_AGG cannot combine DISTINCT with a separator other than ',' on the SQLite backend; "+
					"drop DISTINCT, or use ','")
		}
		call.Args = call.Args[:1]
	}
	return rename(call, "group_concat"), nil
}
