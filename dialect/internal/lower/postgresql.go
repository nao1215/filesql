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

// pgJSONPathCall lowers the functions that take a path as a text array and a
// value as JSON. SQLite's json_set and json_insert take a $ path and read a
// text argument as a string, so the array was handed over as a path -- and
// refused as one -- and a value that reached them would have been quoted.
//
// The blob-returning spellings go to the text ones: SQLite's jsonb_set answers
// JSONB, which is a blob a caller cannot read, where PostgreSQL's answers a
// document.
func pgJSONPathCall(call *ast.FuncCall, name string) (ast.Expr, error) {
	if len(call.Args) < 3 {
		return nil, unsupported(call.Span, "%s takes a document, a path and a value", name)
	}
	path, ok := jsonTextArrayPath(call.Args[1])
	if !ok {
		return nil, unsupported(call.Span,
			"%s is supported for a path written as a text array of keys, as {a} or {a,b}; "+
				"an element that is a number is an index into an array or a key in an object and "+
				"one SQLite path cannot be both", name)
	}
	call.Args[1] = text(path, call.Span)
	// The value is JSON in PostgreSQL and text to SQLite unless it is said to
	// be JSON, so '2' would have gone in as the string "2".
	call.Args[2] = helper("json", call.Span, call.Args[2])
	if strings.HasSuffix(name, "INSERT") {
		return rename(call, "json_insert"), nil
	}
	return rename(call, "json_set"), nil
}

// pgJSONExtractPath lowers the two functions that take a path as one string
// argument per element. The text spelling answers the value unquoted, which is
// what SQLite's json_extract does, and the other answers it as JSON, which is
// what the "->" operator does.
func pgJSONExtractPath(call *ast.FuncCall, name string) (ast.Expr, error) {
	if len(call.Args) < 2 {
		return nil, unsupported(call.Span, "%s takes a document and at least one path element", name)
	}
	var path strings.Builder
	path.WriteByte('$')
	for _, arg := range call.Args[1:] {
		element, ok := literalText(arg)
		if !ok {
			return nil, unsupported(call.Span,
				"%s is supported for path elements written as string literals", name)
		}
		path.WriteString("." + quoteJSONMember(element))
	}
	document := call.Args[0]
	pathText := text(path.String(), call.Span)
	if strings.HasSuffix(name, "_TEXT") {
		return helper("json_extract", call.Span, document, pathText), nil
	}
	return paren(binary(document, ast.JSONGet, pathText, call.Span)), nil
}

// jsonTextArrayPath turns the text array a "#-" takes -- written {a,b} -- into
// the $ path SQLite removes by.
//
// An element that is a number is refused rather than converted, because
// PostgreSQL reads it against the container it lands on: it is a key when that
// is an object and an index when it is an array, and one $ path cannot be both.
func jsonTextArrayPath(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.Literal)
	if !ok || lit.Kind != ast.LitString {
		return "", false
	}
	elements, ok := textArrayElements(lit.Value)
	if !ok || len(elements) == 0 {
		return "", false
	}
	var path strings.Builder
	path.WriteByte('$')
	for _, element := range elements {
		if _, err := strconv.Atoi(element); err == nil {
			return "", false
		}
		path.WriteString("." + quoteJSONMember(element))
	}
	return path.String(), true
}

// textArrayElements reads PostgreSQL's one-dimensional array literal. An
// element may be written bare or in double quotes, and a quoted one carries the
// characters the syntax otherwise reads as punctuation: a comma, a brace, a
// quote of its own behind a backslash, and the whitespace a bare element loses.
// Splitting on the comma alone turned the single key "a,b" into two path
// members and removed nothing.
func textArrayElements(literal string) ([]string, bool) {
	body := strings.TrimSpace(literal)
	if !strings.HasPrefix(body, "{") || !strings.HasSuffix(body, "}") {
		return nil, false
	}
	body = body[1 : len(body)-1]
	if strings.TrimSpace(body) == "" {
		return nil, false
	}
	var (
		elements []string
		element  arrayElement
		quoted   bool
	)
	for i := 0; i < len(body); i++ {
		switch c := body[i]; {
		case quoted && c == '\\':
			if i+1 >= len(body) {
				return nil, false
			}
			i++
			element.write(body[i])
		case c == '"':
			quoted = !quoted
			element.openQuote()
		case quoted:
			element.write(c)
		case c == ',':
			text, ok := element.close()
			if !ok {
				return nil, false
			}
			elements = append(elements, text)
			element = arrayElement{}
		case c == '{' || c == '}':
			// A nested array is not a path.
			return nil, false
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			element.space(c)
		default:
			element.write(c)
		}
	}
	if quoted {
		return nil, false
	}
	text, ok := element.close()
	if !ok {
		return nil, false
	}
	return append(elements, text), true
}

// arrayElement accumulates one element of an array literal. Whitespace outside
// the quotes is not part of the element and whitespace inside them is, so a run
// of spaces is held back until something that is part of the element follows
// it.
type arrayElement struct {
	text    strings.Builder
	spaces  strings.Builder
	quoted  bool
	written bool
}

// write adds a byte that is part of the element, along with any whitespace that
// stood between it and what came before.
func (a *arrayElement) write(c byte) {
	if a.spaces.Len() > 0 {
		a.text.WriteString(a.spaces.String())
		a.spaces.Reset()
	}
	a.text.WriteByte(c)
	a.written = true
}

// space holds back a whitespace byte read outside the quotes.
func (a *arrayElement) space(c byte) {
	if a.written {
		a.spaces.WriteByte(c)
	}
}

// openQuote records that the element carries a quoted part, which makes it a
// value even when that part is empty.
func (a *arrayElement) openQuote() {
	if a.spaces.Len() > 0 {
		a.text.WriteString(a.spaces.String())
		a.spaces.Reset()
	}
	a.quoted = true
	a.written = true
}

// close finishes the element. The whitespace still held back was trailing and
// is dropped. An element written as nothing at all is not a value, and the bare
// word NULL is the array's null rather than a path member.
func (a *arrayElement) close() (string, bool) {
	if !a.written {
		return "", false
	}
	text := a.text.String()
	if !a.quoted && strings.EqualFold(text, "NULL") {
		return "", false
	}
	return text, true
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
			"the #- operator takes a path written as a literal array of names; SQLite removes by a $ path, "+
				"and a path element that is a number is a key on an object and an index on an array, "+
				"which one $ path cannot be both of")
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
		case typeNameDate, typeNameTimestamp, typeNameTimestampTZ, typeNameTimestampWithZone, typeNameTimestampWithoutZone:
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
		// concatenates as the text of its digits rather than as a number.
		if !onlyBinaryDigits(lit.Value) {
			return nil, unsupported(lit.Span, `a bit-string literal holds only "0" and "1"`)
		}
		return lit, nil
	case ast.LitBlob:
		// PostgreSQL has no blob literal: X'41' is the hexadecimal spelling of
		// a bit string, and it names the same value as B'01000001'. The lexer
		// reads the form the way SQLite does, for every dialect, so the
		// expansion happens here rather than there.
		return &ast.Literal{Kind: ast.LitBit, Value: binaryDigits(lit.Value), Span: lit.Span}, nil
	default:
		return lit, nil
	}
}

// onlyBinaryDigits reports whether s is written the way a bit string is.
func onlyBinaryDigits(s string) bool {
	for i := range len(s) {
		if s[i] != '0' && s[i] != '1' {
			return false
		}
	}
	return true
}

// binaryDigits expands hexadecimal digits into the four bits each of them
// stands for, which is what PostgreSQL says X'..' means.
func binaryDigits(hexDigits string) string {
	var out strings.Builder
	out.Grow(len(hexDigits) * 4)
	for i := range len(hexDigits) {
		n, err := strconv.ParseUint(hexDigits[i:i+1], 16, 8)
		if err != nil {
			// The lexer refuses a blob literal holding anything else, so this
			// is unreachable; writing the digit through keeps the length right
			// should it ever be reached.
			out.WriteByte(hexDigits[i])
			continue
		}
		for bit := 3; bit >= 0; bit-- {
			out.WriteByte('0' + byte((n>>bit)&1))
		}
	}
	return out.String()
}

// bitStringCast folds a cast whose operand is a bit-string literal. PostgreSQL
// reads the bits as a base-2 number on the way to an integer and refuses every
// other numeric type, and nothing downstream can tell a bit string from a
// string of the same digits, so the reading has to happen while the literal is
// still one.
func bitStringCast(c *ast.CastExpr, lit *ast.Literal) (ast.Expr, error) {
	switch width, isInteger := bitStringIntegerTargets[c.Type.Name]; {
	case isInteger && lit.Value == "":
		// A bit string of no bits is zero as an integer, which is what
		// PostgreSQL answers where ParseUint would have called it out of range.
		return number(0, c.Span), nil
	case isInteger:
		n, err := strconv.ParseUint(lit.Value, 2, width)
		if err != nil {
			return nil, unsupported(c.Span,
				"a bit string of %d bits is out of range for %s", len(lit.Value), c.Type.Written)
		}
		if width == 32 {
			// The target holds 32 bits, and the top one is its sign.
			return number(int64(int32(n)), c.Span), nil //nolint:gosec // ParseUint has held the value to 32 bits
		}
		return number(int64(n), c.Span), nil //nolint:gosec // the bits are the value; a wider one was refused above
	case bitStringRefusedTargets[c.Type.Name]:
		return nil, unsupported(c.Span,
			"PostgreSQL cannot cast a bit string to %s; cast it to an integer first", c.Type.Written)
	}
	return nil, nil
}

func (r *postgresRules) TypedLiteral(lit *ast.TypedLiteral) (ast.Expr, error) {
	if strings.EqualFold(lit.Type, "INTERVAL") {
		return nil, unsupported(lit.Span,
			"an INTERVAL value is only supported beside a date; SQLite has no interval type")
	}
	return text(lit.Value, lit.Span), nil
}

func (r *postgresRules) Cast(c *ast.CastExpr) (ast.Expr, error) {
	// PostgreSQL writes a boolean cast to text as the word rather than the
	// number SQLite stores it as. The length of a char(n) target still applies,
	// so the word goes through the cast rather than around it.
	if v, ok := boolLiteral(c.Expr); ok && sqliteTypeNames[c.Type.Name] == typeNameText {
		c.Expr = text(boolWord(v), c.Span)
	}
	if lit, ok := c.Expr.(*ast.Literal); ok && lit.Kind == ast.LitBit {
		folded, err := bitStringCast(c, lit)
		if err != nil || folded != nil {
			return folded, err
		}
	}
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
	if err := refuseUnsupportedFunction(dialects.PostgreSQL, call); err != nil {
		return nil, err
	}
	name := callName(call)
	if setReturningFunctions[name] {
		return nil, unsupported(call.Span,
			"%s returns a set of rows, which SQLite has no form for outside a table", name)
	}
	switch name {
	case "JSON_SET", "JSONB_SET", "JSON_INSERT", "JSONB_INSERT":
		return pgJSONPathCall(call, name)
	case "JSON_EXTRACT_PATH", "JSONB_EXTRACT_PATH", "JSON_EXTRACT_PATH_TEXT", "JSONB_EXTRACT_PATH_TEXT":
		return pgJSONExtractPath(call, name)
	case "JSON_BUILD_OBJECT", "JSONB_BUILD_OBJECT":
		// SQLite's json_object takes the same alternating keys and values.
		return rename(call, "json_object"), nil
	case "JSON_BUILD_ARRAY", "JSONB_BUILD_ARRAY":
		return rename(call, "json_array"), nil
	case "TO_JSON", "TO_JSONB":
		// A scalar becomes the JSON that names it, which json_quote writes. A
		// row or an array has no SQLite form, and neither reaches here as one
		// value anyway.
		if len(call.Args) != 1 {
			return nil, unsupported(call.Span, "%s takes one value", callName(call))
		}
		return rename(call, "json_quote"), nil
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
	case "POW":
		// PostgreSQL's own name for it is POWER, and the helper carries that
		// name, so the alias renames onto the same one.
		return rename(call, "postgresql_power"), nil
	case "SQRT", "LN", fnNameLog, "EXP", "POWER", "ACOS", "ASIN", "ACOSH", "ATANH", "COT":
		// PostgreSQL refuses these outside their domain and SQLite's own
		// answer NULL or an infinity there, which reads as missing data
		// rather than as arithmetic the engine refused. COT is the one that
		// moves the other way: PostgreSQL answers an infinity at zero, and
		// the helper registered under the bare name is MySQL's, which
		// refuses it.
		return rename(call, "postgresql_"+strings.ToLower(name)), nil
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
