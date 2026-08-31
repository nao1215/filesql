// Package render writes a syntax tree back as SQLite SQL. It decides quoting
// and parenthesization from the tree, so a rendered query reparses to the same
// shape; it makes no decision that belongs to a source dialect, and a node
// SQLite cannot express is an error here rather than a surprise at run time.
package render

import (
	"strings"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/sqlerr"
)

// Render writes a statement as SQLite SQL.
func Render(stmt ast.Stmt) (string, error) {
	w := &writer{}
	if err := w.stmt(stmt); err != nil {
		return "", err
	}
	if w.err != nil {
		return "", w.err
	}
	return w.b.String(), nil
}

type writer struct {
	b strings.Builder
	// tight suppresses the separator before the next token, for the places
	// where a token belongs to the one before it: a sign and its operand, a
	// function name and its parenthesis, a qualifying dot and the name after
	// it.
	tight bool
	// err holds a refusal raised while writing a token, for the checks that
	// belong to every token rather than to one node. word cannot return an
	// error, so Render reads this instead.
	err error
}

// word writes a token, separating it from what is already written unless the
// two belong together. The rule is the whole of the package's spacing: a space
// before every token except a closing parenthesis, a comma or a dot, and none
// after an opening parenthesis or a dot. A function's own parenthesis is
// written directly rather than through here, because that one belongs to the
// name.
func (w *writer) word(s string) {
	if s == "" {
		return
	}
	if strings.IndexByte(s, 0) >= 0 && w.err == nil {
		w.err = sqlerr.Unsupportedf(
			"a NUL byte in a string or a name is not supported: SQLite ends a statement at the first NUL, so no SQL text can carry one")
	}
	tight := w.tight
	w.tight = false
	if w.b.Len() > 0 && (!tight || w.wouldFuse(s[0])) && w.separate(s[0]) {
		w.b.WriteByte(' ')
	}
	w.b.WriteString(s)
}

// dot writes the dot that qualifies a name. The name after it belongs to it, so
// nothing separates the two. It is written here rather than through word
// because a dot is not a token of its own.
func (w *writer) dot() {
	w.b.WriteByte('.')
	w.tight = true
}

// wouldFuse reports whether writing c straight after what is already written
// would read as one token: two operator characters run together, and "--" opens
// a comment.
func (w *writer) wouldFuse(c byte) bool {
	return operatorByte(w.lastByte()) && operatorByte(c)
}

// operatorByte reports whether a character can be part of an operator.
func operatorByte(c byte) bool {
	switch c {
	case '+', '-', '*', '/', '<', '>', '=', '!', '|', '&', '~', '%':
		return true
	default:
		return false
	}
}

// separate reports whether a space belongs between what has been written and a
// token starting with c.
//
// The question is about the tokens and not about the bytes they end in: a
// number may end in its decimal point, and reading the last byte written as a
// qualifying dot wrote "1.FROM", which SQLite reads as one token it does not
// know. What follows a qualifying dot is held to it by tight instead.
func (w *writer) separate(c byte) bool {
	// A leading dot is a number such as ".25"; a qualification dot is written
	// straight into the buffer rather than through here, so it never arrives.
	switch c {
	case ')', ',':
		return false
	}
	return w.lastByte() != '('
}

func (w *writer) lastByte() byte {
	s := w.b.String()
	if s == "" {
		return 0
	}
	return s[len(s)-1]
}

// name writes an identifier, quoting it only when it has to be: a name the
// caller wrote in quotes, or one that is not a bare identifier. Quoting every
// name would work but would make the translated SQL unreadable, and the
// translated SQL is what a caller sees in an error message.
func (w *writer) name(id ast.Ident) {
	if id.Quoted || !bareIdentifier(id.Name) {
		w.word(QuoteIdent(id.Name))
		return
	}
	w.word(id.Name)
}

// bareIdentifier reports whether a name can be written without quotes.
func bareIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for i := range len(name) {
		c := name[i]
		switch {
		case c == '_' || c >= 0x80:
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return !sqliteKeywords[strings.ToUpper(name)]
}

// sqliteKeywords are the words SQLite reserves, which have to be quoted to be
// used as a name. The list is SQLite's own; a word outside it is safe bare.
var sqliteKeywords = map[string]bool{ //nolint:gochecknoglobals // a fixed table
	"ABORT": true, "ACTION": true, "ADD": true, "AFTER": true, "ALL": true,
	"ALTER": true, "ALWAYS": true, "ANALYZE": true, "AND": true, "AS": true,
	"ASC": true, "ATTACH": true, "AUTOINCREMENT": true, "BEFORE": true,
	"BEGIN": true, "BETWEEN": true, "BY": true, "CASCADE": true, "CASE": true,
	"CAST": true, "CHECK": true, "COLLATE": true, "COLUMN": true, "COMMIT": true,
	"CONFLICT": true, "CONSTRAINT": true, "CREATE": true, "CROSS": true,
	"CURRENT": true, "CURRENT_DATE": true, "CURRENT_TIME": true,
	"CURRENT_TIMESTAMP": true, "DATABASE": true, "DEFAULT": true,
	"DEFERRABLE": true, "DEFERRED": true, "DELETE": true, "DESC": true,
	"DETACH": true, "DISTINCT": true, "DO": true, "DROP": true, "EACH": true,
	"ELSE": true, "END": true, "ESCAPE": true, "EXCEPT": true, "EXCLUDE": true,
	"EXCLUSIVE": true, "EXISTS": true, "EXPLAIN": true, "FAIL": true,
	"FILTER": true, "FIRST": true, "FOLLOWING": true, "FOR": true, "FOREIGN": true,
	"FROM": true, "FULL": true, "GENERATED": true, "GLOB": true, "GROUP": true,
	"GROUPS": true, "HAVING": true, "IF": true, "IGNORE": true, "IMMEDIATE": true,
	"IN": true, "INDEX": true, "INDEXED": true, "INITIALLY": true, "INNER": true,
	"INSERT": true, "INSTEAD": true, "INTERSECT": true, "INTO": true, "IS": true,
	"ISNULL": true, "JOIN": true, "KEY": true, "LAST": true, "LEFT": true,
	"LIKE": true, "LIMIT": true, "MATCH": true, "MATERIALIZED": true,
	"NATURAL": true, "NO": true, "NOT": true, "NOTHING": true, "NOTNULL": true,
	"NULL": true, "NULLS": true, "OF": true, "OFFSET": true, "ON": true,
	"OR": true, "ORDER": true, "OTHERS": true, "OUTER": true, "OVER": true,
	"PARTITION": true, "PLAN": true, "PRAGMA": true, "PRECEDING": true,
	"PRIMARY": true, "QUERY": true, "RAISE": true, "RANGE": true,
	"RECURSIVE": true, "REFERENCES": true, "REGEXP": true, "REINDEX": true,
	"RELEASE": true, "RENAME": true, "REPLACE": true, "RESTRICT": true,
	"RETURNING": true, "RIGHT": true, "ROLLBACK": true, "ROW": true,
	"ROWS": true, "SAVEPOINT": true, "SELECT": true, "SET": true, "TABLE": true,
	"TEMP": true, "TEMPORARY": true, "THEN": true, "TIES": true, "TO": true,
	"TRANSACTION": true, "TRIGGER": true, "UNBOUNDED": true, "UNION": true,
	"UNIQUE": true, "UPDATE": true, "USING": true, "VACUUM": true, "VALUES": true,
	"VIEW": true, "VIRTUAL": true, "WHEN": true, "WHERE": true, "WINDOW": true,
	"WITH": true, "WITHOUT": true,
}

// QuoteIdent renders name as a SQLite double-quoted identifier, doubling any
// embedded double quote.
func QuoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// QuoteString renders value as a SQLite single-quoted string literal, doubling
// any embedded single quote.
func QuoteString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// unsupported reports a node the renderer has no SQLite spelling for. Reaching
// one means lowering left something behind, so the message names the node.
func unsupported(span ast.Span, what string) error {
	return sqlerr.At(sqlerr.ErrUnsupportedSyntax, span.Line, span.Col, "%s has no SQLite form", what)
}

// quoteIfNeeded writes a name that came from the tree as a plain string rather
// than as an Ident, quoting it only when it has to be quoted.
func quoteIfNeeded(name string) string {
	if bareIdentifier(name) {
		return name
	}
	return QuoteIdent(name)
}
