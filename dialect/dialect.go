package dialect

import (
	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/lower"
	"github.com/nao1215/filesql/dialect/internal/parser"
	"github.com/nao1215/filesql/dialect/internal/render"
	"github.com/nao1215/filesql/dialect/internal/runtime"
	"github.com/nao1215/filesql/dialect/internal/sqlerr"
)

// Dialect is a SQL dialect accepted at query time. The storage engine is always
// SQLite; a Dialect only selects how a caller's query text is interpreted before
// it reaches SQLite.
//
// Its value is the lowercase name Parse reads, so a Dialect survives a round
// trip through a flag, a config file or a column.
type Dialect string

const (
	// SQLite is the native dialect. Translate returns the input unchanged.
	SQLite Dialect = "sqlite"
	// MySQL accepts MySQL query syntax.
	MySQL Dialect = "mysql"
	// PostgreSQL accepts PostgreSQL query syntax.
	PostgreSQL Dialect = "postgresql"
	// GoogleSQL accepts GoogleSQL (BigQuery / Cloud Spanner) query syntax.
	GoogleSQL Dialect = "googlesql"
)

// DisplayName returns the dialect spelled the way its own project spells it,
// for a message a person reads: "PostgreSQL" rather than "postgresql". A
// Dialect this package does not name reads back as its own value, and the zero
// Dialect is "".
func (d Dialect) DisplayName() string { return d.internal().DisplayName() }

// internal is the identifier the packages under this one pass around. They
// cannot import this one, which is why the type is declared twice; the
// conversion is free, and TestDialectMirrorsTheInternalIdentifier holds the
// two sets of constants equal.
func (d Dialect) internal() dialects.Dialect { return dialects.Dialect(d) }

// Sentinel errors returned by this package. Use errors.Is to check for them.
var (
	// ErrUnknownDialect indicates a dialect name that does not map to a built-in
	// dialect.
	ErrUnknownDialect = dialects.ErrUnknownDialect
	// ErrUnsupportedSyntax indicates a construct that is valid in the source
	// dialect and that SQLite cannot express, with or without a helper.
	ErrUnsupportedSyntax = sqlerr.ErrUnsupportedSyntax
	// ErrUnsupportedFeature indicates a construct outside the SQL subset this
	// package implements. See the package documentation for that subset.
	ErrUnsupportedFeature = sqlerr.ErrUnsupportedFeature
	// ErrInvalidSyntax indicates the query could not be read: an unterminated
	// string, a stray parenthesis, an expression that stops in the middle.
	ErrInvalidSyntax = sqlerr.ErrInvalidSyntax
)

// Dialects returns every built-in dialect in a stable order.
func Dialects() []Dialect {
	all := dialects.All()
	out := make([]Dialect, len(all))
	for i, d := range all {
		out[i] = Dialect(d)
	}
	return out
}

// Parse maps a user-supplied dialect name to a Dialect. Matching is
// case-insensitive and ignores surrounding whitespace. Aliases are accepted:
// "sqlite3" for SQLite; "postgres" and "pg" for PostgreSQL; "bigquery",
// "spanner", and "zetasql" for GoogleSQL. An unrecognized name returns
// ErrUnknownDialect.
func Parse(name string) (Dialect, error) {
	d, err := dialects.ParseName(name)
	return Dialect(d), err
}

// RegisterFunctions registers the helper functions the translated SQL calls
// with the SQLite driver. It runs once at package initialization and is
// idempotent; calling it reports the same error that registration produced.
func RegisterFunctions() error { return runtime.RegisterFunctions() }

// Translate converts a query written in dialect d into SQLite SQL. When d is
// SQLite the input is returned unchanged, and a dialect this package does not
// implement is refused rather than passed through.
//
// The query is read into a syntax tree, lowered into one SQLite can execute,
// and written back out. A construct outside the supported subset is refused
// with ErrUnsupportedFeature and one SQLite cannot express with
// ErrUnsupportedSyntax; neither is forwarded to SQLite untranslated.
func Translate(d Dialect, query string) (string, error) {
	if d == SQLite {
		return query, nil
	}
	stmt, err := parser.Parse(d.internal(), query)
	if err != nil {
		return "", err
	}
	lowered, err := lower.Lower(d.internal(), stmt)
	if err != nil {
		return "", err
	}
	return render.Render(lowered)
}
