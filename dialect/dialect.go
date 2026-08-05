package dialect

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Dialect is a SQL dialect accepted at query time. The storage engine is always
// SQLite; a Dialect only selects how a caller's query text is interpreted before
// it reaches SQLite.
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

// Sentinel errors returned by this package. Use errors.Is to check for them.
var (
	// ErrUnknownDialect indicates a dialect name that does not map to a built-in
	// dialect and has no registered translator.
	ErrUnknownDialect = errors.New("dialect: unknown SQL dialect")
	// ErrUnsupportedSyntax indicates a construct that is valid in the source
	// dialect but has no equivalent on the SQLite backend.
	ErrUnsupportedSyntax = errors.New("dialect: syntax not supported on SQLite backend")
	// ErrInvalidSyntax indicates the query could not be tokenized (for example an
	// unterminated string or identifier).
	ErrInvalidSyntax = errors.New("dialect: invalid SQL syntax")
)

// Dialects returns every built-in dialect in a stable order.
func Dialects() []Dialect {
	return []Dialect{SQLite, MySQL, PostgreSQL, GoogleSQL}
}

// displayNames spells each dialect the way its own project does. The wire value
// is a lowercase identifier, which is right for parsing a flag value and wrong
// in a sentence a person reads.
var displayNames = map[Dialect]string{
	SQLite:     "SQLite",
	MySQL:      "MySQL",
	PostgreSQL: "PostgreSQL",
	GoogleSQL:  "GoogleSQL",
}

// DisplayName returns the dialect spelled the way its own project spells it, for
// a message a person reads. A dialect with no spelling here — one installed by
// RegisterTranslator — reads back as its wire value, so the result is never
// empty. It lives beside Dialects() so a dialect added to one arrives in the
// other, instead of every caller keeping its own table of names.
func (d Dialect) DisplayName() string {
	if name, ok := displayNames[d]; ok {
		return name
	}
	return string(d)
}

// Parse maps a user-supplied dialect name to a Dialect. Matching is
// case-insensitive and ignores surrounding whitespace. Aliases are accepted:
// "sqlite3" for SQLite; "postgres" and "pg" for PostgreSQL; "bigquery",
// "spanner", and "zetasql" for GoogleSQL. An unrecognized name returns
// ErrUnknownDialect.
func Parse(name string) (Dialect, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case string(SQLite), "sqlite3":
		return SQLite, nil
	case string(MySQL):
		return MySQL, nil
	case string(PostgreSQL), "postgres", "pg":
		return PostgreSQL, nil
	case string(GoogleSQL), "bigquery", "spanner", "zetasql":
		return GoogleSQL, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrUnknownDialect, name)
	}
}

// TranslatorFunc converts a single query written in some dialect into SQLite SQL.
type TranslatorFunc func(query string) (string, error)

var (
	customMu    sync.RWMutex
	customTrans = map[Dialect]TranslatorFunc{}
)

// RegisterTranslator installs a custom translator for name, replacing the
// built-in translator if one exists. It is the extension point for future
// parser-based translators. It is safe for concurrent use.
func RegisterTranslator(name Dialect, fn TranslatorFunc) {
	customMu.Lock()
	defer customMu.Unlock()
	if fn == nil {
		delete(customTrans, name)
		return
	}
	customTrans[name] = fn
}

func lookupTranslator(d Dialect) TranslatorFunc {
	customMu.RLock()
	defer customMu.RUnlock()
	return customTrans[d]
}

// Translate converts a query written in dialect d into SQLite SQL. When d is
// SQLite the input is returned unchanged. A custom translator registered with
// RegisterTranslator takes precedence over the built-in one. Errors wrap
// ErrUnsupportedSyntax, ErrInvalidSyntax, or ErrUnknownDialect.
func Translate(d Dialect, query string) (string, error) {
	if d == SQLite {
		return query, nil
	}
	if fn := lookupTranslator(d); fn != nil {
		return fn(query)
	}
	return builtinTranslate(d, query)
}

// builtinTranslate applies the built-in translation for a non-SQLite dialect:
// tokenize with the dialect's lexical rules, then render back to SQLite SQL.
// Dialect-specific token rewrite rules are applied between these two steps as
// they are implemented; in this package's initial form there are none, so
// translation normalizes lexical differences (identifier quoting, string
// quoting, comment style) only.
func builtinTranslate(d Dialect, query string) (string, error) {
	cfg, ok := lexConfigFor(d)
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownDialect, d)
	}

	tokens, err := tokenize(query, cfg)
	if err != nil {
		return "", err
	}

	tokens, err = rewriteTokens(d, tokens)
	if err != nil {
		return "", err
	}

	return render(tokens), nil
}

// rewriteTokens applies the dialect-specific token rewrite rules. Dialects
// without a rewrite pass yet fall through and are translated by lexical
// normalization alone.
func rewriteTokens(d Dialect, tokens []token) ([]token, error) {
	switch d {
	case MySQL:
		return rewriteMySQL(tokens)
	case PostgreSQL:
		return rewritePostgreSQL(tokens)
	case GoogleSQL:
		return rewriteGoogleSQL(tokens)
	default:
		return tokens, nil
	}
}
