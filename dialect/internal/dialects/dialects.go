// Package dialects holds the SQL dialect identifier. It exists as its own
// package because the lexer, the parser, the lowering layer and the public
// dialect package all need to name a dialect, and none of them may import the
// public package.
package dialects

import (
	"fmt"
	"strings"
)

// Dialect is a SQL dialect accepted at query time. The storage engine is always
// SQLite; a Dialect only selects how a caller's query text is interpreted before
// it reaches SQLite.
type Dialect string

const (
	// SQLite is the native dialect. Translation returns the input unchanged.
	SQLite Dialect = "sqlite"
	// MySQL accepts MySQL query syntax.
	MySQL Dialect = "mysql"
	// PostgreSQL accepts PostgreSQL query syntax.
	PostgreSQL Dialect = "postgresql"
	// GoogleSQL accepts GoogleSQL (BigQuery / Cloud Spanner) query syntax.
	GoogleSQL Dialect = "googlesql"
)

// All returns every built-in dialect in a stable order.
func All() []Dialect {
	return []Dialect{SQLite, MySQL, PostgreSQL, GoogleSQL}
}

// displayNames spells each dialect the way its own project does. The wire value
// is a lowercase identifier, which is right for parsing a flag value and wrong
// in a sentence a person reads.
var displayNames = map[Dialect]string{ //nolint:gochecknoglobals // a fixed table
	SQLite:     "SQLite",
	MySQL:      "MySQL",
	PostgreSQL: "PostgreSQL",
	GoogleSQL:  "GoogleSQL",
}

// DisplayName returns the dialect spelled the way its own project spells it, for
// a message a person reads. A dialect with no spelling here reads back as its
// wire value, so a name a caller built by conversion still has something to
// print; the zero Dialect has neither and returns "".
func (d Dialect) DisplayName() string {
	if name, ok := displayNames[d]; ok {
		return name
	}
	return string(d)
}

// ParseName maps a user-supplied dialect name to a Dialect. Matching is
// case-insensitive and ignores surrounding whitespace. Aliases are accepted:
// "sqlite3" for SQLite; "postgres" and "pg" for PostgreSQL; "bigquery",
// "spanner", and "zetasql" for GoogleSQL.
func ParseName(name string) (Dialect, error) {
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
