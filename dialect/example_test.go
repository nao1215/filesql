package dialect_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/nao1215/filesql/dialect"
	_ "modernc.org/sqlite"
)

func ExampleTranslate() {
	// MySQL quotes an identifier with backticks. SQLite accepts those too, for
	// compatibility, and translation normalizes them to its own double-quote
	// form so the query that reaches the engine is standard SQL.
	sqlite, err := dialect.Translate(dialect.MySQL, "SELECT `name` FROM `users`")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(sqlite)

	// A construct SQLite has no equivalent for is refused rather than passed
	// through to fail as something else.
	_, err = dialect.Translate(dialect.PostgreSQL, "SELECT DISTINCT ON (dept) name FROM staff")
	fmt.Println(errors.Is(err, dialect.ErrUnsupportedSyntax))
	// Output:
	// SELECT "name" FROM "users"
	// true
}

// ExampleTranslate_unsupportedFeature shows the second kind of refusal. A
// construct SQLite cannot express is ErrUnsupportedSyntax; one this package does
// not model is ErrUnsupportedFeature. Neither reaches SQLite, so a query outside
// the supported subset fails here with the construct named rather than there
// with a message about SQLite's parser.
func ExampleTranslate_unsupportedFeature() {
	_, err := dialect.Translate(dialect.MySQL, "SHOW TABLES")
	fmt.Println(errors.Is(err, dialect.ErrUnsupportedFeature))
	fmt.Println(err)
	// Output:
	// true
	// dialect: SQL feature not implemented by this package: the SHOW statement is not implemented at line 1, column 1
}

func ExampleParse() {
	// Matching ignores case and surrounding space, and the aliases each
	// project is also known by are accepted.
	for _, name := range []string{"MySQL", " pg ", "bigquery", "oracle"} {
		d, err := dialect.Parse(name)
		if err != nil {
			fmt.Printf("%q: %v\n", name, errors.Is(err, dialect.ErrUnknownDialect))
			continue
		}
		fmt.Printf("%q: %s\n", name, d)
	}
	// Output:
	// "MySQL": mysql
	// " pg ": postgresql
	// "bigquery": googlesql
	// "oracle": true
}

func ExampleDialects() {
	fmt.Println(dialect.Dialects())
	// Output: [sqlite mysql postgresql googlesql]
}

func ExampleDialect_DisplayName() {
	// The wire value is a lowercase identifier, which is right for a flag and
	// wrong in a sentence a person reads.
	fmt.Println(dialect.PostgreSQL, "->", dialect.PostgreSQL.DisplayName())
	fmt.Println(dialect.GoogleSQL, "->", dialect.GoogleSQL.DisplayName())
	// Output:
	// postgresql -> PostgreSQL
	// googlesql -> GoogleSQL
}

func ExampleTranslate_postgreSQL() {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	// Three PostgreSQL constructs SQLite has no form for: a TO_CHAR template,
	// GREATEST over a column that has a NULL in it, and a range whose bounds
	// are written in either order.
	for _, query := range []string{
		`SELECT TO_CHAR(DATE '2024-03-05', 'FMDay, DD FMMonth YYYY')`,
		`SELECT GREATEST(1, NULL, 2)`,
		`SELECT 5 BETWEEN SYMMETRIC 7 AND 3`,
	} {
		translated, err := dialect.Translate(dialect.PostgreSQL, query)
		if err != nil {
			fmt.Println(err)
			return
		}
		var answer string
		if err := db.QueryRowContext(context.Background(), translated).Scan(&answer); err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(answer)
	}
	// Output:
	// Tuesday, 05 March 2024
	// 2
	// 1
}

func ExampleTranslate_googleSQL() {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	// Three BigQuery constructs SQLite has no form for: a date built out of
	// its fields, a digest printed as hexadecimal, and the SAFE. prefix, which
	// answers NULL where the call would have raised.
	for _, query := range []string{
		`SELECT FORMAT_DATE('%Y-%m-%d', DATE(2024, 3, 5))`,
		`SELECT TO_HEX(MD5('abc'))`,
		`SELECT IFNULL(SAFE.PARSE_DATE('%Y-%m-%d', 'not a date'), 'unparsed')`,
	} {
		translated, err := dialect.Translate(dialect.GoogleSQL, query)
		if err != nil {
			fmt.Println(err)
			return
		}
		var answer string
		if err := db.QueryRowContext(context.Background(), translated).Scan(&answer); err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(answer)
	}
	// Output:
	// 2024-03-05
	// 900150983cd24fb0d6963f7d28e17f72
	// unparsed
}
