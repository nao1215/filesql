package dialect_test

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nao1215/filesql/dialect"
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

func ExampleRegisterTranslator() {
	const shouty = dialect.Dialect("shouty")

	dialect.RegisterTranslator(shouty, func(query string) (string, error) {
		return strings.ToUpper(query), nil
	})

	sqlite, err := dialect.Translate(shouty, "select * from users")
	fmt.Println(sqlite, err)

	// The registry is global, so a translator installed for a test is removed
	// again. Registering nil is how a name is taken back out.
	dialect.RegisterTranslator(shouty, nil)

	_, err = dialect.Translate(shouty, "select * from users")
	fmt.Println(errors.Is(err, dialect.ErrUnknownDialect))
	// Output:
	// SELECT * FROM USERS <nil>
	// true
}
