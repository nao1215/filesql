package dialect_test

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/nao1215/filesql/dialect"
	_ "modernc.org/sqlite"
)

// noSuchFunction is how SQLite says a name resolved to nothing. It says it
// before it counts the arguments, so one call of any shape tells the two apart:
// a name SQLite has answers something else, even when the call is wrong.
var noSuchFunction = regexp.MustCompile(`no such function: ([A-Za-z0-9_]+)`)

// TestEveryEngineFunctionHasAnAnswer holds this package to a rule its tables
// could not state before: for every function name an engine defines, a query
// naming it is translated into something SQLite can run, or refused here by
// name, and never handed to SQLite to fail as a name that does not exist.
//
// That last outcome is the one worth catching. It tells a caller that a name
// they did write does not exist, which reads as a typo in their query rather
// than as a gap here, and it is how fifty-eight names went unnoticed until a
// sweep found them and five aggregates went unnoticed after that sweep, because
// the sweep had walked the scalar functions only.
//
// The names come from the engines rather than from a person: testdata holds
// what MySQL's help tables and PostgreSQL's pg_proc say each of them has, and
// scripts/dump-engine-functions.sh writes those files. A name a later engine
// adds appears when the files are regenerated, and this test fails on it then.
//
// GoogleSQL is not here: BigQuery publishes no catalog of its functions, so its
// table stays hand-written and nothing can check it the way this checks these.
func TestEveryEngineFunctionHasAnAnswer(t *testing.T) {
	t.Parallel()

	if err := dialect.RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error = %v", err)
	}

	for _, tc := range []struct {
		d dialect.Dialect
		// untranslated names the file holding the names this package has no
		// answer for yet, or "" when it has an answer for all of them.
		untranslated string
	}{
		{d: dialect.MySQL},
		{d: dialect.PostgreSQL, untranslated: "untranslated_postgresql.txt"},
	} {
		t.Run(string(tc.d), func(t *testing.T) {
			t.Parallel()

			names := readNames(t, filepath.Join("testdata", "engine_functions_"+string(tc.d)+".txt"))
			if len(names) < 100 {
				t.Fatalf("the corpus holds %d names, which is too few to be the engine's own list", len(names))
			}
			var known []string
			if tc.untranslated != "" {
				known = readNames(t, filepath.Join("testdata", tc.untranslated))
			}

			db, err := sql.Open("sqlite", ":memory:")
			if err != nil {
				t.Fatalf("sql.Open() error = %v", err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Errorf("Close() error = %v", err)
				}
			})

			var leaked []string
			for _, name := range names {
				if reachesSQLiteAsAnUnknownName(t, db, tc.d, name) {
					leaked = append(leaked, name)
				}
			}

			for _, name := range leaked {
				if !slices.Contains(known, name) {
					t.Errorf("%s reaches SQLite as a name it does not have: translate it, or refuse it by name with a reason", name)
				}
			}
			// The other direction: a name that was answered is no longer debt,
			// and leaving it in the file would hide the next one that is.
			for _, name := range known {
				if !slices.Contains(leaked, name) {
					t.Errorf("%s has an answer now; take it out of testdata/%s", name, tc.untranslated)
				}
			}
		})
	}
}

// reachesSQLiteAsAnUnknownName reports whether a call of name under d is
// translated into SQL naming a function SQLite does not have.
func reachesSQLiteAsAnUnknownName(t *testing.T, db *sql.DB, d dialect.Dialect, name string) bool {
	t.Helper()
	ctx := t.Context()

	out, err := dialect.Translate(d, fmt.Sprintf("SELECT %s(1)", name))
	if err != nil {
		// Refused here, which is one of the two answers this test asks for.
		return false
	}
	stmt, err := db.PrepareContext(ctx, out)
	if err == nil {
		if cerr := stmt.Close(); cerr != nil {
			t.Errorf("closing the prepared statement for %s: %v", name, cerr)
		}
		return false
	}
	// Every other error means SQLite resolved the name and objected to
	// something else: the argument count, a type, the shape of the statement.
	return noSuchFunction.MatchString(err.Error())
}

// readNames reads one name per line, ignoring blanks and comments.
func readNames(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path) //nolint:gosec // a path this test builds from a constant
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Errorf("closing %s: %v", path, err)
		}
	}()

	var names []string
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			names = append(names, line)
		}
	}
	if err := s.Err(); err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return names
}
