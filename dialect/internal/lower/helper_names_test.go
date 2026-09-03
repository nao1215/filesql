package lower

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestEveryRenameTargetHasAnArity holds the arity tables to the names the
// lowerings rename a call onto. Without an entry the count is unchecked and
// SQLite answers about the name this package substituted rather than the one
// the caller wrote: CHAR_LENGTH() became length() and the refusal read "wrong
// number of arguments to function length()", about a function nowhere in the
// query. The table held three of the nineteen targets, and nothing connected it
// to them, so this reads the lowerings for every rename and requires an answer.
func TestEveryRenameTargetHasAnArity(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("failed to read the lowering package: %v", err)
	}

	// A rename is written rename(call, "name"); the name is what has to be in
	// a table. A name built rather than written -- the dialect prefixes, which
	// end in an underscore -- is a helper by construction and is skipped.
	target := regexp.MustCompile(`rename\(call, "([A-Za-z_0-9]+)"\)`)
	targets := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		body, readErr := os.ReadFile(entry.Name())
		if readErr != nil {
			t.Fatalf("failed to read %s: %v", entry.Name(), readErr)
		}
		for _, match := range target.FindAllStringSubmatch(string(body), -1) {
			if strings.HasSuffix(match[1], "_") {
				continue // A prefix a caller's name is appended to.
			}
			targets[match[1]] = true
		}
	}
	if len(targets) == 0 {
		t.Fatal("no rename targets were found, so this test checks nothing")
	}

	for name := range targets {
		if _, found := helperArity[strings.ToLower(name)]; found {
			continue
		}
		if _, found := builtinArity[name]; found {
			continue
		}
		t.Errorf("a lowering renames a call onto %q and neither arity table says how many arguments it takes;"+
			" add it to builtinArity, with no counts when it is variadic", name)
	}
}
