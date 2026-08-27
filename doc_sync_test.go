package filesql

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const englishReadmePath = "README.md"

func readReadme(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path) //nolint:gosec // fixed, in-repo documentation path
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}
	return string(raw)
}

func TestReadmeFileExists(t *testing.T) {
	t.Parallel()

	info, err := os.Stat(englishReadmePath)
	if err != nil {
		t.Fatalf("README %s is missing: %v", englishReadmePath, err)
	}
	if info.Size() == 0 {
		t.Fatalf("README %s is empty", englishReadmePath)
	}
}

func TestEnglishReadmeHasRequiredSections(t *testing.T) {
	t.Parallel()

	requiredSections := []string{
		"## Why filesql?",
		"## Features",
		"## Supported File Formats",
		"## Installation",
		"## Quick Start",
		"## Behavior and limits",
		"## Examples",
		"## Contributing",
		"## License",
	}

	content := readReadme(t, englishReadmePath)
	for _, section := range requiredSections {
		if !strings.Contains(content, section) {
			t.Errorf("%s is missing required section %q", englishReadmePath, section)
		}
	}
}

func TestEnglishReadmeHasStableMarkers(t *testing.T) {
	t.Parallel()

	markers := []string{
		"https://github.com/sponsors/nao1215",
		"filesql-logo.png",
		"filesql.Open",
		"pkg.go.dev/github.com/nao1215/filesql",
	}

	content := readReadme(t, englishReadmePath)
	for _, marker := range markers {
		if !strings.Contains(content, marker) {
			t.Errorf("%s is missing marker %q", englishReadmePath, marker)
		}
	}
}

// TestReadmeLeavesTheRulesToGodoc holds README to being about use rather than
// about behavior.
//
// It grew the other way: every change that altered behavior added its
// explanation to README, because the previous explanation was there, and this
// file's own required sections and markers pinned it. The section that started
// as notes became four times the size of Quick Start, and two of its rules had a
// second copy in doc.go free to drift from it. A rule belongs where a caller
// meets it while writing the call.
func TestReadmeLeavesTheRulesToGodoc(t *testing.T) {
	t.Parallel()

	content := readReadme(t, englishReadmePath)

	// Phrases that only a statement of behavior would carry. Each one was in
	// README before the rules moved to godoc.
	for _, rule := range []string{
		"Column types",
		"Important Notes",
		"peak RSS",
		"integer division",
		"is safe to share across goroutines",
		"Control records are derived",
	} {
		if strings.Contains(content, rule) {
			t.Errorf("%s states a rule that belongs in godoc: %q", englishReadmePath, rule)
		}
	}
}

// TestPackageDocStatesTheRulesReadmeGaveUp names what doc.go has to keep now
// that README does not: the sections the rules moved into, and the phrases a
// caller would search for. Without this the move could be undone by deletion
// rather than by a decision.
func TestPackageDocStatesTheRules(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("doc.go")
	if err != nil {
		t.Fatalf("failed to read doc.go: %v", err)
	}
	doc := string(raw)

	for _, section := range []string{
		"# Column Types",
		"# Memory and Streaming",
		"# Concurrency",
		"# Saving Changes",
		"# Excel Sheet Visibility",
		"# ACH and Fedwire",
		"# Cancellation",
	} {
		if !strings.Contains(doc, section) {
			t.Errorf("doc.go is missing section %q", section)
		}
	}

	// Which save keeps a source's line terminator is a distinction a caller acts
	// on: the in-place mode reads it from the file, every other save writes "\n"
	// unless told otherwise, and WithLineEnding is how the second is asked for
	// the first. Keeping only the name of the mode lets the other two halves of
	// the rule go missing, so all three are pinned.
	for _, marker := range []string{
		`EnableAutoSave("")`,
		`EnableAutoSave("./dir")`,
		"WithLineEnding",
		"SetDefaultChunkSize",
		"ErrEncoding",
		"ErrSourceUnavailable",
		"ErrReservedTableName",
		"_filesql_sources",
	} {
		if !strings.Contains(doc, marker) {
			t.Errorf("doc.go is missing marker %q", marker)
		}
	}
}

// TestPackageDocMatchesTheColumnNameRule holds doc.go's Column Name Handling
// section against what the loader does.
//
// The section used to open with "Column names are handled with case-sensitive
// comparison for duplicate detection" and then say in its next sentence that
// names identical after trimming are duplicates "regardless of case
// differences" — two statements that cannot both be true, and neither of which
// is the rule. The rule is two separate comparisons, ASCII case folding and
// whitespace trimming, and what makes them separate is the case below that is
// accepted: " A" beside "a" is equal under neither on its own, and folding a
// trimmed name would have refused it.
func TestPackageDocMatchesTheColumnNameRule(t *testing.T) {
	t.Parallel()

	t.Run("doc.go states the rule", func(t *testing.T) {
		t.Parallel()

		raw, err := os.ReadFile("doc.go")
		if err != nil {
			t.Fatalf("failed to read doc.go: %v", err)
		}
		doc := string(raw)

		if strings.Contains(doc, "case-sensitive") {
			t.Error("doc.go still calls duplicate detection case-sensitive; ASCII case is folded")
		}
		for _, marker := range []string{
			"ErrDuplicateColumn",
			"ASCII letter case",
			"trimmed",
			"JSON, JSONL",
			"zlib",
			"snappy",
			"(.s2)",
			"LZ4",
		} {
			if !strings.Contains(doc, marker) {
				t.Errorf("doc.go is missing marker %q", marker)
			}
		}
	})

	t.Run("the loader keeps the rule", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			name    string
			header  string
			refused bool
		}{
			{"ASCII case alone is a duplicate", "ID,id", true},
			{"whitespace alone is a duplicate", "name, name ", true},
			{"non-ASCII case is not folded", "ä,Ä", false},
			{"trim and fold are not applied together", " A,a", false},
		} {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				path := filepath.Join(t.TempDir(), "header.csv")
				if err := os.WriteFile(path, []byte(tt.header+"\n1,2\n"), 0o600); err != nil {
					t.Fatalf("failed to write the fixture: %v", err)
				}

				db, err := OpenContext(context.Background(), path)
				if db != nil {
					defer db.Close()
				}
				if tt.refused {
					if !errors.Is(err, ErrDuplicateColumn) {
						t.Fatalf("header %q: error = %v, want ErrDuplicateColumn", tt.header, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("header %q must load as two columns: %v", tt.header, err)
				}
			})
		}
	})
}

// TestSecurityPolicyNamesTheCurrentReleaseSeries keeps SECURITY.md's supported
// version table in step with the newest released version in CHANGELOG.md. The
// table is what a reporter reads to decide whether the version they found a
// problem in is still supported, and it is edited by hand at release time, so
// it drifted once already. The CHANGELOG is the source rather than git tags
// because a shallow checkout has no tags and the release step edits the
// CHANGELOG anyway.
func TestSecurityPolicyNamesTheCurrentReleaseSeries(t *testing.T) {
	t.Parallel()

	changelog, err := os.ReadFile("CHANGELOG.md")
	if err != nil {
		t.Fatalf("failed to read CHANGELOG.md: %v", err)
	}
	series, ok := latestReleasedSeries(string(changelog))
	if !ok {
		t.Fatal("CHANGELOG.md has no released version section")
	}

	policy, err := os.ReadFile("SECURITY.md")
	if err != nil {
		t.Fatalf("failed to read SECURITY.md: %v", err)
	}
	want := "| `" + series + "` | Yes |"
	if !strings.Contains(string(policy), want) {
		t.Errorf("SECURITY.md does not mark %s as supported; the table needs a row starting %q", series, want)
	}
}

// latestReleasedSeries reads the newest "## [X.Y.Z] - date" heading of a
// Keep a Changelog document and returns its minor series as "X.Y.x". The
// Unreleased heading carries no date and is skipped.
func latestReleasedSeries(changelog string) (string, bool) {
	for line := range strings.Lines(changelog) {
		rest, found := strings.CutPrefix(strings.TrimSpace(line), "## [")
		if !found {
			continue
		}
		version, _, found := strings.Cut(rest, "]")
		if !found || !strings.Contains(rest, "] - ") {
			continue
		}
		major, tail, found := strings.Cut(version, ".")
		if !found {
			continue
		}
		minor, _, found := strings.Cut(tail, ".")
		if !found {
			continue
		}
		return major + "." + minor + ".x", true
	}
	return "", false
}
