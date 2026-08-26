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
		"## Important Notes",
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
		"OpenContext",
		"DumpDatabase",
		"EnableAutoSave",
		"SetDefaultChunkSize",
		"SetMaxOpenConns",
		// Which save keeps a source's line terminator is a distinction a caller
		// acts on: the in-place mode reads it from the file, every other save
		// writes "\n" unless told otherwise, and WithLineEnding is how the
		// second is asked for the first. A README that keeps only the name of
		// the mode still lets the other two halves of the rule go missing, so
		// all three are pinned.
		`EnableAutoSave("")`,
		`EnableAutoSave("./dir")`,
		"WithLineEnding(LineEndingCRLF)",
	}

	content := readReadme(t, englishReadmePath)
	for _, marker := range markers {
		if !strings.Contains(content, marker) {
			t.Errorf("%s is missing marker %q", englishReadmePath, marker)
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
