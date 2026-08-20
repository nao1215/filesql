package filesql

import (
	"os"
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
