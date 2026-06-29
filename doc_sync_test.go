package filesql

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// README.md is the source of truth for user-facing documentation; the files
// under doc/<lang>/README.md are translations of it. These tests are a
// drift guardrail: they fail when the English README and its translations fall
// out of sync, which is otherwise easy to miss because the headings are
// translated and a reviewer cannot eyeball seven files at once.
//
// They are deliberately keyed off language-independent content (code
// identifiers, URLs, link targets, and the count of top-level sections) rather
// than translated prose, so they stay robust across languages while still
// catching a dropped section or an un-mirrored change.

// englishReadmePath is the canonical README.
const englishReadmePath = "README.md"

// translatedReadmes maps a language code to its README path. It is a function
// rather than a package-level variable to keep state out of globals.
func translatedReadmes() map[string]string {
	return map[string]string{
		"ja":    "doc/ja/README.md",
		"ru":    "doc/ru/README.md",
		"zh-cn": "doc/zh-cn/README.md",
		"ko":    "doc/ko/README.md",
		"es":    "doc/es/README.md",
		"fr":    "doc/fr/README.md",
	}
}

// allReadmePaths returns the English README plus every translation.
func allReadmePaths() []string {
	translations := translatedReadmes()
	paths := make([]string, 0, len(translations)+1)
	paths = append(paths, englishReadmePath)
	for _, p := range translations {
		paths = append(paths, p)
	}
	return paths
}

func readReadme(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path) //nolint:gosec // fixed, in-repo documentation paths
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}
	return string(raw)
}

// TestReadmeFilesExist ensures the English README and every translation are
// present and non-empty, so a deleted or empty translation is caught.
func TestReadmeFilesExist(t *testing.T) {
	t.Parallel()
	for _, path := range allReadmePaths() {
		info, err := os.Stat(path)
		if err != nil {
			t.Errorf("README %s is missing: %v", path, err)
			continue
		}
		if info.Size() == 0 {
			t.Errorf("README %s is empty", path)
		}
	}
}

// TestEnglishReadmeHasRequiredSections asserts the English README keeps its
// first-class sections. Add a heading here when a new top-level section is
// introduced so an accidental removal is caught.
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

// TestReadmesShareStableMarkers asserts that language-independent anchors found
// in the English README also appear in every translation. These markers are the
// public API identifiers, the sponsor link, and the logo that every README is
// built around, so a translation that drops a section (or the English README
// gaining one without the translations following) is caught.
func TestReadmesShareStableMarkers(t *testing.T) {
	t.Parallel()
	markers := []string{
		"https://github.com/sponsors/nao1215", // sponsor link kept in user-facing docs
		"filesql-logo.png",                    // logo image
		"filesql.Open",                        // entry point
		"OpenContext",                         // context-aware entry point
		"DumpDatabase",                        // export API
		"EnableAutoSave",                      // auto-save API
		"SetDefaultChunkSize",                 // streaming knob
		"SetMaxOpenConns",                     // concurrency guidance
	}
	for _, path := range allReadmePaths() {
		content := readReadme(t, path)
		for _, marker := range markers {
			if !strings.Contains(content, marker) {
				t.Errorf("%s is missing marker %q (README drift)", path, marker)
			}
		}
	}
}

// TestReadmesHaveMatchingTopLevelSectionCount asserts every translation has the
// same number of top-level (##) sections as the English README. Because headings
// are translated, the count is the language-independent way to detect a section
// added to or removed from only some files.
func TestReadmesHaveMatchingTopLevelSectionCount(t *testing.T) {
	t.Parallel()
	want := countHeadings(readReadme(t, englishReadmePath), "## ")
	for lang, path := range translatedReadmes() {
		got := countHeadings(readReadme(t, path), "## ")
		if got != want {
			t.Errorf("%s (%s) has %d top-level sections, want %d to match %s",
				path, lang, got, want, englishReadmePath)
		}
	}
}

// countHeadings counts Markdown headings that start with prefix (e.g. "## "),
// skipping fenced code blocks so a commented "## ..." inside a code sample is
// not miscounted.
func countHeadings(content, prefix string) int {
	count := 0
	inFence := false
	for line := range strings.SplitSeq(content, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		// A level-2 heading must not also be a level-3 heading ("### ").
		if strings.HasPrefix(line, prefix) && !strings.HasPrefix(line, prefix+"#") {
			count++
		}
	}
	return count
}

// TestReadmeLanguageBarsLinkEveryLanguage asserts the language switcher links
// are complete: the English README links to every translation, and each
// translation links back to English and to every sibling translation. This
// catches a missing or mistyped language link.
func TestReadmeLanguageBarsLinkEveryLanguage(t *testing.T) {
	t.Parallel()

	english := readReadme(t, englishReadmePath)
	for lang := range translatedReadmes() {
		link := fmt.Sprintf("](./doc/%s/README.md)", lang)
		if !strings.Contains(english, link) {
			t.Errorf("%s does not link to the %s translation (%q)", englishReadmePath, lang, link)
		}
	}

	for lang, path := range translatedReadmes() {
		content := readReadme(t, path)
		if !strings.Contains(content, "](../../README.md)") {
			t.Errorf("%s (%s) does not link back to the English README", path, lang)
		}
		for other := range translatedReadmes() {
			if other == lang {
				continue
			}
			link := fmt.Sprintf("](../%s/README.md)", other)
			if !strings.Contains(content, link) {
				t.Errorf("%s (%s) does not link to the %s translation (%q)", path, lang, other, link)
			}
		}
	}
}
