package filesql

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"unicode"
)

const englishReadmePath = "README.md"

func readReadme(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(englishReadmePath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", englishReadmePath, err)
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
		"## Recipes",
		"## Behavior and limits",
		"## Examples",
		"## Contributing",
		"## License",
	}

	content := readReadme(t)
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

	content := readReadme(t)
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

	content := readReadme(t)

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

				db, err := Open(context.Background(), path)
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

// moduleSignatures is what every exported function and method of this module
// takes, keyed by "package.Name" and, for a method, "package.Receiver.Name".
type moduleSignature struct {
	params   int
	variadic bool
	takesCtx bool
}

// moduleDocs reads every non-test Go file of the module, returning the
// signatures its packages export and every comment group in them.
func moduleDocs(t *testing.T) (map[string]moduleSignature, map[string][]*ast.CommentGroup, map[string]bool, *token.FileSet) {
	t.Helper()

	signatures := map[string]moduleSignature{}
	comments := map[string][]*ast.CommentGroup{}
	exported := map[string]bool{}
	fset := token.NewFileSet()

	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "testdata", "examples", "doc":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if parseErr != nil {
			return parseErr
		}
		pkg := file.Name.Name
		comments[path] = file.Comments
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if !d.Name.IsExported() {
					continue
				}
				exported[d.Name.Name] = true
				sig := moduleSignature{}
				for i, field := range d.Type.Params.List {
					n := max(len(field.Names), 1)
					sig.params += n
					if _, isVariadic := field.Type.(*ast.Ellipsis); isVariadic {
						sig.variadic = true
					}
					if selector, ok := field.Type.(*ast.SelectorExpr); ok && i == 0 && selector.Sel.Name == "Context" {
						sig.takesCtx = true
					}
				}
				key := pkg + "." + d.Name.Name
				if d.Recv != nil {
					key = pkg + "." + receiverName(d.Recv.List[0].Type) + "." + d.Name.Name
				}
				signatures[key] = sig
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						if s.Name.IsExported() {
							exported[s.Name.Name] = true
						}
					case *ast.ValueSpec:
						for _, name := range s.Names {
							if name.IsExported() {
								exported[name.Name] = true
							}
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to read the module: %v", err)
	}
	return signatures, comments, exported, fset
}

// receiverName is the type a method hangs off, without the pointer.
func receiverName(expr ast.Expr) string {
	if star, ok := expr.(*ast.StarExpr); ok {
		return receiverName(star.X)
	}
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return ""
}

// TestDocCommentSnippetsCompile holds the code inside the documentation to the
// signatures the module exports. A snippet is what a reader copies out of
// pkg.go.dev, so one that names fewer arguments than the function takes, or
// passes something that is not a context where a context goes, is documentation
// that cannot be run -- and nothing else checks it, which is how six snippets
// went on calling Open and DumpDatabase without the context they took on.
func TestDocCommentSnippetsCompile(t *testing.T) {
	t.Parallel()

	signatures, comments, _, fset := moduleDocs(t)

	for path, groups := range comments {
		pkg := filepath.Base(filepath.Dir(path))
		if pkg == "." || pkg == "filesql" {
			pkg = "filesql"
		}
		for _, group := range groups {
			for _, comment := range group.List {
				text, isLine := strings.CutPrefix(comment.Text, "//")
				if !isLine || !strings.HasPrefix(text, "\t") {
					continue
				}
				line := strings.TrimSpace(text)
				for _, call := range docCommentCalls(line) {
					name, qualifier := call.name, ""
					if at := strings.LastIndex(name, "."); at >= 0 {
						qualifier, name = name[:at], name[at+1:]
					}
					key := pkg + "." + name
					if qualifier != "" && qualifier != pkg {
						// Another package's call, which this module does not
						// have the signature of.
						continue
					}
					sig, known := signatures[key]
					if !known {
						continue
					}
					least := sig.params
					if sig.variadic {
						least--
					}
					where := fset.Position(comment.Pos())
					if call.args < least {
						t.Errorf("%s: %q calls %s with %d arguments; it takes %d",
							where, line, call.name, call.args, sig.params)
						continue
					}
					if sig.takesCtx && call.first != "" && !looksLikeContext(call.first) {
						t.Errorf("%s: %q passes %s where %s takes a context.Context",
							where, line, call.first, call.name)
					}
				}
			}
		}
	}
}

// TestDocCommentsNameNothingRemoved holds the documentation to the names the
// module still exports. A doc comment that sends a reader to a symbol removed
// two releases ago -- EnableAutoSave pointed at Build for years after it went
// -- costs the reader the search before they conclude it is gone.
func TestDocCommentsNameNothingRemoved(t *testing.T) {
	t.Parallel()

	_, comments, exported, fset := moduleDocs(t)

	// The names a doc comment may spell that this module no longer has. Each
	// was exported once and removed, so a reader meeting one in prose has no
	// way to tell it from a name they simply have not found yet.
	removed := []string{
		"Build", "OpenContext", "DumpDatabaseContext",
		"DumpACHWithTableSet", "DumpFedWireWithTableSet",
		"NewCompressionHandler", "CompressionFactory", "NewCompressionFactory",
		"ErrEmptyJSONOutput", "NewReadOnlyDB",
	}
	for _, name := range removed {
		if exported[name] {
			continue // Still here, so a mention of it is correct.
		}
		for path, groups := range comments {
			for _, group := range groups {
				for _, comment := range group.List {
					if !mentionsIdentifier(comment.Text, name) {
						continue
					}
					t.Errorf("%s: the documentation names %s, which this module no longer exports: %q",
						fset.Position(comment.Pos()), name, strings.TrimSpace(comment.Text))
				}
			}
			_ = path
		}
	}
}

// mentionsIdentifier reports whether text names an identifier as a word rather
// than as part of a longer one, so "Build" is found in "refused by Build" and
// not in "Builder".
//
// A comment whose own text opens with the word is using it as a verb -- "Build
// index mapping for quick lookup" -- rather than naming a symbol, so that one
// is passed over. The cost is a reference that begins a comment and the gain is
// that the check can be run over prose at all.
func mentionsIdentifier(text, name string) bool {
	body := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(text, "//"), "/*"))
	if strings.HasPrefix(body, name+" ") {
		return false
	}
	for at := 0; ; {
		found := strings.Index(text[at:], name)
		if found < 0 {
			return false
		}
		start := at + found
		end := start + len(name)
		beforeOK := start == 0 || !isDocIdentifierByte(text[start-1])
		afterOK := end == len(text) || !isDocIdentifierByte(text[end])
		if beforeOK && afterOK {
			return true
		}
		at = start + 1
	}
}

func isDocIdentifierByte(c byte) bool {
	return c == '_' || c == '.' ||
		(c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// docCommentCall is one call found in a line of a documentation code block.
type docCommentCall struct {
	name  string
	args  int
	first string
}

// docCommentCalls finds the calls in one line of a code block, counting the
// commas that are not inside a nested call, a bracket or a string.
func docCommentCalls(line string) []docCommentCall {
	var found []docCommentCall
	for i := range len(line) {
		if line[i] != '(' {
			continue
		}
		start := i
		for start > 0 && isDocIdentifierByte(line[start-1]) {
			start--
		}
		name := line[start:i]
		if name == "" || (name[0] >= '0' && name[0] <= '9') {
			continue
		}
		args, end := countCallArguments(line[i:])
		if end < 0 {
			continue
		}
		first := ""
		if args > 0 {
			body := line[i+1 : i+end]
			if comma := strings.IndexByte(body, ','); comma >= 0 {
				body = body[:comma]
			}
			first = strings.TrimSpace(body)
		}
		found = append(found, docCommentCall{name: name, args: args, first: first})
	}
	return found
}

// countCallArguments answers how many arguments a call written from s takes and
// where its closing parenthesis is, or -1 when the line does not hold one.
func countCallArguments(s string) (int, int) {
	depth, args := 0, 0
	holding := false
	var quote byte
	for i := range len(s) {
		c := s[i]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '"', '\'', '`':
			quote, holding = c, true
		case '(', '[', '{':
			depth++
			if depth > 1 {
				holding = true
			}
		case ')', ']', '}':
			depth--
			if depth == 0 {
				if holding {
					args++
				}
				return args, i
			}
		case ',':
			if depth == 1 {
				args++
				holding = false
			}
		default:
			if c != ' ' && c != '\t' {
				holding = true
			}
		}
	}
	return 0, -1
}

// looksLikeContext reports whether an argument as it is spelled in a snippet is
// a context.
func looksLikeContext(arg string) bool {
	switch arg {
	case "ctx", "context.Background()", "context.TODO()", "t.Context()":
		return true
	}
	return strings.HasPrefix(arg, "ctx")
}

// TestReadmeLinksResolve holds README's own links to what the repository has. A
// link to a heading is checked against the headings the file holds, derived the
// way GitHub derives an anchor, and a link to a file against the file. Three
// links in the format table pointed at sections that had moved to the godoc,
// and nothing said so because a dead anchor scrolls nowhere rather than
// failing.
func TestReadmeLinksResolve(t *testing.T) {
	t.Parallel()

	readme := readReadme(t)

	anchors := map[string]bool{}
	for line := range strings.Lines(readme) {
		heading, found := strings.CutPrefix(strings.TrimSpace(line), "#")
		if !found {
			continue
		}
		heading = strings.TrimLeft(heading, "#")
		anchors[githubAnchor(strings.TrimSpace(heading))] = true
	}

	link := regexp.MustCompile(`\[[^\]]*\]\(([^)]+)\)`)
	for _, match := range link.FindAllStringSubmatch(readme, -1) {
		target := match[1]
		switch {
		case strings.HasPrefix(target, "http://"), strings.HasPrefix(target, "https://"),
			strings.HasPrefix(target, "mailto:"):
			// Somewhere else, which this test does not reach for.
		case strings.HasPrefix(target, "#"):
			if !anchors[strings.TrimPrefix(target, "#")] {
				t.Errorf("README links to %q and no heading of the file makes that anchor", target)
			}
		default:
			path, _, _ := strings.Cut(target, "#")
			if _, err := os.Stat(path); err != nil {
				t.Errorf("README links to %q, which is not in the repository", target)
			}
		}
	}
}

// githubAnchor is the anchor GitHub derives from a heading: lower case, spaces
// as hyphens, and everything that is not a letter, a digit, a hyphen or an
// underscore dropped.
func githubAnchor(heading string) string {
	var anchor strings.Builder
	for _, r := range strings.ToLower(heading) {
		switch {
		case r == ' ':
			anchor.WriteByte('-')
		case r == '-' || r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r):
			anchor.WriteRune(r)
		}
	}
	return anchor.String()
}
