package lower

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestEveryRenameTargetHasAnArity holds the arity tables to the names the
// lowerings rename a call onto. Without an entry the count is unchecked and
// SQLite answers about the name this package substituted rather than the one
// the caller wrote: CHAR_LENGTH() became length() and the refusal read "wrong
// number of arguments to function length()", about a function nowhere in the
// query. The tables held three of those names and nothing connected them to the
// renames, so this reads the lowerings and requires an answer for each.
//
// A rename whose target is worked out at run time -- from a table of aliases,
// or from the caller's own name -- cannot be read here and is passed over. Those
// are the renames onto this package's own helpers, which the runtime registers
// with their arity, so the table this checks is not where their answer lives.
func TestEveryRenameTargetHasAnArity(t *testing.T) {
	t.Parallel()

	files := lowerSources(t)

	// The package's own string constants, so a rename written as a constant is
	// read as the name it holds.
	constants := map[string]string{}
	for _, file := range files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok || len(value.Names) != len(value.Values) {
					continue
				}
				for i, name := range value.Names {
					if lit, ok := value.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						if text, err := strconv.Unquote(lit.Value); err == nil {
							constants[name.Name] = text
						}
					}
				}
			}
		}
	}

	targets := map[string]bool{}
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) != 2 {
				return true
			}
			fn, ok := call.Fun.(*ast.Ident)
			if !ok || fn.Name != "rename" {
				return true
			}
			switch arg := call.Args[1].(type) {
			case *ast.BasicLit:
				if arg.Kind == token.STRING {
					if text, err := strconv.Unquote(arg.Value); err == nil {
						targets[text] = true
					}
				}
			case *ast.Ident:
				if text, found := constants[arg.Name]; found {
					targets[text] = true
				}
				// An identifier that is not a constant holds a name worked
				// out at run time, which this cannot read.
			}
			return true
		})
	}
	if len(targets) == 0 {
		t.Fatal("no rename targets were found, so this test checks nothing")
	}

	for name := range targets {
		// A prefix a caller's name is appended to is a helper by construction.
		if strings.HasSuffix(name, "_") {
			continue
		}
		lowered := strings.ToLower(name)
		if _, found := helperArity[lowered]; found {
			continue
		}
		if _, found := builtinArity[lowered]; found {
			continue
		}
		t.Errorf("a lowering renames a call onto %q and neither arity table says how many arguments it takes;"+
			" add it to builtinArity, with no counts when it is variadic", name)
	}
}

// lowerSources parses this package's own source files, which is what the test
// above reads to find the renames.
func lowerSources(t *testing.T) []*ast.File {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("failed to read the lowering package: %v", err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("failed to read %s: %v", name, err)
		}
		files = append(files, file)
	}
	if len(files) == 0 {
		t.Fatal("no source files were read, so this test checks nothing")
	}
	return files
}
