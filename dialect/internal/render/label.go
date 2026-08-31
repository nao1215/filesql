package render

import (
	"strings"

	"github.com/nao1215/filesql/dialect/internal/ast"
)

// SQLite names an unaliased result column after the text of the expression that
// produced it, so lowering an expression renames the caller's column. A
// PostgreSQL "amt::text" becomes "postgresql_cast(amt, 'text')" and would reach
// the caller under that name -- as a CSV header, as a JSON key, as whatever the
// result was rendered into. The helper is this package's business; the label is
// the caller's.
//
// So a select item that lowering changed carries its original text as an alias.
// The label is the expression as it was written rather than the name the source
// database would have derived, because that rule is one rule for every dialect:
// PostgreSQL, MySQL and GoogleSQL each name a bare cast differently, and a
// translation that guessed per dialect would be three rules that still all
// disagree with SQLite.
func preservedLabel(item ast.SelectItem, written string) string {
	if item.Source == "" {
		return ""
	}
	// A star stands for however many columns the table has and names none of
	// them, so no engine lets one take an alias. Its source text differs from
	// the rendering whenever the caller wrote whitespace around the qualifying
	// dot, and labeling it wrote SQL nothing can read.
	if _, ok := item.Expr.(*ast.Star); ok {
		return ""
	}
	if sameName(item.Source, strings.TrimSpace(written)) {
		return ""
	}
	return item.Source
}

// sameName reports whether two spellings name the same thing. They do when they
// differ only by identifier quoting: writing "col" back as col does not rename
// the column, and labeling it would only repeat the name.
func sameName(source, written string) bool {
	return source == written ||
		strings.ReplaceAll(source, `"`, "") == strings.ReplaceAll(written, `"`, "")
}
