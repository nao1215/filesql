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
	if item.Source == "" || keepsItsName(item.Expr) {
		return ""
	}
	if strings.TrimSpace(written) == item.Source {
		return ""
	}
	return item.Source
}

// keepsItsName reports whether an item already names itself: a column, a star,
// a bare name or a placeholder. SQLite names such a column after the column,
// and no lowering changes that.
func keepsItsName(e ast.Expr) bool {
	switch e.(type) {
	case *ast.ColumnRef, *ast.Star, *ast.Ident, *ast.Placeholder, *ast.Keyword:
		return true
	default:
		return false
	}
}
