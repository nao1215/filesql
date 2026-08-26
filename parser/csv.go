package parser

import (
	"github.com/nao1215/filesql/internal/reader"
)

// ErrCSVSyntax reports a file that is not CSV: a quote where a field cannot
// hold one, or a quoted field that never closes.
var ErrCSVSyntax = reader.ErrCSVSyntax
