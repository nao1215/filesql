package parser

import (
	"io"

	"github.com/nao1215/filesql/internal/reader"
)

// NormalizeLineEndings wraps reader so a file whose lines end with a lone
// carriage return is read as lines rather than as one very long line.
//
// CSV readers, and everything here that splits on "\n", understand LF and CRLF.
// A file written with the classic Mac OS 9 convention has neither, so without
// this the whole file parses as a single line: the data folds into the column
// names and the table comes out with zero rows, at no error.
//
// The convention is decided from the start of the file rather than by
// translating every carriage return that appears, because a carriage return
// inside a quoted field is data.
func NormalizeLineEndings(r io.Reader) io.Reader {
	return reader.NormalizeLineEndings(r)
}
