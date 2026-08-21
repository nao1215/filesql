package parser

import (
	"io"

	"github.com/nao1215/filesql/internal/reader"
)

// ErrCSVSyntax reports a file that is not CSV: a quote where a field cannot
// hold one, or a quoted field that never closes.
var ErrCSVSyntax = reader.ErrCSVSyntax

// CSVReader reads comma-separated records, keeping the bytes between quotes
// exactly as the file has them.
//
// encoding/csv is otherwise the right answer, and this exists for one
// difference: it removes a carriage return that precedes a line feed inside a
// quoted field. That is documented and callers depend on it, but it makes a
// value change on the way through -- a spreadsheet export whose address cell
// holds a CRLF line break comes back holding LF, so saving the file rewrites a
// row nobody edited. A line break inside quotes is field data, and this reader
// treats it as such.
type CSVReader = reader.CSVReader

// NewCSVReader returns a reader over the comma-separated records in r.
func NewCSVReader(r io.Reader) *CSVReader {
	return reader.NewCSVReader(r)
}
