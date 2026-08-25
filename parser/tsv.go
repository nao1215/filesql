package parser

import (
	"errors"
	"io"

	"github.com/nao1215/filesql/internal/reader"
)

// ErrTSVSyntax reports tab-separated input that does not describe a table: a
// record whose field count differs from the header's. It is the counterpart of
// ErrCSVSyntax, which a caller can already match for the same shape in CSV;
// without one, the two formats reported the same fault in two ways, and TSV
// reported it as no fault at all.
var ErrTSVSyntax = errors.New("invalid TSV syntax")

// ErrTSVUnrepresentable reports a value that tab-separated values cannot hold:
// one containing a tab, which is the delimiter, or a line break, which ends the
// record. There is no escape for either.
var ErrTSVUnrepresentable = errors.New("value cannot be represented in TSV")

// TSVReader reads tab-separated records, taking every field literally.
//
// TSV has no quoting. IANA's text/tab-separated-values says a field is the bytes
// between two tabs, so a double quote in one is an ordinary character. Reading
// TSV with a CSV reader brings CSV's quote handling with it: a value as plain as
// 5'9" tall fails the whole import with `bare " in non-quoted-field`.
type TSVReader = reader.TSVReader

// NewTSVReader returns a reader over the tab-separated records in r.
func NewTSVReader(r io.Reader) *TSVReader {
	return reader.NewTSVReader(r)
}
