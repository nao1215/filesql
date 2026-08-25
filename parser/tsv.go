package parser

import (
	"errors"
	"fmt"
	"io"

	"github.com/nao1215/filesql/internal/reader"
	"github.com/nao1215/filesql/internal/writer"
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

// WriteTSVRecord writes one record as a line of tab-separated fields, and
// reports a field the format cannot hold rather than writing something else.
//
// A CSV writer would quote such a field instead, and to this reader a quote is
// data, so what came back would carry the quotes the writer added.
func WriteTSVRecord(w io.Writer, record []string) error {
	return WriteTSVRecordLineEnding(w, record, "\n")
}

// WriteTSVRecordLineEnding is WriteTSVRecord with the line terminator named,
// for a writer that has to keep the one its destination already uses: a file
// rewritten with a different terminator differs on every line, including the
// ones nobody edited.
//
// An empty lineEnding writes "\n", so a zero value behaves as WriteTSVRecord
// does rather than running the records together.
func WriteTSVRecordLineEnding(w io.Writer, record []string, lineEnding string) error {
	err := writer.TSVRecord(w, record, lineEnding)

	var writeErr *writer.Error
	if errors.As(err, &writeErr) && writeErr.Kind == writer.KindUnrepresentable {
		return fmt.Errorf("%w: %s", ErrTSVUnrepresentable, writeErr.Error())
	}
	return err
}
