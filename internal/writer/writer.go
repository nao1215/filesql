// Package writer turns a table's records into the bytes of a text format.
//
// It is the one implementation of every text format filesql writes. The root
// package wrote a dump's rows and prep wrote a processed file's, each with its
// own copy of the same encoding, and the copies drifted: prep was missing the
// rule that keeps a one-column empty row from being written as a blank line,
// and it was missing the check that refuses a value LTSV cannot hold.
// Both are rules a reader depends on and neither is evident from the code that
// writes the bytes, which is the kind that does not survive being copied.
//
// What is shared is the step from a record to bytes. Where the records come
// from is not: a caller reads them from *sql.Rows, from a slice, or from a map
// per row, and keeps doing so.
package writer

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Format is the text format a table's records are written as.
type Format int

const (
	// FormatCSV is comma-separated values.
	FormatCSV Format = iota
	// FormatTSV is tab-separated values.
	FormatTSV
	// FormatLTSV is labeled tab-separated values.
	FormatLTSV
	// FormatJSONL is one JSON value per line.
	FormatJSONL
)

// Options are the choices a caller makes about the output.
type Options struct {
	// LineEnding terminates each record. Empty writes "\n".
	//
	// It is a string rather than a flag because a save keeps the terminator the
	// file it is replacing already used, and that includes the lone carriage
	// return of a classic Mac OS file.
	LineEnding string
}

// terminator is the line ending to write, with the zero value spelled out.
func (o Options) terminator() string {
	if o.LineEnding == "" {
		return "\n"
	}
	return o.LineEnding
}

// Writer writes one table's records in one format.
//
// A caller hands it the header once and then each record, and calls Flush when
// the table is done. Once Flush returns nothing is left buffered, so a caller
// that stages its output learns there whether the write succeeded.
type Writer struct {
	// dst is where a record's bytes go. It is buf when this package added the
	// buffering, and the caller's writer when encoding/csv is already doing it.
	dst io.Writer
	// buf is the buffering this package added, and nil when it added none.
	buf *bufio.Writer

	format Format
	term   string

	// csv encodes a delimited record. It writes into staged when the terminator
	// is not the one encoding/csv emits, and straight into dst when it is.
	csv    *csv.Writer
	staged *bytes.Buffer

	// labels are the LTSV labels, checked once when the header arrives.
	labels []string

	// line builds one LTSV record. It is kept between records because a
	// strings.Builder cannot be: its Reset drops the buffer rather than
	// truncating it, so reusing one allocates per record all the same.
	line []byte

	// compact holds one JSONL value on its way to a single line.
	compact bytes.Buffer
}

// New returns a writer that writes records of format to dst.
//
// The output is buffered, so a caller that needs the bytes to have arrived must
// call Flush. Buffering is here rather than at each caller because the three
// formats that do not go through encoding/csv wrote a record at a time to an
// unbuffered destination, which for a file is a write per row.
func New(dst io.Writer, format Format, opts Options) *Writer {
	w := &Writer{
		dst:    dst,
		format: format,
		term:   opts.terminator(),
	}

	// encoding/csv terminates a record with "\n" and offers only UseCRLF for an
	// alternative, which rewrites every line feed the writer emits -- inside a
	// quoted field as well as between records -- so a cell holding a line break
	// came back holding a different one. A terminator of its own is staged one
	// record at a time instead. The common terminator needs no staging, and
	// encoding/csv is buffering already, so nothing else buffers over it.
	if format == FormatCSV && w.term == "\n" {
		w.csv = csv.NewWriter(dst)
		return w
	}

	w.buf = bufio.NewWriter(dst)
	w.dst = w.buf
	if format == FormatCSV {
		w.staged = new(bytes.Buffer)
		w.csv = csv.NewWriter(w.staged)
	}
	return w
}

// Header takes the table's column names.
//
// What that means is the format's: CSV and TSV write them as the first record,
// LTSV keeps them as the label of each field and writes nothing, and JSONL has
// no place for them at all. It is called once, before any record.
//
// LTSV keeps the slice rather than a copy of it, so a caller that writes over
// its column names writes over the labels.
func (w *Writer) Header(columns []string) error {
	if err := checkFirstColumn(w.format, columns); err != nil {
		return err
	}
	switch w.format {
	case FormatLTSV:
		for _, col := range columns {
			if err := checkLTSVLabel(col); err != nil {
				return err
			}
		}
		w.labels = columns
		return nil
	case FormatJSONL:
		return nil
	default:
		return w.Record(columns)
	}
}

// Record writes one record.
func (w *Writer) Record(record []string) error {
	switch w.format {
	case FormatTSV:
		return TSVRecord(w.dst, record, w.term)
	case FormatLTSV:
		return w.ltsvRecord(record)
	case FormatJSONL:
		return w.jsonlRecord(record)
	default:
		return w.csvRecord(record)
	}
}

// Flush writes out what is buffered and reports the first failure the
// destination gave, whenever it gave it.
func (w *Writer) Flush() error {
	if w.csv != nil {
		if err := w.flushCSV(); err != nil {
			return err
		}
	}
	if w.buf == nil {
		return nil
	}
	return w.buf.Flush()
}

// loneEmptyField is what a CSV record of one empty field is written as.
//
// Written plainly it is a blank line, and a blank line is not a CSV record: a
// reader skips it, so a one-column table's empty rows disappeared and the write
// reported success. The quotes say "one field, and it is empty", which cannot be
// read as anything else. encoding/csv's writer does not quote an empty field --
// it has no way to know it is the only one on the line -- so this record is
// written around it.
const loneEmptyField = `""`

// csvRecord writes one comma-separated record, taking the lone empty field
// around the csv writer.
func (w *Writer) csvRecord(record []string) error {
	if len(record) == 1 && record[0] == "" {
		// Flushing first keeps the two writers' output in order.
		if err := w.flushCSV(); err != nil {
			return err
		}
		_, err := io.WriteString(w.dst, loneEmptyField+w.term)
		return err
	}

	if w.staged != nil {
		w.staged.Reset()
	}
	if err := w.csv.Write(record); err != nil {
		return err
	}
	if w.staged == nil {
		return nil
	}

	// The record was staged so the terminator is this package's choice rather
	// than the csv writer's; see New.
	if err := w.flushCSV(); err != nil {
		return err
	}
	line := bytes.TrimSuffix(w.staged.Bytes(), []byte("\n"))
	if _, err := w.dst.Write(line); err != nil {
		return err
	}
	_, err := io.WriteString(w.dst, w.term)
	return err
}

// flushCSV pushes what the csv writer holds and reports the first failure the
// destination gave it, whenever it gave it.
func (w *Writer) flushCSV() error {
	w.csv.Flush()
	return w.csv.Error()
}

// ltsvRecord writes one record as labeled fields. A record shorter than the
// header writes the labels it has no value for with an empty value, which is
// the field LTSV has for one.
func (w *Writer) ltsvRecord(record []string) error {
	w.line = w.line[:0]
	for i, label := range w.labels {
		var value string
		if i < len(record) {
			value = record[i]
		}
		if err := checkLTSVValue(label, value); err != nil {
			return err
		}
		if i > 0 {
			w.line = append(w.line, '\t')
		}
		w.line = append(w.line, label...)
		w.line = append(w.line, ':')
		w.line = append(w.line, value...)
	}
	w.line = append(w.line, w.term...)
	_, err := w.dst.Write(w.line)
	return err
}

// jsonlRecord writes one JSON value on a line of its own.
//
// The record is one column holding a JSON value as text, which is how a JSON or
// JSONL input is read. A record holding nothing is skipped rather than written
// as a blank line, which is not a JSONL record.
func (w *Writer) jsonlRecord(record []string) error {
	if len(record) == 0 || record[0] == "" {
		return nil
	}

	// The value is compacted because it may have been read pretty-printed, and a
	// value spanning lines is not one JSONL record.
	w.compact.Reset()
	if err := json.Compact(&w.compact, []byte(record[0])); err != nil {
		return &Error{Kind: KindEncode, Msg: "the value is not JSON", Err: err}
	}
	if _, err := w.compact.WriteTo(w.dst); err != nil {
		return err
	}
	_, err := io.WriteString(w.dst, w.term)
	return err
}

// ltsvForbidden names the characters that end a field or a record in LTSV,
// which is why a value cannot carry one.
//
//nolint:gochecknoglobals // constant-like lookup table
var ltsvForbidden = []struct {
	char rune
	name string
}{
	{char: '\t', name: "tab"},
	{char: '\n', name: "newline"},
	{char: '\r', name: "carriage return"},
}

// checkLTSVValue refuses a value LTSV has no way to hold.
//
// LTSV separates fields with a tab and records with a newline, and defines no
// escape for either. Writing one anyway produced a file that parses as
// something else: a tab inside a value opened a second field, which has no
// label and which a reader drops without a word, and a newline split the record
// in two. Refusing it is what lets a caller say which cell cannot be written,
// where writing it anyway lost the value with nothing to say so.
func checkLTSVValue(column, value string) error {
	for _, f := range ltsvForbidden {
		if strings.ContainsRune(value, f.char) {
			return &Error{
				Kind: KindUnrepresentable,
				Msg:  fmt.Sprintf("LTSV cannot hold a value that contains a %s, and column %q holds a %s", f.name, column, f.name),
			}
		}
	}
	return nil
}

// byteOrderMark is U+FEFF, which a text file carries at its front to say how it
// is encoded rather than to say anything about the text.
const byteOrderMark = "\ufeff"

// checkFirstColumn refuses a first column name that begins with a byte-order
// mark, in the formats that write it at the front of the file, where a reader
// takes it for the encoding mark and drops it. None of them can escape it:
// there is no quoting in TSV or LTSV, and Go's CSV writer does not quote a
// field for a mark.
//
// Only the first name is at the front, so a mark on any later one is written.
func checkFirstColumn(format Format, columns []string) error {
	if format == FormatJSONL || len(columns) == 0 {
		return nil
	}
	if !strings.HasPrefix(columns[0], byteOrderMark) {
		return nil
	}
	return &Error{
		Kind: KindUnrepresentableAsText,
		Msg: fmt.Sprintf(
			"a column name cannot begin with a byte-order mark where it is written at the front of the file, and column %q does",
			columns[0]),
	}
}

// checkLTSVLabel refuses a column name that would not read back as a label. A
// colon is what separates a label from its value, so it is forbidden here on
// top of the characters no field may carry.
//
// Whitespace around a name is refused for the same reason, although it ends
// nothing: LTSV restricts a label to letters, digits, underscore, dot and
// hyphen, so a reader trims one and the column comes back under a different
// name. Writing it anyway renamed the column on a reload with nothing to say
// so -- a table with the column " a" was written as " a:1" and read back as
// "a", which is the outcome the refusals here exist to replace.
func checkLTSVLabel(column string) error {
	if trimmed := strings.TrimSpace(column); trimmed != column {
		return &Error{
			Kind: KindUnrepresentable,
			Msg:  fmt.Sprintf("an LTSV label cannot begin or end with whitespace, and column %q would be read back as %q", column, trimmed),
		}
	}
	if strings.ContainsRune(column, ':') {
		return &Error{
			Kind: KindUnrepresentable,
			Msg:  fmt.Sprintf("an LTSV label cannot contain a colon, and column %q holds a colon", column),
		}
	}
	for _, f := range ltsvForbidden {
		if strings.ContainsRune(column, f.char) {
			return &Error{
				Kind: KindUnrepresentable,
				Msg:  fmt.Sprintf("an LTSV label cannot contain a %s, and column %q holds a %s", f.name, column, f.name),
			}
		}
	}
	return nil
}
