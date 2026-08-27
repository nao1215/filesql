package reader

import (
	"bufio"
	"errors"
	"io"
	"strings"
)

// TSVReader reads tab-separated records, taking every field literally.
//
// TSV has no quoting. IANA's text/tab-separated-values says a field is the bytes
// between two tabs, so a double quote in one is an ordinary character. Reading
// TSV with a CSV reader brings CSV's quote handling with it: a value as plain as
// 5'9" tall fails the whole import with `bare " in non-quoted-field`.
type TSVReader struct {
	// source is the input, kept until the scanner is made, which is on first
	// use so a lowered maxRecord sizes the buffer it is read through.
	source  io.Reader
	scanner *bufio.Scanner
	// fields is the field count of the first record, which decides what a blank
	// line means. See Read.
	fields int
	read   bool
	// line counts the lines handed back, so a refusal can name the one that
	// caused it.
	line int
	// maxRecord bounds one record. Zero reads maxRecordSize; a test lowers it
	// to reach the bound without producing the whole of it.
	maxRecord int
}

// NewTSVReader returns a reader over the tab-separated records in r.
func NewTSVReader(r io.Reader) *TSVReader {
	return &TSVReader{source: r}
}

// recordLimit is the largest record this reader will hold.
func (t *TSVReader) recordLimit() int {
	if t.maxRecord > 0 {
		return t.maxRecord
	}
	return maxRecordSize
}

// records is the scanner, made on first use.
//
// A record is a line, and a line can be as long as the data in it, so the
// buffer grows to the limit rather than stopping at the 64 KiB a scanner
// defaults to -- that default would fail the whole read on a file whose rows
// are merely wide.
func (t *TSVReader) records() *bufio.Scanner {
	if t.scanner == nil {
		limit := t.recordLimit()
		t.scanner = bufio.NewScanner(t.source)
		t.scanner.Buffer(make([]byte, 0, min(bufio.MaxScanTokenSize, limit)), limit)
	}
	return t.scanner
}

// Read returns the next record, or io.EOF when there are none left.
//
// A blank line is a record only in a one-column file, where it is that column's
// empty value. With more columns it cannot be a record of that shape, so it is
// skipped, as a CSV reader skips it.
func (t *TSVReader) Read() ([]string, error) {
	scanner := t.records()
	for scanner.Scan() {
		t.line++
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" && t.read && t.fields > 1 {
			continue
		}

		record := strings.Split(line, "\t")
		if !t.read {
			t.fields = len(record)
			t.read = true
		}
		return record, nil
	}

	if err := scanner.Err(); err != nil {
		// A record past the limit is the one failure worth wording here: the
		// scanner reports its own buffer, which says nothing to a caller holding
		// the file. Everything else is the source's failure and is passed on.
		if errors.Is(err, bufio.ErrTooLong) {
			return nil, recordTooLongError(t.line+1, t.recordLimit())
		}
		return nil, err
	}
	return nil, io.EOF
}

// ReadAll returns every remaining record.
func (t *TSVReader) ReadAll() ([][]string, error) {
	var records [][]string
	for {
		record, err := t.Read()
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
}
