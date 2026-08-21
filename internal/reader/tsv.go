package reader

import (
	"bufio"
	"errors"
	"fmt"
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
	scanner *bufio.Scanner
	// fields is the field count of the first record, which decides what a blank
	// line means. See Read.
	fields int
	read   bool
}

// maxTSVRecordSize caps one record, so a file that is not TSV at all cannot ask
// for unbounded memory one line at a time.
const maxTSVRecordSize = 64 * 1024 * 1024

// NewTSVReader returns a reader over the tab-separated records in r.
func NewTSVReader(r io.Reader) *TSVReader {
	scanner := bufio.NewScanner(r)
	// A record is a line, and a line can be as long as the data in it. The
	// default 64 KiB limit would fail the whole read on a file whose rows are
	// wider than that.
	scanner.Buffer(make([]byte, 0, bufio.MaxScanTokenSize), maxTSVRecordSize)
	return &TSVReader{scanner: scanner}
}

// Read returns the next record, or io.EOF when there are none left.
//
// A blank line is a record only in a one-column file, where it is that column's
// empty value. With more columns it cannot be a record of that shape, so it is
// skipped, as a CSV reader skips it.
func (t *TSVReader) Read() ([]string, error) {
	for t.scanner.Scan() {
		line := strings.TrimSuffix(t.scanner.Text(), "\r")
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

	if err := t.scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read TSV record: %w", err)
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
