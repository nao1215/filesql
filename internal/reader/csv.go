package reader

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

// ErrCSVSyntax reports a file that is not CSV: a quote where a field cannot
// hold one, or a quoted field that never closes.
var ErrCSVSyntax = errors.New("invalid CSV syntax")

// CSVReader reads comma-separated records, keeping the bytes between quotes
// exactly as the file has them.
//
// encoding/csv is otherwise the right answer, and this exists for one
// difference: it removes a carriage return that precedes a line feed inside a
// quoted field. That is documented and callers depend on it, but it makes a
// value change on the way through — a spreadsheet export whose address cell
// holds a CRLF line break comes back holding LF, so saving the file rewrites a
// row nobody edited. A line break inside quotes is field data, and this reader
// treats it as such.
type CSVReader struct {
	r *bufio.Reader
	// Comma separates fields. The zero value reads ',' and only a single-byte
	// delimiter is supported, which is what the callers use.
	Comma rune
	// FieldsPerRecord follows encoding/csv: zero takes the count from the first
	// record and requires the rest to match, a negative value allows any count,
	// and a positive value requires that many.
	FieldsPerRecord int

	// buf holds the record being read, which is one line unless a quoted field
	// carries the record across several.
	buf []byte
	// fields holds every field of the record end to end, and ends holds where
	// each one stops. One conversion to string covers the whole record, which is
	// what keeps a wide table from allocating once per cell.
	fields []byte
	ends   []int
	line   int
}

// NewCSVReader returns a reader over the comma-separated records in r.
func NewCSVReader(r io.Reader) *CSVReader {
	return &CSVReader{r: bufio.NewReader(r), Comma: ','}
}

// Read returns the next record, or io.EOF when there are none left. A blank
// line is not a record and is skipped, which is what encoding/csv does.
func (c *CSVReader) Read() ([]string, error) {
	if err := c.validateComma(); err != nil {
		return nil, err
	}
	for {
		record, err := c.readRecord()
		if err != nil {
			return nil, err
		}
		if record == nil {
			continue
		}
		if err := c.checkFieldCount(record); err != nil {
			return nil, err
		}
		return record, nil
	}
}

// ReadAll returns every remaining record.
func (c *CSVReader) ReadAll() ([][]string, error) {
	var records [][]string
	for {
		record, err := c.Read()
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
}

// checkFieldCount applies the FieldsPerRecord rule, remembering the first
// record's count when the rule is to take it from there.
func (c *CSVReader) checkFieldCount(record []string) error {
	switch {
	case c.FieldsPerRecord < 0:
		return nil
	case c.FieldsPerRecord == 0:
		c.FieldsPerRecord = len(record)
		return nil
	case len(record) != c.FieldsPerRecord:
		return fmt.Errorf("%w: record on line %d: wrong number of fields", ErrCSVSyntax, c.line)
	default:
		return nil
	}
}

// comma is the delimiter as a byte. The zero value reads ','.
//
// The conversion is safe because Read rejects anything at or above utf8.RuneSelf
// before reaching here; see validateComma.
func (c *CSVReader) comma() byte {
	if c.Comma == 0 {
		return ','
	}
	return byte(c.Comma) //nolint:gosec // validateComma has refused a delimiter that does not fit a byte
}

// validateComma refuses a delimiter this reader cannot honor, rather than
// falling back to ',' and splitting the file somewhere the caller did not ask
// for. A silent fallback is the failure this reader exists to stop, one level
// up: the records would come back wrong with nothing to say so.
func (c *CSVReader) validateComma() error {
	switch {
	case c.Comma == 0:
		return nil
	case c.Comma >= utf8.RuneSelf:
		// Fields are found by scanning bytes, so a multi-byte delimiter would
		// match the first byte of a character rather than the character.
		return fmt.Errorf("%w: delimiter %q is not ASCII", ErrCSVSyntax, c.Comma)
	case c.Comma == '"' || c.Comma == '\n' || c.Comma == '\r':
		// These already mean something to a CSV record.
		return fmt.Errorf("%w: delimiter %q is reserved by the format", ErrCSVSyntax, c.Comma)
	default:
		return nil
	}
}

// appendLine reads one line, terminator included, onto buf. It reports whether
// anything was read.
func (c *CSVReader) appendLine() (bool, error) {
	before := len(c.buf)
	for {
		chunk, err := c.r.ReadSlice('\n')
		c.buf = append(c.buf, chunk...)
		switch {
		case err == nil:
			c.line++
			return true, nil
		case errors.Is(err, bufio.ErrBufferFull):
			// A line longer than the buffer arrives in pieces.
			continue
		case errors.Is(err, io.EOF):
			// Whether the read added anything, not whether the record holds
			// anything: a caller looking for the rest of an unclosed quoted field
			// would otherwise be told to keep asking forever.
			if len(c.buf) > before {
				c.line++
				return true, nil
			}
			return false, nil
		default:
			return false, err
		}
	}
}

// readRecord reads one record. It returns a nil record for a blank line, which
// the caller skips.
func (c *CSVReader) readRecord() ([]string, error) {
	c.buf = c.buf[:0]
	got, err := c.appendLine()
	if err != nil {
		return nil, err
	}
	if !got {
		return nil, io.EOF
	}
	if isBlankLine(c.buf) {
		return nil, nil
	}

	c.fields = c.fields[:0]
	c.ends = c.ends[:0]
	pos := 0
	for {
		next, err := c.readField(pos)
		if err != nil {
			return nil, err
		}
		c.ends = append(c.ends, len(c.fields))
		if next < 0 {
			break
		}
		pos = next
	}

	// One string for the record, then a subslice per field. Each subslice shares
	// that string's memory, so the record costs one allocation rather than one
	// per column.
	joined := string(c.fields)
	record := make([]string, len(c.ends))
	start := 0
	for i, end := range c.ends {
		record[i] = joined[start:end]
		start = end
	}
	return record, nil
}

// readField appends the field starting at pos to c.fields and returns the
// position of the next field, or -1 when the record ends here.
func (c *CSVReader) readField(pos int) (int, error) {
	if pos < len(c.buf) && c.buf[pos] == '"' {
		return c.readQuotedField(pos + 1)
	}

	comma := c.comma()
	for i := pos; i < len(c.buf); i++ {
		switch c.buf[i] {
		case comma:
			c.fields = append(c.fields, c.buf[pos:i]...)
			return i + 1, nil
		case '\n':
			c.fields = append(c.fields, c.buf[pos:trimCR(c.buf, i)]...)
			return -1, nil
		case '"':
			return 0, fmt.Errorf("%w: line %d: bare %q in non-quoted field", ErrCSVSyntax, c.line, '"')
		}
	}
	// No terminator: the file ended without one.
	c.fields = append(c.fields, c.buf[pos:]...)
	return -1, nil
}

// readQuotedField reads the rest of a field whose opening quote sits at pos-1.
// Everything up to the closing quote is data, including the delimiter, a line
// feed, and the carriage return before it.
func (c *CSVReader) readQuotedField(pos int) (int, error) {
	comma := c.comma()
	start := pos

	for {
		i := bytes.IndexByte(c.buf[pos:], '"')
		if i < 0 {
			// The quote is not on this line, so the field carries on to the next.
			pos = len(c.buf)
			got, err := c.appendLine()
			if err != nil {
				return 0, err
			}
			if !got {
				return 0, fmt.Errorf("%w: line %d: quoted field is never closed", ErrCSVSyntax, c.line)
			}
			continue
		}
		quote := pos + i

		if quote+1 < len(c.buf) && c.buf[quote+1] == '"' {
			// A doubled quote stands for one: keep the first and skip the second.
			c.fields = append(c.fields, c.buf[start:quote+1]...)
			start = quote + 2
			pos = quote + 2
			continue
		}

		c.fields = append(c.fields, c.buf[start:quote]...)

		switch after := quote + 1; {
		case after >= len(c.buf):
			return -1, nil
		case c.buf[after] == comma:
			return after + 1, nil
		case c.buf[after] == '\n':
			return -1, nil
		case c.buf[after] == '\r' && after+1 < len(c.buf) && c.buf[after+1] == '\n':
			return -1, nil
		default:
			return 0, fmt.Errorf("%w: line %d: %q after a closing quote", ErrCSVSyntax, c.line, c.buf[after])
		}
	}
}

// isBlankLine reports whether line carries no field at all, which is not a
// record.
func isBlankLine(line []byte) bool {
	switch len(line) {
	case 0:
		return true
	case 1:
		return line[0] == '\n'
	case 2:
		return line[0] == '\r' && line[1] == '\n'
	default:
		return false
	}
}

// trimCR is the end of a field terminated by the line feed at i, excluding the
// carriage return of a CRLF terminator.
func trimCR(buf []byte, i int) int {
	if i > 0 && buf[i-1] == '\r' {
		return i - 1
	}
	return i
}
