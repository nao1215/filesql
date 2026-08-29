package reader

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

// The delimiters the two delimited formats are separated by.
const (
	csvDelimiter = ','
	tsvDelimiter = '\t'
)

// maxRecordSize bounds one record of a delimited file.
//
// A record is held whole while it is split into fields, so its size is what the
// read costs however the rows are chunked. That is the file's own business when
// the source is a file: it cannot be longer than itself. A source that is a
// stream has no such size, and a record with no terminator asks for everything
// the sender chooses to send, so the bound is what stops a body arriving as one
// record from costing the whole of it. Both readers hold to it, because it is
// one rule about one danger and two copies of it drifted once already.
const maxRecordSize = 64 << 20

// recordLimitOf is the largest record a read holds, which is maxRecordSize
// unless the caller lowered it.
func recordLimitOf(opts Options) int {
	if opts.maxRecord > 0 {
		return opts.maxRecord
	}
	return maxRecordSize
}

// ErrRecordTooLong reports a record past maxRecordSize. It is separate from the
// syntax errors because the file may be perfectly well formed and simply larger
// than this reader will hold.
var ErrRecordTooLong = errors.New("record too long")

// recordTooLongError words the refusal, naming the line the record starts on and
// the limit it passed, which are what a caller holding the file can act on.
func recordTooLongError(line, limit int) error {
	return fmt.Errorf("%w: line %d is longer than the %s a record may be", ErrRecordTooLong, line, byteSize(limit))
}

// elementTooLongError words the refusal for a record that is not a line, naming
// which element of the array passed the limit.
func elementTooLongError(index, limit int) error {
	return fmt.Errorf("%w: element %d is longer than the %s a record may be", ErrRecordTooLong, index, byteSize(limit))
}

// lineBoundedReader refuses a source that sends one line longer than the bound,
// counting from one terminator to the next.
//
// It is for the formats that are read whole. A file cannot be longer than
// itself, so reading one whole costs what it costs; a stream has no such size,
// and a record with no terminator asks for everything the sender chooses to
// send. Bounding the file would refuse a large one that is perfectly ordinary,
// so what is bounded is the record, which is the same rule the delimited readers
// hold to and the same refusal.
type lineBoundedReader struct {
	src   io.Reader
	limit int
	// run is the bytes read since the last terminator, and line is the number
	// of the record they belong to.
	run  int
	line int
}

// BoundRecords wraps src so one record past the bound every reader here holds
// to is refused. It is for a format read by a library of its own, which has no
// bound of its own to hold to.
func BoundRecords(src io.Reader) io.Reader {
	return newLineBoundedReader(src, maxRecordSize)
}

// newLineBoundedReader wraps src so one line past limit is refused.
func newLineBoundedReader(src io.Reader, limit int) *lineBoundedReader {
	return &lineBoundedReader{src: src, limit: limit, line: 1}
}

// Read implements io.Reader. The bytes read before the refusal are returned with
// it, the way bufio does, so nothing already handed over is lost on the way to
// the error.
func (r *lineBoundedReader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	// The record is measured as the bytes go past rather than after they have,
	// because a whole record and its terminator can arrive in one read: the
	// terminator would reset the count and the record would be accepted having
	// never been measured.
	long := 0
	for _, c := range p[:n] {
		if c == '\n' {
			r.run = 0
			r.line++
			continue
		}
		r.run++
		if r.run > r.limit && long == 0 {
			long = r.line
		}
	}
	if long != 0 {
		return n, recordTooLongError(long, r.limit)
	}
	return n, err
}

// byteSize spells a limit the way it is written down, so a message quotes the
// constant rather than its expansion.
func byteSize(n int) string {
	switch {
	case n >= 1<<20 && n%(1<<20) == 0:
		return fmt.Sprintf("%d MiB", n>>20)
	case n >= 1<<10 && n%(1<<10) == 0:
		return fmt.Sprintf("%d KiB", n>>10)
	default:
		return fmt.Sprintf("%d bytes", n)
	}
}

// recordReader reads the records of a delimited file. CSV and TSV need
// different readers because the formats differ on what a quote means: CSV
// escapes with it, TSV has no escape at all.
type recordReader interface {
	Read() ([]string, error)
}

// delimiterOf is the byte that separates one field from the next in a delimited
// format. It is what the format is, so the two travel together rather than
// being passed side by side and able to disagree.
func delimiterOf(format Format) rune {
	if format == FormatTSV {
		return tsvDelimiter
	}
	return csvDelimiter
}

// newRecordReader picks the reader the format names, over line endings
// normalized as that format spells them, so a carriage-return terminated file
// is read as lines.
func newRecordReader(src io.Reader, format Format, limit int) recordReader {
	normalized := NormalizeLineEndings(src, format)
	if format == FormatTSV {
		tsvReader := NewTSVReader(normalized)
		tsvReader.maxRecord = limit
		return tsvReader
	}

	csvReader := NewCSVReader(normalized)
	csvReader.maxRecord = limit
	csvReader.Comma = delimiterOf(format)
	// Accept a variable field count so a ragged record reaches Reconcile, which
	// is where the caller's policy for one lives, instead of aborting the read.
	csvReader.FieldsPerRecord = -1
	return csvReader
}

// readDelimited reads CSV or TSV rows in chunks.
func readDelimited(src io.Reader, format Format, opts Options, emit Emit) (Result, error) {
	records := newRecordReader(src, format, recordLimitOf(opts))

	headerRecord, err := records.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Result{}, emptyError("empty %s data", format)
		}
		return Result{}, parseError(err, "failed to read %s header", format)
	}
	headerRecord = NameBlankColumns(headerRecord)
	if err := ValidateColumnNames(headerRecord); err != nil {
		return Result{}, err
	}

	header := headerRecord
	result := Result{Header: header}
	rows := newChunker(header, opts, emit)
	rowNum := 0
	for {
		record, err := records.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return Result{}, parseError(err, "failed to read %s record", format)
		}
		rowNum++
		result.Total++

		if opts.Reconcile != nil && len(record) != len(header) {
			var skip bool
			record, skip, err = opts.Reconcile(record, len(header), rowNum)
			if err != nil {
				return Result{}, err
			}
			if skip {
				result.Skipped++
				continue
			}
		}

		if err := rows.add(record); err != nil {
			return Result{}, err
		}
	}

	if err := rows.finish(); err != nil {
		return Result{}, err
	}
	result.Rows = rows.rows
	result.Types = rows.types()
	return result, nil
}

// NameBlankColumns gives a name to every column whose header cell is empty.
//
// A blank header names nothing, so there is no name typed twice in a header
// with two of them -- but two empty names are equal, and the duplicate check
// refused the file with a message about a name it never wrote. A spreadsheet
// exported with spacer columns, and a CSV whose header ends in two commas, both
// arrive that way.
//
// The name is the column's position, which is distinct on its own and is
// something the caller can write in a query. It is checked against the names the
// file did write, so a header saying both "column_3" and nothing in the third
// position does not end up with two columns of one name.
func NameBlankColumns(columns []string) []string {
	blank := false
	for _, column := range columns {
		if column == "" {
			blank = true
			break
		}
	}
	if !blank {
		return columns
	}
	taken := make(map[string]bool, len(columns))
	for _, column := range columns {
		if column != "" {
			taken[LTSVLabelKey(column)] = true
		}
	}
	named := make([]string, len(columns))
	copy(named, columns)
	for i, column := range columns {
		if column != "" {
			continue
		}
		name := blankColumnName(i+1, taken)
		named[i] = name
		taken[LTSVLabelKey(name)] = true
	}
	return named
}

// blankColumnName is the name a blank header at a position takes, moved along
// until it is one no other column has.
func blankColumnName(position int, taken map[string]bool) string {
	name := fmt.Sprintf("column_%d", position)
	for attempt := 2; taken[LTSVLabelKey(name)]; attempt++ {
		name = fmt.Sprintf("column_%d_%d", position, attempt)
	}
	return name
}

// ValidateColumnNames reports a header that names one column twice.
//
// The message quotes the name and gives its 1-based position, because a header
// can duplicate the empty name -- two unnamed columns -- and an unquoted empty
// name printed nothing at all after the colon.
//
// Two names are the same column if either comparison says so, and the two are
// kept apart rather than combined into one key. Whitespace is filesql's own
// rule -- " name " and "name" are one name typed twice -- while case is
// SQLite's, because SQLite is what ends up holding the columns. Folding a
// trimmed name would apply both at once and refuse " A" beside "a", which
// neither rule refuses on its own and which SQLite keeps as two columns.
func ValidateColumnNames(columns []string) error {
	trimmed := make(map[string]bool, len(columns))
	folded := make(map[string]bool, len(columns))
	for i, col := range columns {
		trimmedName := strings.TrimSpace(col)
		foldedName := ASCIIFold(col)
		if trimmed[trimmedName] || folded[foldedName] {
			return duplicateColumnError("%q (column %d)", col, i+1)
		}
		trimmed[trimmedName] = true
		folded[foldedName] = true
	}
	return nil
}

// LTSVLabelKey is how two LTSV labels are compared for being one column.
//
// LTSV carries its labels on every record rather than in a header, so the
// duplicate check runs per record and had its own comparison, which was exact.
// A record holding "A:1\ta:2" therefore reached SQLite, which folds ASCII case,
// and failed as a raw CREATE TABLE error with no duplicate-column sentinel to
// match -- the outcome the check exists to replace, left in the one format
// whose labels do not go through ValidateColumnNames.
func LTSVLabelKey(label string) string {
	return ASCIIFold(strings.TrimSpace(label))
}

// ASCIIFold lowercases the ASCII letters in s and leaves every other byte as it
// is, which is how SQLite compares two column names: its default case folding
// stops at ASCII, so "ä" and "Ä" stay two names. Folding with strings.ToLower
// would make them one and refuse a header SQLite accepts.
//
// Case has to be folded somewhere, because leaving it out did not make "ID" and
// "id" two columns: it moved the refusal to SQLite, which reported it as a
// failed CREATE TABLE wrapped in a database-operation error -- no sentinel to
// match and no column position, which is the outcome this check exists to
// replace.
func ASCIIFold(s string) string {
	var folded []byte
	for i := range len(s) {
		c := s[i]
		if c < 'A' || c > 'Z' {
			continue
		}
		if folded == nil {
			folded = []byte(s)
		}
		folded[i] = c + ('a' - 'A')
	}
	if folded == nil {
		return s
	}
	return string(folded)
}
