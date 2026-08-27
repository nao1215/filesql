package reader

import (
	"errors"
	"io"
	"strings"
)

// The delimiters the two delimited formats are separated by.
const (
	csvDelimiter = ','
	tsvDelimiter = '\t'
)

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
func newRecordReader(src io.Reader, format Format) recordReader {
	normalized := NormalizeLineEndings(src, format)
	if format == FormatTSV {
		return NewTSVReader(normalized)
	}

	csvReader := NewCSVReader(normalized)
	csvReader.Comma = delimiterOf(format)
	// Accept a variable field count so a ragged record reaches Reconcile, which
	// is where the caller's policy for one lives, instead of aborting the read.
	csvReader.FieldsPerRecord = -1
	return csvReader
}

// readDelimited reads CSV or TSV rows in chunks.
func readDelimited(src io.Reader, format Format, opts Options, emit Emit) (Result, error) {
	records := newRecordReader(src, format)

	headerRecord, err := records.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Result{}, emptyError("empty %s data", format)
		}
		return Result{}, parseError(err, "failed to read %s header", format)
	}
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
