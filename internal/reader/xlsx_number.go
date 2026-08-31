package reader

import (
	"slices"

	"github.com/xuri/excelize/v2"
)

// NormalizeXLSXNumbers rewrites the cells a sheet draws as a plain number into
// the number the file stores, leaving every other cell as GetRows rendered it.
//
// A sheet draws a number the way its format says, and the drawing is not the
// value: a number of more than fifteen significant digits is drawn rounded to
// fifteen, so an 18-digit order number came back as 1.23456789012346E+18 and two
// such numbers that differ by one loaded as one value; a whole-number format
// draws 1234.5 as 1235; a scientific format draws it as 1.23E+03, which reads
// back as 1230. The digits are in the file either way -- the cell's stored text
// is what excelize answers with RawCellValue -- so the value is taken from
// there.
//
// Only a cell whose drawing is itself a plain number is rewritten. A percentage,
// a thousands-separated amount, an accounting figure, a fraction, a boolean and
// a date are drawn as something a reader means to see, and the stored number
// behind one says nothing on its own, so those keep their drawing. A date is the
// exception that is handled elsewhere, by rewriting it into ISO 8601.
func NormalizeXLSXNumbers(f *excelize.File, sheet string, rows [][]string) [][]string {
	if !mayRedrawANumber(f, rows) {
		return rows
	}
	// The stored text is read a row at a time rather than as a second sheet, so
	// what the pass costs is the reading and not another copy of the sheet.
	stored, err := f.Rows(sheet)
	if err != nil {
		// The drawing is what there is, which is what this returned before.
		return rows
	}
	defer func() { _ = stored.Close() }()
	for r := range rows {
		if !stored.Next() {
			break
		}
		raw, err := stored.Columns(excelize.Options{RawCellValue: true})
		if err != nil {
			return rows
		}
		for c := range rows[r] {
			if c >= len(raw) {
				break
			}
			if rows[r][c] == raw[c] || !isPlainNumber(rows[r][c]) || !isPlainNumber(raw[c]) {
				continue
			}
			rows[r][c] = raw[c]
		}
	}
	return rows
}

// mayRedrawANumber reports whether a sheet could hold a cell whose drawing is
// not the number behind it. Reading the stored text costs a second pass over the
// sheet, so it is worth knowing that there is something to find first.
//
// Two things make a drawing differ: a number format, which the workbook's style
// table names, and a number of more than fifteen significant digits, which the
// drawing itself carries the mark of. The style table is a part of the file on
// its own and cheap to read; the mark is found in the strings already in hand.
func mayRedrawANumber(f *excelize.File, rows [][]string) bool {
	for _, row := range rows {
		if slices.ContainsFunc(row, drawnShort) {
			return true
		}
	}
	return workbookFormatsNumbers(f)
}

// drawnShort reports whether a drawing carries the mark of a number that did
// not fit fifteen significant digits: an exponent, or that many digits.
func drawnShort(cell string) bool {
	digits := 0
	for i := range len(cell) {
		switch c := cell[i]; {
		case c >= '0' && c <= '9':
			digits++
		case c == 'e' || c == 'E':
			return digits > 0
		}
	}
	return digits >= 15
}

// workbookFormatsNumbers reports whether the workbook defines a format that
// could draw a number as a different one. It reads the style table alone, the
// way the date normalization does, and answers true for a table too long to
// walk, since that is no longer a question this can settle cheaply.
//
// A date format is not one of them: it draws a calendar day, which is not a
// plain number and so is never rewritten here, and the cells wearing one are
// rewritten into ISO 8601 by the pass that follows. Counting it would make every
// workbook with a date column read its sheets twice for nothing.
func workbookFormatsNumbers(f *excelize.File) bool {
	for id := range maxCellFormats {
		style, err := f.GetStyle(id)
		if err != nil || style == nil {
			return false
		}
		if style.NumFmt == 0 && style.CustomNumFmt == nil {
			continue
		}
		if !styleHoldsDate(f, id) {
			return true
		}
	}
	return true
}

// isPlainNumber reports whether text spells a number the way a sheet draws one
// with no format: an optional sign, digits with at most one point, and an
// optional exponent. The forms Go's parser also takes -- an infinity, a NaN, a
// hexadecimal float, digits split by underscores -- are not what a sheet draws,
// and taking them for numbers would rewrite a text cell that merely reads like
// one.
func isPlainNumber(text string) bool {
	if text == "" {
		return false
	}
	i := 0
	if text[i] == '+' || text[i] == '-' {
		i++
	}
	digits, points := 0, 0
	for ; i < len(text); i++ {
		c := text[i]
		switch {
		case c >= '0' && c <= '9':
			digits++
		case c == '.':
			points++
			if points > 1 {
				return false
			}
		case c == 'e' || c == 'E':
			return digits > 0 && isExponent(text[i+1:])
		default:
			return false
		}
	}
	return digits > 0
}

// isExponent reports whether text is what follows the e of a number: an
// optional sign and at least one digit.
func isExponent(text string) bool {
	if text == "" {
		return false
	}
	i := 0
	if text[i] == '+' || text[i] == '-' {
		i++
	}
	if i == len(text) {
		return false
	}
	for ; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return false
		}
	}
	return true
}
