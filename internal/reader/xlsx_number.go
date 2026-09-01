package reader

import (
	"slices"

	"github.com/xuri/excelize/v2"
)

// normalizeXLSXNumbers rewrites the numeric cells of a sheet's rows into the
// numbers the file stores, leaving every other cell as GetRows rendered it.
//
// A sheet draws a number the way its format says, and the drawing is not the
// value. A number of more than fifteen significant digits is drawn rounded to
// fifteen, so an 18-digit order number came back as 1.23456789012346E+18 and two
// such numbers that differ by one loaded as one value; a whole-number format
// draws 1234.5 as 1235 and a scientific one draws it as 1.23E+03, which reads
// back as 1230; a percentage of 0.5 is drawn "50%", a thousands-separated amount
// "1,235" and a fraction "1234 1/2", none of which is a number at all, so the
// column came back as text and SUM over it answered 0. A format is how a
// spreadsheet paints a number, and the number is what a query is about.
//
// Two kinds of cell keep what the sheet says. One is a cell that does not store
// a number: a boolean is stored as 1 and drawn TRUE, and a string is text
// whatever its format says. The other is a number whose format draws a moment
// rather than a quantity: a time of day, an elapsed duration, and a date, which
// is rewritten into ISO 8601 by the pass that follows rather than left as the
// serial behind it.
//
// This is the reading that asks the library for each cell's style. The loader
// reads the sheet's own XML instead, which answers the same and costs less; the
// two are held to that in the reader's tests.
func normalizeXLSXNumbers(f *excelize.File, sheet string, rows [][]string) [][]string {
	if !mayRedrawANumber(f, rows) {
		return rows
	}
	// A column of one kind of number shares a style, so what a style draws is
	// worked out once per style rather than once per cell.
	redraws := make(map[int]bool)
	for r, row := range rows {
		for c := range row {
			if row[c] == "" {
				continue
			}
			axis, err := excelize.CoordinatesToCellName(c+1, r+1)
			if err != nil {
				continue
			}
			styleID, err := f.GetCellStyle(sheet, axis)
			if err != nil {
				continue
			}
			moment, seen := redraws[styleID]
			if !seen {
				moment = styleHoldsDate(f, styleID) || styleDrawsAClock(f, styleID)
				redraws[styleID] = moment
			}
			if moment || !storesASerial(f, sheet, axis) {
				continue
			}
			raw, err := f.GetCellValue(sheet, axis, excelize.Options{RawCellValue: true})
			if err != nil || !isPlainNumber(raw) {
				continue
			}
			rows[r][c] = raw
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
	return anyDrawnShort(rows) || workbookFormatsNumbers(f)
}

// anyDrawnShort reports whether any drawing carries the mark of a number that
// did not fit fifteen significant digits, which a sheet with no number format
// at all is the case for.
func anyDrawnShort(rows [][]string) bool {
	for _, row := range rows {
		if slices.ContainsFunc(row, drawnShort) {
			return true
		}
	}
	return false
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
