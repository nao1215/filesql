package parser

import (
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

// NormalizeXLSXDates rewrites the date cells of a sheet's rows into ISO 8601,
// leaving every other cell as GetRows rendered it.
//
// A workbook stores a date as a serial number and a number format, and GetRows
// applies the format: the same day arrives as "03-15-23", "2023-03-15", or
// "Mar 15" depending on how the sheet was formatted, and none of the first two
// forms sorts chronologically. A column of dates became a column of
// format-dependent text, so ORDER BY sorted it lexically and a comparison
// against an ISO literal never matched.
//
// ISO 8601 is the form the datetime inference already recognizes, which is what
// makes the column a datetime rather than text. A cell whose time of day is
// midnight is written as a plain date, because that is what a date cell holds;
// anything else keeps its time.
//
// Only cells the workbook itself calls dates are touched. A number formatted as
// a number, and text that merely looks like a date, are left alone.
func NormalizeXLSXDates(f *excelize.File, sheet string, rows [][]string) [][]string {
	for r, row := range rows {
		for c := range row {
			if row[c] == "" {
				continue
			}
			axis, err := excelize.CoordinatesToCellName(c+1, r+1)
			if err != nil {
				continue
			}
			if !cellHoldsDate(f, sheet, axis) {
				continue
			}
			if iso, ok := isoFromSerial(f, sheet, axis); ok {
				rows[r][c] = iso
			}
		}
	}
	return rows
}

// builtinDateNumberFormats are the number-format IDs a workbook reserves for
// dates and times. A cell is a date because of how it is formatted; nothing else
// in the file says so, since the value itself is a serial number.
var builtinDateNumberFormats = map[int]struct{}{
	14: {}, 15: {}, 16: {}, 17: {}, 18: {}, 19: {}, 20: {}, 21: {}, 22: {},
	45: {}, 46: {}, 47: {},
}

// cellHoldsDate reports whether a cell's number format makes it a date.
//
// GetCellType is not the question: it reports the type attribute the XML
// carries, and a date is stored as an ordinary number with no attribute at all,
// so it answers "unset" for exactly the cells this exists to find.
func cellHoldsDate(f *excelize.File, sheet, axis string) bool {
	styleID, err := f.GetCellStyle(sheet, axis)
	if err != nil {
		return false
	}
	style, err := f.GetStyle(styleID)
	if err != nil || style == nil {
		return false
	}
	if _, ok := builtinDateNumberFormats[style.NumFmt]; ok {
		return true
	}
	return style.CustomNumFmt != nil && isDateNumberFormat(*style.CustomNumFmt)
}

// isDateNumberFormat reports whether a custom number format renders a date or a
// time. The format language spells those with y, m, d, h and s; everything
// inside quotes or brackets is literal text or a condition, so it is skipped —
// a currency format quoting the word "days" is not a date.
func isDateNumberFormat(format string) bool {
	inQuote := false
	inBracket := false
	for i := range len(format) {
		switch c := format[i]; {
		case c == '"':
			inQuote = !inQuote
		case c == '[':
			inBracket = true
		case c == ']':
			inBracket = false
		case inQuote || inBracket:
		case c == 'y' || c == 'Y' || c == 'd' || c == 'D' || c == 'h' || c == 'H' || c == 's' || c == 'S':
			return true
		case c == 'm' || c == 'M':
			// "m" is both month and minute, and either makes this a date format.
			return true
		}
	}
	return false
}

// isoFromSerial reads a cell's stored serial and renders it as ISO 8601. It
// reports false when the cell holds something other than a serial — a date
// stored as text, which is already in whatever form the file gave it, and has no
// serial to convert.
func isoFromSerial(f *excelize.File, sheet, axis string) (string, bool) {
	raw, err := f.GetCellValue(sheet, axis, excelize.Options{RawCellValue: true})
	if err != nil {
		return "", false
	}
	serial, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return "", false
	}
	// The 1904 epoch is a workbook-wide setting; excelize resolves it when it
	// renders, and reading it here would mean parsing the workbook properties
	// for every cell. The 1900 epoch is what a file written this century uses.
	at, err := excelize.ExcelDateToTime(serial, false)
	if err != nil {
		return "", false
	}
	if at.Hour() == 0 && at.Minute() == 0 && at.Second() == 0 && at.Nanosecond() == 0 {
		return at.Format(time.DateOnly), true
	}
	return at.Format(time.DateTime), true
}
