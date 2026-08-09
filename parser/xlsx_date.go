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
	// The epoch is a workbook-wide setting, so it is asked once. A file written
	// on a Mac before 2016 counts from 1904, and reading every serial against
	// 1900 would put every date in such a file four years and a day early.
	date1904 := false
	if props, err := f.GetWorkbookProps(); err == nil && props.Date1904 != nil {
		date1904 = *props.Date1904
	}

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
			if iso, ok := isoFromSerial(f, sheet, axis, date1904); ok {
				rows[r][c] = iso
			}
		}
	}
	return rows
}

// builtinDateNumberFormats are the number-format IDs whose rendering names a
// calendar day. A cell is a date because of how it is formatted; nothing else in
// the file says so, since the value itself is a serial number.
//
// The time-only formats are deliberately absent. 18 to 21 render a time of day
// and 45 to 47 an elapsed duration — 46 is "[h]:mm:ss", where 1.5 means 36
// hours, not a day and a half after the epoch. Reading either as a calendar
// datetime invents a date the cell never held.
var builtinDateNumberFormats = map[int]struct{}{
	// The English-stable date formats.
	14: {}, 15: {}, 16: {}, 17: {}, 22: {},
	// The East Asian locale date formats, which a workbook written in Japanese,
	// Chinese, or Korean uses for the same thing — 27 is "yyyy年m月". The
	// time-only IDs among them (32 to 35, 55, 56) are left out for the same
	// reason 18 to 21 are.
	27: {}, 28: {}, 29: {}, 30: {}, 31: {}, 36: {},
	50: {}, 51: {}, 52: {}, 53: {}, 54: {}, 57: {}, 58: {},
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

// isDateNumberFormat reports whether a custom number format names a calendar
// day.
//
// A year or a day token is what says so. "m" is not enough on its own, because
// it is both month and minute, and neither "hh:mm" nor "mm:ss" is a date. A
// bracketed hour, minute or second is an elapsed duration — "[h]:mm" of 1.5 is
// 36 hours — so a format holding one is never a date, whatever else it says.
//
// Everything inside quotes is literal text, so a currency format quoting a word
// with a "d" in it is not a date either.
func isDateNumberFormat(format string) bool {
	inQuote := false
	inBracket := false
	elapsed := false
	dated := false
	for i := 0; i < len(format); i++ {
		c := format[i]
		switch {
		case c == '\\' || c == '_' || c == '*':
			// These escape the character after them, which is drawn as itself:
			// "0 \d" ends in a literal "d" and is a number, not a date.
			i++
		case c == '"':
			inQuote = !inQuote
		case inQuote:
		case c == '[':
			inBracket = true
		case c == ']':
			inBracket = false
		case inBracket:
			if c == 'h' || c == 'H' || c == 'm' || c == 'M' || c == 's' || c == 'S' {
				elapsed = true
			}
		case c == 'y' || c == 'Y' || c == 'd' || c == 'D':
			dated = true
		}
	}
	return dated && !elapsed
}

// isoFromSerial reads a cell's stored serial and renders it as ISO 8601. It
// reports false when the cell holds something other than a serial — a date
// stored as text, which is already in whatever form the file gave it, and has no
// serial to convert.
func isoFromSerial(f *excelize.File, sheet, axis string, date1904 bool) (string, bool) {
	raw, err := f.GetCellValue(sheet, axis, excelize.Options{RawCellValue: true})
	if err != nil {
		return "", false
	}
	serial, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return "", false
	}
	at, err := excelize.ExcelDateToTime(serial, date1904)
	if err != nil {
		return "", false
	}
	if at.Hour() == 0 && at.Minute() == 0 && at.Second() == 0 && at.Nanosecond() == 0 {
		return at.Format(time.DateOnly), true
	}
	return at.Format(time.DateTime), true
}
