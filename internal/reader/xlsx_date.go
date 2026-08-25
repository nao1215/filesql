package reader

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
	// A cell is a date because of its style, so a workbook whose style table
	// holds no date format has no date cells and there is nothing here to do.
	// Finding that out is worth doing first because of what the alternative
	// costs: the style table is a part of the file on its own, while asking
	// about one cell's style makes the library build the whole sheet as
	// objects. For an 18.5 MB workbook of 200,000 rows that is 24 MB against
	// 1470 MB, and the streaming row read that produced these rows is 267 MB,
	// so one such question multiplies the cost of the load by six.
	if !definesADateStyle(f) {
		return rows
	}

	// The epoch is a workbook-wide setting, so it is asked once. A file written
	// on a Mac before 2016 counts from 1904, and reading every serial against
	// 1900 would put every date in such a file four years and a day early.
	date1904 := false
	if props, err := f.GetWorkbookProps(); err == nil && props.Date1904 != nil {
		date1904 = *props.Date1904
	}

	// Whether a cell holds a date is decided by its style alone, and a column of
	// dates shares one, so the answer is worked out once per style rather than
	// once per cell: without this a wide sheet costs two random-access lookups
	// into the workbook for every cell it holds, on top of the read that
	// produced these rows.
	dated := make(map[int]bool)

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
			holdsDate, seen := dated[styleID]
			if !seen {
				holdsDate = styleHoldsDate(f, styleID)
				dated[styleID] = holdsDate
			}
			if !holdsDate {
				continue
			}
			if iso, ok := isoFromSerial(f, sheet, axis, date1904); ok {
				rows[r][c] = iso
			}
		}
	}
	return rows
}

// maxCellFormats bounds the walk over the style table. Excel allows 65,490 cell
// formats in a workbook, so a table that has not ended by then is not one this
// can reason about, and the answer falls back to looking at the cells.
const maxCellFormats = 1 << 16

// definesADateStyle reports whether the workbook's style table holds a number
// format that names a calendar day.
//
// It reads the style table alone, which is cheap, and says nothing about which
// cells wear which style. A false answer is conclusive -- no style, no date
// cell -- and a true one only means the cells have to be looked at.
func definesADateStyle(f *excelize.File) bool {
	for id := range maxCellFormats {
		style, err := f.GetStyle(id)
		if err != nil || style == nil {
			return false
		}
		if styleHoldsDate(f, id) {
			return true
		}
	}
	// A table this long is not one to draw a conclusion from.
	return true
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

// styleHoldsDate reports whether a number format makes the cells wearing it
// dates.
//
// The cell's own type is not the question: GetCellType reports the type
// attribute the XML carries, and a date is stored as an ordinary number with no
// attribute at all, so it answers "unset" for exactly the cells this exists to
// find.
func styleHoldsDate(f *excelize.File, styleID int) bool {
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
// bracket holding an elapsed unit is not a date either — "[h]:mm" of 1.5 is 36
// hours — but a bracket may equally hold a color, a condition or a locale, and
// judging it by the letters in it read the "m" of "[Magenta]" and the "h" of
// "[White]" as elapsed units and left a colored date column as text. An elapsed
// unit is one letter repeated and nothing else, which is what tells the two
// apart.
//
// Everything inside quotes is literal text, so a currency format quoting a word
// with a "d" in it is not a date either.
func isDateNumberFormat(format string) bool {
	inQuote := false
	inBracket := false
	elapsed := false
	dated := false
	var bracket []byte
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
			bracket = bracket[:0]
		case c == ']':
			inBracket = false
			if isElapsedUnit(bracket) {
				elapsed = true
			}
		case inBracket:
			bracket = append(bracket, c)
		case c == 'y' || c == 'Y' || c == 'd' || c == 'D':
			dated = true
		}
	}
	return dated && !elapsed
}

// isElapsedUnit reports whether a bracket's content is an elapsed unit rather
// than a color, a condition or a locale. Excel writes an elapsed unit as one of
// h, m or s repeated and nothing else, so "[hh]" is one and "[Magenta]" is a
// color that happens to start with a letter one of them uses.
func isElapsedUnit(bracket []byte) bool {
	if len(bracket) == 0 {
		return false
	}
	first := lowerASCII(bracket[0])
	if first != 'h' && first != 'm' && first != 's' {
		return false
	}
	for _, c := range bracket[1:] {
		if lowerASCII(c) != first {
			return false
		}
	}
	return true
}

// lowerASCII is c in lower case, for the ASCII letters a number format is
// written with.
func lowerASCII(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
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
	var ok bool
	serial, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return "", false
	}
	if !date1904 {
		serial, ok = serialIn1900System(serial)
		if !ok {
			return "", false
		}
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

// serialIn1900System is serial adjusted for the calendar the 1900 date system
// actually keeps, and whether it names a day at all.
//
// Serial 1 is January 1, 1900 and serial 60 is a February 29, 1900 that no
// calendar has, kept so files count days the way Lotus 1-2-3 did. Counting
// plain days from 1899-12-30, which is what the conversion below this does, is
// therefore right from serial 61 on and a day early before it. A serial below 1
// is before the system starts, and the phantom day is not a day, so neither
// converts: the cell keeps the text the workbook shows, for the reason an
// elapsed duration does.
//
// This corrects the conversion rather than the file. It can go away if
// excelize.ExcelDateToTime ever agrees with excelize's own rendering of the same
// cell, which is to say when ExcelDateToTime(1, false) returns 1900-01-01.
func serialIn1900System(serial float64) (float64, bool) {
	switch {
	case serial < 1:
		return 0, false
	case serial < 60:
		return serial + 1, true
	case serial < 61:
		return 0, false
	default:
		return serial, true
	}
}
