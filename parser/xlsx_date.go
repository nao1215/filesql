package parser

import (
	"github.com/nao1215/filesql/internal/reader"
	"github.com/xuri/excelize/v2"
)

// NormalizeXLSXDates rewrites the date cells of a sheet's rows into ISO 8601,
// leaving every other cell as GetRows rendered it.
//
// A workbook stores a date as a serial number and a number format, and GetRows
// applies the format: the same day arrives as "03-15-23", "2023-03-15", or
// "Mar 15" depending on how the sheet was formatted, and none of the first two
// forms sorts chronologically. ISO 8601 is the form the datetime inference
// recognizes, which is what makes the column a datetime rather than text.
//
// Only cells the workbook itself calls dates are touched. A number formatted as
// a number, and text that merely looks like a date, are left alone.
func NormalizeXLSXDates(f *excelize.File, sheet string, rows [][]string) [][]string {
	return reader.NormalizeXLSXDates(f, sheet, rows)
}
