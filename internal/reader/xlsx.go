package reader

import (
	"bytes"
	"io"

	"github.com/xuri/excelize/v2"
)

// Workbook is an open Excel workbook.
//
// A workbook holds several sheets and a table holds one, so opening and reading
// are separate: a caller that maps every sheet onto its own table opens once and
// reads each sheet, and one that wants a single table reads the first sheet its
// policy admits.
type Workbook struct {
	file *excelize.File
	// data is the workbook as it arrived. It is what lets the date
	// normalization read the sheet's own XML instead of asking the library
	// about one cell at a time, which is the difference between 422 MB and
	// 1939 MB on an 18.5 MB workbook of 200,000 rows.
	data []byte
}

// OpenWorkbook reads a workbook from src.
//
// The bytes are buffered whole because the format is a zip archive that is read
// by random access, which a stream cannot provide.
func OpenWorkbook(src io.Reader) (*Workbook, error) {
	data, err := io.ReadAll(src)
	if err != nil {
		return nil, parseError(err, "failed to read XLSX data")
	}
	if len(data) == 0 {
		return nil, emptyError("empty XLSX file")
	}
	file, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		return nil, parseError(err, "failed to open XLSX file")
	}
	return &Workbook{file: file, data: data}, nil
}

// Close releases the workbook.
func (w *Workbook) Close() error {
	return w.file.Close()
}

// Select returns the sheets policy admits, in workbook order, and the names it
// left out.
func (w *Workbook) Select(policy ExcelSheetPolicy) (loaded, skipped []string, err error) {
	return SelectExcelSheets(w.file, policy)
}

// Source is the workbook as sheet selection sees it, for a caller that reports
// on the sheets rather than reading them.
func (w *Workbook) Source() ExcelSheetSource {
	return w.file
}

// NoExcelSheetsError explains a workbook that contributed nothing. It separates
// one with no sheets at all from one whose sheets were all left out by the
// policy, because the two need different things done about them: the first file
// is broken, the second is a setting the caller chose.
func NoExcelSheetsError(f ExcelSheetSource, policy ExcelSheetPolicy) error {
	if policy == ExcelSheetPolicyVisibleOnly && len(f.GetSheetList()) > 0 {
		return emptyError("no visible sheets found in XLSX file")
	}
	return emptyError("no sheets found in XLSX file")
}

// ReadSheet reads one sheet of the workbook in chunks.
//
// A sheet holding nothing a table can be made from -- no rows, or no row that
// names any column -- is reported with Kind KindEmpty, so a caller loading every
// sheet can pass over it while one loading a single sheet can refuse.
func (w *Workbook) ReadSheet(name string, opts Options, emit Emit) (Result, error) {
	rows, err := w.file.GetRows(name)
	if err != nil {
		return Result{}, parseError(err, "failed to read sheet %s", name)
	}
	// A workbook stores a date as a serial number and a number format, so the
	// cells that hold one are rewritten into the ISO 8601 the datetime inference
	// recognizes. Without this a column of dates is text in whatever shape the
	// sheet was formatted, and ORDER BY sorts it lexically.
	rows = w.normalizeDates(name, rows)

	if len(rows) == 0 {
		return Result{}, emptyError("empty XLSX sheet")
	}

	// A sheet may begin with rows holding no cell at all, which name no column.
	for len(rows) > 0 && len(rows[0]) == 0 {
		rows = rows[1:]
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return Result{}, emptyError("no headers found in XLSX")
	}

	header := rows[0]
	if err := ValidateColumnNames(header); err != nil {
		return Result{}, err
	}

	result := Result{Header: header}
	records := newChunker(header, opts, emit)
	for i, row := range rows[1:] {
		// A row holding no cell at all is not a record, the way a blank line is
		// not one in any other format read here. It is also what a sheet is made
		// of between its last written row and a stray cell further down: those
		// rows arrive by the million from a file of a few kilobytes, and padding
		// each one to the header's width was the whole cost of loading such a
		// workbook. A row whose cells are present and empty is a different thing
		// and stays a record, the way a CSV line reading "," is one.
		if len(row) == 0 {
			continue
		}
		// A workbook stores no cell for a trailing empty one, so a row ending in
		// blanks arrives short and means what the padding says. More cells than
		// the header has means the opposite -- there is data in a column the
		// header does not name -- and truncating it dropped that data with no
		// error and no count to say it had happened.
		if len(row) > len(header) {
			return Result{}, parseError(nil, "row %d has %d cells where the header has %d", i+2, len(row), len(header))
		}
		record := make([]string, len(header))
		copy(record, row)

		result.Total++
		if err := records.add(record); err != nil {
			return Result{}, err
		}
	}

	if err := records.finish(); err != nil {
		return Result{}, err
	}
	result.Rows = records.rows
	result.Types = records.types()
	return result, nil
}

// readXLSX reads the first sheet the policy admits, which is the one table a
// caller with no way to name a sheet can mean.
func readXLSX(src io.Reader, opts Options, emit Emit) (result Result, err error) {
	workbook, err := OpenWorkbook(src)
	if err != nil {
		return Result{}, err
	}
	defer func() {
		if closeErr := workbook.Close(); closeErr != nil && err == nil {
			err = parseError(closeErr, "failed to close XLSX")
		}
	}()

	sheets, _, err := workbook.Select(opts.ExcelSheetPolicy)
	if err != nil {
		return Result{}, err
	}
	if len(sheets) == 0 {
		return Result{}, NoExcelSheetsError(workbook.Source(), opts.ExcelSheetPolicy)
	}
	return workbook.ReadSheet(sheets[0], opts, emit)
}

// normalizeDates rewrites a sheet's date cells into ISO 8601, reading the
// sheet's own XML for the styles rather than asking the library cell by cell.
//
// The two ways agree on which cells are dates and on what they become; they
// differ in what finding out costs. Asking the library makes it unmarshal the
// whole worksheet into its object model, which for an 18.5 MB workbook of
// 200,000 rows is 1470 MB on top of the 267 MB the rows themselves cost.
// Reading the XML costs what the date cells cost. The asking way remains for
// the exported NormalizeXLSXDates, which is handed an open workbook and no
// bytes, and it is also what this falls back to when the workbook's parts do
// not say where the sheet is -- a sheet missing its dates would be worse than
// a slow one.
func (w *Workbook) normalizeDates(sheet string, rows [][]string) [][]string {
	dateStyles, complete := dateStyleIDs(w.file)
	if complete && len(dateStyles) == 0 {
		return rows
	}
	if len(w.data) == 0 || !complete {
		return NormalizeXLSXDates(w.file, sheet, rows)
	}

	// A file written on a Mac before 2016 counts from 1904, and reading every
	// serial against 1900 would put every date in such a file four years and a
	// day early.
	date1904 := false
	if props, err := w.file.GetWorkbookProps(); err == nil && props.Date1904 != nil {
		date1904 = *props.Date1904
	}

	dates, ok := dateCellsFromXML(w.data, sheet, dateStyles, date1904)
	if !ok {
		return NormalizeXLSXDates(w.file, sheet, rows)
	}
	for cell, iso := range dates {
		r, c := cell.row-1, cell.col-1
		if r < 0 || r >= len(rows) || c < 0 || c >= len(rows[r]) {
			continue
		}
		rows[r][c] = iso
	}
	return rows
}
