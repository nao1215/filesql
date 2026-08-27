package filesql

import (
	"database/sql"
	"fmt"
	"io"

	"github.com/nao1215/filesql/internal/infer"
	"github.com/nao1215/filesql/internal/reader"
	"github.com/xuri/excelize/v2"
)

// xlsxSheet is one sheet of a workbook being written. Rows are opened when the
// sheet is reached rather than up front, because a *sql.Rows holds a cursor and
// only one can be read at a time.
type xlsxSheet struct {
	// name is the sheet name, already adapted to what Excel accepts.
	name string
	// open yields the sheet's columns and rows.
	open func() ([]string, *sql.Rows, error)
}

// writeXLSXTableData writes SQLite table data to Excel XLSX format as a
// single-sheet workbook named after the table.
func writeXLSXTableData(w io.Writer, tableName string, columns []string, rows *sql.Rows) error {
	// The sheet name is what a reader turns back into a table name, so it comes
	// from the table this dump is of, adapted to what Excel accepts.
	return writeXLSXWorkbook(w, []xlsxSheet{{
		name: excelSheetName(tableName),
		open: func() ([]string, *sql.Rows, error) { return columns, rows, nil },
	}})
}

// writeXLSXWorkbook writes sheets as one workbook. A workbook overwritten in
// place goes through here with every one of its sheets, so a file of several
// sheets comes back whole rather than being refused or flattened to one.
func writeXLSXWorkbook(w io.Writer, sheets []xlsxSheet) error {
	return writeXLSXWorkbookOnto(w, nil, sheets)
}

// writeXLSXWorkbookOnto writes sheets into base, or into a new workbook when
// base is nil.
//
// A save that replaces a workbook writes onto the workbook it is replacing, so
// what this package does not hold survives the save: a sheet the sheet policy
// chose not to load used to be deleted from the caller's file, and a column
// width, a merged range and a comment were gone from the sheets it did load.
// Only the rows of a sheet a table was loaded from are rewritten, since those
// rows are what the table is.
func writeXLSXWorkbookOnto(w io.Writer, base *excelize.File, sheets []xlsxSheet) error {
	if len(sheets) == 0 {
		return fmt.Errorf("%w: no sheets to write", ErrEmptyData)
	}

	f := base
	if f == nil {
		f = excelize.NewFile()
	}
	defer func() {
		_ = f.Close() // Ignore close error
	}()

	for _, sheet := range sheets {
		had, err := xlsxSheetBefore(f, sheet.name, base != nil)
		if err != nil {
			return err
		}
		written, err := writeXLSXSheet(f, sheet, had.values)
		if err != nil {
			return err
		}
		if err := trimXLSXSheet(f, sheet.name, had.extent, written); err != nil {
			return err
		}
	}

	// excelize starts a workbook with a default sheet. It is only ours to remove
	// once a sheet of our own exists, and not at all if a sheet reused its name,
	// and never when the workbook came from the caller rather than from here.
	if _, err := f.GetSheetIndex(defaultSheetName); err == nil && base == nil {
		hasOwn := false
		for _, sheet := range sheets {
			if sheet.name == defaultSheetName {
				hasOwn = true
				break
			}
		}
		if !hasOwn {
			if err := f.DeleteSheet(defaultSheetName); err != nil {
				return fmt.Errorf("failed to delete default sheet: %w", err)
			}
		}
	}

	// Why Write and not SaveAs: SaveAs picks the container format from the file
	// extension, and the caller stages the write, so the only name available here
	// carries a temporary suffix that Excel rejects. Any compression the caller
	// asked for is already wrapped around w.
	if err := f.Write(w); err != nil {
		return fmt.Errorf("%w: failed to write Excel file: %w", ErrIOOperation, err)
	}

	return nil
}

// unchangedXLSXCell reports whether the cell at row and column, both counted
// from one, already reads as value in the sheet a save is writing onto.
//
// A cell holds more than the text a table can carry. It may hold a formula and
// the value that formula last evaluated to, and it may hold a date as a serial
// number with a format that renders it. Writing the loaded text back over such
// a cell says nothing the cell did not already say and takes the rest with it:
// the formula became an empty cell, so the workbook lost the rule that produced
// its numbers, and the date became text, so the column no longer sorted or
// calculated as dates. Leaving the cell alone keeps both. A cell whose value did
// change is written, which is the point of the save, and a formula that no
// longer produces the value it is next to cannot stay.
func unchangedXLSXCell(before [][]string, row, column int, value string) bool {
	if row > len(before) {
		return false
	}
	cells := before[row-1]
	if column > len(cells) {
		// A workbook stores no cell for a trailing empty one, so a row that ends
		// before this column holds nothing here, and nothing is what an empty
		// value would write.
		return value == ""
	}
	return sameCellValue(cells[column-1], value)
}

// sameCellValue reports whether a cell already says what a save is about to
// write, comparing two numbers as numbers and everything else as text.
//
// The two sides are spellings from different places. The sheet renders the
// number it stores, so a cell holding 2 shows "2"; the loaded value is rendered
// so that a text dump reloads with the same column type, so a REAL column
// spells the same number "2.0" and a large integer "1e+15". Compared as text
// those differ, so every whole number of a REAL column looked edited and was
// written back as a string -- a save that changed nothing turned a column a
// spreadsheet was summing into text. What decides whether a cell changed is the
// value it holds, and two spellings of one number are one value.
//
// Text is still compared as text. A zero-padded code stays in a text column
// under this package's own rule, so "007" and "7" reach here as text and are
// two different cells, which is what a comparison by number would deny.
func sameCellValue(held, value string) bool {
	if held == value {
		return true
	}
	heldNumber, ok := numericCellValue(held)
	if !ok {
		return false
	}
	newNumber, ok := numericCellValue(value)
	if !ok {
		return false
	}
	return heldNumber == newNumber
}

// numericCellValue is the number a cell's text spells, for the spellings this
// package calls numbers. A value it would keep as text -- a zero-padded code, a
// literal past int64, a number with padding around it -- is not one of them, so
// it stays a string here as it does in a column.
func numericCellValue(text string) (float64, bool) {
	if !infer.IsInteger(text) && !infer.IsFloat(text) {
		return 0, false
	}
	return infer.Float64(text)
}

// xlsxExtent is how far a sheet's values reached before it was rewritten.
type xlsxExtent struct {
	rows    int
	columns int
}

// xlsxSheetPrior is a sheet as it stood before a save wrote onto it.
type xlsxSheetPrior struct {
	// extent is how far its values reached, so what the table no longer covers
	// can be removed afterwards.
	extent xlsxExtent
	// values is the sheet as the loader read it, which is what a cell has to be
	// compared against to tell an edit from a value that came out of the file
	// unchanged. It is nil for a workbook being built from nothing.
	values [][]string
}

// xlsxSheetBefore reads a sheet about to be rewritten. Nothing is removed up
// front: writing over a cell keeps the style it carries, where clearing the
// sheet first would take the styles, the merged ranges and the comments with
// it. A sheet the workbook does not have yet has nothing to read, and neither
// does a workbook this package is building from nothing.
//
// The values are read the way the loader read them, dates included, so a cell
// that comes back the same string it went in as is recognizable as untouched.
func xlsxSheetBefore(f *excelize.File, sheetName string, ontoExisting bool) (xlsxSheetPrior, error) {
	if !ontoExisting {
		return xlsxSheetPrior{}, nil
	}
	if _, err := f.GetSheetIndex(sheetName); err != nil {
		return xlsxSheetPrior{}, nil //nolint:nilerr // A sheet that is not there yet; writeXLSXSheet creates it.
	}
	rows, err := f.GetRows(sheetName)
	if err != nil {
		return xlsxSheetPrior{}, fmt.Errorf("failed to read sheet %s: %w", sheetName, err)
	}
	prior := xlsxSheetPrior{
		extent: xlsxExtent{rows: len(rows)},
		values: reader.NormalizeXLSXDates(f, sheetName, rows),
	}
	for _, row := range rows {
		prior.extent.columns = max(prior.extent.columns, len(row))
	}
	return prior, nil
}

// trimXLSXSheet removes what the rewritten table no longer covers: rows below
// its last one and columns to the right of its last. Removing from the far end
// inward keeps the indexes gathered before the write correct as the sheet
// shrinks. A sheet that grew has nothing to trim.
func trimXLSXSheet(f *excelize.File, sheetName string, had xlsxExtent, wrote xlsxExtent) error {
	for row := had.rows; row > wrote.rows; row-- {
		if err := f.RemoveRow(sheetName, row); err != nil {
			return fmt.Errorf("failed to remove row %d of sheet %s: %w", row, sheetName, err)
		}
	}
	for column := had.columns; column > wrote.columns; column-- {
		name, err := excelize.ColumnNumberToName(column)
		if err != nil {
			return fmt.Errorf("failed to name column %d of sheet %s: %w", column, sheetName, err)
		}
		if err := f.RemoveCol(sheetName, name); err != nil {
			return fmt.Errorf("failed to remove column %s of sheet %s: %w", name, sheetName, err)
		}
	}
	return nil
}

// writeXLSXSheet adds one sheet to f and fills it. A cell whose value matches
// what before already holds is left alone.
func writeXLSXSheet(f *excelize.File, sheet xlsxSheet, before [][]string) (xlsxExtent, error) {
	columns, rows, err := sheet.open()
	if err != nil {
		return xlsxExtent{}, err
	}
	if rows != nil {
		defer rows.Close()
	}
	if len(columns) == 0 {
		return xlsxExtent{}, fmt.Errorf("%w: no columns defined", ErrEmptyData)
	}

	if sheet.name != defaultSheetName {
		if _, err := f.NewSheet(sheet.name); err != nil {
			return xlsxExtent{}, fmt.Errorf("failed to create sheet %s: %w", sheet.name, err)
		}
	}

	// Set headers
	for i, col := range columns {
		if unchangedXLSXCell(before, 1, i+1, col) {
			continue
		}
		cell, err := excelize.CoordinatesToCellName(i+1, 1)
		if err != nil {
			return xlsxExtent{}, fmt.Errorf("failed to generate cell name for column %d: %w", i+1, err)
		}
		if err := f.SetCellValue(sheet.name, cell, col); err != nil {
			return xlsxExtent{}, fmt.Errorf("failed to set header %s: %w", col, err)
		}
	}

	// Prepare for scanning rows
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Write data rows
	rowIndex := 2 // Start from row 2 (after header)
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return xlsxExtent{}, fmt.Errorf("failed to scan row: %w", err)
		}

		for i, val := range values {
			// Every cell is written as text, the same string the text formats
			// produce, so one table dumped twice does not disagree with itself.
			cellValue := formatDumpValue(val)
			if unchangedXLSXCell(before, rowIndex, i+1, cellValue) {
				continue
			}
			cell, err := excelize.CoordinatesToCellName(i+1, rowIndex)
			if err != nil {
				return xlsxExtent{}, fmt.Errorf("failed to generate cell name for column %d, row %d: %w", i+1, rowIndex, err)
			}
			if err := f.SetCellValue(sheet.name, cell, cellValue); err != nil {
				return xlsxExtent{}, fmt.Errorf("failed to set cell value at %s: %w", cell, err)
			}
		}
		rowIndex++
	}

	if err := rows.Err(); err != nil {
		return xlsxExtent{}, fmt.Errorf("error reading rows: %w", err)
	}

	return xlsxExtent{rows: rowIndex - 1, columns: len(columns)}, nil
}
