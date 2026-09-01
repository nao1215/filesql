package filesql

import (
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

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
func writeXLSXWorkbookOnto(w io.Writer, base *reader.Workbook, sheets []xlsxSheet) error {
	if len(sheets) == 0 {
		return fmt.Errorf("%w: no sheets to write", ErrEmptyData)
	}

	var f *excelize.File
	if base != nil {
		f = base.File()
	} else {
		f = excelize.NewFile()
	}
	defer func() {
		_ = f.Close() // Ignore close error
	}()

	for _, sheet := range sheets {
		had, err := xlsxSheetBefore(base, sheet.name)
		if err != nil {
			return err
		}
		written, err := writeXLSXSheet(f, sheet, had)
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
	//
	// Whether the sheet is there is the index, not the error: GetSheetIndex
	// answers -1 with no error for a sheet a workbook does not hold, so testing
	// the error asked nothing.
	if index, err := f.GetSheetIndex(defaultSheetName); err == nil && index >= 0 && base == nil {
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

// xlsxExtent is how far a sheet's values reached before it was rewritten: the
// last row and the last column they occupied, rather than how many of each
// there were, since a sheet may hold a row the table is not made of.
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
	// layout is where on the sheet the table sat, so the save writes it back to
	// the rows it came from. It is zero for a workbook being built from nothing,
	// which puts the header on row one.
	layout reader.SheetLayout
}

// headerRow is the sheet row a save writes the header to.
func (p xlsxSheetPrior) headerRow() int {
	if p.layout.HeaderRow == 0 {
		return 1
	}
	return p.layout.HeaderRow
}

// recordRow is the sheet row a save writes record i to, numbering records from
// zero. A record the sheet did not have goes under the last one it did.
func (p xlsxSheetPrior) recordRow(i int) int {
	if i < len(p.layout.RecordRows) {
		return p.layout.RecordRows[i]
	}
	last := p.headerRow()
	if n := len(p.layout.RecordRows); n > 0 {
		last = p.layout.RecordRows[n-1]
	}
	return last + 1 + (i - len(p.layout.RecordRows))
}

// xlsxSheetBefore reads a sheet about to be rewritten. Nothing is removed up
// front: writing over a cell keeps the style it carries, where clearing the
// sheet first would take the styles, the merged ranges and the comments with
// it. A sheet the workbook does not have yet has nothing to read, and neither
// does a workbook this package is building from nothing.
//
// The values are read the way the loader read them, dates included, so a cell
// that comes back the same string it went in as is recognizable as untouched.
func xlsxSheetBefore(base *reader.Workbook, sheetName string) (xlsxSheetPrior, error) {
	if base == nil {
		return xlsxSheetPrior{}, nil
	}
	f := base.File()
	// A missing sheet is an index of -1 and no error; an error means a name no
	// sheet could carry, which is worth reporting rather than treating as a
	// sheet to create. Branching on the error alone answered "not there yet"
	// for both, so the absent sheet fell through to GetRows below and failed
	// the save with the message this branch exists to avoid.
	index, err := f.GetSheetIndex(sheetName)
	if err != nil {
		return xlsxSheetPrior{}, fmt.Errorf("failed to look up sheet %s: %w", sheetName, err)
	}
	if index < 0 {
		return xlsxSheetPrior{}, nil // A sheet that is not there yet; writeXLSXSheet creates it.
	}
	rows, err := f.GetRows(sheetName)
	if err != nil {
		return xlsxSheetPrior{}, fmt.Errorf("failed to read sheet %s: %w", sheetName, err)
	}
	prior := xlsxSheetPrior{
		extent: xlsxExtent{rows: len(rows)},
		layout: base.LayoutOf(sheetName, rows),
		values: reader.NormalizeXLSXDates(f, sheetName, reader.NormalizeXLSXNumbers(f, sheetName, rows)),
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

// numericColumns reports, per column, whether SQLite declares it a number. A
// column this package inferred as text is not one, whatever its values spell.
// A read that cannot answer leaves every column text, which is what every cell
// was written as before this.
func numericColumns(rows *sql.Rows) []bool {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil
	}
	numeric := make([]bool, len(types))
	for i, t := range types {
		switch strings.ToUpper(t.DatabaseTypeName()) {
		case sqlTypeInteger, sqlTypeReal:
			numeric[i] = true
		}
	}
	return numeric
}

// xlsxCellValue is what a cell is written with: the number a numeric column's
// value spells, or the text every other value is written as.
//
// An integer is written as an integer rather than through a float64, which
// holds only the first fifteen or so digits of one: a column may hold an int64
// past what a float64 spells exactly, and rounding it here would change the
// value the save was asked to write.
func xlsxCellValue(text string, numeric bool) any {
	if !numeric {
		return text
	}
	if n, err := strconv.ParseInt(text, 10, 64); err == nil {
		return n
	}
	if f, ok := numericCellValue(text); ok {
		return f
	}
	return text
}

// xmlControlRune finds the first character in s that an XML 1.0 document has no
// way to spell: a control character other than tab, line feed and carriage
// return, which are the three XML admits. A worksheet is XML, and the library
// writing one replaces each of the others with U+FFFD, so a cell holding a NUL
// or an ASCII escape used to come back changed under a dump that reported
// success. Refusing is what every other format here does with a value it cannot
// hold. U+007F and the characters above it are left alone, because XML 1.0
// admits them and the workbook carries them unchanged.
//
// This comes out if excelize grows a way to report or refuse the substitution
// itself.
// The scan is over bytes rather than runes: every character it looks for is
// below 0x20, and no byte of a multi-byte UTF-8 sequence is, so a byte scan
// answers the same question without decoding the string. This runs on every
// cell of a workbook being written.
func xmlControlRune(s string) (rune, bool) {
	for i := range len(s) {
		c := s[i]
		if c >= 0x20 || c == '\t' || c == '\n' || c == '\r' {
			continue
		}
		return rune(c), true
	}
	return 0, false
}

// xlsxUnrepresentableError reports a value an XLSX cell cannot carry, in the
// shape the TSV and LTSV refusals already have: the table is fine, the format
// is not, and CSV can hold what this cannot.
func xlsxUnrepresentableError(column string, r rune) error {
	return fmt.Errorf("%w: XLSX cannot hold a value that contains %q, and column %q holds one; dump this table as CSV instead",
		ErrUnsupportedFormat, r, column)
}

// xlsxNotUTF8Error reports text a worksheet cannot carry because it is not
// characters. A workbook is XML and holds text, so bytes that are not valid
// UTF-8 went in as U+FFFD and the table came back changed with nothing said.
func xlsxNotUTF8Error(what string, position int) error {
	return fmt.Errorf("%w: XLSX holds characters rather than bytes, and %s %d is not valid UTF-8; dump this table as Parquet instead",
		ErrUnsupportedFormat, what, position)
}

// writeXLSXSheet adds one sheet to f and fills it. A cell whose value matches
// what before already holds is left alone.
func writeXLSXSheet(f *excelize.File, sheet xlsxSheet, prior xlsxSheetPrior) (xlsxExtent, error) {
	before := prior.values
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

	// A sheet carries its names in a header row, so a column with no name is
	// written as an empty cell and read back under a name taken from its
	// position -- and where that column is the last one it is worse, because a
	// worksheet stores cells rather than a rectangle and the library writing one
	// does not store a trailing empty value, so the header comes back one cell
	// short of the rows under it and the read refuses a workbook this package
	// wrote. Either way the table does not come back as it went out.
	for i, column := range columns {
		if column == "" {
			return xlsxExtent{}, fmt.Errorf(
				"%w: XLSX cannot hold a table with an unnamed column, since a sheet names its columns in a header row and reads an empty cell there as a name taken from its position, and column %d has no name; dump this table as LTSV or Parquet instead",
				ErrUnsupportedFormat, i+1)
		}
	}

	// The table goes back to the rows it came from, which are the rows from the
	// top only for a sheet holding no row without a cell in it.
	headerRow := prior.headerRow()

	// Set headers
	for i, col := range columns {
		if r, found := xmlControlRune(col); found {
			return xlsxExtent{}, xlsxUnrepresentableError(col, r)
		}
		if !utf8.ValidString(col) {
			return xlsxExtent{}, xlsxNotUTF8Error("column name", i+1)
		}
		if unchangedXLSXCell(before, headerRow, i+1, col) {
			continue
		}
		cell, err := excelize.CoordinatesToCellName(i+1, headerRow)
		if err != nil {
			return xlsxExtent{}, fmt.Errorf("failed to generate cell name for column %d: %w", i+1, err)
		}
		if err := f.SetCellValue(sheet.name, cell, col); err != nil {
			return xlsxExtent{}, fmt.Errorf("failed to set header %s: %w", col, err)
		}
	}

	numeric := numericColumns(rows)

	// Prepare for scanning rows
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Write data rows
	record := 0
	rowIndex := headerRow
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return xlsxExtent{}, fmt.Errorf("failed to scan row: %w", err)
		}
		rowIndex = prior.recordRow(record)
		record++

		for i, val := range values {
			// Every cell is formatted as the text the text formats produce, so
			// one table dumped twice does not disagree with itself; a numeric
			// column's cell then goes in as the number that text spells.
			cellValue := formatDumpValue(val)
			if r, found := xmlControlRune(cellValue); found {
				return xlsxExtent{}, xlsxUnrepresentableError(columns[i], r)
			}
			if !utf8.ValidString(cellValue) {
				return xlsxExtent{}, xlsxNotUTF8Error("value", i+1)
			}
			if unchangedXLSXCell(before, rowIndex, i+1, cellValue) {
				continue
			}
			cell, err := excelize.CoordinatesToCellName(i+1, rowIndex)
			if err != nil {
				return xlsxExtent{}, fmt.Errorf("failed to generate cell name for column %d, row %d: %w", i+1, rowIndex, err)
			}
			if err := f.SetCellValue(sheet.name, cell, xlsxCellValue(cellValue, i < len(numeric) && numeric[i])); err != nil {
				return xlsxExtent{}, fmt.Errorf("failed to set cell value at %s: %w", cell, err)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return xlsxExtent{}, fmt.Errorf("error reading rows: %w", err)
	}

	// The extent is the last row the table reaches rather than how many rows it
	// has, since the two differ by whatever the sheet holds between them.
	return xlsxExtent{rows: rowIndex, columns: len(columns)}, nil
}
