package filesql

import (
	"fmt"
	"io"

	"github.com/nao1215/filesql/internal/reader"
)

// ExcelSheetPolicy decides which sheets of a workbook a load reads.
//
// A workbook can hide a sheet, and hiding one is how a spreadsheet keeps
// scratch calculations, lookup tables and stale drafts out of the way of the
// sheets it means to present. Whether those belong in a database is the
// caller's judgment rather than something this package can decide, so which
// sheets to take is a setting and not a rule.
type ExcelSheetPolicy int

const (
	// ExcelSheetPolicyAll loads every sheet of a workbook, hidden or not. It is
	// the zero value, so a caller that names no policy keeps the behavior
	// filesql had before the setting existed.
	ExcelSheetPolicyAll ExcelSheetPolicy = iota

	// ExcelSheetPolicyVisibleOnly loads only the sheets a workbook shows.
	// Hidden and very hidden sheets are both left out: a workbook tells the two
	// apart, but a caller asking for what it presents means neither.
	ExcelSheetPolicyVisibleOnly
)

// String names the policy.
func (p ExcelSheetPolicy) String() string { return p.internal().String() }

// internal is the reading side's spelling of the same policy. The two enums
// mirror each other rather than sharing one type, because the reading side is
// not part of this package's public surface; TestExcelSheetPolicyMirrorsTheReader
// holds the pairs equal.
func (p ExcelSheetPolicy) internal() reader.ExcelSheetPolicy {
	return reader.ExcelSheetPolicy(p)
}

// ExcelSheet is one sheet of a workbook and whether the workbook shows it.
type ExcelSheet struct {
	// Name is the sheet name as the workbook stores it, before any sanitizing a
	// caller applies to turn it into a table name.
	Name string
	// Visible is false for a sheet the workbook does not show, whether it is
	// hidden or very hidden.
	Visible bool
}

// excelSheetsOf is the reading side's sheets in this package's own type.
func excelSheetsOf(sheets []reader.ExcelSheet) []ExcelSheet {
	out := make([]ExcelSheet, len(sheets))
	for i, sheet := range sheets {
		out[i] = ExcelSheet{Name: sheet.Name, Visible: sheet.Visible}
	}
	return out
}

// ExcelSheetsInFile reports the sheets of the workbook at path, in the order the
// workbook stores them, and whether it shows each one. The path may carry a
// compression suffix, exactly as it may for a load.
//
// It is exported for a caller that has to explain a load rather than only
// perform one: which sheets the file holds, and which of them a policy left
// behind. Answering that by reopening the file with a spreadsheet library would
// be a second implementation of the rule filesql itself applies, free to drift
// from it.
func ExcelSheetsInFile(path string) (sheets []ExcelSheet, err error) {
	source, cleanup, err := NewCompressionFactory().CreateReaderForFile(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cleanupErr := cleanup(); cleanupErr != nil && err == nil {
			err = fmt.Errorf("%w: failed to close %s: %w", ErrIOOperation, path, cleanupErr)
		}
	}()
	return ExcelSheetsInReader(source)
}

// ExcelSheetsInReader is ExcelSheetsInFile for a workbook that has no path. The
// reader must yield the workbook's own bytes; a codec around them has to be
// unwrapped first, as it has no name to be detected from.
func ExcelSheetsInReader(source io.Reader) (sheets []ExcelSheet, err error) {
	workbook, err := reader.OpenWorkbook(source)
	if err != nil {
		return nil, wrapReadError(err)
	}
	defer func() {
		if closeErr := workbook.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("%w: failed to close XLSX file: %w", ErrIOOperation, closeErr)
		}
	}()
	return excelSheetList(workbook.Source())
}

// excelSheetList is the reader's sheet listing with filesql's sentinel
// attached, so a workbook whose visibility cannot be read fails as a parse
// error like every other unreadable input here.
func excelSheetList(f reader.ExcelSheetSource) ([]ExcelSheet, error) {
	sheets, err := reader.ExcelSheets(f)
	if err != nil {
		return nil, wrapReadError(err)
	}
	return excelSheetsOf(sheets), nil
}

// selectExcelSheets is the reader's sheet selection with filesql's sentinel
// attached. It is the single point every Excel load path here calls to turn an
// open workbook into the list of sheets it contributes.
func selectExcelSheets(f reader.ExcelSheetSource, policy ExcelSheetPolicy) (loaded, skipped []string, err error) {
	loaded, skipped, err = reader.SelectExcelSheets(f, policy.internal())
	if err != nil {
		return nil, nil, wrapReadError(err)
	}
	return loaded, skipped, nil
}

// noExcelSheetsError explains an XLSX file that contributed nothing. It
// separates a workbook with no sheets at all from one whose sheets were all
// left out by the policy, because the two need different things done about
// them: the first file is broken, the second is a setting the caller chose.
func noExcelSheetsError(f reader.ExcelSheetSource, policy ExcelSheetPolicy) error {
	return wrapReadError(reader.NoExcelSheetsError(f, policy.internal()))
}
