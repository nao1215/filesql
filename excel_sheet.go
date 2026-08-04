package filesql

import (
	"bytes"
	"fmt"
	"io"

	"github.com/nao1215/filesql/parser"
	"github.com/xuri/excelize/v2"
)

// ExcelSheetPolicy decides which sheets of a workbook a load reads.
//
// It is an alias of the parser package's type, not a second enum wrapping it,
// so the value a caller builds here is the one every Excel read path applies
// and the two can never disagree about what a policy means.
type ExcelSheetPolicy = parser.ExcelSheetPolicy

const (
	// ExcelSheetPolicyAll loads every sheet of a workbook, hidden or not. It is
	// the zero value, so a caller that names no policy keeps the behaviour
	// filesql had before the setting existed.
	ExcelSheetPolicyAll = parser.ExcelSheetPolicyAll

	// ExcelSheetPolicyVisibleOnly loads only the sheets a workbook shows.
	// Hidden and very hidden sheets are both left out; see the parser package's
	// constant for why the two are not told apart.
	ExcelSheetPolicyVisibleOnly = parser.ExcelSheetPolicyVisibleOnly
)

// ExcelSheet is one sheet of a workbook and whether the workbook shows it.
type ExcelSheet = parser.ExcelSheet

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
	reader, cleanup, err := NewCompressionFactory().CreateReaderForFile(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cleanupErr := cleanup(); cleanupErr != nil && err == nil {
			err = fmt.Errorf("%w: failed to close %s: %w", ErrIOOperation, path, cleanupErr)
		}
	}()
	return ExcelSheetsInReader(reader)
}

// ExcelSheetsInReader is ExcelSheetsInFile for a workbook that has no path. The
// reader must yield the workbook's own bytes; a codec around them has to be
// unwrapped first, as it has no name to be detected from.
func ExcelSheetsInReader(reader io.Reader) (sheets []ExcelSheet, err error) {
	// excelize needs random access, and the reader may be a stream, so the
	// workbook is buffered whole — the same thing every other XLSX path here
	// does with it.
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read XLSX data: %w", ErrIOOperation, err)
	}
	f, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open XLSX file: %w", ErrParsing, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("%w: failed to close XLSX file: %w", ErrIOOperation, closeErr)
		}
	}()
	return excelSheetList(f)
}

// excelSheetList is parser.ExcelSheets with filesql's sentinel attached, so a
// workbook whose visibility cannot be read fails as a parse error like every
// other unreadable input here.
func excelSheetList(f parser.ExcelSheetSource) ([]ExcelSheet, error) {
	sheets, err := parser.ExcelSheets(f)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrParsing, err)
	}
	return sheets, nil
}

// selectExcelSheets is parser.SelectExcelSheets with filesql's sentinel
// attached. It is the single point every Excel load path here calls to turn an
// open workbook into the list of sheets it contributes.
func selectExcelSheets(f parser.ExcelSheetSource, policy ExcelSheetPolicy) (loaded, skipped []string, err error) {
	loaded, skipped, err = parser.SelectExcelSheets(f, policy)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrParsing, err)
	}
	return loaded, skipped, nil
}

// noExcelSheetsError explains an XLSX file that contributed nothing. It
// separates a workbook with no sheets at all from one whose sheets were all
// left out by the policy, because the two need different things done about
// them: the first file is broken, the second is a setting the caller chose.
func noExcelSheetsError(f parser.ExcelSheetSource, policy ExcelSheetPolicy) error {
	if policy == ExcelSheetPolicyVisibleOnly && len(f.GetSheetList()) > 0 {
		return fmt.Errorf("%w: no visible sheets found in XLSX file", ErrEmptyData)
	}
	return fmt.Errorf("%w: no sheets found in XLSX file", ErrEmptyData)
}
