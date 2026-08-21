package parser

import (
	"github.com/nao1215/filesql/internal/reader"
)

// ExcelSheetPolicy decides which sheets of a workbook a read looks at.
//
// A workbook can hide a sheet, and hiding one is how a spreadsheet keeps
// scratch calculations, lookup tables, and stale drafts out of the way of the
// sheets it means to present. Whether those belong in a database is the
// caller's judgment rather than something this package can decide, so which
// sheets to take is a setting and not a rule.
type ExcelSheetPolicy = reader.ExcelSheetPolicy

const (
	// ExcelSheetPolicyAll reads every sheet, hidden or not. It is the zero
	// value, so a caller that names no policy keeps the behavior this package
	// had before the setting existed.
	ExcelSheetPolicyAll = reader.ExcelSheetPolicyAll

	// ExcelSheetPolicyVisibleOnly reads only the sheets a workbook shows.
	//
	// Excel separates "hidden", which a user can undo from the sheet tabs, from
	// "very hidden", which only the VBA editor can. The library underneath
	// reports one boolean covering both, so neither does this package: a sheet
	// the workbook does not show is left out under this policy, and no claim is
	// made about which of the two ways it was hidden.
	ExcelSheetPolicyVisibleOnly = reader.ExcelSheetPolicyVisibleOnly
)

// ExcelSheet is one sheet of a workbook and whether the workbook shows it.
type ExcelSheet = reader.ExcelSheet

// ExcelSheetSource is the part of an open workbook that sheet selection reads.
//
// It is an interface rather than the concrete workbook type for two reasons:
// the excelize dependency stays out of this package's signatures, and the
// selection rules -- order, filtering, and what happens when a visibility
// cannot be read -- become testable without a workbook that has to be coaxed
// into failing.
type ExcelSheetSource = reader.ExcelSheetSource

// ExcelSheets returns every sheet of the workbook in the order the workbook
// stores them, with the visibility it reports for each.
//
// A visibility that cannot be read is an error, not an assumption. Defaulting
// to visible would load a sheet the workbook hides; defaulting to hidden would
// drop one it shows.
func ExcelSheets(f ExcelSheetSource) ([]ExcelSheet, error) {
	return reader.ExcelSheets(f)
}

// SelectExcelSheets returns the sheets policy admits, in workbook order, and
// the names it left out.
//
// Every Excel read goes through this rather than filtering at its own call to
// the sheet list, so "which sheets does this workbook contribute?" has one
// answer whatever opened the file.
func SelectExcelSheets(f ExcelSheetSource, policy ExcelSheetPolicy) (loaded, skipped []string, err error) {
	return reader.SelectExcelSheets(f, policy)
}
