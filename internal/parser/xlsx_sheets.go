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
