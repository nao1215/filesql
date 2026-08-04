package parser

import "fmt"

// ExcelSheetPolicy decides which sheets of a workbook a read looks at.
//
// A workbook can hide a sheet, and hiding one is how a spreadsheet keeps
// scratch calculations, lookup tables, and stale drafts out of the way of the
// sheets it means to present. Whether those belong in a database is the
// caller's judgment rather than something this package can decide, so which
// sheets to take is a setting and not a rule.
type ExcelSheetPolicy int

const (
	// ExcelSheetPolicyAll reads every sheet, hidden or not. It is the zero
	// value, so a caller that names no policy keeps the behavior this package
	// had before the setting existed.
	ExcelSheetPolicyAll ExcelSheetPolicy = iota

	// ExcelSheetPolicyVisibleOnly reads only the sheets a workbook shows.
	//
	// Excel separates "hidden", which a user can undo from the sheet tabs, from
	// "very hidden", which only the VBA editor can. The library underneath
	// reports one boolean covering both, so neither does this package: a sheet
	// the workbook does not show is left out under this policy, and no claim is
	// made about which of the two ways it was hidden.
	ExcelSheetPolicyVisibleOnly
)

// String renders the policy for messages and logs.
func (p ExcelSheetPolicy) String() string {
	switch p {
	case ExcelSheetPolicyVisibleOnly:
		return "visible-only"
	case ExcelSheetPolicyAll:
		return "all"
	default:
		return fmt.Sprintf("ExcelSheetPolicy(%d)", int(p))
	}
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

// ExcelSheetSource is the part of an open workbook that sheet selection reads.
//
// It is an interface rather than the concrete workbook type for two reasons:
// the excelize dependency stays out of this package's signatures, and the
// selection rules — order, filtering, and what happens when a visibility cannot
// be read — become testable without a workbook that has to be coaxed into
// failing.
type ExcelSheetSource interface {
	// GetSheetList returns the sheet names in the order the workbook stores them.
	GetSheetList() []string
	// GetSheetVisible reports whether the workbook shows the named sheet.
	GetSheetVisible(sheet string) (bool, error)
}

// ExcelSheets returns every sheet of the workbook in the order the workbook
// stores them, with the visibility it reports for each.
//
// A visibility that cannot be read is an error, not an assumption. Defaulting
// to visible would load a sheet the workbook hides; defaulting to hidden would
// drop one it shows. Either way the caller is told something untrue about the
// file, which is worse than being told the file could not be understood.
func ExcelSheets(f ExcelSheetSource) ([]ExcelSheet, error) {
	names := f.GetSheetList()
	sheets := make([]ExcelSheet, 0, len(names))
	for _, name := range names {
		visible, err := f.GetSheetVisible(name)
		if err != nil {
			return nil, fmt.Errorf("failed to read the visibility of sheet %q: %w", name, err)
		}
		sheets = append(sheets, ExcelSheet{Name: name, Visible: visible})
	}
	return sheets, nil
}

// SelectExcelSheets returns the sheets policy admits, in workbook order, and
// the names it left out.
//
// Every Excel read goes through this rather than filtering at its own call to
// the sheet list, so "which sheets does this workbook contribute?" has one
// answer whatever opened the file. That matters most for the caller that maps
// sheets onto table names: the names it must keep apart are the ones actually
// loaded, and a sheet nobody reads cannot collide with one that is read.
func SelectExcelSheets(f ExcelSheetSource, policy ExcelSheetPolicy) (loaded, skipped []string, err error) {
	sheets, err := ExcelSheets(f)
	if err != nil {
		return nil, nil, err
	}
	loaded = make([]string, 0, len(sheets))
	for _, sheet := range sheets {
		if policy == ExcelSheetPolicyVisibleOnly && !sheet.Visible {
			skipped = append(skipped, sheet.Name)
			continue
		}
		loaded = append(loaded, sheet.Name)
	}
	return loaded, skipped, nil
}
