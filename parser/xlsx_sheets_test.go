package parser

import (
	"errors"
	"strings"
	"testing"
)

// fakeSheetSource is a workbook whose sheet list and per-sheet visibility are
// stated outright, including the visibility that cannot be read. A real
// workbook cannot be made to fail that call on demand, and "what happens when
// the visibility is unknown?" is the question the whole policy turns on.
type fakeSheetSource struct {
	names   []string
	visible map[string]bool
	failOn  map[string]error
}

func (f fakeSheetSource) GetSheetList() []string { return f.names }

func (f fakeSheetSource) GetSheetVisible(sheet string) (bool, error) {
	if err, ok := f.failOn[sheet]; ok {
		return false, err
	}
	return f.visible[sheet], nil
}

func newFakeSheetSource(order []string, hidden ...string) fakeSheetSource {
	visible := make(map[string]bool, len(order))
	for _, name := range order {
		visible[name] = true
	}
	for _, name := range hidden {
		visible[name] = false
	}
	return fakeSheetSource{names: order, visible: visible}
}

func TestExcelSheets(t *testing.T) {
	t.Parallel()

	t.Run("reports every sheet in workbook order with its visibility", func(t *testing.T) {
		t.Parallel()
		src := newFakeSheetSource([]string{"First", "Internal", "Last"}, "Internal")

		got, err := ExcelSheets(src)
		if err != nil {
			t.Fatalf("ExcelSheets: %v", err)
		}
		want := []ExcelSheet{
			{Name: "First", Visible: true},
			{Name: "Internal", Visible: false},
			{Name: "Last", Visible: true},
		}
		if len(got) != len(want) {
			t.Fatalf("got %d sheets, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("sheet %d = %+v, want %+v", i, got[i], want[i])
			}
		}
	})

	t.Run("reports a visibility that cannot be read instead of guessing", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("sheet index out of range")
		src := fakeSheetSource{
			names:   []string{"First", "Broken"},
			visible: map[string]bool{"First": true},
			failOn:  map[string]error{"Broken": boom},
		}

		_, err := ExcelSheets(src)
		if !errors.Is(err, boom) {
			t.Fatalf("error = %v, want it to wrap %v", err, boom)
		}
		if !strings.Contains(err.Error(), `"Broken"`) {
			t.Errorf("error %q should name the sheet it failed on", err)
		}
	})
}

func TestSelectExcelSheets(t *testing.T) {
	t.Parallel()

	order := []string{"Visible", "Hidden", "AlsoVisible", "VeryHidden"}
	src := newFakeSheetSource(order, "Hidden", "VeryHidden")

	t.Run("the all policy keeps every sheet in workbook order", func(t *testing.T) {
		t.Parallel()
		loaded, skipped, err := SelectExcelSheets(src, ExcelSheetPolicyAll)
		if err != nil {
			t.Fatalf("SelectExcelSheets: %v", err)
		}
		if strings.Join(loaded, ",") != strings.Join(order, ",") {
			t.Errorf("loaded = %v, want %v", loaded, order)
		}
		if len(skipped) != 0 {
			t.Errorf("skipped = %v, want nothing skipped", skipped)
		}
	})

	t.Run("the visible-only policy keeps the shown sheets in workbook order", func(t *testing.T) {
		t.Parallel()
		loaded, skipped, err := SelectExcelSheets(src, ExcelSheetPolicyVisibleOnly)
		if err != nil {
			t.Fatalf("SelectExcelSheets: %v", err)
		}
		if want := "Visible,AlsoVisible"; strings.Join(loaded, ",") != want {
			t.Errorf("loaded = %v, want %s", loaded, want)
		}
		// Both ways of hiding a sheet are left out, and the skipped list keeps the
		// workbook's order too so a caller can report them predictably.
		if want := "Hidden,VeryHidden"; strings.Join(skipped, ",") != want {
			t.Errorf("skipped = %v, want %s", skipped, want)
		}
	})

	t.Run("a visibility that cannot be read fails the selection", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("no such sheet")
		bad := fakeSheetSource{
			names:   []string{"Broken"},
			visible: map[string]bool{},
			failOn:  map[string]error{"Broken": boom},
		}
		if _, _, err := SelectExcelSheets(bad, ExcelSheetPolicyVisibleOnly); !errors.Is(err, boom) {
			t.Fatalf("error = %v, want it to wrap %v", err, boom)
		}
	})

	t.Run("a workbook with no sheets selects nothing without failing", func(t *testing.T) {
		t.Parallel()
		loaded, skipped, err := SelectExcelSheets(fakeSheetSource{}, ExcelSheetPolicyVisibleOnly)
		if err != nil {
			t.Fatalf("SelectExcelSheets: %v", err)
		}
		if len(loaded) != 0 || len(skipped) != 0 {
			t.Errorf("loaded = %v, skipped = %v, want both empty", loaded, skipped)
		}
	})
}

func TestExcelSheetPolicyString(t *testing.T) {
	t.Parallel()

	tests := map[ExcelSheetPolicy]string{
		ExcelSheetPolicyAll:         "all",
		ExcelSheetPolicyVisibleOnly: "visible-only",
		ExcelSheetPolicy(7):         "ExcelSheetPolicy(7)",
	}
	for policy, want := range tests {
		if got := policy.String(); got != want {
			t.Errorf("ExcelSheetPolicy(%d).String() = %q, want %q", int(policy), got, want)
		}
	}
}
