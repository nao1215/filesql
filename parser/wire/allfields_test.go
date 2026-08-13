package wire

import (
	"reflect"
	"slices"
	"testing"

	"github.com/moov-io/wire"
	"github.com/nao1215/filesql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validateOptionsField is the one pointer field of FEDWireMessage that is not a
// message section: it carries the reader's validation switches and has no
// columns, so the section rules below do not apply to it.
const validateOptionsField = "ValidateOptions"

// fullRecord returns a record that gives every column a distinct non-empty
// value. Using the column's own name as its value means a value that lands in
// the wrong field is visible in the failure message instead of matching by
// accident.
func fullRecord(headers []string) []string {
	return slices.Clone(headers)
}

// sectionFields returns the names of the message sections of fwm that are nil
// and the ones that are set.
func sectionFields(fwm *wire.FEDWireMessage) (nilSections, setSections []string) {
	value := reflect.ValueOf(*fwm)
	for _, f := range reflect.VisibleFields(value.Type()) {
		if f.Type.Kind() != reflect.Pointer || f.Name == validateOptionsField {
			continue
		}
		if value.FieldByIndex(f.Index).IsNil() {
			nilSections = append(nilSections, f.Name)
			continue
		}
		setSections = append(setSections, f.Name)
	}
	return nilSections, setSections
}

// TestApplyModifications_EveryColumnRoundTrips writes a value into every column
// of the message table and reads the message back out. A column that
// applyModifications forgets, or that messageRecord writes to a different
// position, shows up here as a mismatch on that column; the per-section tests
// reach only the sections they name.
func TestApplyModifications_EveryColumnRoundTrips(t *testing.T) {
	t.Parallel()

	headers := messageHeaders()
	record := fullRecord(headers)

	fwm := &wire.FEDWireMessage{}
	applyModifications(fwm, &parser.TableData{
		Headers: headers,
		Records: [][]string{record},
	})

	got := messageRecord(fwm)
	require.Len(t, got, len(headers), "messageRecord must return one value per header")
	for i, h := range headers {
		assert.Equalf(t, record[i], got[i], "column %q did not survive the round trip", h)
	}
}

// TestEnsureNonNilSubStructs_AllocatesEverySection checks that a record with a
// value in every column leaves no section nil. A section that stays nil silently
// drops the values a caller wrote into its columns.
func TestEnsureNonNilSubStructs_AllocatesEverySection(t *testing.T) {
	t.Parallel()

	headers := messageHeaders()
	record := fullRecord(headers)

	fwm := &wire.FEDWireMessage{}
	ensureNonNilSubStructs(fwm, buildHeaderIndex(headers), record)

	nilSections, _ := sectionFields(fwm)
	assert.Emptyf(t, nilSections, "these sections stay nil even though their columns hold values: %v", nilSections)
}

// TestEnsureNonNilSubStructs_AllocatesNothingForEmptyRecord is the other half of
// the rule: an untouched row must not invent sections, because an allocated
// empty section is written back out as an empty tag.
func TestEnsureNonNilSubStructs_AllocatesNothingForEmptyRecord(t *testing.T) {
	t.Parallel()

	headers := messageHeaders()
	record := make([]string, len(headers))

	fwm := &wire.FEDWireMessage{}
	ensureNonNilSubStructs(fwm, buildHeaderIndex(headers), record)

	_, setSections := sectionFields(fwm)
	assert.Emptyf(t, setSections, "these sections were allocated for an all-empty record: %v", setSections)
}
