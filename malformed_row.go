package filesql

import (
	"fmt"

	"github.com/nao1215/filesql/internal/reader"
)

// MalformedRowPolicy controls how a record that does not fit the columns of its
// table is handled during import: a delimited (CSV/TSV) record whose field count
// differs from the header row, or an LTSV record holding a field that is not a
// label and a value.
//
// A workbook's rows are checked by the XLSX reader instead, which pads a short
// one and refuses one wider than its header whatever this policy says, and
// Parquet and JSON/JSONL carry nothing per row to disagree about.
//
// What LTSV settles itself is the missing half of the question: a record naming
// only some of the columns is padded, since LTSV writes its labels on every
// record and leaving one out is how the format says a value is absent. A field
// with no label is the other half, and it is this policy's: the field is data
// the table has no column for, so the record carrying it is one the policy
// decides about.
type MalformedRowPolicy int

const (
	// MalformedRowStop aborts the import with an error on the first record whose
	// field count differs from the header. It is the default because a ragged
	// row usually signals a corrupt or misaligned file, and silently dropping or
	// reshaping data would hide the problem.
	MalformedRowStop MalformedRowPolicy = iota

	// MalformedRowSkip discards any record whose field count differs from the
	// header and imports the remaining well-formed records.
	MalformedRowSkip

	// MalformedRowFill keeps every short record by padding it with empty strings.
	// A long record is rejected so source data is never silently discarded --
	// filling is for a record that is missing values, and a record carrying more
	// than the header names is not that.
	MalformedRowFill
)

// String returns the lowercase name of the policy, matching the values accepted
// on the command line.
func (p MalformedRowPolicy) String() string {
	switch p {
	case MalformedRowStop:
		return "stop"
	case MalformedRowSkip:
		return "skip"
	case MalformedRowFill:
		return "fill"
	default:
		return fmt.Sprintf("MalformedRowPolicy(%d)", int(p))
	}
}

// reconcileFieldCount reshapes a delimited record whose field count does not
// match the header according to the policy. It returns the record to insert,
// whether the caller should skip the record entirely, and an error when the
// policy is to stop.
func reconcileFieldCount(record []string, want, rowNum int, policy MalformedRowPolicy) (out []string, skip bool, err error) {
	if len(record) == want {
		return record, false, nil
	}
	switch policy {
	case MalformedRowSkip:
		return nil, true, nil
	case MalformedRowFill:
		if len(record) > want {
			return nil, false, fmt.Errorf("%w: row %d has %d fields, want at most %d", ErrColumnMismatch, rowNum, len(record), want)
		}
		return fitRecord(record, want), false, nil
	default: // MalformedRowStop
		return nil, false, fmt.Errorf("%w: row %d has %d fields, want %d", ErrColumnMismatch, rowNum, len(record), want)
	}
}

// reconcileUnlabeledFields applies the policy to an LTSV record holding fields
// that name no label. It returns whether the caller should skip the record, or
// the error that ends the read.
//
// Filling refuses it for the reason filling refuses a long delimited record:
// there is no column to pad that would hold what the field says, so keeping the
// record would be discarding the field.
func reconcileUnlabeledFields(fields []string, rowNum int, policy MalformedRowPolicy) (skip bool, err error) {
	if policy == MalformedRowSkip {
		return true, nil
	}
	return false, fmt.Errorf("%w: row %d holds a field that names no label: %s",
		ErrColumnMismatch, rowNum, reader.QuoteFields(fields))
}

// fitRecord returns a copy of a short record resized to exactly want fields by
// padding missing values with empty strings.
func fitRecord(record []string, want int) []string {
	out := make([]string, want)
	copy(out, record)
	return out
}
