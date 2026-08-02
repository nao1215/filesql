package filesql

import "fmt"

// MalformedRowPolicy controls how a delimited (CSV/TSV) record whose field count
// differs from the header row is handled during import.
//
// The policy only applies to delimited text formats, where a row can have more
// or fewer fields than the header. Other formats do not have this failure mode:
// XLSX and LTSV already pad missing cells, and Parquet and JSON/JSONL carry no
// per-row field-count concept.
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
	// A long record is rejected so source data is never silently discarded.
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

// fitRecord returns a copy of a short record resized to exactly want fields by
// padding missing values with empty strings.
func fitRecord(record []string, want int) []string {
	out := make([]string, want)
	copy(out, record)
	return out
}
