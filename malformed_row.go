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

	// MalformedRowFill keeps every record, padding a short record with empty
	// strings and truncating the extra trailing fields of a long record so it
	// matches the header width.
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
		return fitRecord(record, want), false, nil
	default: // MalformedRowStop
		return nil, false, fmt.Errorf("%w: row %d has %d fields, want %d", ErrColumnMismatch, rowNum, len(record), want)
	}
}

// fitRecord returns a copy of record resized to exactly want fields: a short
// record is padded with empty strings and a long record is truncated.
func fitRecord(record []string, want int) []string {
	out := make([]string, want)
	copy(out, record)
	return out
}
