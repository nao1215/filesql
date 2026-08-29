package reader

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// labelOrder collects LTSV labels, keeping each one once and in the order it
// was first seen. LTSV has no header line, so the columns can only be the
// labels the records carry; the set has to be built while reading, and the
// order has to be remembered rather than recovered from the set afterwards.
type labelOrder struct {
	seen  map[string]bool
	order []string
}

func newLabelOrder() *labelOrder {
	return &labelOrder{seen: make(map[string]bool)}
}

// add records a label, ignoring one already seen. Two labels differing only in
// case are one label, the way they are within a single record and the way
// SQLite compares the column names it ends up holding: keeping them apart made
// "id" and "ID" two columns, and the table SQLite was then asked to create was
// refused with an error naming neither the file nor the rule. The spelling kept
// is the one that named the column first.
func (l *labelOrder) add(name string) {
	folded := LTSVLabelKey(name)
	if l.seen[folded] {
		return
	}
	l.seen[folded] = true
	l.order = append(l.order, name)
}

// readLTSV reads labeled tab-separated records in chunks.
//
// The whole input is read first because the columns are whatever labels the
// records carry, and the last record can still name one the earlier ones did
// not.
func readLTSV(src io.Reader, opts Options, emit Emit) (Result, error) {
	// A record is held whole while its pairs are read, so one record with no
	// terminator would otherwise make the read cost the whole stream. The file
	// is still read whole -- the columns are whatever labels the records carry
	// -- but a single record is bounded the way a delimited record and a JSONL
	// line are.
	bounded := newLineBoundedReader(NormalizeLineEndings(src, FormatLTSV), recordLimitOf(opts))
	content, err := io.ReadAll(bounded)
	if err != nil {
		if errors.Is(err, ErrRecordTooLong) {
			return Result{}, err
		}
		return Result{}, parseError(err, "failed to read LTSV")
	}
	lines := strings.Split(string(content), "\n")

	// First pass: collect the labels in the order they first appear. A map would
	// lose the order, and the column order is the file's to decide.
	labels := newLabelOrder()
	for _, line := range lines {
		for pair := range ltsvPairs(line) {
			if key, _, ok := ltsvPair(pair); ok {
				labels.add(key)
			}
		}
	}
	if len(labels.order) == 0 {
		return Result{}, emptyError("no valid LTSV records found")
	}

	header := labels.order
	// The labels are the columns, so a file naming more of them than a table
	// holds is refused here rather than by the CREATE TABLE that follows.
	if err := ValidateColumnCount(len(header)); err != nil {
		return Result{}, err
	}
	result := Result{Header: header}
	rows := newChunker(header, opts, emit)
	rowNum := 0
	for _, line := range lines {
		values := make(map[string]string)
		// Labels are compared folded; see LTSVLabelKey. The map is keyed that way
		// so a record finds its value under the column the first record named,
		// whatever case this one wrote it in.
		seen := make(map[string]struct{})
		var unlabeled []string
		for pair := range ltsvPairs(line) {
			key, value, ok := ltsvPair(pair)
			if !ok {
				// A field with nothing to name it is data no column can hold.
				// An empty one is not a field at all: a line ending in a tab,
				// and a line of nothing but tabs, are how a file separates its
				// records rather than how it writes one.
				if pair != "" {
					unlabeled = append(unlabeled, pair)
				}
				continue
			}
			// A label repeated within the same record cannot be two distinct
			// columns; keeping the last value would silently drop the earlier one,
			// so reject it.
			folded := LTSVLabelKey(key)
			if _, dup := seen[folded]; dup {
				return Result{}, duplicateColumnError("%q in LTSV record", key)
			}
			seen[folded] = struct{}{}
			values[folded] = value
		}
		if len(values) == 0 && len(unlabeled) == 0 {
			continue
		}
		rowNum++

		if len(unlabeled) > 0 {
			skip, err := refuseUnlabeled(opts.Unlabeled, unlabeled, rowNum)
			if err != nil {
				return Result{}, err
			}
			if skip {
				result.Total++
				result.Skipped++
				continue
			}
		}

		record := make([]string, len(header))
		for i, key := range header {
			record[i] = values[LTSVLabelKey(key)]
		}
		result.Total++
		if err := rows.add(record); err != nil {
			return Result{}, err
		}
	}

	if err := rows.finish(); err != nil {
		return Result{}, err
	}
	result.Rows = rows.rows
	result.Types = rows.types()
	return result, nil
}

// refuseUnlabeled asks the caller what becomes of a record holding fields that
// name no label. A caller that named no answer gets the strict one, since a
// read with no policy of its own has nothing to weigh the alternative against.
func refuseUnlabeled(unlabeled Unlabeled, fields []string, rowNum int) (bool, error) {
	if unlabeled == nil {
		return false, invalidError(nil, "row %d holds a field that names no label: %s", rowNum, QuoteFields(fields))
	}
	return unlabeled(fields, rowNum)
}

// QuoteFields spells the fields of a record for a message, quoted so a field
// that is empty or only spaces is visible and shortened so a long line does not
// become the whole error. It is exported because the caller that decides what
// becomes of such a record words its own refusal, and one wording of the same
// list is better than two.
func QuoteFields(fields []string) string {
	const shown = 3
	quoted := make([]string, 0, shown)
	for _, field := range fields[:min(len(fields), shown)] {
		quoted = append(quoted, strconv.Quote(truncateLine(field, 40)))
	}
	if len(fields) > shown {
		return strings.Join(quoted, ", ") + fmt.Sprintf(" and %d more", len(fields)-shown)
	}
	return strings.Join(quoted, ", ")
}

// ltsvPairs is the label-value pairs one line holds. Only the line terminator is
// removed: TrimSpace took the trailing spaces of the last field with it, so a
// value ending in a space lost it.
func ltsvPairs(line string) func(func(string) bool) {
	return strings.SplitSeq(strings.TrimRight(line, "\r"), "\t")
}

// ltsvPair splits one pair into its label and value, reporting whether it is a
// pair at all. The value is the bytes up to the next tab or newline. Trimming
// it lost whitespace the writer had written and CSV would have kept, so the
// same data read from two formats disagreed. The label is trimmed because a
// space around one is malformed either way.
func ltsvPair(pair string) (key, value string, ok bool) {
	label, rest, found := strings.Cut(pair, ":")
	if !found {
		return "", "", false
	}
	return strings.TrimSpace(label), rest, true
}
