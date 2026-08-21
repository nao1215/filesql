package reader

import (
	"io"
	"strings"

	"github.com/nao1215/filesql/internal/infer"
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
	content, err := io.ReadAll(NormalizeLineEndings(src))
	if err != nil {
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
	evidence := make([]infer.Evidence, len(header))
	chunkSize := chunkSizeOf(opts)
	result := Result{Header: header}

	var chunk [][]string
	emitted := false
	for _, line := range lines {
		values := make(map[string]string)
		// Labels are compared folded; see LTSVLabelKey. The map is keyed that way
		// so a record finds its value under the column the first record named,
		// whatever case this one wrote it in.
		seen := make(map[string]struct{})
		for pair := range ltsvPairs(line) {
			key, value, ok := ltsvPair(pair)
			if !ok {
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
		if len(values) == 0 {
			continue
		}

		record := make([]string, len(header))
		for i, key := range header {
			record[i] = values[LTSVLabelKey(key)]
		}
		addEvidence(evidence, record)
		chunk = append(chunk, record)
		result.Total++

		if len(chunk) >= chunkSize {
			result.Rows += len(chunk)
			if err := emit(&Chunk{Header: header, Records: chunk, Types: typesOf(evidence)}); err != nil {
				return Result{}, err
			}
			chunk = nil
			emitted = true
		}
	}

	if len(chunk) > 0 || !emitted {
		result.Rows += len(chunk)
		if err := emit(&Chunk{Header: header, Records: chunk, Types: typesOf(evidence)}); err != nil {
			return Result{}, err
		}
	}
	result.Types = typesOf(evidence)
	return result, nil
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
