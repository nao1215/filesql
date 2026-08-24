package frame

import (
	"math"
	"strconv"

	"github.com/nao1215/filesql/internal/infer"
)

// Row is one row of a DataFrame, addressed by column name.
//
// A DataFrame stores values typed rather than as the text they were read from,
// so a CSV column of digits arrives as int64 and a decimal column as float64.
// A predicate written against the raw map therefore matches nothing and reports
// nothing: row["id"] == "1" is false when the value is int64(1). The accessors
// below return the value in the form the caller asks for, so a predicate can be
// written against the file rather than against a guess about it.
//
// The callbacks of Filter, Mutate, and the aggregation functions take
// map[string]any, which converts to Row for free:
//
//	filtered := df.Filter(func(row map[string]any) bool {
//	    id, ok := frame.Row(row).String("id")
//	    return ok && id == "1"
//	})
//
// Every accessor reports false instead of silently returning a zero value when
// the column is absent, holds nil, or holds something it cannot represent, so a
// mistyped column name is visible in the predicate rather than costing rows.
type Row map[string]any

// String returns the value of column as text.
//
// A value stored as text is returned unchanged, so a zero-padded code stays
// "007". Only decimal formatting is rewritten by the load, so every other value
// comes back spelled as the file had it; a real is rendered in the shortest
// form that reads back exactly, which for a whole 2.0 is "2".
//
// The second result is false when the column is absent or holds nil. An empty
// cell is a value, so it returns ("", true).
func (r Row) String(column string) (string, bool) {
	value, ok := r[column]
	if !ok {
		return "", false
	}

	switch v := value.(type) {
	case string:
		return v, true
	case int64:
		return strconv.FormatInt(v, 10), true
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), true
	default:
		return "", false
	}
}

// Int returns the value of column as an int64.
//
// Text that spells an integer converts, so a column kept as text because its
// values are zero-padded is still comparable as a number: "007" is 7. A real
// converts only when it is whole and fits, because rounding 7.5 to 7 would
// answer a question the caller did not ask.
//
// The second result is false when the column is absent, holds nil, or holds a
// value with no exact integer form.
func (r Row) Int(column string) (int64, bool) {
	switch v := r[column].(type) {
	case int64:
		return v, true
	case float64:
		// The bounds are the float64 values that bracket the int64 range; the
		// endpoints themselves are excluded because int64's own limits are not
		// exactly representable as float64.
		if math.IsNaN(v) || v != math.Trunc(v) || v <= math.MinInt64 || v >= math.MaxInt64 {
			return 0, false
		}
		return int64(v), true
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}

// Float returns the value of column as a float64.
//
// An integer widens, and text that spells a number converts, so a column kept
// as text is still comparable as a quantity. The spellings that convert are
// the ones a data file writes numbers in — digits, sign, decimal point,
// exponent — not Go's: an underscore separator ("1_000"), a hex float
// ("0x1p4"), and the digit-free words Inf and NaN have no numeric form here,
// the same answer Row.Int and SQLite's affinity give the same text.
//
// The second result is false when the column is absent, holds nil, or holds a
// value with no numeric form.
func (r Row) Float(column string) (float64, bool) {
	switch v := r[column].(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case string:
		if !infer.HasDigit(v) || infer.HasGoOnlyNumericSyntax(v) {
			return 0, false
		}
		return infer.Float64(v)
	default:
		return 0, false
	}
}
