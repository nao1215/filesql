package frame

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
)

// GroupedDataFrame represents a DataFrame grouped by one or more columns.
type GroupedDataFrame struct {
	df     *DataFrame
	groups []string
}

// AggFunc is a function type for custom aggregation.
// It receives a slice of values from the same group and returns the aggregated result.
type AggFunc func(values []any) any

// ErrColumnNotFound is returned when a specified column does not exist in the DataFrame.
var ErrColumnNotFound = errors.New("column not found")

// GroupBy groups the DataFrame by the specified columns.
// Returns a GroupedDataFrame that can be used with aggregation functions.
// Returns an error if any of the specified columns do not exist in the DataFrame.
//
// Example:
//
//	grouped, err := df.GroupBy("category")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	result := grouped.Sum("amount")
func (df *DataFrame) GroupBy(columns ...string) (*GroupedDataFrame, error) {
	// Validate that all group columns exist
	columnSet := make(map[string]struct{}, len(df.columns))
	for _, col := range df.columns {
		columnSet[col] = struct{}{}
	}

	for _, col := range columns {
		if _, exists := columnSet[col]; !exists {
			return nil, fmt.Errorf("%w: %s", ErrColumnNotFound, col)
		}
	}

	return &GroupedDataFrame{
		df:     df,
		groups: columns,
	}, nil
}

// groupKey generates a unique key for a group based on the group columns.
func (gdf *GroupedDataFrame) groupKey(row map[string]any) string {
	parts := make([]string, len(gdf.groups))
	for i, col := range gdf.groups {
		parts[i] = fmt.Sprintf("%v", row[col])
	}
	return strings.Join(parts, "\x00")
}

// buildGroups organizes rows into groups based on group columns.
func (gdf *GroupedDataFrame) buildGroups() map[string][]map[string]any {
	groups := make(map[string][]map[string]any)
	for _, row := range gdf.df.rows {
		key := gdf.groupKey(row)
		groups[key] = append(groups[key], row)
	}
	return groups
}

// Agg performs a custom aggregation on the specified column.
// The result column is named "agg_{column}".
// Returns an error if the specified column does not exist in the DataFrame.
//
// Example:
//
//	median, err := grouped.Agg("amount", func(values []any) any {
//	    sorted := sortValues(values)
//	    return sorted[len(sorted)/2]
//	})
func (gdf *GroupedDataFrame) Agg(column string, fn AggFunc) (*DataFrame, error) {
	if err := gdf.validateColumn(column); err != nil {
		return nil, err
	}
	return gdf.aggregate(column, fn, "agg_"+column), nil
}

// validateColumn checks if the specified column exists in the DataFrame.
func (gdf *GroupedDataFrame) validateColumn(column string) error {
	for _, col := range gdf.df.columns {
		if col == column {
			return nil
		}
	}
	return fmt.Errorf("%w: %s", ErrColumnNotFound, column)
}

// aggregate is the internal implementation for all aggregation operations.
func (gdf *GroupedDataFrame) aggregate(column string, fn AggFunc, resultColumn string) *DataFrame {
	groups := gdf.buildGroups()

	// Build result columns: group columns + result column
	resultColumns := make([]string, 0, len(gdf.groups)+1)
	resultColumns = append(resultColumns, gdf.groups...)
	resultColumns = append(resultColumns, resultColumn)

	// Process each group
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	slices.Sort(keys) // ensure deterministic order

	rows := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		groupRows := groups[key]
		if len(groupRows) == 0 {
			continue
		}

		// Extract values for the target column
		values := make([]any, len(groupRows))
		for i, row := range groupRows {
			values[i] = row[column]
		}

		// Build result row
		resultRow := make(map[string]any, len(resultColumns))

		// Copy group column values from first row of group
		for _, grpCol := range gdf.groups {
			resultRow[grpCol] = groupRows[0][grpCol]
		}

		// Apply aggregation function
		resultRow[resultColumn] = fn(values)

		rows = append(rows, resultRow)
	}

	return &DataFrame{
		columns: resultColumns,
		rows:    rows,
	}
}

// Count returns a DataFrame with the count of rows in each group.
// The result column is named "count".
//
// Example:
//
//	counts := df.GroupBy("category").Count()
func (gdf *GroupedDataFrame) Count() *DataFrame {
	groups := gdf.buildGroups()

	// Build result columns: group columns + "count"
	resultColumns := make([]string, 0, len(gdf.groups)+1)
	resultColumns = append(resultColumns, gdf.groups...)
	resultColumns = append(resultColumns, "count")

	// Process each group
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	rows := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		groupRows := groups[key]
		if len(groupRows) == 0 {
			continue
		}

		resultRow := make(map[string]any, len(resultColumns))
		for _, grpCol := range gdf.groups {
			resultRow[grpCol] = groupRows[0][grpCol]
		}
		resultRow["count"] = int64(len(groupRows))

		rows = append(rows, resultRow)
	}

	return &DataFrame{
		columns: resultColumns,
		rows:    rows,
	}
}

// Sum returns a DataFrame with the sum of values in the specified column for each group.
// The result column is named "sum_{column}".
// Returns an error if the specified column does not exist in the DataFrame.
//
// Example:
//
//	totals, err := df.GroupBy("category").Sum("amount")
func (gdf *GroupedDataFrame) Sum(column string) (*DataFrame, error) {
	if err := gdf.validateColumn(column); err != nil {
		return nil, err
	}
	if err := gdf.requireNumeric(column, "sum"); err != nil {
		return nil, err
	}
	return gdf.aggregate(column, AggSum, "sum_"+column), nil
}

// Mean returns a DataFrame with the mean of values in the specified column for each group.
// The result column is named "mean_{column}".
// Returns an error if the specified column does not exist in the DataFrame.
//
// Example:
//
//	averages, err := df.GroupBy("category").Mean("amount")
func (gdf *GroupedDataFrame) Mean(column string) (*DataFrame, error) {
	if err := gdf.validateColumn(column); err != nil {
		return nil, err
	}
	if err := gdf.requireNumeric(column, "mean"); err != nil {
		return nil, err
	}
	return gdf.aggregate(column, AggMean, "mean_"+column), nil
}

// Min returns a DataFrame with the minimum value in the specified column for each group.
// The result column is named "min_{column}".
// Returns an error if the specified column does not exist in the DataFrame.
//
// Example:
//
//	minimums, err := df.GroupBy("category").Min("amount")
func (gdf *GroupedDataFrame) Min(column string) (*DataFrame, error) {
	if err := gdf.validateColumn(column); err != nil {
		return nil, err
	}
	return gdf.aggregate(column, AggMin, "min_"+column), nil
}

// Max returns a DataFrame with the maximum value in the specified column for each group.
// The result column is named "max_{column}".
// Returns an error if the specified column does not exist in the DataFrame.
//
// Example:
//
//	maximums, err := df.GroupBy("category").Max("amount")
func (gdf *GroupedDataFrame) Max(column string) (*DataFrame, error) {
	if err := gdf.validateColumn(column); err != nil {
		return nil, err
	}
	return gdf.aggregate(column, AggMax, "max_"+column), nil
}

// requireNumeric reports an error when nothing in the column is a number, so an
// aggregate that has no answer says so instead of returning one.
//
// Sum over such a column answered 0 for every group, which is indistinguishable
// from a real total of zero, and Mean answered nil. Both are what a mistyped
// column name that happens to exist, or a column of text, looks like — the
// caller learns nothing from the result.
//
// A column holding some numbers is not refused: skipping the values that are
// not is what these aggregates document, and the answer is still built from
// data. Only a column with no number in it at all has no answer to give.
func (gdf *GroupedDataFrame) requireNumeric(column, aggregate string) error {
	for _, row := range gdf.df.rows {
		if _, ok := toFloat64(row[column]); ok {
			return nil
		}
	}
	return fmt.Errorf("cannot %s column %q: no value in it is a number", aggregate, column)
}

// Built-in aggregation functions
//
// These functions handle type conversion automatically:
//   - Numeric types (int, int8-64, uint, uint8-64, float32, float64) are converted to float64
//   - Non-numeric types (string, bool, nil, etc.) are ignored in calculations
//   - Returns nil when no valid numeric values exist (except AggSum which returns 0.0)

// AggCount counts the number of non-nil values.
// Returns int64 count. Note: this counts all non-nil values, not just numeric ones.
var AggCount AggFunc = func(values []any) any {
	count := 0
	for _, v := range values {
		if v != nil {
			count++
		}
	}
	return int64(count)
}

// AggSum calculates the sum of numeric values.
// Non-numeric values (including nil, strings, bools) are silently ignored.
// Returns nil if no value was numeric, and float64 otherwise.
//
// nil rather than 0.0, because a 0.0 that means "nothing here was a number" and
// a 0.0 that means "the numbers added to zero" are different answers and looked
// identical. It is also what AggMean, AggMin and AggMax already return for a
// group with nothing to work on.
var AggSum AggFunc = func(values []any) any {
	sum := 0.0
	found := false
	for _, v := range values {
		if f, ok := toFloat64(v); ok {
			sum += f
			found = true
		}
	}
	if !found {
		return nil
	}
	return sum
}

// AggMean calculates the arithmetic mean of numeric values.
// Non-numeric values (including nil, strings, bools) are silently ignored.
// Returns nil if no numeric values exist. Otherwise returns float64.
var AggMean AggFunc = func(values []any) any {
	sum := 0.0
	count := 0
	for _, v := range values {
		if f, ok := toFloat64(v); ok {
			sum += f
			count++
		}
	}
	if count == 0 {
		return nil
	}
	return sum / float64(count)
}

// AggMin finds the smallest value, following SQLite's ordering: a number is
// smaller than any text, and text compares lexically. Returns float64 when the
// group holds a number, string when it holds only text, and nil when it holds
// neither.
//
// Text used to be ignored, so the minimum of a column of words was nil — an
// answer the data had, discarded. SQLite's MIN over the same column returns the
// lexically smallest string.
var AggMin AggFunc = func(values []any) any {
	minNum := math.MaxFloat64
	foundNum := false
	var minText string
	foundText := false
	for _, v := range values {
		if f, ok := toFloat64(v); ok {
			if f < minNum {
				minNum = f
			}
			foundNum = true
			continue
		}
		if s, ok := aggText(v); ok {
			if !foundText || s < minText {
				minText = s
			}
			foundText = true
		}
	}
	// A number sorts before any text, so text decides only when there is no
	// number at all.
	if foundNum {
		return minNum
	}
	if foundText {
		return minText
	}
	return nil
}

// AggMax finds the largest value, following the same ordering as AggMin: any
// text is larger than any number, so text decides whenever the group holds some.
var AggMax AggFunc = func(values []any) any {
	maxNum := -math.MaxFloat64
	foundNum := false
	var maxText string
	foundText := false
	for _, v := range values {
		if f, ok := toFloat64(v); ok {
			if f > maxNum {
				maxNum = f
			}
			foundNum = true
			continue
		}
		if s, ok := aggText(v); ok {
			if !foundText || s > maxText {
				maxText = s
			}
			foundText = true
		}
	}
	if foundText {
		return maxText
	}
	if foundNum {
		return maxNum
	}
	return nil
}

// aggText reports the text form of a value that is not a number, and whether
// there is one. A nil is not text: it is the absence of a value, which is what
// SQLite's aggregates skip.
func aggText(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	default:
		return "", false
	}
}

// toFloat64 attempts to convert a value to float64.
// Returns the converted value and true if successful, or 0 and false otherwise.
func toFloat64(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case int16:
		return float64(val), true
	case int8:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint64:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint8:
		return float64(val), true
	default:
		return 0, false
	}
}
