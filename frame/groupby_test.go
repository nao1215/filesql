package frame

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGroupedDataFrame_Count(t *testing.T) {
	t.Parallel()

	t.Run("counts rows per group", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(1)},
			{"category": "A", "value": int64(2)},
			{"category": "B", "value": int64(3)},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		result := grouped.Count()

		assert.Equal(t, 2, result.Len())
		assert.Equal(t, []string{"category", "count"}, result.Columns())

		records := result.ToRecords()
		// Results are sorted by group key
		assert.Equal(t, "A", records[0]["category"])
		assert.Equal(t, int64(2), records[0]["count"])
		assert.Equal(t, "B", records[1]["category"])
		assert.Equal(t, int64(1), records[1]["count"])
	})

	t.Run("handles empty DataFrame", func(t *testing.T) {
		t.Parallel()

		// Empty DataFrame with no columns - GroupBy with any column should error
		df := NewDataFrameFromRecords([]map[string]any{})

		_, err := df.GroupBy("category")
		require.ErrorIs(t, err, ErrColumnNotFound)
	})

	t.Run("handles DataFrame with zero rows but valid columns", func(t *testing.T) {
		t.Parallel()

		// Create DataFrame with columns but no data rows
		// Use Filter to create empty result while preserving columns
		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "amount": int64(100)},
		})
		emptyDf := df.Filter(func(_ map[string]any) bool {
			return false // Filter out all rows
		})

		grouped, err := emptyDf.GroupBy("category")
		require.NoError(t, err)
		result := grouped.Count()

		assert.Equal(t, 0, result.Len())
	})
}

func TestGroupedDataFrame_Sum(t *testing.T) {
	t.Parallel()

	t.Run("sums values per group", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "amount": int64(100)},
			{"category": "A", "amount": int64(200)},
			{"category": "B", "amount": int64(50)},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		result, err := grouped.Sum("amount")
		require.NoError(t, err)

		assert.Equal(t, 2, result.Len())
		assert.Equal(t, []string{"category", "sum_amount"}, result.Columns())

		records := result.ToRecords()
		assert.Equal(t, "A", records[0]["category"])
		assert.Equal(t, 300.0, records[0]["sum_amount"])
		assert.Equal(t, "B", records[1]["category"])
		assert.Equal(t, 50.0, records[1]["sum_amount"])
	})

	t.Run("handles float values", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "amount": 1.5},
			{"category": "A", "amount": 2.5},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		result, err := grouped.Sum("amount")
		require.NoError(t, err)

		records := result.ToRecords()
		assert.Equal(t, 4.0, records[0]["sum_amount"])
	})

	t.Run("ignores non-numeric values", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "amount": int64(100)},
			{"category": "A", "amount": "not a number"},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		result, err := grouped.Sum("amount")
		require.NoError(t, err)

		records := result.ToRecords()
		assert.Equal(t, 100.0, records[0]["sum_amount"])
	})
}

func TestGroupedDataFrame_Mean(t *testing.T) {
	t.Parallel()

	t.Run("calculates mean per group", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(10)},
			{"category": "A", "value": int64(20)},
			{"category": "A", "value": int64(30)},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		result, err := grouped.Mean("value")
		require.NoError(t, err)

		assert.Equal(t, []string{"category", "mean_value"}, result.Columns())

		records := result.ToRecords()
		assert.Equal(t, 20.0, records[0]["mean_value"])
	})

	// A column with no number in it has no mean, and answering nil said so only
	// to a caller who thought to look.
	t.Run("refuses a column with nothing numeric in it", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": "text"},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		_, err = grouped.Mean("value")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "value")
	})
}

func TestGroupedDataFrame_Min(t *testing.T) {
	t.Parallel()

	t.Run("finds minimum per group", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(30)},
			{"category": "A", "value": int64(10)},
			{"category": "A", "value": int64(20)},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		result, err := grouped.Min("value")
		require.NoError(t, err)

		assert.Equal(t, []string{"category", "min_value"}, result.Columns())

		records := result.ToRecords()
		assert.Equal(t, 10.0, records[0]["min_value"])
	})

	// Text has a minimum, and it used to be thrown away for a nil. SQLite's MIN
	// over the same column answers the lexically smallest string.
	t.Run("returns the lexically smallest text", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": "banana"},
			{"category": "A", "value": "apple"},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		result, err := grouped.Min("value")
		require.NoError(t, err)

		records := result.ToRecords()
		assert.Equal(t, "apple", records[0]["min_value"])
	})
}

func TestGroupedDataFrame_Max(t *testing.T) {
	t.Parallel()

	t.Run("finds maximum per group", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(10)},
			{"category": "A", "value": int64(30)},
			{"category": "A", "value": int64(20)},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		result, err := grouped.Max("value")
		require.NoError(t, err)

		assert.Equal(t, []string{"category", "max_value"}, result.Columns())

		records := result.ToRecords()
		assert.Equal(t, 30.0, records[0]["max_value"])
	})
}

func TestGroupedDataFrame_Agg(t *testing.T) {
	t.Parallel()

	t.Run("applies custom aggregation function", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(1)},
			{"category": "A", "value": int64(2)},
			{"category": "A", "value": int64(3)},
		})

		// Custom function: return first value
		first := func(values []any) any {
			if len(values) > 0 {
				return values[0]
			}
			return nil
		}

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		result, err := grouped.Agg("value", first)
		require.NoError(t, err)

		assert.Equal(t, []string{"category", "agg_value"}, result.Columns())

		records := result.ToRecords()
		assert.Equal(t, int64(1), records[0]["agg_value"])
	})
}

func TestGroupedDataFrame_MultipleGroupColumns(t *testing.T) {
	t.Parallel()

	t.Run("groups by multiple columns", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"region": "East", "category": "A", "amount": int64(100)},
			{"region": "East", "category": "A", "amount": int64(200)},
			{"region": "East", "category": "B", "amount": int64(50)},
			{"region": "West", "category": "A", "amount": int64(150)},
		})

		grouped, err := df.GroupBy("region", "category")
		require.NoError(t, err)
		result, err := grouped.Sum("amount")
		require.NoError(t, err)

		assert.Equal(t, []string{"region", "category", "sum_amount"}, result.Columns())
		assert.Equal(t, 3, result.Len())

		records := result.ToRecords()
		// Check that groups are created correctly
		foundEastA := false
		for _, r := range records {
			if r["region"] == "East" && r["category"] == "A" {
				assert.Equal(t, 300.0, r["sum_amount"])
				foundEastA = true
			}
		}
		assert.True(t, foundEastA, "Should have East-A group")
	})
}

func TestGroupBy_ColumnNotFound(t *testing.T) {
	t.Parallel()

	t.Run("returns error for non-existent column", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(1)},
		})

		_, err := df.GroupBy("nonexistent")

		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrColumnNotFound))
		assert.Contains(t, err.Error(), "nonexistent")
	})

	t.Run("returns error for any non-existent column in multi-column groupby", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(1)},
		})

		_, err := df.GroupBy("category", "nonexistent")

		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrColumnNotFound))
	})
}

func TestToFloat64(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		input    any
		expected float64
		ok       bool
	}{
		{"float64", 3.14, 3.14, true},
		{"float32", float32(3.14), float64(float32(3.14)), true},
		{"int", 42, 42.0, true},
		{"int64", int64(42), 42.0, true},
		{"int32", int32(42), 42.0, true},
		{"int16", int16(42), 42.0, true},
		{"int8", int8(42), 42.0, true},
		{"uint", uint(42), 42.0, true},
		{"uint64", uint64(42), 42.0, true},
		{"uint32", uint32(42), 42.0, true},
		{"uint16", uint16(42), 42.0, true},
		{"uint8", uint8(42), 42.0, true},
		{"string", "42", 0, false},
		{"nil", nil, 0, false},
		{"bool", true, 0, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, ok := toFloat64(tc.input)

			assert.Equal(t, tc.ok, ok)
			if ok {
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestAggFunctions(t *testing.T) {
	t.Parallel()

	t.Run("AggCount counts non-nil values", func(t *testing.T) {
		t.Parallel()

		values := []any{1, nil, 3, nil, 5}
		result := AggCount(values)

		assert.Equal(t, int64(3), result)
	})

	t.Run("AggSum handles mixed types", func(t *testing.T) {
		t.Parallel()

		values := []any{int64(10), 20.5, "skip", int32(5)}
		result := AggSum(values)

		assert.Equal(t, 35.5, result)
	})

	t.Run("AggMean returns nil for empty numeric values", func(t *testing.T) {
		t.Parallel()

		values := []any{"a", "b", nil}
		result := AggMean(values)

		assert.Nil(t, result)
	})

	t.Run("AggMin returns the smallest text when there is no number", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "text", AggMin([]any{"text", nil}))
		assert.Equal(t, "a", AggMin([]any{"b", "a"}))
	})

	t.Run("AggMin returns nil when there is nothing at all", func(t *testing.T) {
		t.Parallel()

		assert.Nil(t, AggMin([]any{nil, nil}))
	})

	t.Run("AggMax returns the largest text when there is no number", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "text", AggMax([]any{"text", nil}))
		assert.Equal(t, "b", AggMax([]any{"a", "b"}))
	})

	t.Run("AggMax returns nil when there is nothing at all", func(t *testing.T) {
		t.Parallel()

		assert.Nil(t, AggMax([]any{nil, nil}))
	})

	// SQLite's ordering: any text is larger than any number, and a number is
	// smaller than any text.
	t.Run("text outranks a number for Max and loses to one for Min", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "b", AggMax([]any{1, "b"}))
		assert.InDelta(t, 1.0, AggMin([]any{1, "b"}), 0)
	})
}

func TestGroupBy_NoArguments(t *testing.T) {
	t.Parallel()

	t.Run("global aggregation with Count", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(10)},
			{"category": "B", "value": int64(20)},
			{"category": "A", "value": int64(30)},
		})

		grouped, err := df.GroupBy()
		require.NoError(t, err)
		result := grouped.Count()

		assert.Equal(t, 1, result.Len())
		records := result.ToRecords()
		assert.Equal(t, int64(3), records[0]["count"])
	})

	t.Run("global aggregation with Sum", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"value": int64(10)},
			{"value": int64(20)},
			{"value": int64(30)},
		})

		grouped, err := df.GroupBy()
		require.NoError(t, err)
		result, err := grouped.Sum("value")
		require.NoError(t, err)

		assert.Equal(t, 1, result.Len())
		records := result.ToRecords()
		assert.Equal(t, 60.0, records[0]["sum_value"])
	})

	t.Run("global aggregation with Mean", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"value": int64(10)},
			{"value": int64(20)},
			{"value": int64(30)},
		})

		grouped, err := df.GroupBy()
		require.NoError(t, err)
		result, err := grouped.Mean("value")
		require.NoError(t, err)

		assert.Equal(t, 1, result.Len())
		records := result.ToRecords()
		assert.Equal(t, 20.0, records[0]["mean_value"])
	})

	t.Run("global aggregation with Min", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"value": int64(30)},
			{"value": int64(10)},
			{"value": int64(20)},
		})

		grouped, err := df.GroupBy()
		require.NoError(t, err)
		result, err := grouped.Min("value")
		require.NoError(t, err)

		assert.Equal(t, 1, result.Len())
		records := result.ToRecords()
		assert.Equal(t, 10.0, records[0]["min_value"])
	})

	t.Run("global aggregation with Max", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"value": int64(10)},
			{"value": int64(30)},
			{"value": int64(20)},
		})

		grouped, err := df.GroupBy()
		require.NoError(t, err)
		result, err := grouped.Max("value")
		require.NoError(t, err)

		assert.Equal(t, 1, result.Len())
		records := result.ToRecords()
		assert.Equal(t, 30.0, records[0]["max_value"])
	})
}

func TestGroupBy_NilAndMissingValues(t *testing.T) {
	t.Parallel()

	t.Run("nil values in group key generate '<nil>' string key", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(10)},
			{"category": nil, "value": int64(20)},
			{"category": nil, "value": int64(30)},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		result, err := grouped.Sum("value")
		require.NoError(t, err)

		assert.Equal(t, 2, result.Len())

		// Find the nil group
		records := result.ToRecords()
		var nilGroupSum float64
		for _, r := range records {
			if r["category"] == nil {
				nilGroupSum, _ = r["sum_value"].(float64) //nolint:errcheck // test code
			}
		}
		assert.Equal(t, 50.0, nilGroupSum) // 20 + 30
	})

	t.Run("missing values in group column are treated as nil", func(t *testing.T) {
		t.Parallel()

		// Create records where some rows don't have the group column
		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(10)},
			{"value": int64(20)}, // No "category" key
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		result, err := grouped.Sum("value")
		require.NoError(t, err)

		assert.Equal(t, 2, result.Len())
	})

	t.Run("nil values in aggregation column are ignored", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(10)},
			{"category": "A", "value": nil},
			{"category": "A", "value": int64(30)},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		result, err := grouped.Sum("value")
		require.NoError(t, err)

		records := result.ToRecords()
		assert.Equal(t, 40.0, records[0]["sum_value"]) // 10 + 30 (nil ignored)
	})

	t.Run("missing aggregation column values are nil", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(10)},
			{"category": "A"}, // No "value" key
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		result, err := grouped.Sum("value")
		require.NoError(t, err)

		records := result.ToRecords()
		assert.Equal(t, 10.0, records[0]["sum_value"]) // Only first row has value
	})
}

func TestGroupBy_NonExistentAggregationColumn(t *testing.T) {
	t.Parallel()

	t.Run("Sum on non-existent column returns error", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(10)},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		_, err = grouped.Sum("nonexistent")

		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrColumnNotFound))
		assert.Contains(t, err.Error(), "nonexistent")
	})

	t.Run("Mean on non-existent column returns error", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(10)},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		_, err = grouped.Mean("nonexistent")

		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrColumnNotFound))
	})

	t.Run("Min on non-existent column returns error", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(10)},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		_, err = grouped.Min("nonexistent")

		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrColumnNotFound))
	})

	t.Run("Max on non-existent column returns error", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(10)},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		_, err = grouped.Max("nonexistent")

		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrColumnNotFound))
	})

	t.Run("Agg on non-existent column returns error", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": int64(10)},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		_, err = grouped.Agg("nonexistent", AggSum)

		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrColumnNotFound))
	})
}

// TestAggregatesSayWhenTheyHaveNoAnswer covers the two ways an aggregate over a
// column of text used to answer as if it had one: Sum said 0, which is what a
// real total of zero says, and Min said nil for a column whose minimum the data
// plainly holds.
func TestAggregatesSayWhenTheyHaveNoAnswer(t *testing.T) {
	t.Parallel()

	t.Run("Sum refuses a column with nothing numeric in it", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrame(strings.NewReader("cat,val\na,x\na,y\n"), CSV)
		require.NoError(t, err)
		grouped, err := df.GroupBy("cat")
		require.NoError(t, err)

		_, err = grouped.Sum("val")

		require.Error(t, err, "0 for every group is what a real total of zero looks like")
		assert.Contains(t, err.Error(), "val")
	})

	// A column holding both numbers and text is a text column to the DataFrame,
	// so none of its values is a number and the aggregate has nothing to add.
	// It answered 0; it now says so.
	t.Run("Sum refuses a column of numbers mixed with text", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrame(strings.NewReader("cat,val\na,1\na,x\na,2\n"), CSV)
		require.NoError(t, err)
		grouped, err := df.GroupBy("cat")
		require.NoError(t, err)

		_, err = grouped.Sum("val")

		require.Error(t, err)
	})

	t.Run("Sum adds a numeric column as before", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrame(strings.NewReader("cat,val\na,1\na,2\n"), CSV)
		require.NoError(t, err)
		grouped, err := df.GroupBy("cat")
		require.NoError(t, err)

		result, err := grouped.Sum("val")

		require.NoError(t, err)
		assert.InDelta(t, 3.0, result.ToRecords()[0]["sum_val"], 0)
	})

	t.Run("Min and Max answer a text column the way SQLite does", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrame(strings.NewReader("k,s\na,banana\na,apple\n"), CSV)
		require.NoError(t, err)
		grouped, err := df.GroupBy("k")
		require.NoError(t, err)

		minimum, err := grouped.Min("s")
		require.NoError(t, err)
		maximum, err := grouped.Max("s")
		require.NoError(t, err)

		assert.Equal(t, "apple", minimum.ToRecords()[0]["min_s"])
		assert.Equal(t, "banana", maximum.ToRecords()[0]["max_s"])
	})

	t.Run("a group with nothing numeric gets nil, not zero", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrame(strings.NewReader("cat,val\na,1\nb,\n"), CSV)
		require.NoError(t, err)
		grouped, err := df.GroupBy("cat")
		require.NoError(t, err)

		result, err := grouped.Sum("val")
		require.NoError(t, err)

		byGroup := map[string]any{}
		for _, r := range result.ToRecords() {
			cat, ok := r["cat"].(string)
			require.True(t, ok, "the grouped column is text")
			byGroup[cat] = r["sum_val"]
		}
		assert.InDelta(t, 1.0, byGroup["a"], 0)
		assert.Nil(t, byGroup["b"], "the group held no value, which is not a total of zero")
	})
}
