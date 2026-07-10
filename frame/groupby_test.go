package frame

import (
	"errors"
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

	t.Run("returns nil for non-numeric values only", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": "text"},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		result, err := grouped.Mean("value")
		require.NoError(t, err)

		records := result.ToRecords()
		assert.Nil(t, records[0]["mean_value"])
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

	t.Run("returns nil for non-numeric values only", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "A", "value": "text"},
		})

		grouped, err := df.GroupBy("category")
		require.NoError(t, err)
		result, err := grouped.Min("value")
		require.NoError(t, err)

		records := result.ToRecords()
		assert.Nil(t, records[0]["min_value"])
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

	t.Run("AggMin returns nil for no numeric values", func(t *testing.T) {
		t.Parallel()

		values := []any{"text", nil}
		result := AggMin(values)

		assert.Nil(t, result)
	})

	t.Run("AggMax returns nil for no numeric values", func(t *testing.T) {
		t.Parallel()

		values := []any{"text", nil}
		result := AggMax(values)

		assert.Nil(t, result)
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
