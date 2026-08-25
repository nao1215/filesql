//go:build benchmark

package frame

import (
	"path/filepath"
	"strings"
	"testing"
)

// Benchmark data sizes
const (
	smallRows  = 100
	mediumRows = 1000
	largeRows  = 10000
)

// generateCSVData creates CSV data with the specified number of rows.
func generateCSVData(rows int) string {
	var sb strings.Builder
	sb.WriteString("id,name,category,amount,price\n")
	categories := []string{"A", "B", "C", "D", "E"}
	for i := range rows {
		cat := categories[i%len(categories)]
		sb.WriteString(strings.Join([]string{
			itoa(i),
			"Product" + itoa(i),
			cat,
			itoa((i % 100) + 1),
			itoa((i%1000)+1) + ".50",
		}, ","))
		sb.WriteString("\n")
	}
	return sb.String()
}

// itoa is a simple int to string conversion for benchmarks.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	if i < 0 {
		return "-" + itoa(-i)
	}
	var digits []byte
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return string(digits)
}

// generateRecords creates a slice of maps with the specified number of rows.
func generateRecords(rows int) []map[string]any {
	records := make([]map[string]any, rows)
	categories := []string{"A", "B", "C", "D", "E"}
	for i := range rows {
		records[i] = map[string]any{
			"id":       int64(i),
			"name":     "Product" + itoa(i),
			"category": categories[i%len(categories)],
			"amount":   int64((i % 100) + 1),
			"price":    float64((i%1000)+1) + 0.50,
		}
	}
	return records
}

// BenchmarkNewDataFrame benchmarks CSV parsing.
func BenchmarkNewDataFrame(b *testing.B) {
	benchmarks := []struct {
		name string
		rows int
	}{
		{"Small_100rows", smallRows},
		{"Medium_1000rows", mediumRows},
		{"Large_10000rows", largeRows},
	}

	for _, bm := range benchmarks {
		csvData := generateCSVData(bm.rows)
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				reader := strings.NewReader(csvData)
				_, err := NewDataFrame(reader, CSV)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkNewDataFrameFromRecords benchmarks DataFrame creation from records.
func BenchmarkNewDataFrameFromRecords(b *testing.B) {
	benchmarks := []struct {
		name string
		rows int
	}{
		{"Small_100rows", smallRows},
		{"Medium_1000rows", mediumRows},
		{"Large_10000rows", largeRows},
	}

	for _, bm := range benchmarks {
		records := generateRecords(bm.rows)
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				_ = NewDataFrameFromRecords(records)
			}
		})
	}
}

// BenchmarkSort benchmarks the Sort operation, whose comparator is the hot
// path of every ordered frame.
func BenchmarkSort(b *testing.B) {
	benchmarks := []struct {
		name string
		rows int
	}{
		{"Small_100rows", smallRows},
		{"Medium_1000rows", mediumRows},
		{"Large_10000rows", largeRows},
	}

	for _, bm := range benchmarks {
		records := generateRecords(bm.rows)
		df := NewDataFrameFromRecords(records)
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				if _, err := df.Sort("amount", Ascending); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFilter benchmarks the Filter operation.
func BenchmarkFilter(b *testing.B) {
	benchmarks := []struct {
		name string
		rows int
	}{
		{"Small_100rows", smallRows},
		{"Medium_1000rows", mediumRows},
		{"Large_10000rows", largeRows},
	}

	filterFn := func(row map[string]any) bool {
		amount, ok := row["amount"].(int64)
		return ok && amount > 50
	}

	for _, bm := range benchmarks {
		records := generateRecords(bm.rows)
		df := NewDataFrameFromRecords(records)
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				_ = df.Filter(filterFn)
			}
		})
	}
}

// BenchmarkSelect benchmarks the Select operation.
func BenchmarkSelect(b *testing.B) {
	benchmarks := []struct {
		name string
		rows int
	}{
		{"Small_100rows", smallRows},
		{"Medium_1000rows", mediumRows},
		{"Large_10000rows", largeRows},
	}

	for _, bm := range benchmarks {
		records := generateRecords(bm.rows)
		df := NewDataFrameFromRecords(records)
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				_, _ = df.Select("id", "name", "amount")
			}
		})
	}
}

// BenchmarkMutate benchmarks the Mutate operation.
func BenchmarkMutate(b *testing.B) {
	benchmarks := []struct {
		name string
		rows int
	}{
		{"Small_100rows", smallRows},
		{"Medium_1000rows", mediumRows},
		{"Large_10000rows", largeRows},
	}

	mutateFn := func(row map[string]any) any {
		amount, _ := row["amount"].(int64)
		price, _ := row["price"].(float64)
		return float64(amount) * price
	}

	for _, bm := range benchmarks {
		records := generateRecords(bm.rows)
		df := NewDataFrameFromRecords(records)
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				_ = df.Mutate("total", mutateFn)
			}
		})
	}
}

// BenchmarkGroupBySum benchmarks GroupBy with Sum aggregation.
func BenchmarkGroupBySum(b *testing.B) {
	benchmarks := []struct {
		name string
		rows int
	}{
		{"Small_100rows", smallRows},
		{"Medium_1000rows", mediumRows},
		{"Large_10000rows", largeRows},
	}

	for _, bm := range benchmarks {
		records := generateRecords(bm.rows)
		df := NewDataFrameFromRecords(records)
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				grouped, _ := df.GroupBy("category")
				_, _ = grouped.Sum("amount")
			}
		})
	}
}

// BenchmarkGroupByMean benchmarks GroupBy with Mean aggregation.
func BenchmarkGroupByMean(b *testing.B) {
	benchmarks := []struct {
		name string
		rows int
	}{
		{"Small_100rows", smallRows},
		{"Medium_1000rows", mediumRows},
		{"Large_10000rows", largeRows},
	}

	for _, bm := range benchmarks {
		records := generateRecords(bm.rows)
		df := NewDataFrameFromRecords(records)
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				grouped, _ := df.GroupBy("category")
				_, _ = grouped.Mean("price")
			}
		})
	}
}

// BenchmarkGroupByCount benchmarks GroupBy with Count aggregation.
func BenchmarkGroupByCount(b *testing.B) {
	benchmarks := []struct {
		name string
		rows int
	}{
		{"Small_100rows", smallRows},
		{"Medium_1000rows", mediumRows},
		{"Large_10000rows", largeRows},
	}

	for _, bm := range benchmarks {
		records := generateRecords(bm.rows)
		df := NewDataFrameFromRecords(records)
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				grouped, _ := df.GroupBy("category")
				_ = grouped.Count()
			}
		})
	}
}

// BenchmarkChainedOperations benchmarks a realistic workflow with multiple operations.
func BenchmarkChainedOperations(b *testing.B) {
	benchmarks := []struct {
		name string
		rows int
	}{
		{"Small_100rows", smallRows},
		{"Medium_1000rows", mediumRows},
		{"Large_10000rows", largeRows},
	}

	for _, bm := range benchmarks {
		records := generateRecords(bm.rows)
		df := NewDataFrameFromRecords(records)
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				// Filter -> Mutate -> Select -> GroupBy -> Sum
				result := df.Filter(func(row map[string]any) bool {
					amount, ok := row["amount"].(int64)
					return ok && amount > 20
				}).Mutate("total", func(row map[string]any) any {
					amount, _ := row["amount"].(int64)
					price, _ := row["price"].(float64)
					return float64(amount) * price
				})

				selected, _ := result.Select("category", "total")
				grouped, _ := selected.GroupBy("category")
				_, _ = grouped.Sum("total")
			}
		})
	}
}

// BenchmarkToRecords benchmarks the ToRecords operation.
func BenchmarkToRecords(b *testing.B) {
	benchmarks := []struct {
		name string
		rows int
	}{
		{"Small_100rows", smallRows},
		{"Medium_1000rows", mediumRows},
		{"Large_10000rows", largeRows},
	}

	for _, bm := range benchmarks {
		records := generateRecords(bm.rows)
		df := NewDataFrameFromRecords(records)
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				_ = df.ToRecords()
			}
		})
	}
}

// BenchmarkGlobalAggregation benchmarks global aggregation (GroupBy with no columns).
func BenchmarkGlobalAggregation(b *testing.B) {
	benchmarks := []struct {
		name string
		rows int
	}{
		{"Small_100rows", smallRows},
		{"Medium_1000rows", mediumRows},
		{"Large_10000rows", largeRows},
	}

	for _, bm := range benchmarks {
		records := generateRecords(bm.rows)
		df := NewDataFrameFromRecords(records)
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				grouped, _ := df.GroupBy()
				_, _ = grouped.Sum("amount")
			}
		})
	}
}

// BenchmarkToCSV measures writing a frame out as CSV, which is the encoding
// half of the round trip BenchmarkNewDataFrame measures the reading half of.
func BenchmarkToCSV(b *testing.B) {
	benchmarks := []struct {
		name string
		rows int
	}{
		{"small", smallRows},
		{"medium", mediumRows},
		{"large", largeRows},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			df, err := NewDataFrame(strings.NewReader(generateCSVData(bm.rows)), CSV)
			if err != nil {
				b.Fatal(err)
			}
			path := filepath.Join(b.TempDir(), "out.csv")

			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				if err := df.ToCSV(path); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkToTSV measures the same write as tab-separated records, which take
// a different path: TSV has no quoting, so it is written literally rather than
// through encoding/csv.
func BenchmarkToTSV(b *testing.B) {
	df, err := NewDataFrame(strings.NewReader(generateCSVData(largeRows)), CSV)
	if err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(b.TempDir(), "out.tsv")

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if err := df.ToTSV(path); err != nil {
			b.Fatal(err)
		}
	}
}
