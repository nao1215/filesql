//go:build benchmark

package filesql

import (
	"context"
	"path/filepath"
	"testing"
)

// BenchmarkOpenContext benchmarks the OpenContext function with a large CSV file.
// This benchmark uses testdata/benchmark/customers100000.csv (100,000 rows).
//
// Run with: make benchmark
func BenchmarkOpenContext(b *testing.B) {
	csvPath := filepath.Join("testdata", "benchmark", "customers100000.csv")

	b.ResetTimer()
	for b.Loop() {
		db, err := OpenContext(context.Background(), csvPath)
		if err != nil {
			b.Fatalf("OpenContext failed: %v", err)
		}
		if err := db.Close(); err != nil {
			b.Fatalf("db.Close failed: %v", err)
		}
	}
}

// BenchmarkOpenContextParallel benchmarks the OpenContext function in parallel.
// This benchmark tests concurrent performance with multiple goroutines.
func BenchmarkOpenContextParallel(b *testing.B) {
	csvPath := filepath.Join("testdata", "benchmark", "customers100000.csv")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			db, err := OpenContext(context.Background(), csvPath)
			if err != nil {
				b.Fatalf("OpenContext failed: %v", err)
			}
			if err := db.Close(); err != nil {
				b.Fatalf("db.Close failed: %v", err)
			}
		}
	})
}
