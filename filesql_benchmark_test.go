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

// BenchmarkOpenWithAutoSave benchmarks opening the same file with auto-save
// enabled, which is what BenchmarkOpenContext does not cover.
//
// The auto-save path used to close the loader database and stream every file a
// second time into a connection of its own, so opening with auto-save cost two
// loads where opening without it cost one. It now keeps the loaded data, and
// this is where that shows.
func BenchmarkOpenWithAutoSave(b *testing.B) {
	csvPath := filepath.Join("testdata", "benchmark", "customers100000.csv")
	outputDir := b.TempDir()

	b.ResetTimer()
	for b.Loop() {
		validated, err := NewBuilder().AddPath(csvPath).EnableAutoSave(outputDir).Build(context.Background())
		if err != nil {
			b.Fatalf("Build failed: %v", err)
		}
		db, err := validated.Open(context.Background())
		if err != nil {
			b.Fatalf("Open failed: %v", err)
		}
		if err := db.Close(); err != nil {
			b.Fatalf("db.Close failed: %v", err)
		}
	}
}
