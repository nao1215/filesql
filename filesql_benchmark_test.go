//go:build benchmark

package filesql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/xuri/excelize/v2"
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

// BenchmarkOverwriteWorkbook benchmarks an in-place save of an Excel workbook,
// which is the write path that reads the file it is replacing and writes the
// loaded sheets onto it rather than building a new workbook.
//
// The workbook is rebuilt each iteration because the save consumes it, and the
// timer is stopped while that happens, so what is measured is the load and the
// save rather than the fixture.
func BenchmarkOverwriteWorkbook(b *testing.B) {
	const rows, columns = 5000, 8

	dir := b.TempDir()
	path := filepath.Join(dir, "book.xlsx")

	write := func() {
		book := excelize.NewFile()
		header := make([]any, columns)
		for c := range columns {
			header[c] = fmt.Sprintf("col%d", c)
		}
		if err := book.SetSheetRow("Sheet1", "A1", &header); err != nil {
			b.Fatalf("SetSheetRow failed: %v", err)
		}
		cells := make([]any, columns)
		for r := range rows {
			for c := range columns {
				cells[c] = fmt.Sprintf("r%dc%d", r, c)
			}
			if err := book.SetSheetRow("Sheet1", fmt.Sprintf("A%d", r+2), &cells); err != nil {
				b.Fatalf("SetSheetRow failed: %v", err)
			}
		}
		if err := book.SaveAs(path); err != nil {
			b.Fatalf("SaveAs failed: %v", err)
		}
		if err := book.Close(); err != nil {
			b.Fatalf("Close failed: %v", err)
		}
	}

	for b.Loop() {
		b.StopTimer()
		write()
		b.StartTimer()

		validated, err := NewBuilder().AddPath(path).EnableAutoSave("").Build(context.Background())
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

// BenchmarkOpenParquet benchmarks loading a Parquet file of 100,000 rows, which
// is the read path the column-chunk bound sits on.
//
// The file is written from the CSV of the same rows the first time the benchmark
// runs, outside the timer, so no binary fixture of this size is kept in the
// repository.
func BenchmarkOpenParquet(b *testing.B) {
	path := filepath.Join(b.TempDir(), "customers.parquet")
	writeBenchmarkParquet(b, path)

	b.ResetTimer()
	for b.Loop() {
		db, err := OpenContext(context.Background(), path)
		if err != nil {
			b.Fatalf("OpenContext failed: %v", err)
		}
		if err := db.Close(); err != nil {
			b.Fatalf("db.Close failed: %v", err)
		}
	}
}

// BenchmarkDumpParquet measures the Parquet write path: the 100,000-row
// benchmark table dumped as one Parquet file per iteration.
func BenchmarkDumpParquet(b *testing.B) {
	db, err := OpenContext(context.Background(), filepath.Join("testdata", "benchmark", "customers100000.csv"))
	if err != nil {
		b.Fatalf("OpenContext failed: %v", err)
	}
	defer db.Close()
	dir := b.TempDir()

	b.ResetTimer()
	for b.Loop() {
		if err := DumpDatabase(db, dir, NewDumpOptions().WithFormat(OutputFormatParquet)); err != nil {
			b.Fatalf("DumpDatabase failed: %v", err)
		}
	}
}

// writeBenchmarkParquet converts the benchmark CSV into a Parquet file at path.
func writeBenchmarkParquet(b *testing.B, path string) {
	b.Helper()

	db, err := OpenContext(context.Background(), filepath.Join("testdata", "benchmark", "customers100000.csv"))
	if err != nil {
		b.Fatalf("OpenContext failed: %v", err)
	}
	defer db.Close()

	dir := filepath.Dir(path)
	if err := DumpDatabase(db, dir, NewDumpOptions().WithFormat(OutputFormatParquet)); err != nil {
		b.Fatalf("DumpDatabase failed: %v", err)
	}
	if err := os.Rename(filepath.Join(dir, "customers100000.parquet"), path); err != nil {
		b.Fatalf("rename failed: %v", err)
	}
}
