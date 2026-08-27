//go:build benchmark

package filesql

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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

// BenchmarkOpenJSONL benchmarks the line-oriented JSON read, which is the third
// format whose records are lines and the one with no benchmark of its own. Its
// cost is a line at a time, so this is where the bound on a line shows.
func BenchmarkOpenJSONL(b *testing.B) {
	const rows = 100000

	var body bytes.Buffer
	for i := range rows {
		fmt.Fprintf(&body, `{"id":%d,"name":"customer%d","amount":%d.5}`+"\n", i, i, i)
	}
	lines := body.Bytes()

	b.ResetTimer()
	for b.Loop() {
		validated, err := buildForTest(

			context.Background(), NewBuilder().
				AddReader(bytes.NewReader(lines), "events", FileTypeJSONL))

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

// BenchmarkOpenJSON benchmarks loading the same rows as a JSON array, which is
// the shape JSONL does not cover: an array is decoded element by element by
// encoding/json rather than read a line at a time, and each element is read
// through the bound that stops one element from asking for the whole of a
// stream. This is where that bound's cost shows.
func BenchmarkOpenJSON(b *testing.B) {
	const rows = 100000

	var body bytes.Buffer
	body.WriteByte('[')
	for i := range rows {
		if i > 0 {
			body.WriteByte(',')
		}
		fmt.Fprintf(&body, `{"id":%d,"name":"customer%d","amount":%d.5}`, i, i, i)
	}
	body.WriteByte(']')
	document := body.Bytes()

	b.ResetTimer()
	for b.Loop() {
		validated, err := buildForTest(

			context.Background(), NewBuilder().
				AddReader(bytes.NewReader(document), "events", FileTypeJSON))

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

// BenchmarkOpenReader benchmarks loading the same rows through AddReader, which
// is the path BenchmarkOpenContext does not cover: a reader cannot be read
// twice, so its rows are staged as text and copied into the typed table once
// the last one has been read. The copy is where that path's extra cost is, and
// where the blank-cell rule is applied, so this is what measures both.
func BenchmarkOpenReader(b *testing.B) {
	csvPath := filepath.Join("testdata", "benchmark", "customers100000.csv")
	body, err := os.ReadFile(csvPath) //nolint:gosec // Benchmark fixture in the repository.
	if err != nil {
		b.Fatalf("read fixture failed: %v", err)
	}

	b.ResetTimer()
	for b.Loop() {
		validated, err := buildForTest(

			context.Background(), NewBuilder().
				AddReader(bytes.NewReader(body), "customers", FileTypeCSV))

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
		validated, err := buildForTest(context.Background(), NewBuilder().AddPath(csvPath).EnableAutoSave(outputDir))
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

// BenchmarkOverwriteWorkbookOfNumbers benchmarks the same in-place save over a
// workbook of numbers, which is where deciding whether a cell changed costs
// something: two text cells settle on the first comparison, while two numbers
// spelled differently have to be read as numbers before the answer is known.
func BenchmarkOverwriteWorkbookOfNumbers(b *testing.B) {
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
				// One column carries a decimal, which makes every column REAL
				// and every whole number in them a differently spelled cell.
				cells[c] = float64(r*columns + c)
			}
			cells[0] = float64(r) + 0.5
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

		validated, err := buildForTest(context.Background(), NewBuilder().AddPath(path).EnableAutoSave(""))
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

		validated, err := buildForTest(context.Background(), NewBuilder().AddPath(path).EnableAutoSave(""))
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

// BenchmarkDumpText measures the three text write paths against the same
// 100,000-row table: CSV through encoding/csv, TSV written literally, and LTSV
// built one labeled field at a time.
//
// The CSV path is also under BenchmarkOpenWithAutoSave, which measures a load
// and a save together; this measures the write on its own, and it is the only
// place the other two formats are measured at all.
func BenchmarkDumpText(b *testing.B) {
	db, err := OpenContext(context.Background(), filepath.Join("testdata", "benchmark", "customers100000.csv"))
	if err != nil {
		b.Fatalf("OpenContext failed: %v", err)
	}
	defer db.Close()

	formats := []struct {
		name   string
		format OutputFormat
	}{
		{name: "csv", format: OutputFormatCSV},
		{name: "tsv", format: OutputFormatTSV},
		{name: "ltsv", format: OutputFormatLTSV},
	}

	for _, tt := range formats {
		b.Run(tt.name, func(b *testing.B) {
			dir := b.TempDir()
			options := NewDumpOptions().WithFormat(tt.format)

			b.ResetTimer()
			for b.Loop() {
				if err := DumpDatabase(db, dir, options); err != nil {
					b.Fatalf("DumpDatabase failed: %v", err)
				}
			}
		})
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

// benchmarkCompressedCopy writes the benchmark CSV through a codec into b's
// temporary directory and returns the path, so a compressed load is measured
// against the same rows the uncompressed one uses.
func benchmarkCompressedCopy(b *testing.B, compression CompressionType) string {
	b.Helper()

	plain, err := os.ReadFile(filepath.Join("testdata", "benchmark", "customers100000.csv"))
	if err != nil {
		b.Fatalf("read the benchmark CSV: %v", err)
	}
	path := filepath.Join(b.TempDir(), "customers100000.csv"+compression.Extension())
	file, err := os.Create(path) //nolint:gosec // Path from b.TempDir()
	if err != nil {
		b.Fatalf("create %s: %v", path, err)
	}
	writer, closeWriter, err := NewCompressionHandler(compression).CreateWriter(file)
	if err != nil {
		b.Fatalf("create the %s writer: %v", compression.Extension(), err)
	}
	if _, err := writer.Write(plain); err != nil {
		b.Fatalf("write the compressed copy: %v", err)
	}
	if err := closeWriter(); err != nil {
		b.Fatalf("close the %s writer: %v", compression.Extension(), err)
	}
	if err := file.Close(); err != nil {
		b.Fatalf("close %s: %v", path, err)
	}
	return path
}

// BenchmarkOpenCompressed measures loading through the two codecs whose headers
// are inspected before the decoder is built, so the cost of that inspection is
// visible next to the decompression it stands in front of.
func BenchmarkOpenCompressed(b *testing.B) {
	for _, compression := range []CompressionType{CompressionXZ, CompressionZSTD, CompressionGZ} {
		b.Run(compression.Extension(), func(b *testing.B) {
			path := benchmarkCompressedCopy(b, compression)

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
		})
	}
}

// TestLoadMemoryFootprint reports what loading a CSV costs, at several sizes.
//
// It exists because the number this package had been tracking was the wrong
// one. `go test -benchmem` reports B/op, which counts every byte the operation
// ever allocated, garbage included — for the 100,000-row fixture that is around
// 141MB, and it says nothing about how much memory is held at once. The
// question a caller actually has is whether a given file fits, and the answer
// has two parts that B/op conflates:
//
//   - The Go heap stays small and flat. Loading is chunked, so the parser holds
//     roughly a chunk, not the file.
//   - The rows land in the in-memory SQLite database, which is not on the Go
//     heap. That is where the footprint is, and only RSS shows it.
//
// Run with: go test -tags benchmark -run TestLoadMemoryFootprint -v .
//
// The RSS reading is Linux-only; elsewhere the test reports heap alone.
//
// RSS is a coarse instrument: the allocator never returns pages, so one size's
// reading carries every earlier size's baseline and the absolute ratio comes out
// too high. The marginal column is the honest figure — the extra resident bytes
// per extra file byte between two consecutive sizes, which cancels the baseline.
// It agrees with running each size in a process of its own.
func TestLoadMemoryFootprint(t *testing.T) {
	header, body := readBenchmarkFixture(t)

	t.Logf("%9s %10s %14s %14s %10s", "rows", "file MB", "heap after MB", "RSS delta MB", "marginal")

	var prevFileMB, prevDeltaMB float64
	for _, rows := range []int{100000, 200000, 400000, 800000} {
		path := writeCSV(t, header, body, rows)
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}

		runtime.GC()
		runtime.GC()
		rssBefore := readRSS()

		ctx := context.Background()
		db, err := OpenContext(ctx, path)
		if err != nil {
			t.Fatalf("OpenContext(%d rows): %v", rows, err)
		}

		var got int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM data").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != rows {
			t.Fatalf("row count = %d, want %d", got, rows)
		}

		runtime.GC()
		rssAfter := readRSS()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)

		fileMB := float64(fi.Size()) / (1 << 20)
		heapMB := float64(ms.HeapAlloc) / (1 << 20)
		if rssBefore == 0 || rssAfter == 0 {
			t.Logf("%9d %10.1f %14.1f %14s %10s", rows, fileMB, heapMB, "n/a", "n/a")
		} else {
			deltaMB := (float64(rssAfter) - float64(rssBefore)) / (1 << 20)
			marginal := "-"
			if prevFileMB > 0 {
				marginal = fmt.Sprintf("%.2fx", (deltaMB-prevDeltaMB)/(fileMB-prevFileMB))
			}
			t.Logf("%9d %10.1f %14.1f %14.1f %10s", rows, fileMB, heapMB, deltaMB, marginal)
			prevFileMB, prevDeltaMB = fileMB, deltaMB
		}

		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// readBenchmarkFixture returns the benchmark CSV's header and its data rows.
func readBenchmarkFixture(t *testing.T) (string, []string) {
	t.Helper()

	f, err := os.Open(filepath.Join("testdata", "benchmark", "customers100000.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var lines []string
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if len(lines) < 2 {
		t.Fatalf("fixture has %d lines, want a header and at least one row", len(lines))
	}
	return lines[0], lines[1:]
}

// writeCSV writes a CSV of the requested row count, cycling the fixture's rows.
func writeCSV(t *testing.T, header string, body []string, rows int) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "data.csv")
	f, err := os.Create(path) //nolint:gosec // path is under t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(f)
	if _, err := fmt.Fprintln(w, header); err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if _, err := fmt.Fprintln(w, body[i%len(body)]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// readRSS returns the process's resident set size, or 0 where it cannot be read.
// The Go heap alone understates a load, because the rows are held by the
// in-memory SQLite database rather than by Go.
func readRSS() uint64 {
	return readStatusField("VmRSS:")
}

// readStatusField reads one kilobyte-valued field of /proc/self/status.
func readStatusField(name string) uint64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, name) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return kb * 1024
	}
	return 0
}

// TestLoadMemoryFootprintByFormat reports what the same rows cost through each
// format, because "about twice the file's size" was measured on CSV alone and
// the formats do not read alike: CSV, TSV and JSON arrays arrive in chunks,
// while Parquet and XLSX are read whole before they become rows. The question a
// caller has is whether a file fits, and the answer turns out to depend on which
// format the same table is written in.
//
// Run with: go test -tags benchmark -run TestLoadMemoryFootprintByFormat -v .
//
// Each load runs in a process of its own and reports its peak resident memory,
// which is what the reader that holds a file whole actually costs. Measuring
// several loads inside one process cannot show it: the allocator does not give
// pages back, so the second load reads as free and the third as negative.
//
// The row counts stay modest because a workbook of a million rows takes minutes
// to write, and because what is compared is the ratio rather than the ceiling.
func TestLoadMemoryFootprintByFormat(t *testing.T) {
	if os.Getenv(footprintPathEnv) != "" {
		t.Skip("this process is the child that loads one file")
	}
	header, body := readBenchmarkFixture(t)

	t.Logf("%9s %-8s %10s %12s %8s %10s", "rows", "format", "file MB", "peak RSS MB", "ratio", "marginal")

	for _, format := range []struct {
		name  string
		write func(t *testing.T, csvPath string) string
	}{
		{name: "csv", write: func(_ *testing.T, csvPath string) string { return csvPath }},
		{name: "parquet", write: func(t *testing.T, csvPath string) string {
			return convertCSV(t, csvPath, OutputFormatParquet, ".parquet")
		}},
		{name: "xlsx", write: func(t *testing.T, csvPath string) string { return convertCSV(t, csvPath, OutputFormatXLSX, ".xlsx") }},
		// The same rows in one column. A workbook is read whole, so what it
		// costs follows the cells it holds rather than the size of the file --
		// which is a zip, and shrinks when every row is short. Without this
		// shape beside the one above, the table reads as if the format had one
		// multiplier.
		{name: "xlsx/1col", write: func(t *testing.T, csvPath string) string {
			return convertCSV(t, oneColumnCSV(t, csvPath), OutputFormatXLSX, ".xlsx")
		}},
	} {
		var prevFileMB, prevPeakMB float64
		for _, rows := range []int{50000, 100000, 200000} {
			path := format.write(t, writeCSV(t, header, body, rows))
			fi, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}

			peak := loadInChildProcess(t, path, rows)
			fileMB := float64(fi.Size()) / (1 << 20)
			if peak == 0 {
				t.Logf("%9d %-8s %10.1f %12s %8s %10s", rows, format.name, fileMB, "n/a", "n/a", "n/a")
				continue
			}
			peakMB := float64(peak) / (1 << 20)
			// The ratio carries the process's own baseline, which the marginal
			// column cancels: the extra resident bytes per extra file byte
			// between two sizes is what the format costs.
			marginal := "-"
			if prevFileMB > 0 {
				marginal = fmt.Sprintf("%.1fx", (peakMB-prevPeakMB)/(fileMB-prevFileMB))
			}
			t.Logf("%9d %-8s %10.1f %12.1f %7.1fx %10s", rows, format.name, fileMB, peakMB, peakMB/fileMB, marginal)
			prevFileMB, prevPeakMB = fileMB, peakMB
		}
	}
}

// oneColumnCSV rewrites a CSV as its first column alone, keeping the row count.
// It is what puts a second shape of workbook in the footprint table.
func oneColumnCSV(t *testing.T, csvPath string) string {
	t.Helper()

	in, err := os.Open(csvPath) //nolint:gosec // path is under t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close() }()

	// Named the way convertCSV expects, which takes the dumped file by name.
	path := filepath.Join(t.TempDir(), "data.csv")
	out, err := os.Create(path) //nolint:gosec // path is under t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(out)
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if i := strings.IndexByte(line, ','); i >= 0 {
			line = line[:i]
		}
		if _, err := fmt.Fprintln(w, line); err != nil {
			t.Fatal(err)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// footprintPathEnv names the file the child process loads. Its presence is also
// what tells a process it is the child.
const footprintPathEnv = "FILESQL_FOOTPRINT_PATH"

// footprintRowsEnv names how many rows the child must find, so a measurement of
// a load that did not happen cannot pass for one that did.
const footprintRowsEnv = "FILESQL_FOOTPRINT_ROWS"

// loadInChildProcess runs TestLoadOneFileFootprint in a process of its own and
// returns the peak resident memory it reported, or zero where the platform does
// not report one.
func loadInChildProcess(t *testing.T, path string, rows int) uint64 {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=TestLoadOneFileFootprint", "-test.v")
	cmd.Env = append(os.Environ(),
		footprintPathEnv+"="+path,
		footprintRowsEnv+"="+strconv.Itoa(rows),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child load of %s: %v\n%s", path, err, out)
	}
	for line := range strings.Lines(string(out)) {
		_, peak, found := strings.Cut(strings.TrimSpace(line), footprintPeakPrefix)
		if !found {
			continue
		}
		value, err := strconv.ParseUint(peak, 10, 64)
		if err != nil {
			t.Fatalf("child reported %q: %v", peak, err)
		}
		return value
	}
	return 0
}

// footprintPeakPrefix marks the one line of the child's output the parent reads.
const footprintPeakPrefix = "FOOTPRINT_PEAK_BYTES="

// TestLoadOneFileFootprint loads the file the environment names and reports the
// process's peak resident memory. It is the child half of the test above and
// does nothing on its own.
func TestLoadOneFileFootprint(t *testing.T) {
	path := os.Getenv(footprintPathEnv)
	if path == "" {
		t.Skip("no file named; this is the parent process")
	}
	want, err := strconv.Atoi(os.Getenv(footprintRowsEnv))
	if err != nil {
		t.Fatalf("%s: %v", footprintRowsEnv, err)
	}

	ctx := context.Background()
	db, err := OpenContext(ctx, path)
	if err != nil {
		t.Fatalf("OpenContext(%s): %v", path, err)
	}
	var got int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM data").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("row count = %d, want %d", got, want)
	}
	t.Logf("%s%d", footprintPeakPrefix, readPeakRSS())
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// readPeakRSS returns the highest resident set size the process has reached, or
// zero where it cannot be read. Unlike the current size, it survives the load
// finishing and the pages being handed back.
func readPeakRSS() uint64 {
	return readStatusField("VmHWM:")
}

// convertCSV writes the CSV's rows out again in another format, so the same
// table can be measured through each reader.
func convertCSV(t *testing.T, csvPath string, format OutputFormat, ext string) string {
	t.Helper()

	ctx := context.Background()
	db, err := OpenContext(ctx, csvPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	dir := t.TempDir()
	if err := DumpDatabase(db, dir, NewDumpOptions().WithFormat(format)); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "data"+ext)
}
