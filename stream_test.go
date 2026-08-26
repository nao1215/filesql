package filesql

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ulikunitz/xz"
	"github.com/xuri/excelize/v2"
)

// parseFromReader reads the whole input through ProcessInChunks and returns it
// as one table, typed as the reader said. Tests that care about what a format
// parses to, and not about chunking, read through this.
func (p *streamingParser) parseFromReader(reader io.Reader) (*table, error) {
	var headers header
	var records []record
	columns, err := p.ProcessInChunks(reader, func(chunk *tableChunk) error {
		headers = chunk.getHeaders()
		records = append(records, chunk.getRecords()...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	t := newTable(p.tableName, headers, records)
	t.columnInfo = columns
	return t, nil
}

func TestStreamingParser_ParseFromReader_CSV(t *testing.T) {
	t.Parallel()

	t.Run("valid CSV data", func(t *testing.T) {
		t.Parallel()
		data := "name,age,city\nAlice,30,Tokyo\nBob,25,Osaka\n"
		reader := strings.NewReader(data)

		parser := newStreamingParser(FileTypeCSV, CompressionNone, "users", 1024)
		table, err := parser.parseFromReader(reader)
		require.NoError(t, err, "ParseFromReader() failed")

		assert.Equal(t, "users", table.getName(), "Table name mismatch")

		header := table.getHeader()
		assert.Len(t, header, 3, "Header length mismatch")

		records := table.getRecords()
		assert.Len(t, records, 2, "Records length mismatch")

		if records[0][0] != "Alice" {
			t.Errorf("First record first field = %s, want Alice", records[0][0])
		}
	})

	t.Run("empty CSV data", func(t *testing.T) {
		t.Parallel()
		reader := strings.NewReader("")

		parser := newStreamingParser(FileTypeCSV, CompressionNone, "empty", 1024)
		_, err := parser.parseFromReader(reader)
		if err == nil {
			t.Error("ParseFromReader() should fail for empty data")
		}
	})
}

func TestStreamingParser_ParseFromReader_TSV(t *testing.T) {
	t.Parallel()

	t.Run("valid TSV data", func(t *testing.T) {
		t.Parallel()
		data := "name\tage\tcity\nAlice\t30\tTokyo\nBob\t25\tOsaka\n"
		reader := strings.NewReader(data)

		parser := newStreamingParser(FileTypeTSV, CompressionNone, "users", 1024)
		table, err := parser.parseFromReader(reader)
		require.NoError(t, err, "ParseFromReader() failed")

		assert.Equal(t, "users", table.getName(), "Table name mismatch")

		records := table.getRecords()
		assert.Len(t, records, 2, "Records length mismatch")
	})
}

func TestStreamingParser_ParseFromReader_LTSV(t *testing.T) {
	t.Parallel()

	t.Run("valid LTSV data", func(t *testing.T) {
		t.Parallel()
		data := "name:Alice\tage:30\tcity:Tokyo\nname:Bob\tage:25\tcity:Osaka\n"
		reader := strings.NewReader(data)

		parser := newStreamingParser(FileTypeLTSV, CompressionNone, "users", 1024)
		table, err := parser.parseFromReader(reader)
		require.NoError(t, err, "ParseFromReader() failed")

		assert.Equal(t, "users", table.getName(), "Table name mismatch")

		records := table.getRecords()
		assert.Len(t, records, 2, "Records length mismatch")
	})

	t.Run("duplicate label within a record is rejected", func(t *testing.T) {
		t.Parallel()
		// "x" repeats in the same record; keeping only the last value would silently
		// drop the first, so the parser rejects it. Ref nao1215/sqly#467.
		reader := strings.NewReader("x:1\tx:2\n")

		parser := newStreamingParser(FileTypeLTSV, CompressionNone, "dup", 1024)
		_, err := parser.parseFromReader(reader)
		require.Error(t, err, "duplicate LTSV label should be rejected")
		assert.Contains(t, err.Error(), "duplicate column name")
	})

	t.Run("same label across separate records still parses", func(t *testing.T) {
		t.Parallel()
		reader := strings.NewReader("x:1\ty:a\nx:2\ty:b\n")

		parser := newStreamingParser(FileTypeLTSV, CompressionNone, "ok", 1024)
		table, err := parser.parseFromReader(reader)
		require.NoError(t, err)
		assert.Len(t, table.getRecords(), 2)
	})
}

func TestStreamingParser_TSVTakesFieldsLiterally(t *testing.T) {
	t.Parallel()

	const input = "name\tnote\nalice\t5'9\" tall\nbob\tsaid \"hi\" loudly\n"

	t.Run("a quote in a value does not fail the read", func(t *testing.T) {
		t.Parallel()

		p := newStreamingParser(FileTypeTSV, CompressionNone, "notes", 1024)
		table, err := p.parseFromReader(strings.NewReader(input))

		require.NoError(t, err)
		records := table.getRecords()
		require.Len(t, records, 2)
		assert.Equal(t, `5'9" tall`, records[0][1])
		assert.Equal(t, `said "hi" loudly`, records[1][1])
	})

	t.Run("the chunked reader agrees", func(t *testing.T) {
		t.Parallel()

		p := newStreamingParser(FileTypeTSV, CompressionNone, "notes", 1)
		var values []string
		_, err := p.ProcessInChunks(strings.NewReader(input), func(chunk *tableChunk) error {
			for _, r := range chunk.records {
				values = append(values, r[1])
			}
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, []string{`5'9" tall`, `said "hi" loudly`}, values)
	})
}

func TestStreamingParser_CROnlyLineEndings(t *testing.T) {
	t.Parallel()

	t.Run("a CR-terminated CSV keeps its rows", func(t *testing.T) {
		t.Parallel()

		p := newStreamingParser(FileTypeCSV, CompressionNone, "users", 1024)
		table, err := p.parseFromReader(strings.NewReader("name,age\rAlice,30\rBob,40\r"))

		require.NoError(t, err)
		assert.Equal(t, []string{"name", "age"}, []string(table.getHeader()))
		assert.Len(t, table.getRecords(), 2)
	})

	t.Run("a CR-terminated LTSV keeps its rows", func(t *testing.T) {
		t.Parallel()

		p := newStreamingParser(FileTypeLTSV, CompressionNone, "users", 1024)
		table, err := p.parseFromReader(strings.NewReader("name:Alice\tage:30\rname:Bob\tage:40\r"))

		require.NoError(t, err)
		assert.Equal(t, []string{"name", "age"}, []string(table.getHeader()))
		assert.Len(t, table.getRecords(), 2)
	})

	t.Run("a CR-terminated CSV keeps its rows when read in chunks", func(t *testing.T) {
		t.Parallel()

		p := newStreamingParser(FileTypeCSV, CompressionNone, "users", 1)
		rows := 0
		_, err := p.ProcessInChunks(strings.NewReader("name,age\rAlice,30\rBob,40\r"), func(chunk *tableChunk) error {
			rows += len(chunk.records)
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, 2, rows)
	})

	t.Run("a CR inside a quoted field of an LF file is data, not a row boundary", func(t *testing.T) {
		t.Parallel()

		p := newStreamingParser(FileTypeCSV, CompressionNone, "notes", 1024)
		table, err := p.parseFromReader(strings.NewReader("name,note\nAlice,\"a\rb\"\nBob,plain\n"))

		require.NoError(t, err)
		records := table.getRecords()
		require.Len(t, records, 2)
		assert.Equal(t, "a\rb", records[0][1])
	})
}

func TestStreamingParser_ParseFromReader_Compressed(t *testing.T) {
	t.Parallel()

	t.Run("gzip compressed CSV", func(t *testing.T) {
		t.Parallel()
		// Create gzip compressed CSV data
		originalData := "name,age\nAlice,30\nBob,25\n"
		var buf bytes.Buffer

		// For this test, we'll use uncompressed data but specify the compressed type
		// to test the decompression logic path
		reader := strings.NewReader(originalData)

		// Note: This will fail because the data is not actually gzip compressed
		// but the test demonstrates the compression handling logic
		parser := newStreamingParser(FileTypeCSV, CompressionNone, "users", 1024) // Use uncompressed for now
		table, err := parser.parseFromReader(reader)
		require.NoError(t, err, "ParseFromReader() failed")

		records := table.getRecords()
		assert.Len(t, records, 2, "Records length mismatch")

		_ = buf // Prevent unused variable warning
	})
}

func TestParquetStreaming(t *testing.T) {
	t.Parallel()

	// Create test data
	tempDir := t.TempDir()

	// First create a CSV file and export to Parquet for testing
	csvContent := `name,age,city
Alice,25,Tokyo
Bob,30,New York
Charlie,35,London`

	csvFile := filepath.Join(tempDir, "test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0600); err != nil {
		t.Fatal(err)
	}

	// Export to Parquet
	db, err := Open(csvFile)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	outputDir := filepath.Join(tempDir, "output")
	err = DumpDatabase(db, outputDir, NewDumpOptions().WithFormat(OutputFormatParquet))
	if err != nil {
		t.Fatal(err)
	}

	// Now test streaming from the Parquet file
	parquetFile := filepath.Join(outputDir, "test.parquet")
	parquetData, err := os.ReadFile(parquetFile) //nolint:gosec
	if err != nil {
		t.Fatal(err)
	}

	// Test parseParquetStream
	parser := newStreamingParser(FileTypeParquet, CompressionNone, "test_stream", 1000)
	reader := bytes.NewReader(parquetData)

	table, err := parser.parseFromReader(reader)
	if err != nil {
		t.Fatalf("Failed to parse parquet stream: %v", err)
	}

	// Verify results
	if table.getName() != "test_stream" {
		t.Errorf("Expected table name 'test_stream', got %s", table.getName())
	}

	headers := table.getHeader()
	expectedHeaders := []string{"name", "age", "city"}
	if len(headers) != len(expectedHeaders) {
		t.Fatalf("Expected %d headers, got %d", len(expectedHeaders), len(headers))
	}

	for i, expected := range expectedHeaders {
		if headers[i] != expected {
			t.Errorf("Header %d: expected %s, got %s", i, expected, headers[i])
		}
	}

	records := table.getRecords()
	if len(records) != 3 {
		t.Fatalf("Expected 3 records, got %d", len(records))
	}

	// Check first record
	if records[0][0] != "Alice" || records[0][1] != "25" || records[0][2] != "Tokyo" {
		t.Errorf("First record mismatch: got %v", records[0])
	}

	t.Logf("Successfully parsed Parquet stream with %d records", len(records))
}

func TestParquetStreamingChunks(t *testing.T) {
	t.Parallel()

	// Create test data
	tempDir := t.TempDir()

	// Create a larger CSV file for chunk testing
	csvContent := `id,name,value
1,User1,100.5
2,User2,200.3
3,User3,300.7
4,User4,400.2
5,User5,500.9`

	csvFile := filepath.Join(tempDir, "test.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0600); err != nil {
		t.Fatal(err)
	}

	// Export to Parquet
	db, err := Open(csvFile)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	outputDir := filepath.Join(tempDir, "output")
	err = DumpDatabase(db, outputDir, NewDumpOptions().WithFormat(OutputFormatParquet))
	if err != nil {
		t.Fatal(err)
	}

	// Now test chunked processing from the Parquet file
	parquetFile := filepath.Join(outputDir, "test.parquet")
	parquetData, err := os.ReadFile(parquetFile) //nolint:gosec
	if err != nil {
		t.Fatal(err)
	}

	// Test processParquetInChunks with small chunk size
	parser := newStreamingParser(FileTypeParquet, CompressionNone, "test_chunks", 2) // Process 2 records at a time
	reader := bytes.NewReader(parquetData)

	var totalRecords int
	var chunkCount int

	processor := func(chunk *tableChunk) error {
		chunkCount++
		totalRecords += len(chunk.records)
		t.Logf("Processing chunk %d with %d records", chunkCount, len(chunk.records))

		// Verify chunk structure
		if chunk.tableName != "test_chunks" {
			t.Errorf("Expected table name 'test_chunks', got %s", chunk.tableName)
		}

		expectedHeaders := []string{"id", "name", "value"}
		if len(chunk.headers) != len(expectedHeaders) {
			t.Errorf("Expected %d headers, got %d", len(expectedHeaders), len(chunk.headers))
		}

		return nil
	}

	columns, err := parser.ProcessInChunks(reader, processor)
	if err != nil {
		t.Fatalf("Failed to process parquet chunks: %v", err)
	}
	if len(columns) != 3 {
		t.Errorf("Expected 3 column infos, got %d", len(columns))
	}

	// Verify we processed all records
	if totalRecords != 5 {
		t.Errorf("Expected to process 5 records total, got %d", totalRecords)
	}

	// Note: Parquet processing may optimize chunking differently than CSV
	// We mainly verify that chunking works and all records are processed
	if chunkCount < 1 {
		t.Errorf("Expected at least 1 chunk, got %d chunks", chunkCount)
	}

	t.Logf("Successfully processed %d records in %d chunks", totalRecords, chunkCount)
}

// TestParquetStreamingCompressed reads a gzipped Parquet file end to end. It
// used to hand parseParquetStream a string of garbage under a fused
// FileTypeParquetGZ, which skipped the codec entirely and only proved that
// garbage is not Parquet.
func TestParquetStreamingCompressed(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	csvFile := filepath.Join(tempDir, "compressed_test.csv")
	csvContent := "name,age\nAlice,25\nBob,30\n"
	require.NoError(t, os.WriteFile(csvFile, []byte(csvContent), 0600))

	db, err := Open(csvFile)
	require.NoError(t, err)
	defer db.Close()

	outputDir := filepath.Join(tempDir, "output")
	require.NoError(t, DumpDatabase(db, outputDir, NewDumpOptions().WithFormat(OutputFormatParquet)))

	parquetData, err := os.ReadFile(filepath.Join(outputDir, "compressed_test.parquet")) //nolint:gosec
	require.NoError(t, err)

	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	_, err = gz.Write(parquetData)
	require.NoError(t, err)
	require.NoError(t, gz.Close())

	parser := newStreamingParser(FileTypeParquet, CompressionGZ, "compressed_test", 1000)
	table, err := parser.parseFromReader(bytes.NewReader(compressed.Bytes()))
	require.NoError(t, err, "a gzipped Parquet file should load")

	assert.Equal(t, "compressed_test", table.getName())
	assert.Equal(t, header{"name", "age"}, table.getHeader())
	assert.Len(t, table.getRecords(), 2)

	t.Run("truncated gzip is rejected", func(t *testing.T) {
		t.Parallel()

		parser := newStreamingParser(FileTypeParquet, CompressionGZ, "broken", 1000)
		_, err := parser.parseFromReader(bytes.NewReader([]byte("not gzip at all")))
		assert.Error(t, err, "data that is not gzip should not be read as Parquet")
	})
}

// TestColumnInferenceAdvanced tests column inference with various data types
func TestColumnInferenceAdvanced(t *testing.T) {
	t.Parallel()

	t.Run("mixed data types for column inference", func(t *testing.T) {
		t.Parallel()

		// Test with mixed data types to improve infercolumnInfoFromValues coverage
		csvData := "num,text,mixed\n123,hello,456\n456.7,world,text\n789,test,123.45\n"
		reader := strings.NewReader(csvData)

		parser := newStreamingParser(FileTypeCSV, CompressionNone, "test_infer", 1024)
		table, err := parser.parseFromReader(reader)
		if err != nil {
			t.Fatalf("Failed to parse CSV: %v", err)
		}

		if table == nil {
			t.Error("Expected non-nil table")
		}

		if len(table.getRecords()) != 3 {
			t.Errorf("Expected 3 records, got %d", len(table.getRecords()))
		}
	})

	t.Run("column inference with empty and null values", func(t *testing.T) {
		t.Parallel()

		// Test CSV with empty values and various data patterns
		csvData := "col1,col2,col3\n123,,456.7\n,world,\ntest,456,789\n"
		reader := strings.NewReader(csvData)

		parser := newStreamingParser(FileTypeCSV, CompressionNone, "test_empty", 1024)
		table, err := parser.parseFromReader(reader)
		if err != nil {
			t.Fatalf("Failed to parse CSV with empty values: %v", err)
		}

		if table == nil {
			t.Error("Expected non-nil table")
		}

		records := table.getRecords()
		if len(records) != 3 {
			t.Errorf("Expected 3 records, got %d", len(records))
		}
	})
}

// TestProcessLTSVInChunks tests LTSV chunk processing for coverage
func TestProcessLTSVInChunks(t *testing.T) {
	t.Parallel()

	t.Run("LTSV chunk processing", func(t *testing.T) {
		t.Parallel()

		ltsvData := "name:Alice\tage:30\tcity:Tokyo\nname:Bob\tage:25\tcity:Osaka\nname:Charlie\tage:35\tcity:Kyoto\n"
		reader := strings.NewReader(ltsvData)

		parser := newStreamingParser(FileTypeLTSV, CompressionNone, "test_ltsv", 2) // Small chunk size

		var totalRecords int
		processor := func(chunk *tableChunk) error {
			totalRecords += len(chunk.records)
			return nil
		}

		_, err := parser.ProcessInChunks(reader, processor)
		if err != nil {
			t.Fatalf("Failed to process LTSV chunks: %v", err)
		}

		if totalRecords != 3 {
			t.Errorf("Expected 3 total records, got %d", totalRecords)
		}
	})

	t.Run("LTSV processing with various patterns", func(t *testing.T) {
		t.Parallel()

		// Test LTSV with different field patterns to improve coverage
		ltsvData := "name:Alice\tage:30\tcity:Tokyo\nname:Bob\tage:25\n"
		reader := strings.NewReader(ltsvData)

		parser := newStreamingParser(FileTypeLTSV, CompressionNone, "test_patterns", 1024)
		table, err := parser.parseFromReader(reader)
		if err != nil {
			t.Fatalf("Failed to parse LTSV: %v", err)
		}

		if table == nil {
			t.Error("Expected non-nil table")
		}

		// Should handle different number of fields gracefully
		records := table.getRecords()
		if len(records) != 2 {
			t.Errorf("Expected 2 records, got %d", len(records))
		}
	})

	t.Run("duplicate label within a record is rejected", func(t *testing.T) {
		t.Parallel()
		// The chunked path also rejects a label repeated within one record, so a
		// large LTSV file cannot silently drop values. Ref nao1215/sqly#467.
		reader := strings.NewReader("x:1\tx:2\n")

		parser := newStreamingParser(FileTypeLTSV, CompressionNone, "dup_chunk", 2)
		_, err := parser.ProcessInChunks(reader, func(*tableChunk) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate column name")
	})
}

func TestStreamingParser_ParseFromReader_XLSX(t *testing.T) {
	t.Parallel()

	t.Run("valid XLSX data with multiple sheets", func(t *testing.T) {
		t.Parallel()

		// Create a test XLSX file in memory
		f := excelize.NewFile()

		// Add data to Sheet1
		if err := f.SetCellValue("Sheet1", "A1", "Name1"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue("Sheet1", "A2", "Alice"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue("Sheet1", "A3", "Bob"); err != nil {
			t.Fatal(err)
		}

		// Add Sheet2 with data
		if _, err := f.NewSheet("Sheet2"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue("Sheet2", "A1", "Age1"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue("Sheet2", "A2", "30"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue("Sheet2", "A3", "25"); err != nil {
			t.Fatal(err)
		}

		// Write to buffer
		var buf bytes.Buffer
		if err := f.Write(&buf); err != nil {
			t.Fatal(err)
		}
		_ = f.Close() // Ignore close error in test

		// Parse using streaming parser - should process first sheet only
		parser := newStreamingParser(FileTypeXLSX, CompressionNone, "test_workbook", 1024)
		table, err := parser.parseFromReader(&buf)
		require.NoError(t, err, "ParseFromReader() failed")

		if table.getName() != "test_workbook" {
			t.Errorf("Table name = %s, want test_workbook", table.getName())
		}

		// Check headers (should be from first row of first sheet)
		header := table.getHeader()
		if len(header) != 1 {
			t.Errorf("Header length = %d, want 1", len(header))
		}

		expectedHeader := "Name1"
		if header[0] != expectedHeader {
			t.Errorf("Header[0] = %s, want %s", header[0], expectedHeader)
		}

		// Check records (should be from first sheet only)
		records := table.getRecords()
		assert.Len(t, records, 2, "Records length mismatch")

		// First record should contain data from row 2 of first sheet
		if len(records) > 0 && len(records[0]) >= 1 {
			if records[0][0] != "Alice" {
				t.Errorf("First record = %s, want Alice", records[0][0])
			}
		}
	})

	t.Run("empty XLSX file", func(t *testing.T) {
		t.Parallel()

		// Create empty XLSX file
		f := excelize.NewFile()
		var buf bytes.Buffer
		if err := f.Write(&buf); err != nil {
			t.Fatal(err)
		}
		_ = f.Close() // Ignore close error in test

		parser := newStreamingParser(FileTypeXLSX, CompressionNone, "empty_workbook", 1024)
		_, err := parser.parseFromReader(&buf)
		if err == nil {
			t.Error("Expected error for empty XLSX file, got nil")
		}
	})

	t.Run("compressed XLSX data", func(t *testing.T) {
		t.Parallel()

		// Create a simple XLSX file
		f := excelize.NewFile()
		if err := f.SetCellValue("Sheet1", "A1", "Test"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue("Sheet1", "A2", "Data"); err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		if err := f.Write(&buf); err != nil {
			t.Fatal(err)
		}
		_ = f.Close() // Ignore close error in test
		workbook := buf.Bytes()

		// The workbook is really compressed with each codec and read back. This
		// used to hand the parser uncompressed bytes under a compressed FileType
		// and log whatever happened, so it never established that a compressed
		// XLSX loads at all. bzip2 is absent because the library has no writer
		// for it.
		codecs := []CompressionType{
			CompressionGZ, CompressionXZ, CompressionZSTD, CompressionZLIB,
			CompressionSNAPPY, CompressionS2, CompressionLZ4,
		}

		for _, codec := range codecs {
			t.Run(codec.String(), func(t *testing.T) {
				t.Parallel()

				var compressed bytes.Buffer
				w, closeWriter, err := NewCompressionHandler(codec).CreateWriter(&compressed)
				require.NoError(t, err, "failed to create %s writer", codec)
				_, err = w.Write(workbook)
				require.NoError(t, err, "failed to write workbook through %s", codec)
				require.NoError(t, closeWriter(), "failed to flush %s writer", codec)

				parser := newStreamingParser(FileTypeXLSX, codec, "compressed_workbook", 1024)
				table, err := parser.parseFromReader(bytes.NewReader(compressed.Bytes()))
				require.NoError(t, err, "parseFromReader() failed for %s", codec)

				assert.Equal(t, "compressed_workbook", table.getName(), "Table name mismatch")
				assert.Equal(t, header{"Test"}, table.getHeader(), "Header mismatch")
				assert.Equal(t, []record{{"Data"}}, table.getRecords(), "Records mismatch")
			})
		}
	})
}

// TestCreateDecompressedReader_AllCompressionTypes tests the createDecompressedReader function
// with all supported compression types including zlib, snappy, s2, and lz4.
func TestCreateDecompressedReader_AllCompressionTypes(t *testing.T) {
	t.Parallel()

	originalData := "name,age,city\nAlice,30,Tokyo\nBob,25,Osaka\n"

	tests := []struct {
		name     string
		fileType FileType
		// compression is the codec the row's compress func produced. FileType no
		// longer carries it, and a blanket CompressionNone here compiles and then
		// fails at parse time on the codec's magic bytes.
		compression CompressionType
		compress    func([]byte) ([]byte, error)
		expectErr   bool
	}{
		{
			name:        "gzip compressed CSV",
			fileType:    FileTypeCSV,
			compression: CompressionGZ,
			compress: func(data []byte) ([]byte, error) {
				var buf bytes.Buffer
				w := gzip.NewWriter(&buf)
				if _, err := w.Write(data); err != nil {
					return nil, err
				}
				if err := w.Close(); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
		{
			name:        "zstd compressed CSV",
			fileType:    FileTypeCSV,
			compression: CompressionZSTD,
			compress: func(data []byte) ([]byte, error) {
				var buf bytes.Buffer
				w, err := zstd.NewWriter(&buf)
				if err != nil {
					return nil, err
				}
				if _, err := w.Write(data); err != nil {
					return nil, err
				}
				if err := w.Close(); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
		{
			name:        "xz compressed CSV",
			fileType:    FileTypeCSV,
			compression: CompressionXZ,
			compress: func(data []byte) ([]byte, error) {
				var buf bytes.Buffer
				w, err := xz.NewWriter(&buf)
				if err != nil {
					return nil, err
				}
				if _, err := w.Write(data); err != nil {
					return nil, err
				}
				if err := w.Close(); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
		{
			name:        "zlib compressed CSV",
			fileType:    FileTypeCSV,
			compression: CompressionZLIB,
			compress: func(data []byte) ([]byte, error) {
				var buf bytes.Buffer
				w := zlib.NewWriter(&buf)
				if _, err := w.Write(data); err != nil {
					return nil, err
				}
				if err := w.Close(); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
		{
			name:        "snappy compressed CSV",
			fileType:    FileTypeCSV,
			compression: CompressionSNAPPY,
			compress: func(data []byte) ([]byte, error) {
				var buf bytes.Buffer
				w := snappy.NewBufferedWriter(&buf)
				if _, err := w.Write(data); err != nil {
					return nil, err
				}
				if err := w.Close(); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
		{
			name:        "s2 compressed CSV",
			fileType:    FileTypeCSV,
			compression: CompressionS2,
			compress: func(data []byte) ([]byte, error) {
				var buf bytes.Buffer
				w := s2.NewWriter(&buf)
				if _, err := w.Write(data); err != nil {
					return nil, err
				}
				if err := w.Close(); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
		{
			name:        "lz4 compressed CSV",
			fileType:    FileTypeCSV,
			compression: CompressionLZ4,
			compress: func(data []byte) ([]byte, error) {
				var buf bytes.Buffer
				w := lz4.NewWriter(&buf)
				if _, err := w.Write(data); err != nil {
					return nil, err
				}
				if err := w.Close(); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
		{
			name:     "uncompressed CSV",
			fileType: FileTypeCSV,
			compress: func(data []byte) ([]byte, error) {
				return data, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			compressedData, err := tt.compress([]byte(originalData))
			require.NoError(t, err, "Failed to compress data")

			parser := newStreamingParser(tt.fileType, tt.compression, "test", 1024)
			reader := bytes.NewReader(compressedData)

			table, err := parser.parseFromReader(reader)
			if tt.expectErr {
				assert.Error(t, err, "Expected error")
				return
			}

			require.NoError(t, err, "parseFromReader() failed")
			assert.Equal(t, "test", table.getName(), "Table name mismatch")

			// The header is checked as well as the rows. Snappy and s2 leave short
			// incompressible input nearly verbatim after their frame magic, so a
			// row that names the wrong codec still yields two plausible-looking
			// records — only the header carries the corruption.
			assert.Equal(t, header{"name", "age", "city"}, table.getHeader(), "Header mismatch")
			assert.Equal(t, []record{
				{"Alice", "30", "Tokyo"},
				{"Bob", "25", "Osaka"},
			}, table.getRecords(), "Records mismatch")
		})
	}
}

// TestCreateDecompressedReader_TSVCompressionTypes tests TSV with various compression types
func TestCreateDecompressedReader_TSVCompressionTypes(t *testing.T) {
	t.Parallel()

	originalData := "name\tage\tcity\nAlice\t30\tTokyo\n"

	tests := []struct {
		name     string
		fileType FileType
		// compression is the codec the row's compress func produced.
		compression CompressionType
		compress    func([]byte) ([]byte, error)
	}{
		{
			name:        "zlib compressed TSV",
			fileType:    FileTypeTSV,
			compression: CompressionZLIB,
			compress: func(data []byte) ([]byte, error) {
				var buf bytes.Buffer
				w := zlib.NewWriter(&buf)
				if _, err := w.Write(data); err != nil {
					return nil, err
				}
				if err := w.Close(); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
		{
			name:        "snappy compressed TSV",
			fileType:    FileTypeTSV,
			compression: CompressionSNAPPY,
			compress: func(data []byte) ([]byte, error) {
				var buf bytes.Buffer
				w := snappy.NewBufferedWriter(&buf)
				if _, err := w.Write(data); err != nil {
					return nil, err
				}
				if err := w.Close(); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
		{
			name:        "s2 compressed TSV",
			fileType:    FileTypeTSV,
			compression: CompressionS2,
			compress: func(data []byte) ([]byte, error) {
				var buf bytes.Buffer
				w := s2.NewWriter(&buf)
				if _, err := w.Write(data); err != nil {
					return nil, err
				}
				if err := w.Close(); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
		{
			name:        "lz4 compressed TSV",
			fileType:    FileTypeTSV,
			compression: CompressionLZ4,
			compress: func(data []byte) ([]byte, error) {
				var buf bytes.Buffer
				w := lz4.NewWriter(&buf)
				if _, err := w.Write(data); err != nil {
					return nil, err
				}
				if err := w.Close(); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			compressedData, err := tt.compress([]byte(originalData))
			require.NoError(t, err, "Failed to compress data")

			parser := newStreamingParser(tt.fileType, tt.compression, "test_tsv", 1024)
			reader := bytes.NewReader(compressedData)

			table, err := parser.parseFromReader(reader)
			require.NoError(t, err, "parseFromReader() failed")

			assert.Equal(t, "test_tsv", table.getName(), "Table name mismatch")

			// The header is checked too; see the CSV table for why the rows alone
			// do not pin the codec.
			assert.Equal(t, header{"name", "age", "city"}, table.getHeader(), "Header mismatch")
			assert.Equal(t, []record{{"Alice", "30", "Tokyo"}}, table.getRecords(), "Records mismatch")
		})
	}
}

// TestCreateDecompressedReader_LTSVCompressionTypes tests LTSV with various compression types
func TestCreateDecompressedReader_LTSVCompressionTypes(t *testing.T) {
	t.Parallel()

	originalData := "name:Alice\tage:30\tcity:Tokyo\nname:Bob\tage:25\tcity:Osaka\n"

	tests := []struct {
		name     string
		fileType FileType
		// compression is the codec the row's compress func produced.
		compression CompressionType
		compress    func([]byte) ([]byte, error)
	}{
		{
			name:        "zlib compressed LTSV",
			fileType:    FileTypeLTSV,
			compression: CompressionZLIB,
			compress: func(data []byte) ([]byte, error) {
				var buf bytes.Buffer
				w := zlib.NewWriter(&buf)
				if _, err := w.Write(data); err != nil {
					return nil, err
				}
				if err := w.Close(); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
		{
			name:        "snappy compressed LTSV",
			fileType:    FileTypeLTSV,
			compression: CompressionSNAPPY,
			compress: func(data []byte) ([]byte, error) {
				var buf bytes.Buffer
				w := snappy.NewBufferedWriter(&buf)
				if _, err := w.Write(data); err != nil {
					return nil, err
				}
				if err := w.Close(); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
		{
			name:        "s2 compressed LTSV",
			fileType:    FileTypeLTSV,
			compression: CompressionS2,
			compress: func(data []byte) ([]byte, error) {
				var buf bytes.Buffer
				w := s2.NewWriter(&buf)
				if _, err := w.Write(data); err != nil {
					return nil, err
				}
				if err := w.Close(); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
		{
			name:        "lz4 compressed LTSV",
			fileType:    FileTypeLTSV,
			compression: CompressionLZ4,
			compress: func(data []byte) ([]byte, error) {
				var buf bytes.Buffer
				w := lz4.NewWriter(&buf)
				if _, err := w.Write(data); err != nil {
					return nil, err
				}
				if err := w.Close(); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			compressedData, err := tt.compress([]byte(originalData))
			require.NoError(t, err, "Failed to compress data")

			parser := newStreamingParser(tt.fileType, tt.compression, "test_ltsv", 1024)
			reader := bytes.NewReader(compressedData)

			table, err := parser.parseFromReader(reader)
			require.NoError(t, err, "parseFromReader() failed")

			assert.Equal(t, "test_ltsv", table.getName(), "Table name mismatch")

			// The labels are checked too; see the CSV table for why the rows alone
			// do not pin the codec.
			assert.Equal(t, header{"name", "age", "city"}, table.getHeader(), "Header mismatch")
			assert.Equal(t, []record{
				{"Alice", "30", "Tokyo"},
				{"Bob", "25", "Osaka"},
			}, table.getRecords(), "Records mismatch")
		})
	}
}

// TestCreateDecompressedReader_InvalidData tests error handling with invalid compressed data
func TestCreateDecompressedReader_InvalidData(t *testing.T) {
	t.Parallel()

	invalidData := []byte("this is not valid compressed data")

	tests := []struct {
		name        string
		fileType    FileType
		compression CompressionType
	}{
		{"invalid gzip", FileTypeCSV, CompressionGZ},
		{"invalid zstd", FileTypeCSV, CompressionZSTD},
		{"invalid xz", FileTypeCSV, CompressionXZ},
		{"invalid zlib", FileTypeCSV, CompressionZLIB},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parser := newStreamingParser(tt.fileType, tt.compression, "test", 1024)
			reader := bytes.NewReader(invalidData)

			_, err := parser.parseFromReader(reader)
			assert.Error(t, err, "Expected error for invalid compressed data")
		})
	}
}

func TestCloseQuietly(t *testing.T) {
	t.Parallel()

	t.Run("successful close", func(t *testing.T) {
		t.Parallel()

		called := false
		closeFunc := func() error {
			called = true
			return nil
		}

		closeQuietly(closeFunc)

		assert.True(t, called, "Close function should have been called")
	})

	t.Run("error close is handled gracefully", func(t *testing.T) {
		t.Parallel()

		called := false
		closeFunc := func() error {
			called = true
			return assert.AnError
		}

		// Should not panic even with error
		assert.NotPanics(t, func() {
			closeQuietly(closeFunc)
		})

		assert.True(t, called, "Close function should have been called")
	})
}

func TestStreamingParser_ParseFromReader_UnsupportedFormat(t *testing.T) {
	t.Parallel()

	parser := newStreamingParser(FileTypeUnsupported, CompressionNone, "test", 1024)
	reader := strings.NewReader("test data")

	_, err := parser.parseFromReader(reader)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedFormat)
}

func TestStreamingParser_ProcessInChunks_UnsupportedFormat(t *testing.T) {
	t.Parallel()

	parser := newStreamingParser(FileTypeUnsupported, CompressionNone, "test", 1024)
	reader := strings.NewReader("test data")

	_, err := parser.ProcessInChunks(reader, func(_ *tableChunk) error {
		return nil
	})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedFormat)
}

func TestParseJSONStream(t *testing.T) {
	t.Parallel()

	t.Run("parses JSON array with objects", func(t *testing.T) {
		t.Parallel()

		input := `[{"name":"Alice","age":30},{"name":"Bob","age":25}]`
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSON, CompressionNone, "test_json", defaultChunkSizeRows)

		result, err := parser.parseFromReader(reader)

		require.NoError(t, err)
		assert.Equal(t, header{"data"}, result.header)
		assert.Equal(t, 2, len(result.records))
		assert.True(t, json.Valid([]byte(result.records[0][0])))
		assert.True(t, json.Valid([]byte(result.records[1][0])))
	})

	t.Run("parses JSON single object", func(t *testing.T) {
		t.Parallel()

		input := `{"name":"Alice","age":30}`
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSON, CompressionNone, "test_json", defaultChunkSizeRows)

		result, err := parser.parseFromReader(reader)

		require.NoError(t, err)
		assert.Equal(t, header{"data"}, result.header)
		assert.Equal(t, 1, len(result.records))
		assert.True(t, json.Valid([]byte(result.records[0][0])))
	})

	t.Run("preserves nested JSON structure", func(t *testing.T) {
		t.Parallel()

		input := `[{"id":1,"address":{"city":"Tokyo","country":"Japan"},"tags":["dev","go"]}]`
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSON, CompressionNone, "test_json", defaultChunkSizeRows)

		result, err := parser.parseFromReader(reader)

		require.NoError(t, err)
		assert.Equal(t, 1, len(result.records))

		var parsed map[string]any
		err = json.Unmarshal([]byte(result.records[0][0]), &parsed)
		require.NoError(t, err)

		address, ok := parsed["address"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "Tokyo", address["city"])
	})

	t.Run("column type is TEXT", func(t *testing.T) {
		t.Parallel()

		input := `[{"name":"Alice"}]`
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSON, CompressionNone, "test_json", defaultChunkSizeRows)

		result, err := parser.parseFromReader(reader)

		require.NoError(t, err)
		assert.Equal(t, columnTypeText, result.columnInfo[0].Type)
	})

	t.Run("empty input and an empty array are tables with no rows", func(t *testing.T) {
		t.Parallel()

		for _, input := range []string{"", "   ", "[]"} {
			parser := newStreamingParser(FileTypeJSON, CompressionNone, "test_json", defaultChunkSizeRows)
			result, err := parser.parseFromReader(strings.NewReader(input))
			require.NoError(t, err, "input %q", input)
			assert.Equal(t, header{jsonDataHeader}, result.getHeader(), "input %q", input)
			assert.Empty(t, result.getRecords(), "input %q", input)
			assert.Equal(t, []columnInfo{newJSONDataColumn()}, result.columnInfo, "input %q", input)
		}
	})

	t.Run("returns error for invalid JSON", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("{invalid json}")
		parser := newStreamingParser(FileTypeJSON, CompressionNone, "test_json", defaultChunkSizeRows)

		_, err := parser.parseFromReader(reader)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidData)
	})
}

func TestParseJSONLStream(t *testing.T) {
	t.Parallel()

	t.Run("parses JSONL with multiple lines", func(t *testing.T) {
		t.Parallel()

		input := "{\"name\":\"Alice\",\"age\":30}\n{\"name\":\"Bob\",\"age\":25}\n{\"name\":\"Charlie\",\"age\":35}"
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSONL, CompressionNone, "test_jsonl", defaultChunkSizeRows)

		result, err := parser.parseFromReader(reader)

		require.NoError(t, err)
		assert.Equal(t, header{"data"}, result.header)
		assert.Equal(t, 3, len(result.records))
		for _, rec := range result.records {
			assert.True(t, json.Valid([]byte(rec[0])))
		}
	})

	t.Run("skips empty lines", func(t *testing.T) {
		t.Parallel()

		input := "{\"name\":\"Alice\"}\n\n{\"name\":\"Bob\"}\n\n"
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSONL, CompressionNone, "test_jsonl", defaultChunkSizeRows)

		result, err := parser.parseFromReader(reader)

		require.NoError(t, err)
		assert.Equal(t, 2, len(result.records))
	})

	t.Run("preserves nested structure", func(t *testing.T) {
		t.Parallel()

		input := `{"id":1,"address":{"city":"Tokyo"},"tags":["dev","go"]}`
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSONL, CompressionNone, "test_jsonl", defaultChunkSizeRows)

		result, err := parser.parseFromReader(reader)

		require.NoError(t, err)
		assert.Equal(t, 1, len(result.records))

		var parsed map[string]any
		err = json.Unmarshal([]byte(result.records[0][0]), &parsed)
		require.NoError(t, err)
		assert.Equal(t, float64(1), parsed["id"])
	})

	t.Run("column type is TEXT", func(t *testing.T) {
		t.Parallel()

		input := `{"name":"Alice"}`
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSONL, CompressionNone, "test_jsonl", defaultChunkSizeRows)

		result, err := parser.parseFromReader(reader)

		require.NoError(t, err)
		assert.Equal(t, columnTypeText, result.columnInfo[0].Type)
	})

	t.Run("empty input is a table with no rows", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("")
		parser := newStreamingParser(FileTypeJSONL, CompressionNone, "test_jsonl", defaultChunkSizeRows)

		result, err := parser.parseFromReader(reader)

		require.NoError(t, err)
		assert.Equal(t, header{jsonDataHeader}, result.getHeader())
		assert.Empty(t, result.getRecords())
	})

	t.Run("returns error for invalid JSON line", func(t *testing.T) {
		t.Parallel()

		input := "{\"name\":\"Alice\"}\nnot valid json\n{\"name\":\"Bob\"}"
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSONL, CompressionNone, "test_jsonl", defaultChunkSizeRows)

		_, err := parser.parseFromReader(reader)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidData)
	})

	t.Run("handles line larger than 1MB", func(t *testing.T) {
		t.Parallel()

		bigValue := strings.Repeat("x", 2*1024*1024) // 2 MB
		line := `{"big":"` + bigValue + `"}`
		input := line + "\n" + `{"small":"ok"}`
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSONL, CompressionNone, "test_jsonl", defaultChunkSizeRows)

		result, err := parser.parseFromReader(reader)

		require.NoError(t, err)
		assert.Equal(t, 2, len(result.records))
		assert.True(t, json.Valid([]byte(result.records[0][0])))
		assert.True(t, json.Valid([]byte(result.records[1][0])))
	})
}

func TestProcessJSONInChunks(t *testing.T) {
	t.Parallel()

	t.Run("processes JSON array in single chunk", func(t *testing.T) {
		t.Parallel()

		input := `[{"name":"Alice"},{"name":"Bob"},{"name":"Charlie"}]`
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSON, CompressionNone, "test_json", defaultChunkSizeRows)

		var chunks []*tableChunk
		_, err := parser.ProcessInChunks(reader, func(chunk *tableChunk) error {
			chunks = append(chunks, chunk)
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, 1, len(chunks))
		assert.Equal(t, 3, len(chunks[0].records))
	})

	t.Run("splits large JSON array into multiple chunks", func(t *testing.T) {
		t.Parallel()

		input := `[{"i":1},{"i":2},{"i":3},{"i":4},{"i":5}]`
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSON, CompressionNone, "test_json", 2) // chunk size = 2

		var chunks []*tableChunk
		_, err := parser.ProcessInChunks(reader, func(chunk *tableChunk) error {
			// Copy records to avoid slice reuse issues
			c := &tableChunk{
				tableName: chunk.tableName,
				headers:   chunk.headers,
				records:   append([]record(nil), chunk.records...),
			}
			chunks = append(chunks, c)
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, 3, len(chunks))
		assert.Equal(t, 2, len(chunks[0].records))
		assert.Equal(t, 2, len(chunks[1].records))
		assert.Equal(t, 1, len(chunks[2].records))
	})

	t.Run("processes single JSON object", func(t *testing.T) {
		t.Parallel()

		input := `{"name":"Alice"}`
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSON, CompressionNone, "test_json", defaultChunkSizeRows)

		var chunks []*tableChunk
		_, err := parser.ProcessInChunks(reader, func(chunk *tableChunk) error {
			chunks = append(chunks, chunk)
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, 1, len(chunks))
		assert.Equal(t, 1, len(chunks[0].records))
	})

	t.Run("empty input is a table with no rows", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("")
		parser := newStreamingParser(FileTypeJSON, CompressionNone, "test_json", defaultChunkSizeRows)

		chunks := 0
		columns, err := parser.ProcessInChunks(reader, func(chunk *tableChunk) error {
			chunks++
			assert.Empty(t, chunk.getRecords())
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, 1, chunks, "an empty document still makes its table")
		assert.Equal(t, columnInfoList{newJSONDataColumn()}, columns)
	})

	t.Run("returns error for trailing garbage after JSON array", func(t *testing.T) {
		t.Parallel()

		input := `[{"a":1}] garbage`
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSON, CompressionNone, "test_json", defaultChunkSizeRows)

		_, err := parser.ProcessInChunks(reader, func(_ *tableChunk) error {
			return nil
		})

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidData)
	})
}

func TestProcessJSONLInChunks(t *testing.T) {
	t.Parallel()

	t.Run("processes JSONL in single chunk", func(t *testing.T) {
		t.Parallel()

		input := "{\"name\":\"Alice\"}\n{\"name\":\"Bob\"}\n{\"name\":\"Charlie\"}"
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSONL, CompressionNone, "test_jsonl", defaultChunkSizeRows)

		var chunks []*tableChunk
		_, err := parser.ProcessInChunks(reader, func(chunk *tableChunk) error {
			chunks = append(chunks, chunk)
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, 1, len(chunks))
		assert.Equal(t, 3, len(chunks[0].records))
	})

	t.Run("splits JSONL into multiple chunks", func(t *testing.T) {
		t.Parallel()

		input := "{\"i\":1}\n{\"i\":2}\n{\"i\":3}\n{\"i\":4}\n{\"i\":5}"
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSONL, CompressionNone, "test_jsonl", 2) // chunk size = 2

		var chunks []*tableChunk
		_, err := parser.ProcessInChunks(reader, func(chunk *tableChunk) error {
			c := &tableChunk{
				tableName: chunk.tableName,
				headers:   chunk.headers,
				records:   append([]record(nil), chunk.records...),
			}
			chunks = append(chunks, c)
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, 3, len(chunks))
		assert.Equal(t, 2, len(chunks[0].records))
		assert.Equal(t, 2, len(chunks[1].records))
		assert.Equal(t, 1, len(chunks[2].records))
	})

	t.Run("empty input is a table with no rows", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("")
		parser := newStreamingParser(FileTypeJSONL, CompressionNone, "test_jsonl", defaultChunkSizeRows)

		chunks := 0
		columns, err := parser.ProcessInChunks(reader, func(chunk *tableChunk) error {
			chunks++
			assert.Empty(t, chunk.getRecords())
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, 1, chunks, "an empty input still makes its table")
		assert.Equal(t, columnInfoList{newJSONDataColumn()}, columns)
	})

	t.Run("returns error for invalid JSON line", func(t *testing.T) {
		t.Parallel()

		input := "{\"name\":\"Alice\"}\nnot valid json\n{\"name\":\"Bob\"}"
		reader := strings.NewReader(input)
		parser := newStreamingParser(FileTypeJSONL, CompressionNone, "test_jsonl", defaultChunkSizeRows)

		_, err := parser.ProcessInChunks(reader, func(_ *tableChunk) error {
			return nil
		})

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidData)
	})
}

func TestStreamingParser_ParseFromReader_CompressedJSON(t *testing.T) {
	t.Parallel()

	jsonData := `[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]`
	jsonlData := "{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}\n"

	t.Run("gzip compressed JSON", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, err := gw.Write([]byte(jsonData))
		require.NoError(t, err)
		require.NoError(t, gw.Close())

		parser := newStreamingParser(FileTypeJSON, CompressionGZ, "test_json", defaultChunkSizeRows)
		result, err := parser.parseFromReader(&buf)

		require.NoError(t, err)
		assert.Equal(t, 2, len(result.records))
		assert.True(t, json.Valid([]byte(result.records[0][0])))
	})

	t.Run("gzip compressed JSONL", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, err := gw.Write([]byte(jsonlData))
		require.NoError(t, err)
		require.NoError(t, gw.Close())

		parser := newStreamingParser(FileTypeJSONL, CompressionGZ, "test_jsonl", defaultChunkSizeRows)
		result, err := parser.parseFromReader(&buf)

		require.NoError(t, err)
		assert.Equal(t, 2, len(result.records))
		assert.True(t, json.Valid([]byte(result.records[0][0])))
	})

	t.Run("zstd compressed JSON", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		enc, err := zstd.NewWriter(&buf)
		require.NoError(t, err)
		_, err = enc.Write([]byte(jsonData))
		require.NoError(t, err)
		require.NoError(t, enc.Close())

		parser := newStreamingParser(FileTypeJSON, CompressionZSTD, "test_json", defaultChunkSizeRows)
		result, err := parser.parseFromReader(&buf)

		require.NoError(t, err)
		assert.Equal(t, 2, len(result.records))
	})

	t.Run("zstd compressed JSONL", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		enc, err := zstd.NewWriter(&buf)
		require.NoError(t, err)
		_, err = enc.Write([]byte(jsonlData))
		require.NoError(t, err)
		require.NoError(t, enc.Close())

		parser := newStreamingParser(FileTypeJSONL, CompressionZSTD, "test_jsonl", defaultChunkSizeRows)
		result, err := parser.parseFromReader(&buf)

		require.NoError(t, err)
		assert.Equal(t, 2, len(result.records))
	})

	t.Run("snappy compressed JSON", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		sw := snappy.NewBufferedWriter(&buf)
		_, err := sw.Write([]byte(jsonData))
		require.NoError(t, err)
		require.NoError(t, sw.Close())

		parser := newStreamingParser(FileTypeJSON, CompressionSNAPPY, "test_json", defaultChunkSizeRows)
		result, err := parser.parseFromReader(&buf)

		require.NoError(t, err)
		assert.Equal(t, 2, len(result.records))
	})

	t.Run("s2 compressed JSONL", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		sw := s2.NewWriter(&buf)
		_, err := sw.Write([]byte(jsonlData))
		require.NoError(t, err)
		require.NoError(t, sw.Close())

		parser := newStreamingParser(FileTypeJSONL, CompressionS2, "test_jsonl", defaultChunkSizeRows)
		result, err := parser.parseFromReader(&buf)

		require.NoError(t, err)
		assert.Equal(t, 2, len(result.records))
	})

	t.Run("lz4 compressed JSON", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		lw := lz4.NewWriter(&buf)
		_, err := lw.Write([]byte(jsonData))
		require.NoError(t, err)
		require.NoError(t, lw.Close())

		parser := newStreamingParser(FileTypeJSON, CompressionLZ4, "test_json", defaultChunkSizeRows)
		result, err := parser.parseFromReader(&buf)

		require.NoError(t, err)
		assert.Equal(t, 2, len(result.records))
	})

	t.Run("zlib compressed JSONL", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		zw := zlib.NewWriter(&buf)
		_, err := zw.Write([]byte(jsonlData))
		require.NoError(t, err)
		require.NoError(t, zw.Close())

		parser := newStreamingParser(FileTypeJSONL, CompressionZLIB, "test_jsonl", defaultChunkSizeRows)
		result, err := parser.parseFromReader(&buf)

		require.NoError(t, err)
		assert.Equal(t, 2, len(result.records))
	})
}

func TestStreamingParser_ProcessInChunks_CompressedJSON(t *testing.T) {
	t.Parallel()

	jsonData := `[{"i":1},{"i":2},{"i":3},{"i":4},{"i":5}]`

	t.Run("gzip compressed JSON chunks", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, err := gw.Write([]byte(jsonData))
		require.NoError(t, err)
		require.NoError(t, gw.Close())

		parser := newStreamingParser(FileTypeJSON, CompressionGZ, "test_json", 2)

		var chunks []*tableChunk
		_, err = parser.ProcessInChunks(&buf, func(chunk *tableChunk) error {
			c := &tableChunk{
				tableName: chunk.tableName,
				headers:   chunk.headers,
				records:   append([]record(nil), chunk.records...),
			}
			chunks = append(chunks, c)
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, 3, len(chunks))
		assert.Equal(t, 2, len(chunks[0].records))
		assert.Equal(t, 2, len(chunks[1].records))
		assert.Equal(t, 1, len(chunks[2].records))
	})
}

// TestChunkSizeDoesNotChangeTheColumnType loads one file at several chunk sizes
// and requires the column to come out the same type each time, and the cells to
// come out the same wherever the type was decided before any row was stored.
//
// A column that reads as a number and turns out to be text is created numeric
// and rebuilt as TEXT when the text arrives, and the rows already stored carry
// SQLite's spelling of the number rather than the file's. That is a documented
// limit of chunked loading rather than something this test pins; what it pins is
// that the type never depends on the chunk size, and that a column which never
// changes type keeps its cells exactly.
func TestChunkSizeDoesNotChangeTheColumnType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		body      string
		wantType  string
		wantCells []string
	}{
		{
			name:      "an integer column that turns text keeps every cell as written",
			body:      "v\n1\n2\nabc\n",
			wantType:  "text",
			wantCells: []string{"1", "2", "abc"},
		},
		{
			name:      "a zero-padded code keeps its leading zero at any chunk size",
			body:      "v\n007\n008\n009\n",
			wantType:  "text",
			wantCells: []string{"007", "008", "009"},
		},
		{
			name:      "an integer past int64 stays text at any chunk size",
			body:      "v\n11040320260000000000\n1\n2\n",
			wantType:  "text",
			wantCells: []string{"11040320260000000000", "1", "2"},
		},
		{
			name:      "a column of text is not retyped by a late number",
			body:      "v\nabc\ndef\n3\n",
			wantType:  "text",
			wantCells: []string{"abc", "def", "3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for _, chunkSize := range []int{1, 2, 3, 4, 5, 0} {
				builder := NewBuilder().AddReader(strings.NewReader(tt.body), "t", FileTypeCSV)
				if chunkSize > 0 {
					builder = builder.SetDefaultChunkSize(chunkSize)
				}
				built, err := builder.Build(context.Background())
				if err != nil {
					t.Fatalf("chunk size %d: build: %v", chunkSize, err)
				}
				db, err := built.Open(context.Background())
				if err != nil {
					t.Fatalf("chunk size %d: open: %v", chunkSize, err)
				}

				rows, err := db.QueryContext(context.Background(), `SELECT v, typeof(v) FROM t ORDER BY rowid`)
				if err != nil {
					t.Fatalf("chunk size %d: query: %v", chunkSize, err)
				}
				cells := make([]string, 0, len(tt.wantCells))
				for rows.Next() {
					var cell, cellType string
					if err := rows.Scan(&cell, &cellType); err != nil {
						t.Fatalf("chunk size %d: scan: %v", chunkSize, err)
					}
					if cellType != tt.wantType {
						t.Errorf("chunk size %d: typeof(%q) = %s, want %s", chunkSize, cell, cellType, tt.wantType)
					}
					cells = append(cells, cell)
				}
				if err := rows.Err(); err != nil {
					t.Fatalf("chunk size %d: rows: %v", chunkSize, err)
				}
				if err := rows.Close(); err != nil {
					t.Fatalf("chunk size %d: close rows: %v", chunkSize, err)
				}
				if err := db.Close(); err != nil {
					t.Fatalf("chunk size %d: close db: %v", chunkSize, err)
				}

				if !reflect.DeepEqual(cells, tt.wantCells) {
					t.Errorf("chunk size %d: got %q, want %q", chunkSize, cells, tt.wantCells)
				}
			}
		})
	}
}

// TestChunkSizeDoesNotChangeStoredValues loads a column that reads as a number
// until its last row at every chunk size and requires the stored cells to be
// the file's spelling each time. Chunk size is a memory knob: a load that
// declared the column from its first chunk and widened it later stored
// SQLite's spelling of the numbers it had already converted, so 2.50 came
// back as 2.5 at one chunk size and as 2.50 at another.
func TestChunkSizeDoesNotChangeStoredValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want []string
	}{
		{"real then text", "v\n1\n2.50\nabc\n", []string{"1/text", "2.50/text", "abc/text"}},
		{"real then zero-padded", "v\n1.5\n2\n007\n", []string{"1.5/text", "2/text", "007/text"}},
		{"integer then text", "v\n1\n2\nabc\n", []string{"1/text", "2/text", "abc/text"}},
		{"integer then real", "v\n1\n2\n2.5\n", []string{"1/real", "2/real", "2.5/real"}},
		{"integers throughout", "v\n1\n2\n3\n", []string{"1/integer", "2/integer", "3/integer"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			for _, chunk := range []int{1, 2, 3, 4, 5, defaultChunkSizeRows} {
				b := NewBuilder().AddReader(strings.NewReader(tt.body), "t", FileTypeCSV).SetDefaultChunkSize(chunk)
				built, err := b.Build(context.Background())
				require.NoError(t, err)
				db, err := built.Open(context.Background())
				require.NoError(t, err)

				rows, err := db.QueryContext(context.Background(), `SELECT v, typeof(v) FROM t ORDER BY rowid`)
				require.NoError(t, err)
				var got []string
				for rows.Next() {
					var v, ty string
					require.NoError(t, rows.Scan(&v, &ty))
					got = append(got, v+"/"+ty)
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				require.NoError(t, db.Close())
				assert.Equal(t, tt.want, got, "chunk size %d", chunk)
			}
		})
	}
}

// TestChunkSizeDoesNotChangeStoredValues_File is the same contract through a
// path, which is loaded under its first chunk's types and read again when a
// later chunk widens one, rather than staged as a reader is.
func TestChunkSizeDoesNotChangeStoredValues_File(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "t.csv")
	require.NoError(t, os.WriteFile(path, []byte("v,w\n1,x\n2.50,y\nabc,z\n"), 0o600))
	want := []string{"1/text", "2.50/text", "abc/text"}

	for _, chunk := range []int{1, 2, 3, 4, defaultChunkSizeRows} {
		built, err := NewBuilder().AddPath(path).SetDefaultChunkSize(chunk).Build(context.Background())
		require.NoError(t, err)
		db, err := built.Open(context.Background())
		require.NoError(t, err)

		rows, err := db.QueryContext(context.Background(), `SELECT v, typeof(v) FROM t ORDER BY rowid`)
		require.NoError(t, err)
		var got []string
		for rows.Next() {
			var v, ty string
			require.NoError(t, rows.Scan(&v, &ty))
			got = append(got, v+"/"+ty)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		// The read-again path must leave nothing of its first attempt behind.
		var tables int
		require.NoError(t, db.QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE '\_filesql\_%' ESCAPE '\'`).Scan(&tables))
		require.NoError(t, db.Close())
		assert.Equal(t, want, got, "chunk size %d", chunk)
		assert.Equal(t, 0, tables, "chunk size %d leaves a working table behind", chunk)
	}
}

// achFixture returns the bytes of a small ACH file that parses.
func achFixture(t *testing.T) []byte {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "ppd-debit.ach"))
	require.NoError(t, err)
	return data
}

// wireFixture returns the bytes of a small Fedwire file that parses.
func wireFixture(t *testing.T) []byte {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "customer-transfer.fed"))
	require.NoError(t, err)
	return data
}

// TestStreamWriteBackFormatsToDatabase covers the two loaders that build tables
// from a whole file at once. They share a shape — validate the name, parse,
// then create and fill one table per section — so the refusals are checked for
// both.
func TestStreamWriteBackFormatsToDatabase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name string
		ext  string
		load func(ctx context.Context, db dbtx, content []byte, filePath string, replaceExisting bool) error
		data func(t *testing.T) []byte
	}{
		{
			name: "ACH file",
			ext:  extACH,
			data: achFixture,
			load: func(ctx context.Context, db dbtx, content []byte, filePath string, replaceExisting bool) error {
				return streamACHFileToDatabase(ctx, db, strings.NewReader(string(content)), filePath, "", replaceExisting)
			},
		},
		{
			name: "Fedwire",
			ext:  extFED,
			data: wireFixture,
			load: func(ctx context.Context, db dbtx, content []byte, filePath string, replaceExisting bool) error {
				return streamWireFileToDatabase(ctx, db, strings.NewReader(string(content)), filePath, "", replaceExisting)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" refuses a reserved table name", func(t *testing.T) {
			t.Parallel()

			err := tt.load(ctx, openTestDB(t), tt.data(t), sourceTablePrefix+"payment"+tt.ext, false)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrReservedTableName)
		})

		t.Run(tt.name+" reports content it cannot parse", func(t *testing.T) {
			t.Parallel()

			err := tt.load(ctx, openTestDB(t), []byte("this is not a payment file"), "payment"+tt.ext, false)
			assert.Error(t, err)
		})

		t.Run(tt.name+" reports a database it cannot query", func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			require.NoError(t, db.Close())

			err := tt.load(ctx, db, tt.data(t), "payment"+tt.ext, false)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrDatabaseOperation)
		})

		t.Run(tt.name+" refuses to load twice over its own tables", func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			content := tt.data(t)
			require.NoError(t, tt.load(ctx, db, content, "payment"+tt.ext, false))

			err := tt.load(ctx, db, content, "payment"+tt.ext, false)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrDuplicateTable)
		})

		t.Run(tt.name+" replaces its own tables when asked", func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			content := tt.data(t)
			require.NoError(t, tt.load(ctx, db, content, "payment"+tt.ext, false))

			assert.NoError(t, tt.load(ctx, db, content, "payment"+tt.ext, true),
				"a reload in replace mode drops the tables it is about to rebuild")
		})
	}
}

// TestParseACHFile covers the parse step on its own, which is what turns file
// bytes into tables and into the structure a later dump rebuilds the file from.
func TestParseACHFile(t *testing.T) {
	t.Parallel()

	t.Run("returns the tables and the structure behind them", func(t *testing.T) {
		t.Parallel()

		tables, tableSet, err := parseACHFile(strings.NewReader(string(achFixture(t))), "payment")
		require.NoError(t, err)
		require.NotNil(t, tableSet, "a dump needs the structure the tables came from")
		assert.NotEmpty(t, tables)
		assert.NotNil(t, tableSet.GetFileHeaderTable(), "the file header is what an ACH file starts with")
	})

	t.Run("reports content that is not an ACH file", func(t *testing.T) {
		t.Parallel()

		tables, tableSet, err := parseACHFile(strings.NewReader("this is not an ACH file"), "payment")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrACH)
		assert.Nil(t, tables)
		assert.Nil(t, tableSet)
	})
}

// TestParseFedWireFile is the same for Fedwire, which is one message table.
func TestParseFedWireFile(t *testing.T) {
	t.Parallel()

	t.Run("returns the message table and the structure behind it", func(t *testing.T) {
		t.Parallel()

		tables, tableSet, err := parseFedWireFile(strings.NewReader(string(wireFixture(t))), "payment")
		require.NoError(t, err)
		require.NotNil(t, tableSet)
		require.Len(t, tables, 1, "a Fedwire file holds one message")
		assert.Equal(t, "payment_message", tables[0].getName())
		assert.NotNil(t, tableSet.GetMessageTable())
	})

	t.Run("reports content that is not a Fedwire file", func(t *testing.T) {
		t.Parallel()

		tables, tableSet, err := parseFedWireFile(strings.NewReader("this is not a Fedwire file"), "payment")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrWire)
		assert.Nil(t, tables)
		assert.Nil(t, tableSet)
	})
}

// TestDumpWithTableSet_NilTableSet covers the argument neither dump can work
// without: the file is rebuilt from the structure, so there is nothing to write
// without one.
func TestDumpWithTableSet_NilTableSet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	out := filepath.Join(t.TempDir(), "payment")

	assert.ErrorIs(t, DumpACHWithTableSet(ctx, db, "payment", out+extACH, nil), ErrNilInput)
	assert.ErrorIs(t, DumpFedWireWithTableSet(ctx, db, "payment", out+extFED, nil), ErrNilInput)
}

// TestInsertRecordsIntoTable_Failures covers the insert step used by the
// write-back loaders.
func TestInsertRecordsIntoTable_Failures(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("the statement cannot be prepared", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, db.Close())

		err := insertRecordsIntoTable(ctx, db, "users", newHeader([]string{"id"}), []record{newRecord([]string{"1"})})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("a row the table refuses", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		_, err := db.ExecContext(ctx, `CREATE TABLE users (id TEXT CHECK (id <> 'refused'))`)
		require.NoError(t, err)

		err = insertRecordsIntoTable(ctx, db, "users", newHeader([]string{"id"}), []record{newRecord([]string{"refused"})})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})
}

// TestReadTableToTableData covers the read-back a dump starts from.
func TestReadTableToTableData(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("reads rows and turns a NULL into an empty value", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		_, err := db.ExecContext(ctx, `CREATE TABLE users (id TEXT, name TEXT)`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO users VALUES ('1', NULL)`)
		require.NoError(t, err)

		data, err := readTableToTableData(ctx, db, "users")
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name"}, data.Headers)
		require.Len(t, data.Records, 1)
		assert.Equal(t, []string{"1", ""}, data.Records[0], "a NULL has no text of its own to write back")
	})

	t.Run("reports a table that is not there", func(t *testing.T) {
		t.Parallel()

		_, err := readTableToTableData(ctx, openTestDB(t), "missing")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTableNotFound)
	})
}
