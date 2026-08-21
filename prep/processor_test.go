package prep

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/nao1215/filesql/parser"
	"github.com/parquet-go/parquet-go"
)

// TestRecord is a test struct for processing
type TestRecord struct {
	Name  string `prep:"trim" validate:"required"`
	Email string `prep:"trim"`
	Age   string
}

func TestProcessor_Process_CSV(t *testing.T) {
	t.Parallel()

	csvData := `name,email,age
  John Doe  ,john@example.com,30
Jane Smith,jane@example.com,25
  ,invalid,
Bob Wilson,bob@example.com,35
`

	tests := []struct {
		name           string
		input          string
		wantRowCount   int
		wantValidCount int
		wantErrorCount int
	}{
		{
			name:           "CSV with trim and required validation",
			input:          csvData,
			wantRowCount:   4,
			wantValidCount: 3, // One row has empty name after trim
			wantErrorCount: 1, // One required validation error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			processor := NewProcessor(parser.CSV)
			var records []TestRecord

			reader, result, err := processor.Process(strings.NewReader(tt.input), &records)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			if reader == nil {
				t.Fatal("Process() returned nil reader")
			}

			if result.RowCount != tt.wantRowCount {
				t.Errorf("RowCount = %d, want %d", result.RowCount, tt.wantRowCount)
			}

			if result.ValidRowCount != tt.wantValidCount {
				t.Errorf("ValidRowCount = %d, want %d", result.ValidRowCount, tt.wantValidCount)
			}

			if len(result.Errors) != tt.wantErrorCount {
				t.Errorf("len(Errors) = %d, want %d", len(result.Errors), tt.wantErrorCount)
			}

			// Verify struct population
			if len(records) != tt.wantRowCount {
				t.Errorf("len(records) = %d, want %d", len(records), tt.wantRowCount)
			}

			// Verify trim was applied
			if len(records) > 0 && records[0].Name != "John Doe" {
				t.Errorf("Name not trimmed: got %q, want %q", records[0].Name, "John Doe")
			}
		})
	}
}

func TestProcessor_Process_TSV(t *testing.T) {
	t.Parallel()

	tsvData := "name\temail\tage\n  Alice  \talice@example.com\t28\nBob\tbob@example.com\t32\n"

	processor := NewProcessor(parser.TSV)
	var records []TestRecord

	reader, result, err := processor.Process(strings.NewReader(tsvData), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if reader == nil {
		t.Fatal("Process() returned nil reader")
	}

	if result.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", result.RowCount)
	}

	if len(records) != 2 {
		t.Errorf("len(records) = %d, want 2", len(records))
	}

	// Verify trim was applied
	if records[0].Name != "Alice" {
		t.Errorf("Name not trimmed: got %q, want %q", records[0].Name, "Alice")
	}
}

func TestProcessor_Process_LTSV(t *testing.T) {
	t.Parallel()

	ltsvData := "name:Charlie\temail:charlie@example.com\tage:40\nname:Diana\temail:diana@example.com\tage:35\n"

	processor := NewProcessor(parser.LTSV)
	var records []TestRecord

	reader, result, err := processor.Process(strings.NewReader(ltsvData), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if reader == nil {
		t.Fatal("Process() returned nil reader")
	}

	if result.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", result.RowCount)
	}
}

func TestProcessor_OutputReader(t *testing.T) {
	t.Parallel()

	csvData := `name,email,age
  John  ,john@example.com,30
`

	processor := NewProcessor(parser.CSV)
	var records []TestRecord

	reader, _, err := processor.Process(strings.NewReader(csvData), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	// Read the output
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	outputStr := string(output)

	// Verify the output contains trimmed data
	if !strings.Contains(outputStr, "John") {
		t.Errorf("Output should contain trimmed name 'John', got: %s", outputStr)
	}

	// Verify output is valid CSV (contains header and data)
	lines := strings.Split(strings.TrimSpace(outputStr), "\n")
	if len(lines) != 2 {
		t.Errorf("Output should have 2 lines (header + 1 data row), got %d", len(lines))
	}
}

func TestProcessor_ValidationError(t *testing.T) {
	t.Parallel()

	csvData := `name,email,age
,john@example.com,30
`

	processor := NewProcessor(parser.CSV)
	var records []TestRecord

	_, result, err := processor.Process(strings.NewReader(csvData), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if !result.HasErrors() {
		t.Error("Expected validation errors for empty required field")
	}

	validationErrors := result.ValidationErrors()
	if len(validationErrors) != 1 {
		t.Errorf("Expected 1 validation error, got %d", len(validationErrors))
	}

	if len(validationErrors) > 0 {
		ve := validationErrors[0]
		if ve.Row != 1 {
			t.Errorf("ValidationError.Row = %d, want 1", ve.Row)
		}
		if ve.Field != "Name" {
			t.Errorf("ValidationError.Field = %q, want %q", ve.Field, "Name")
		}
		if ve.Tag != "required" {
			t.Errorf("ValidationError.Tag = %q, want %q", ve.Tag, "required")
		}
	}
}

func TestProcessor_EmptyFile(t *testing.T) {
	t.Parallel()

	processor := NewProcessor(parser.CSV)
	var records []TestRecord

	_, _, err := processor.Process(strings.NewReader(""), &records)
	if err == nil {
		t.Error("Expected error for empty file")
	}
}

func TestProcessor_InvalidStructSlicePointer(t *testing.T) {
	t.Parallel()

	processor := NewProcessor(parser.CSV)

	// Test with non-pointer
	var records []TestRecord
	_, _, err := processor.Process(strings.NewReader("a,b,c\n1,2,3"), records)
	if err == nil {
		t.Error("Expected error for non-pointer")
	}

	// Test with pointer to non-slice
	var record TestRecord
	_, _, err = processor.Process(strings.NewReader("a,b,c\n1,2,3"), &record)
	if err == nil {
		t.Error("Expected error for pointer to non-slice")
	}
}

func TestProcessor_Process_Parquet(t *testing.T) {
	t.Parallel()

	// Create a parquet file in memory
	type ParquetRow struct {
		Name  string `parquet:"name"`
		Email string `parquet:"email"`
		Age   string `parquet:"age"`
	}

	rows := []ParquetRow{
		{Name: "  John Doe  ", Email: "john@example.com", Age: "30"},
		{Name: "Jane Smith", Email: "jane@example.com", Age: "25"},
		{Name: "", Email: "invalid@example.com", Age: "40"},
	}

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[ParquetRow](&buf)
	_, err := writer.Write(rows)
	if err != nil {
		t.Fatalf("failed to write parquet data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close parquet writer: %v", err)
	}

	processor := NewProcessor(parser.Parquet)
	var records []TestRecord

	reader, result, err := processor.Process(bytes.NewReader(buf.Bytes()), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if reader == nil {
		t.Fatal("Process() returned nil reader")
	}

	if result.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", result.RowCount)
	}

	// One row has empty name after trim, which fails required validation
	if result.ValidRowCount != 2 {
		t.Errorf("ValidRowCount = %d, want 2", result.ValidRowCount)
	}

	if len(result.Errors) != 1 {
		t.Errorf("len(Errors) = %d, want 1", len(result.Errors))
	}

	// Verify struct population
	if len(records) != 3 {
		t.Errorf("len(records) = %d, want 3", len(records))
	}

	// Verify trim was applied
	if len(records) > 0 && records[0].Name != "John Doe" {
		t.Errorf("Name not trimmed: got %q, want %q", records[0].Name, "John Doe")
	}

	// Verify columns were captured
	expectedColumns := []string{"name", "email", "age"}
	if len(result.Columns) != len(expectedColumns) {
		t.Errorf("len(Columns) = %d, want %d", len(result.Columns), len(expectedColumns))
	}
	for i, col := range result.Columns {
		if col != expectedColumns[i] {
			t.Errorf("Columns[%d] = %q, want %q", i, col, expectedColumns[i])
		}
	}

	// Verify original format
	if result.OriginalFormat != parser.Parquet {
		t.Errorf("OriginalFormat = %v, want %v", result.OriginalFormat, parser.Parquet)
	}
}

// EdgeCaseRecord is a struct for edge case testing
type EdgeCaseRecord struct {
	Col1 string `name:"col1" prep:"trim"`
	Col2 string `name:"col2" prep:"trim"`
	Col3 string `name:"col3" prep:"trim"`
}

func TestProcessor_CSV_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        string
		wantRowCount int
		wantColCount int
		wantErr      bool
		wantCol1     string // expected value for first record's Col1 (empty string means skip check)
		wantCol1Len  int    // expected length of Col1 (0 means skip check)
		checkRow     int    // which row to check for wantCol3 (-1 means no check)
		wantCol3     string // expected value for Col3 at checkRow
	}{
		{
			name:         "very long line",
			input:        "col1,col2,col3\n" + strings.Repeat("a", 10000) + ",b,c\n",
			wantRowCount: 1,
			wantColCount: 3,
			wantErr:      false,
			wantCol1Len:  10000,
			checkRow:     -1,
		},
		{
			name:         "many columns (50)",
			input:        strings.Join(makeHeaders(50), ",") + "\n" + strings.Join(makeValues(50), ",") + "\n",
			wantRowCount: 1,
			wantColCount: 50,
			wantErr:      false,
			checkRow:     -1,
		},
		{
			name:         "uneven rows - short row",
			input:        "col1,col2,col3\na,b,c\nd,e\nf,g,h\n",
			wantRowCount: 0,
			wantColCount: 0,
			wantErr:      true, // the parser package returns an error for a mismatched column count
			checkRow:     -1,
		},
		{
			name:         "empty file",
			input:        "",
			wantRowCount: 0,
			wantColCount: 0,
			wantErr:      true, // ErrEmptyFile
			checkRow:     -1,
		},
		{
			name:         "header only",
			input:        "col1,col2,col3\n",
			wantRowCount: 0,
			wantColCount: 3,
			wantErr:      false,
			checkRow:     -1,
		},
		{
			name:         "quoted fields with commas",
			input:        "col1,col2,col3\n\"a,b\",c,d\n",
			wantRowCount: 1,
			wantColCount: 3,
			wantErr:      false,
			wantCol1:     "a,b",
			checkRow:     -1,
		},
		{
			name:         "quoted fields with newlines",
			input:        "col1,col2,col3\n\"line1\nline2\",b,c\n",
			wantRowCount: 1,
			wantColCount: 3,
			wantErr:      false,
			wantCol1:     "line1\nline2",
			checkRow:     -1,
		},
		{
			name:         "unicode content",
			input:        "col1,col2,col3\n日本語,한국어,中文\n",
			wantRowCount: 1,
			wantColCount: 3,
			wantErr:      false,
			wantCol1:     "日本語",
			checkRow:     -1,
		},
		{
			name:         "whitespace-only values",
			input:        "col1,col2,col3\n   ,\t\t,  \n",
			wantRowCount: 1,
			wantColCount: 3,
			wantErr:      false,
			checkRow:     -1,
			// trim preprocessor removes whitespace - checked separately
		},
		{
			name:         "empty values between commas",
			input:        "col1,col2,col3\n,,\na,,c\n,b,\n",
			wantRowCount: 3,
			wantColCount: 3,
			wantErr:      false,
			checkRow:     -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			processor := NewProcessor(parser.CSV)
			var records []EdgeCaseRecord

			reader, result, err := processor.Process(strings.NewReader(tt.input), &records)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if reader == nil {
				t.Fatal("Process() returned nil reader")
			}

			if result.RowCount != tt.wantRowCount {
				t.Errorf("RowCount = %d, want %d", result.RowCount, tt.wantRowCount)
			}

			if len(result.Columns) != tt.wantColCount {
				t.Errorf("Column count = %d, want %d", len(result.Columns), tt.wantColCount)
			}

			if len(records) > 0 {
				if tt.wantCol1 != "" && records[0].Col1 != tt.wantCol1 {
					t.Errorf("Col1 = %q, want %q", records[0].Col1, tt.wantCol1)
				}
				if tt.wantCol1Len > 0 && len(records[0].Col1) != tt.wantCol1Len {
					t.Errorf("Col1 length = %d, want %d", len(records[0].Col1), tt.wantCol1Len)
				}
			}

			if tt.checkRow >= 0 && tt.checkRow < len(records) {
				if records[tt.checkRow].Col3 != tt.wantCol3 {
					t.Errorf("Row %d Col3 = %q, want %q", tt.checkRow, records[tt.checkRow].Col3, tt.wantCol3)
				}
			}
		})
	}
}

func TestProcessor_CSV_WhitespaceValues(t *testing.T) {
	t.Parallel()

	input := "col1,col2,col3\n   ,\t\t,  \n"
	processor := NewProcessor(parser.CSV)
	var records []EdgeCaseRecord

	_, _, err := processor.Process(strings.NewReader(input), &records)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(records))
	}

	// trim preprocessor should remove whitespace
	if records[0].Col1 != "" {
		t.Errorf("Whitespace-only Col1 should be trimmed to empty: got %q", records[0].Col1)
	}
}

func TestProcessor_CSV_EmptyValues(t *testing.T) {
	t.Parallel()

	input := "col1,col2,col3\n,,\na,,c\n,b,\n"
	processor := NewProcessor(parser.CSV)
	var records []EdgeCaseRecord

	_, _, err := processor.Process(strings.NewReader(input), &records)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("Expected 3 records, got %d", len(records))
	}

	// First row: all empty
	if records[0].Col1 != "" || records[0].Col2 != "" || records[0].Col3 != "" {
		t.Errorf("First row should have all empty: got %q, %q, %q", records[0].Col1, records[0].Col2, records[0].Col3)
	}
}

// makeHeaders creates n unique header names
func makeHeaders(n int) []string {
	headers := make([]string, n)
	// Ensure first 3 are col1, col2, col3 for struct mapping
	for i := range n {
		if i < 3 {
			headers[i] = "col" + string(rune('1'+i))
		} else {
			headers[i] = "column_" + strconv.Itoa(i)
		}
	}
	return headers
}

// makeValues creates n test values
func makeValues(n int) []string {
	values := make([]string, n)
	for i := range n {
		values[i] = "val" + string(rune('a'+i%26))
	}
	return values
}

func TestProcessor_CSV_LargeColumnCount(t *testing.T) {
	t.Parallel()

	// Test with 100 columns to ensure no issues with many columns
	colCount := 100
	headers := make([]string, colCount)
	values := make([]string, colCount)
	for i := range colCount {
		headers[i] = "c" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		values[i] = "v" + string(rune('0'+i%10))
	}

	// Map first 3 columns to struct
	headers[0], headers[1], headers[2] = "col1", "col2", "col3"

	input := strings.Join(headers, ",") + "\n" + strings.Join(values, ",") + "\n"

	processor := NewProcessor(parser.CSV)
	var records []EdgeCaseRecord

	_, result, err := processor.Process(strings.NewReader(input), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", result.RowCount)
	}

	if len(result.Columns) != colCount {
		t.Errorf("Column count = %d, want %d", len(result.Columns), colCount)
	}
}

func TestProcessor_CSV_ManyRows(t *testing.T) {
	t.Parallel()

	// Test with 1000 rows to ensure no issues with many rows
	rowCount := 1000
	var buf strings.Builder
	buf.WriteString("col1,col2,col3\n")
	for i := range rowCount {
		buf.WriteString("a" + string(rune('0'+i%10)) + ",b,c\n")
	}

	processor := NewProcessor(parser.CSV)
	var records []EdgeCaseRecord

	_, result, err := processor.Process(strings.NewReader(buf.String()), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if result.RowCount != rowCount {
		t.Errorf("RowCount = %d, want %d", result.RowCount, rowCount)
	}

	if len(records) != rowCount {
		t.Errorf("len(records) = %d, want %d", len(records), rowCount)
	}
}

// JSONRecord is a test struct for JSON/JSONL processing.
// The parser package stores JSON data in a single "data" column containing raw JSON strings.
type JSONRecord struct {
	Data string `name:"data" prep:"trim" validate:"required"`
}

func TestProcessor_Process_JSON(t *testing.T) {
	t.Parallel()

	jsonData := `[{"key":"value1"},{"key":"value2"},{"key":"value3"}]`

	processor := NewProcessor(parser.JSON)
	var records []JSONRecord

	reader, result, err := processor.Process(strings.NewReader(jsonData), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if reader == nil {
		t.Fatal("Process() returned nil reader")
	}

	if result.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", result.RowCount)
	}

	if result.ValidRowCount != 3 {
		t.Errorf("ValidRowCount = %d, want 3", result.ValidRowCount)
	}

	if len(records) != 3 {
		t.Errorf("len(records) = %d, want 3", len(records))
	}

	// Verify struct population — each record contains raw JSON
	if len(records) > 0 && records[0].Data != `{"key":"value1"}` {
		t.Errorf("records[0].Data = %q, want %q", records[0].Data, `{"key":"value1"}`)
	}

	// Verify output format is JSONL
	s, ok := reader.(Stream)
	if !ok {
		t.Fatal("reader does not implement Stream")
	}
	if s.Format() != parser.JSONL {
		t.Errorf("Format() = %v, want JSONL", s.Format())
	}
	if s.OriginalFormat() != parser.JSON {
		t.Errorf("OriginalFormat() = %v, want JSON", s.OriginalFormat())
	}
}

func TestProcessor_Process_JSONL(t *testing.T) {
	t.Parallel()

	jsonlData := "{\"name\":\"Alice\"}\n{\"name\":\"Bob\"}\n{\"name\":\"Charlie\"}\n"

	processor := NewProcessor(parser.JSONL)
	var records []JSONRecord

	reader, result, err := processor.Process(strings.NewReader(jsonlData), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if reader == nil {
		t.Fatal("Process() returned nil reader")
	}

	if result.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", result.RowCount)
	}

	if len(records) != 3 {
		t.Errorf("len(records) = %d, want 3", len(records))
	}

	if len(records) > 0 && records[0].Data != `{"name":"Alice"}` {
		t.Errorf("records[0].Data = %q, want %q", records[0].Data, `{"name":"Alice"}`)
	}

	// Verify output format is JSONL
	s, ok := reader.(Stream)
	if !ok {
		t.Fatal("reader does not implement Stream")
	}
	if s.Format() != parser.JSONL {
		t.Errorf("Format() = %v, want JSONL", s.Format())
	}
}

func TestProcessor_JSON_OutputReader(t *testing.T) {
	t.Parallel()

	jsonData := `[{"a":1},{"a":2}]`

	processor := NewProcessor(parser.JSON)
	var records []JSONRecord

	reader, _, err := processor.Process(strings.NewReader(jsonData), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	outputStr := string(output)
	lines := strings.Split(strings.TrimSpace(outputStr), "\n")
	if len(lines) != 2 {
		t.Errorf("Output should have 2 lines, got %d: %q", len(lines), outputStr)
	}

	if lines[0] != `{"a":1}` {
		t.Errorf("line[0] = %q, want %q", lines[0], `{"a":1}`)
	}
	if lines[1] != `{"a":2}` {
		t.Errorf("line[1] = %q, want %q", lines[1], `{"a":2}`)
	}
}

func TestProcessor_JSON_Validation(t *testing.T) {
	t.Parallel()

	// Use a struct that requires numeric validation on the raw JSON string.
	// A JSON object like {"key":"value"} is not numeric, so validation fails.
	type NumericRecord struct {
		Data string `name:"data" validate:"numeric"`
	}

	jsonData := `[{"key":"value"}]`

	processor := NewProcessor(parser.JSON)
	var records []NumericRecord

	_, result, err := processor.Process(strings.NewReader(jsonData), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if !result.HasErrors() {
		t.Error("Expected validation errors for non-numeric JSON value")
	}

	if result.ValidRowCount != 0 {
		t.Errorf("ValidRowCount = %d, want 0", result.ValidRowCount)
	}
}

func TestProcessor_JSONL_EmptyFile(t *testing.T) {
	t.Parallel()

	processor := NewProcessor(parser.JSONL)
	var records []JSONRecord

	_, _, err := processor.Process(strings.NewReader(""), &records)
	if err == nil {
		t.Error("Expected error for empty JSONL file")
	}
}

func TestProcessor_JSON_SingleObject(t *testing.T) {
	t.Parallel()

	jsonData := `{"key":"value"}`

	processor := NewProcessor(parser.JSON)
	var records []JSONRecord

	_, result, err := processor.Process(strings.NewReader(jsonData), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", result.RowCount)
	}

	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}

	if records[0].Data != `{"key":"value"}` {
		t.Errorf("records[0].Data = %q, want %q", records[0].Data, `{"key":"value"}`)
	}
}

func TestProcessor_JSON_InvalidJSONAfterPrep(t *testing.T) {
	t.Parallel()

	// nullify={} turns {} into "", emptying the JSON data.
	// This produces a PrepError("empty_json_data"). Process succeeds because
	// the second row still produces valid JSONL output.
	type NullifyRecord struct {
		Data string `name:"data" prep:"nullify={}"`
	}

	jsonData := `[{}, {"key":"value"}]`

	processor := NewProcessor(parser.JSON)
	var records []NullifyRecord

	reader, result, err := processor.Process(strings.NewReader(jsonData), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	// First row: {} → nullified to "" → PrepError("empty_json_data") → not counted as valid
	// Second row: {"key":"value"} → untouched → valid JSON → counted as valid
	if result.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", result.RowCount)
	}

	if result.ValidRowCount != 1 {
		t.Errorf("ValidRowCount = %d, want 1", result.ValidRowCount)
	}

	// Verify the PrepError was recorded for the emptied row
	prepErrors := result.PrepErrors()
	if len(prepErrors) != 1 {
		t.Fatalf("Expected 1 PrepError, got %d", len(prepErrors))
	}
	if prepErrors[0].Tag != "empty_json_data" {
		t.Errorf("PrepError.Tag = %q, want %q", prepErrors[0].Tag, "empty_json_data")
	}

	// Verify JSONL output has 1 line (the empty row is skipped, matching ValidRowCount)
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) != 1 {
		t.Errorf("JSONL output should have 1 line, got %d: %q", len(lines), string(output))
	}
	if lines[0] != `{"key":"value"}` {
		t.Errorf("JSONL output line = %q, want %q", lines[0], `{"key":"value"}`)
	}
}

func TestProcessor_JSON_TruncateDestroysJSON(t *testing.T) {
	t.Parallel()

	// truncate=5 on {"key":"value"} produces {"key → invalid JSON.
	// Process returns ErrInvalidJSONAfterPrep because invalid JSON lines
	// would cause downstream JSONL parsers to fail.
	type TruncateRecord struct {
		Data string `name:"data" prep:"truncate=5"`
	}

	jsonData := `[{"key":"value"}]`

	processor := NewProcessor(parser.JSON)
	var records []TruncateRecord

	_, _, err := processor.Process(strings.NewReader(jsonData), &records)
	if err == nil {
		t.Fatal("Expected error for invalid JSON after truncate, got nil")
	}

	if !errors.Is(err, ErrInvalidJSONAfterPrep) {
		t.Errorf("err = %v, want ErrInvalidJSONAfterPrep", err)
	}

	// Verify the error message includes the row number and truncated value
	errMsg := err.Error()
	if !strings.Contains(errMsg, "row 1") {
		t.Errorf("error should contain row number, got: %s", errMsg)
	}
	if !strings.Contains(errMsg, `{"key`) {
		t.Errorf("error should contain truncated value, got: %s", errMsg)
	}
}

func TestProcessor_JSON_AllRowsEmptied(t *testing.T) {
	t.Parallel()

	// When all JSON rows become empty after preprocessing, Process returns
	// ErrEmptyJSONOutput because an empty JSONL stream is unparseable.
	type NullifyAllRecord struct {
		Data string `name:"data" prep:"nullify={}"`
	}

	jsonData := `[{}]`

	processor := NewProcessor(parser.JSON)
	var records []NullifyAllRecord

	_, _, err := processor.Process(strings.NewReader(jsonData), &records)
	if err == nil {
		t.Fatal("Expected error for all-empty JSON output, got nil")
	}

	if !errors.Is(err, ErrEmptyJSONOutput) {
		t.Errorf("err = %v, want ErrEmptyJSONOutput", err)
	}
}

func TestProcessor_JSON_PrettyPrinted(t *testing.T) {
	t.Parallel()

	// Pretty-printed JSON contains newlines within each element.
	// writeJSONL must compact each element to a single line, otherwise
	// downstream JSONL parsers see partial JSON on each line and fail.
	jsonData := `[
  {
    "name": "Alice",
    "age": 30
  },
  {
    "name": "Bob",
    "age": 25
  }
]`

	processor := NewProcessor(parser.JSON)
	var records []JSONRecord

	reader, result, err := processor.Process(strings.NewReader(jsonData), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", result.RowCount)
	}

	// Read JSONL output and verify each line is exactly one compact JSON value
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) != 2 {
		t.Fatalf("Output should have 2 lines, got %d: %q", len(lines), string(output))
	}

	// Each line must be valid JSON and contain no newlines (compact)
	want := []string{
		`{"name":"Alice","age":30}`,
		`{"name":"Bob","age":25}`,
	}
	for i, line := range lines {
		if !json.Valid([]byte(line)) {
			t.Errorf("line %d is not valid JSON: %q", i+1, line)
		}
		if line != want[i] {
			t.Errorf("line %d = %q, want %q", i+1, line, want[i])
		}
	}
}

func TestProcessor_JSON_PrettyPrintedGzip(t *testing.T) {
	t.Parallel()

	// Verify that compressed pretty-printed JSON also produces compact JSONL.
	// Decompression is handled by the parser package, but the full pipeline
	// (decompress → parse → prep → compact → JSONL) should be exercised.
	prettyJSON := `[
  {"name": "Alice", "age": 30},
  {"name": "Bob", "age": 25}
]`

	// Gzip compress in memory
	var compressed bytes.Buffer
	gw := gzip.NewWriter(&compressed)
	if _, err := gw.Write([]byte(prettyJSON)); err != nil {
		t.Fatalf("gzip write error: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close error: %v", err)
	}

	processor := NewProcessor(parser.JSONGZ)
	var records []JSONRecord

	reader, result, err := processor.Process(bytes.NewReader(compressed.Bytes()), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", result.RowCount)
	}

	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) != 2 {
		t.Fatalf("Output should have 2 lines, got %d: %q", len(lines), string(output))
	}

	for i, line := range lines {
		if !json.Valid([]byte(line)) {
			t.Errorf("line %d is not valid JSON: %q", i+1, line)
		}
		if line != strings.TrimSpace(line) {
			t.Errorf("line %d has leading/trailing whitespace: %q", i+1, line)
		}
		if strings.Contains(line, "\t") {
			t.Errorf("line %d contains tab character: %q", i+1, line)
		}
	}
}

// TestSetFieldValue_IntTypes tests type conversion for int, int8, int16, int32, int64 fields
// via Process(), comparing results with go-cmp.
func TestSetFieldValue_IntTypes(t *testing.T) {
	t.Parallel()

	type IntRecord struct {
		ValInt   int   `name:"val_int"`
		ValInt8  int8  `name:"val_int8"`
		ValInt16 int16 `name:"val_int16"`
		ValInt32 int32 `name:"val_int32"`
		ValInt64 int64 `name:"val_int64"`
	}

	t.Run("valid int values are converted correctly", func(t *testing.T) {
		t.Parallel()
		csvData := "val_int,val_int8,val_int16,val_int32,val_int64\n42,127,32767,2147483647,9223372036854775807\n-100,-128,-32768,-2147483648,-9223372036854775808\n"
		var records []IntRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		want := []IntRecord{
			{ValInt: 42, ValInt8: 127, ValInt16: 32767, ValInt32: 2147483647, ValInt64: 9223372036854775807},
			{ValInt: -100, ValInt8: -128, ValInt16: -32768, ValInt32: -2147483648, ValInt64: -9223372036854775808},
		}
		if diff := cmp.Diff(want, records); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}
		if result.RowCount != 2 {
			t.Errorf("RowCount = %d, want 2", result.RowCount)
		}
	})

	t.Run("empty int values default to zero", func(t *testing.T) {
		t.Parallel()
		csvData := "val_int,val_int8,val_int16,val_int32,val_int64\n,,,,\n"
		var records []IntRecord

		processor := NewProcessor(FileTypeCSV)
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		want := []IntRecord{{}}
		if diff := cmp.Diff(want, records); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("invalid int value produces type_conversion error", func(t *testing.T) {
		t.Parallel()
		csvData := "val_int,val_int8,val_int16,val_int32,val_int64\nnot-a-number,0,0,0,0\n"
		var records []IntRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if len(result.Errors) == 0 {
			t.Fatal("expected at least 1 error for invalid int, got 0")
		}
		var pe *PrepError
		if !errors.As(result.Errors[0], &pe) {
			t.Fatalf("expected PrepError, got %T", result.Errors[0])
		}
		if pe.Row != 1 {
			t.Errorf("Row = %d, want 1", pe.Row)
		}
		if pe.Column != "val_int" {
			t.Errorf("Column = %q, want %q", pe.Column, "val_int")
		}
		if pe.Tag != "type_conversion" {
			t.Errorf("Tag = %q, want %q", pe.Tag, "type_conversion")
		}
	})

	t.Run("int8 overflow produces type_conversion error", func(t *testing.T) {
		t.Parallel()
		csvData := "val_int,val_int8,val_int16,val_int32,val_int64\n0,128,0,0,0\n"
		var records []IntRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if len(result.Errors) == 0 {
			t.Fatal("expected at least 1 error for int8 overflow, got 0")
		}
		var pe *PrepError
		if !errors.As(result.Errors[0], &pe) {
			t.Fatalf("expected PrepError, got %T", result.Errors[0])
		}
		if pe.Row != 1 {
			t.Errorf("Row = %d, want 1", pe.Row)
		}
		if pe.Column != "val_int8" {
			t.Errorf("Column = %q, want %q", pe.Column, "val_int8")
		}
		if pe.Tag != "type_conversion" {
			t.Errorf("Tag = %q, want %q", pe.Tag, "type_conversion")
		}
	})

	t.Run("platform-specific int max values are converted correctly", func(t *testing.T) {
		t.Parallel()
		// Use math.MaxInt/MinInt which are platform-dependent (32-bit or 64-bit)
		maxIntStr := strconv.FormatInt(int64(int(^uint(0)>>1)), 10)
		minIntStr := strconv.FormatInt(int64(-int(^uint(0)>>1)-1), 10)
		csvData := "val_int,val_int8,val_int16,val_int32,val_int64\n" +
			maxIntStr + ",0,0,0,0\n" +
			minIntStr + ",0,0,0,0\n"
		var records []IntRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if len(result.Errors) != 0 {
			t.Errorf("expected 0 errors for platform max int, got %d: %v", len(result.Errors), result.Errors)
		}
		maxInt := int(^uint(0) >> 1)
		minInt := -maxInt - 1
		if records[0].ValInt != maxInt {
			t.Errorf("ValInt = %d, want %d (platform max)", records[0].ValInt, maxInt)
		}
		if records[1].ValInt != minInt {
			t.Errorf("ValInt = %d, want %d (platform min)", records[1].ValInt, minInt)
		}
	})
}

// TestSetFieldValue_UintPlatformMax tests platform-dependent uint max value conversion
func TestSetFieldValue_UintPlatformMax(t *testing.T) {
	t.Parallel()

	type UintRecord struct {
		ValUint   uint   `name:"val_uint"`
		ValUint8  uint8  `name:"val_uint8"`
		ValUint16 uint16 `name:"val_uint16"`
		ValUint32 uint32 `name:"val_uint32"`
		ValUint64 uint64 `name:"val_uint64"`
	}

	t.Run("platform-specific uint max value is converted correctly", func(t *testing.T) {
		t.Parallel()
		maxUintStr := strconv.FormatUint(uint64(^uint(0)), 10)
		csvData := "val_uint,val_uint8,val_uint16,val_uint32,val_uint64\n" +
			maxUintStr + ",0,0,0,0\n"
		var records []UintRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if len(result.Errors) != 0 {
			t.Errorf("expected 0 errors for platform max uint, got %d: %v", len(result.Errors), result.Errors)
		}
		maxUint := ^uint(0)
		if records[0].ValUint != maxUint {
			t.Errorf("ValUint = %d, want %d (platform max)", records[0].ValUint, maxUint)
		}
	})
}

// TestSetFieldValue_UintTypes tests type conversion for uint, uint8, uint16, uint32, uint64 fields
// via Process(), comparing results with go-cmp.
func TestSetFieldValue_UintTypes(t *testing.T) {
	t.Parallel()

	type UintRecord struct {
		ValUint   uint   `name:"val_uint"`
		ValUint8  uint8  `name:"val_uint8"`
		ValUint16 uint16 `name:"val_uint16"`
		ValUint32 uint32 `name:"val_uint32"`
		ValUint64 uint64 `name:"val_uint64"`
	}

	t.Run("valid uint values are converted correctly", func(t *testing.T) {
		t.Parallel()
		csvData := "val_uint,val_uint8,val_uint16,val_uint32,val_uint64\n42,255,65535,4294967295,18446744073709551615\n"
		var records []UintRecord

		processor := NewProcessor(FileTypeCSV)
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		want := []UintRecord{
			{ValUint: 42, ValUint8: 255, ValUint16: 65535, ValUint32: 4294967295, ValUint64: 18446744073709551615},
		}
		if diff := cmp.Diff(want, records); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("empty uint values default to zero", func(t *testing.T) {
		t.Parallel()
		csvData := "val_uint,val_uint8,val_uint16,val_uint32,val_uint64\n,,,,\n"
		var records []UintRecord

		processor := NewProcessor(FileTypeCSV)
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		want := []UintRecord{{}}
		if diff := cmp.Diff(want, records); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("negative value for uint produces type_conversion error", func(t *testing.T) {
		t.Parallel()
		csvData := "val_uint,val_uint8,val_uint16,val_uint32,val_uint64\n-1,0,0,0,0\n"
		var records []UintRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if len(result.Errors) == 0 {
			t.Fatal("expected at least 1 error for negative uint, got 0")
		}
		var pe *PrepError
		if !errors.As(result.Errors[0], &pe) {
			t.Fatalf("expected PrepError, got %T", result.Errors[0])
		}
		if pe.Row != 1 {
			t.Errorf("Row = %d, want 1", pe.Row)
		}
		if pe.Column != "val_uint" {
			t.Errorf("Column = %q, want %q", pe.Column, "val_uint")
		}
		if pe.Tag != "type_conversion" {
			t.Errorf("Tag = %q, want %q", pe.Tag, "type_conversion")
		}
	})

	t.Run("non-numeric value for uint produces type_conversion error", func(t *testing.T) {
		t.Parallel()
		csvData := "val_uint,val_uint8,val_uint16,val_uint32,val_uint64\nabc,0,0,0,0\n"
		var records []UintRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if len(result.Errors) == 0 {
			t.Fatal("expected at least 1 error for non-numeric uint, got 0")
		}
		var pe *PrepError
		if !errors.As(result.Errors[0], &pe) {
			t.Fatalf("expected PrepError, got %T", result.Errors[0])
		}
		if pe.Row != 1 {
			t.Errorf("Row = %d, want 1", pe.Row)
		}
		if pe.Column != "val_uint" {
			t.Errorf("Column = %q, want %q", pe.Column, "val_uint")
		}
		if pe.Tag != "type_conversion" {
			t.Errorf("Tag = %q, want %q", pe.Tag, "type_conversion")
		}
	})
}

// TestSetFieldValue_FloatTypes tests type conversion for float32 and float64 fields
// via Process(), comparing results with go-cmp.
func TestSetFieldValue_FloatTypes(t *testing.T) {
	t.Parallel()

	type FloatRecord struct {
		ValFloat32 float32 `name:"val_float32"`
		ValFloat64 float64 `name:"val_float64"`
	}

	t.Run("valid float values are converted correctly", func(t *testing.T) {
		t.Parallel()
		csvData := "val_float32,val_float64\n1.5,3.14\n"
		var records []FloatRecord

		processor := NewProcessor(FileTypeCSV)
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		want := []FloatRecord{{ValFloat32: 1.5, ValFloat64: 3.14}}
		if diff := cmp.Diff(want, records); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("empty float values default to zero", func(t *testing.T) {
		t.Parallel()
		csvData := "val_float32,val_float64\n,\n"
		var records []FloatRecord

		processor := NewProcessor(FileTypeCSV)
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		want := []FloatRecord{{}}
		if diff := cmp.Diff(want, records); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("invalid float value produces type_conversion error", func(t *testing.T) {
		t.Parallel()
		csvData := "val_float32,val_float64\nnot-float,0\n"
		var records []FloatRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if len(result.Errors) == 0 {
			t.Fatal("expected at least 1 error for invalid float, got 0")
		}
		var pe *PrepError
		if !errors.As(result.Errors[0], &pe) {
			t.Fatalf("expected PrepError, got %T", result.Errors[0])
		}
		if pe.Row != 1 {
			t.Errorf("Row = %d, want 1", pe.Row)
		}
		if pe.Column != "val_float32" {
			t.Errorf("Column = %q, want %q", pe.Column, "val_float32")
		}
		if pe.Tag != "type_conversion" {
			t.Errorf("Tag = %q, want %q", pe.Tag, "type_conversion")
		}
	})
}

// TestSetFieldValue_BoolType tests type conversion for bool fields
// via Process(), comparing results with go-cmp.
func TestSetFieldValue_BoolType(t *testing.T) {
	t.Parallel()

	type BoolRecord struct {
		ValBool bool   `name:"val_bool"`
		Dummy   string `name:"dummy"`
	}

	t.Run("true/false/1/0 are converted correctly", func(t *testing.T) {
		t.Parallel()
		csvData := "val_bool,dummy\ntrue,a\nfalse,b\n1,c\n0,d\n"
		var records []BoolRecord

		processor := NewProcessor(FileTypeCSV)
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		want := []BoolRecord{
			{ValBool: true, Dummy: "a"},
			{ValBool: false, Dummy: "b"},
			{ValBool: true, Dummy: "c"},
			{ValBool: false, Dummy: "d"},
		}
		if diff := cmp.Diff(want, records); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("empty bool value defaults to false", func(t *testing.T) {
		t.Parallel()
		csvData := "val_bool,dummy\n,x\n"
		var records []BoolRecord

		processor := NewProcessor(FileTypeCSV)
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		want := []BoolRecord{{ValBool: false, Dummy: "x"}}
		if diff := cmp.Diff(want, records); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("invalid bool value produces type_conversion error", func(t *testing.T) {
		t.Parallel()
		csvData := "val_bool,dummy\nnot-bool,x\n"
		var records []BoolRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if len(result.Errors) == 0 {
			t.Fatal("expected at least 1 error for invalid bool, got 0")
		}
		var pe *PrepError
		if !errors.As(result.Errors[0], &pe) {
			t.Fatalf("expected PrepError, got %T", result.Errors[0])
		}
		if pe.Row != 1 {
			t.Errorf("Row = %d, want 1", pe.Row)
		}
		if pe.Column != "val_bool" {
			t.Errorf("Column = %q, want %q", pe.Column, "val_bool")
		}
		if pe.Tag != "type_conversion" {
			t.Errorf("Tag = %q, want %q", pe.Tag, "type_conversion")
		}
	})
}

// TestSetFieldValue_StringType tests string field handling via Process().
func TestSetFieldValue_StringType(t *testing.T) {
	t.Parallel()

	type StringRecord struct {
		Name  string
		Email string
	}

	t.Run("string values are set correctly", func(t *testing.T) {
		t.Parallel()
		csvData := "name,email\nhello,world@example.com\n"
		var records []StringRecord

		processor := NewProcessor(FileTypeCSV)
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		want := []StringRecord{{Name: "hello", Email: "world@example.com"}}
		if diff := cmp.Diff(want, records); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("empty string values are set as empty", func(t *testing.T) {
		t.Parallel()
		csvData := "name,email\n,\n"
		var records []StringRecord

		processor := NewProcessor(FileTypeCSV)
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		want := []StringRecord{{Name: "", Email: ""}}
		if diff := cmp.Diff(want, records); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}
	})
}

// TestSetFieldValue_MixedTypes tests a struct with multiple type fields in one pass.
func TestSetFieldValue_MixedTypes(t *testing.T) {
	t.Parallel()

	type MixedRecord struct {
		Name   string
		Age    int
		Score  float64
		Active bool
		Level  uint8
	}

	csvData := "name,age,score,active,level\nAlice,30,95.5,true,5\nBob,,,,\n"
	var records []MixedRecord

	processor := NewProcessor(FileTypeCSV)
	_, _, err := processor.Process(strings.NewReader(csvData), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	want := []MixedRecord{
		{Name: "Alice", Age: 30, Score: 95.5, Active: true, Level: 5},
		{Name: "Bob", Age: 0, Score: 0, Active: false, Level: 0},
	}
	if diff := cmp.Diff(want, records); diff != "" {
		t.Errorf("records mismatch (-want +got):\n%s", diff)
	}
}

func TestWithValidRowsOnly(t *testing.T) {
	t.Parallel()

	type Record struct {
		Name  string `validate:"required"`
		Email string `validate:"email"`
	}

	t.Run("only valid rows in output and struct slice", func(t *testing.T) {
		t.Parallel()
		csvData := "name,email\nAlice,alice@example.com\n,invalid\nBob,bob@example.com\n"
		var records []Record
		processor := NewProcessor(FileTypeCSV, WithValidRowsOnly())
		reader, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		// Total rows should be 3, valid rows should be 2
		if result.RowCount != 3 {
			t.Errorf("RowCount = %d, want 3", result.RowCount)
		}
		if result.ValidRowCount != 2 {
			t.Errorf("ValidRowCount = %d, want 2", result.ValidRowCount)
		}

		// Struct slice should contain only 2 valid records
		if len(records) != 2 {
			t.Fatalf("len(records) = %d, want 2", len(records))
		}
		want := []Record{
			{Name: "Alice", Email: "alice@example.com"},
			{Name: "Bob", Email: "bob@example.com"},
		}
		if diff := cmp.Diff(want, records); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}

		// Output should contain only valid rows
		output, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		outputStr := string(output)
		if strings.Contains(outputStr, "invalid") {
			t.Errorf("output should not contain invalid rows, got:\n%s", outputStr)
		}
		lines := strings.Split(strings.TrimSpace(outputStr), "\n")
		// Header + 2 valid rows = 3 lines
		if len(lines) != 3 {
			t.Errorf("output lines = %d, want 3 (header + 2 valid rows), got:\n%s", len(lines), outputStr)
		}
	})

	t.Run("all rows valid produces same output as default", func(t *testing.T) {
		t.Parallel()
		csvData := "name,email\nAlice,alice@example.com\nBob,bob@example.com\n"

		var records1 []Record
		proc1 := NewProcessor(FileTypeCSV)
		reader1, result1, err := proc1.Process(strings.NewReader(csvData), &records1)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		out1, err := io.ReadAll(reader1)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}

		var records2 []Record
		proc2 := NewProcessor(FileTypeCSV, WithValidRowsOnly())
		reader2, result2, err := proc2.Process(strings.NewReader(csvData), &records2)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		out2, err := io.ReadAll(reader2)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}

		if result1.RowCount != result2.RowCount || result1.ValidRowCount != result2.ValidRowCount {
			t.Errorf("result counts differ: default=%d/%d, validOnly=%d/%d",
				result1.RowCount, result1.ValidRowCount, result2.RowCount, result2.ValidRowCount)
		}
		if string(out1) != string(out2) {
			t.Errorf("output differs:\ndefault:\n%s\nvalidOnly:\n%s", out1, out2)
		}
	})

	t.Run("all rows invalid produces empty output", func(t *testing.T) {
		t.Parallel()
		csvData := "name,email\n,invalid1\n,invalid2\n"
		var records []Record
		processor := NewProcessor(FileTypeCSV, WithValidRowsOnly())
		reader, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if result.ValidRowCount != 0 {
			t.Errorf("ValidRowCount = %d, want 0", result.ValidRowCount)
		}
		if len(records) != 0 {
			t.Errorf("len(records) = %d, want 0", len(records))
		}
		output, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		// Only header line should remain
		lines := strings.Split(strings.TrimSpace(string(output)), "\n")
		if len(lines) != 1 {
			t.Errorf("output lines = %d, want 1 (header only), got:\n%s", len(lines), output)
		}
	})
}

// errWriter is a writer that always returns an error, used for testing write error paths.
type errWriter struct{}

func (errWriter) Write([]byte) (int, error) {
	return 0, errors.New("write error")
}

func TestWriteCSV_ErrorPath(t *testing.T) {
	t.Parallel()

	p := NewProcessor(FileTypeCSV)

	t.Run("header write error", func(t *testing.T) {
		t.Parallel()
		err := p.writeCSV(errWriter{}, []string{"a", "b"}, [][]string{{"1", "2"}})
		if err == nil {
			t.Error("expected error from errWriter, got nil")
		}
	})
}

func TestWriteTSV_ErrorPath(t *testing.T) {
	t.Parallel()

	p := &Processor{fileType: parser.TSV}

	t.Run("header write error", func(t *testing.T) {
		t.Parallel()
		err := p.writeTSV(errWriter{}, []string{"a", "b"}, [][]string{{"1", "2"}})
		if err == nil {
			t.Error("expected error from errWriter, got nil")
		}
	})
}

func TestWriteLTSV_ErrorPath(t *testing.T) {
	t.Parallel()

	p := &Processor{fileType: parser.LTSV}

	t.Run("write error returns error", func(t *testing.T) {
		t.Parallel()
		err := p.writeLTSV(errWriter{}, []string{"key"}, [][]string{{"value"}})
		if err == nil {
			t.Error("expected error from errWriter, got nil")
		}
	})
}

func TestWriteJSONL_ErrorPath(t *testing.T) {
	t.Parallel()

	p := &Processor{fileType: parser.JSONL}

	t.Run("write error returns error", func(t *testing.T) {
		t.Parallel()
		err := p.writeJSONL(errWriter{}, [][]string{{`{"key":"value"}`}})
		if err == nil {
			t.Error("expected error from errWriter, got nil")
		}
	})

	t.Run("empty record is skipped", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		err := p.writeJSONL(&buf, [][]string{{""}, {}, {`{"a":1}`}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(buf.String(), `{"a":1}`) {
			t.Errorf("expected valid JSON in output, got: %s", buf.String())
		}
		// Empty records should be skipped, so only 1 line
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		if len(lines) != 1 {
			t.Errorf("expected 1 line (empty records skipped), got %d: %s", len(lines), buf.String())
		}
	})
}

// TestProcess_SliceReset verifies that reusing the same destination slice
// does not carry over stale elements from a previous Process call.
func TestProcess_SliceReset(t *testing.T) {
	t.Parallel()

	type Row struct {
		Name string `validate:"required"`
	}

	processor := NewProcessor(parser.CSV)

	// First call: populate the slice with one element
	records := []Row{{Name: "stale"}}
	_, _, err := processor.Process(strings.NewReader("name\nAlice\n"), &records)
	if err != nil {
		t.Fatalf("first Process: %v", err)
	}
	if len(records) != 1 || records[0].Name != "Alice" {
		t.Fatalf("first Process: got %v, want [{Alice}]", records)
	}

	// Second call with different data; old element must not survive
	_, _, err = processor.Process(strings.NewReader("name\nBob\n"), &records)
	if err != nil {
		t.Fatalf("second Process: %v", err)
	}
	if len(records) != 1 || records[0].Name != "Bob" {
		t.Errorf("second Process: got %v, want [{Bob}]", records)
	}
}

// TestProcess_SentinelErrorWrapping verifies that parser errors are
// wrapped so callers can match with errors.Is.
func TestProcess_SentinelErrorWrapping(t *testing.T) {
	t.Parallel()

	type Row struct {
		Col1 string
	}

	t.Run("empty file wraps ErrEmptyFile", func(t *testing.T) {
		t.Parallel()
		processor := NewProcessor(parser.CSV)
		var records []Row
		_, _, err := processor.Process(strings.NewReader(""), &records)
		if err == nil {
			t.Fatal("expected error for empty input")
		}
		if !errors.Is(err, ErrEmptyFile) {
			t.Errorf("expected errors.Is(err, ErrEmptyFile), got: %v", err)
		}
	})

	t.Run("unsupported file type wraps ErrUnsupportedFileType", func(t *testing.T) {
		t.Parallel()
		processor := NewProcessor(parser.FileType(9999))
		var records []Row
		_, _, err := processor.Process(strings.NewReader("data"), &records)
		if err == nil {
			t.Fatal("expected error for unsupported file type")
		}
		if !errors.Is(err, ErrUnsupportedFileType) {
			t.Errorf("expected errors.Is(err, ErrUnsupportedFileType), got: %v", err)
		}
	})
}

// TestProcess_CrossFieldValidationWithDefault verifies that cross-field
// validators work correctly when the target column is absent from the input
// but has a default value via prep:"default=...".
func TestProcess_CrossFieldValidationWithDefault(t *testing.T) {
	t.Parallel()

	type Row struct {
		Status  string `prep:"default=active"`
		Comment string `validate:"required_if=Status active"`
	}

	csvData := "comment\nhello\n"
	processor := NewProcessor(parser.CSV)
	var records []Row

	_, result, err := processor.Process(strings.NewReader(csvData), &records)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	// Status is not in CSV headers but gets default="active" via prep.
	// required_if=Status active should trigger, and comment="hello" satisfies it.
	if result.ValidRowCount != 1 {
		t.Errorf("ValidRowCount = %d, want 1; errors: %v", result.ValidRowCount, result.Errors)
	}

	// Now test with empty comment — should fail required_if
	csvData2 := "comment\n\n"
	var records2 []Row
	_, result2, err := processor.Process(strings.NewReader(csvData2), &records2)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if result2.ValidRowCount != 0 {
		t.Errorf("ValidRowCount = %d, want 0 (comment is empty while status=active)", result2.ValidRowCount)
	}
}

// TestProcess_TagCaching verifies that calling Process multiple times
// on the same Processor with the same struct type reuses cached tags.
func TestProcess_TagCaching(t *testing.T) {
	t.Parallel()

	type Row struct {
		Name string `prep:"trim" validate:"required"`
	}

	processor := NewProcessor(parser.CSV)

	for i := range 3 {
		var records []Row
		_, result, err := processor.Process(
			strings.NewReader("name\n  Alice  \n"),
			&records,
		)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if result.ValidRowCount != 1 {
			t.Errorf("iteration %d: ValidRowCount = %d, want 1", i, result.ValidRowCount)
		}
		if len(records) != 1 || records[0].Name != "Alice" {
			t.Errorf("iteration %d: records = %v, want [{Alice}]", i, records)
		}
	}
}

// TestProcessToWriter verifies that ProcessToWriter writes output
// directly to a writer and returns the same result as Process.
func TestProcessToWriter(t *testing.T) {
	t.Parallel()

	type Row struct {
		Name  string `prep:"trim" validate:"required"`
		Email string `prep:"trim,lowercase"`
	}

	csvData := "name,email\n  John  ,JOHN@EXAMPLE.COM\n"
	processor := NewProcessor(parser.CSV)

	// Use Process for reference
	var records1 []Row
	reader, result1, err := processor.Process(strings.NewReader(csvData), &records1)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	refOutput, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	// Use ProcessToWriter
	var records2 []Row
	var buf bytes.Buffer
	result2, err := processor.ProcessToWriter(strings.NewReader(csvData), &records2, &buf)
	if err != nil {
		t.Fatalf("ProcessToWriter: %v", err)
	}

	if result1.RowCount != result2.RowCount {
		t.Errorf("RowCount mismatch: Process=%d, ProcessToWriter=%d", result1.RowCount, result2.RowCount)
	}
	if result1.ValidRowCount != result2.ValidRowCount {
		t.Errorf("ValidRowCount mismatch: Process=%d, ProcessToWriter=%d", result1.ValidRowCount, result2.ValidRowCount)
	}
	if string(refOutput) != buf.String() {
		t.Errorf("output mismatch:\nProcess:         %q\nProcessToWriter: %q", string(refOutput), buf.String())
	}
	if len(records1) != len(records2) {
		t.Errorf("records length mismatch: %d vs %d", len(records1), len(records2))
	}
}

// TestProcessToWriter_NilWriter verifies that ProcessToWriter returns
// ErrNilWriter instead of panicking when a nil writer is passed.
func TestProcessToWriter_NilWriter(t *testing.T) {
	t.Parallel()

	type Row struct {
		Name string
	}

	processor := NewProcessor(parser.CSV)
	var records []Row

	_, err := processor.ProcessToWriter(strings.NewReader("name\nAlice\n"), &records, nil)
	if err == nil {
		t.Fatal("expected error for nil writer")
	}
	if !errors.Is(err, ErrNilWriter) {
		t.Errorf("expected errors.Is(err, ErrNilWriter), got: %v", err)
	}

	// typed-nil: var w io.Writer = (*bytes.Buffer)(nil)
	var typedNil *bytes.Buffer
	var records2 []Row
	_, err = processor.ProcessToWriter(strings.NewReader("name\nAlice\n"), &records2, typedNil)
	if err == nil {
		t.Fatal("expected error for typed-nil writer")
	}
	if !errors.Is(err, ErrNilWriter) {
		t.Errorf("typed-nil: expected errors.Is(err, ErrNilWriter), got: %v", err)
	}
}

// TestProcessToWriter_WriterError verifies that ProcessToWriter propagates
// write errors from the underlying writer.
func TestProcessToWriter_WriterError(t *testing.T) {
	t.Parallel()

	type Row struct {
		Name string
	}

	processor := NewProcessor(parser.CSV)
	var records []Row

	_, err := processor.ProcessToWriter(strings.NewReader("name\nAlice\n"), &records, errWriter{})
	if err == nil {
		t.Fatal("expected error from failing writer")
	}
	if !strings.Contains(err.Error(), "write error") {
		t.Errorf("expected write error in message, got: %v", err)
	}
}

// TestProcessToWriter_WithValidRowsOnly verifies that ProcessToWriter
// respects the WithValidRowsOnly option.
func TestProcessToWriter_WithValidRowsOnly(t *testing.T) {
	t.Parallel()

	type Row struct {
		Name string `validate:"required"`
	}

	// CSV with 3 rows: Alice (valid), empty name (invalid), Bob (valid)
	csvData := "name\nAlice\n\"\"\nBob\n"
	processor := NewProcessor(parser.CSV, WithValidRowsOnly())

	var records []Row
	var buf bytes.Buffer
	result, err := processor.ProcessToWriter(strings.NewReader(csvData), &records, &buf)
	if err != nil {
		t.Fatalf("ProcessToWriter: %v", err)
	}

	if result.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", result.RowCount)
	}
	if result.ValidRowCount != 2 {
		t.Errorf("ValidRowCount = %d, want 2", result.ValidRowCount)
	}
	if len(records) != 2 {
		t.Errorf("records length = %d, want 2", len(records))
	}
	// Output should contain header + 2 valid rows only
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 { // header + 2 data rows
		t.Errorf("output lines = %d, want 3 (header + 2 valid rows), got:\n%s", len(lines), buf.String())
	}
}

// TestProcessToWriter_JSONEmptyOutput verifies that ProcessToWriter returns
// ErrEmptyJSONOutput when all JSON rows are empty after preprocessing.
func TestProcessToWriter_JSONEmptyOutput(t *testing.T) {
	t.Parallel()

	type Row struct {
		Data string `prep:"nullify=null"`
	}

	jsonlData := "null\n"
	processor := NewProcessor(parser.JSONL)
	var records []Row
	var buf bytes.Buffer

	_, err := processor.ProcessToWriter(strings.NewReader(jsonlData), &records, &buf)
	if err == nil {
		t.Fatal("expected ErrEmptyJSONOutput")
	}
	if !errors.Is(err, ErrEmptyJSONOutput) {
		t.Errorf("expected errors.Is(err, ErrEmptyJSONOutput), got: %v", err)
	}
}

// TestProcess_NilReader verifies that passing a nil reader returns
// ErrNilReader instead of ErrEmptyFile.
func TestProcess_NilReader(t *testing.T) {
	t.Parallel()

	type Row struct {
		Name string
	}

	processor := NewProcessor(parser.CSV)
	var records []Row

	_, _, err := processor.Process(nil, &records)
	if err == nil {
		t.Fatal("expected error for nil reader")
	}
	if !errors.Is(err, ErrNilReader) {
		t.Errorf("expected errors.Is(err, ErrNilReader), got: %v", err)
	}
	if errors.Is(err, ErrEmptyFile) {
		t.Error("nil reader should NOT match ErrEmptyFile")
	}
}

// TestSetFieldValue_UnsupportedTypes verifies that unsupported field types
// produce an error rather than silently succeeding.
func TestSetFieldValue_UnsupportedTypes(t *testing.T) {
	t.Parallel()

	type Row struct {
		Name string
		Data []byte
	}

	csvData := "name,data\nAlice,hello\n"
	processor := NewProcessor(parser.CSV)
	var records []Row

	_, result, err := processor.Process(strings.NewReader(csvData), &records)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	// The []byte (slice) field should trigger a type_conversion PrepError
	if !result.HasErrors() {
		t.Error("expected errors for unsupported field type ([]byte)")
	}
	found := false
	for _, e := range result.PrepErrors() {
		if strings.Contains(e.Message, "unsupported field type") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected 'unsupported field type' error, got: %v", result.Errors)
	}
}

// TestCachedParseStructType_Concurrent verifies that the sync.Map-based
// struct tag cache is safe under concurrent access.
func TestCachedParseStructType_Concurrent(t *testing.T) {
	t.Parallel()

	type Row struct {
		Name  string `prep:"trim" validate:"required"`
		Email string `prep:"lowercase" validate:"email"`
		Age   string `validate:"numeric"`
	}

	csvData := "name,email,age\n  Alice  ,ALICE@EXAMPLE.COM,30\n"
	processor := NewProcessor(parser.CSV)

	const goroutines = 50
	errs := make(chan error, goroutines)

	for range goroutines {
		go func() {
			var records []Row
			_, result, err := processor.Process(strings.NewReader(csvData), &records)
			if err != nil {
				errs <- fmt.Errorf("Process: %w", err)
				return
			}
			if result.ValidRowCount != 1 {
				errs <- fmt.Errorf("ValidRowCount = %d, want 1", result.ValidRowCount)
				return
			}
			if len(records) != 1 || records[0].Name != "Alice" {
				errs <- fmt.Errorf("records = %v, want [{Alice ...}]", records)
				return
			}
			errs <- nil
		}()
	}

	for range goroutines {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
}

// TestWrapParseError_AllBranches verifies that wrapParseError maps every
// known parser error message to the correct sentinel error. This guards
// against silent breakage if the parser package changes its error wording.
func TestWrapParseError_AllBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		msg      string
		sentinel error
	}{
		// Exact matches from emptyFileMessages
		{"empty parquet file", ErrEmptyFile},
		{"empty XLSX sheet", ErrEmptyFile},
		{"no valid LTSV records found", ErrEmptyFile},
		{"no sheets found in XLSX file", ErrEmptyFile},
		{"no headers found in XLSX", ErrEmptyFile},
		// Dynamic "empty <format> data" pattern
		{"empty CSV data", ErrEmptyFile},
		{"empty TSV data", ErrEmptyFile},
		{"empty JSON data", ErrEmptyFile},
		{"empty JSONL data", ErrEmptyFile},
		{"empty LTSV data", ErrEmptyFile},
		// Dynamic "empty <format> array" pattern
		{"empty JSON array", ErrEmptyFile},
		// Unsupported file type
		{"unsupported file type", ErrUnsupportedFileType},
		// Nil reader
		{"reader cannot be nil", ErrNilReader},
	}

	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			t.Parallel()
			wrapped := wrapParseError(errors.New(tt.msg))
			if !errors.Is(wrapped, tt.sentinel) {
				t.Errorf("wrapParseError(%q) should match %v, got: %v", tt.msg, tt.sentinel, wrapped)
			}
		})
	}

	t.Run("unknown error passes through unchanged", func(t *testing.T) {
		t.Parallel()
		orig := errors.New("something unexpected")
		got := wrapParseError(orig)
		if !errors.Is(got, orig) {
			t.Errorf("expected original error, got: %v", got)
		}
	})

	t.Run("nil error returns nil", func(t *testing.T) {
		t.Parallel()
		if got := wrapParseError(nil); got != nil {
			t.Errorf("expected nil, got: %v", got)
		}
	})
}

// TestProcessToWriter_NilReader verifies that ProcessToWriter returns
// ErrNilReader when a nil reader is passed (same path as Process).
func TestProcessToWriter_NilReader(t *testing.T) {
	t.Parallel()

	type Row struct {
		Name string
	}

	processor := NewProcessor(parser.CSV)
	var records []Row
	var buf bytes.Buffer

	_, err := processor.ProcessToWriter(nil, &records, &buf)
	if err == nil {
		t.Fatal("expected error for nil reader")
	}
	if !errors.Is(err, ErrNilReader) {
		t.Errorf("expected errors.Is(err, ErrNilReader), got: %v", err)
	}
	if errors.Is(err, ErrEmptyFile) {
		t.Error("nil reader via ProcessToWriter should NOT match ErrEmptyFile")
	}
}

// TestProcess_RefusesAFieldWithNoColumn pins that a struct field naming a
// column the input does not have is reported as what it is.
//
// It used to be filled with the zero value and then validated, so a field named
// Emails against a column named email produced `row 1, column "emails": value
// is required` on a file whose every row has an email. The error pointed at the
// data, which is fine, instead of at the mapping, which is not — and a caller
// had no way to tell the two apart.
//
// The same mistake made prep do nothing at all for JSON and JSONL, whose rows
// arrive as a single "data" column: a struct written against the object's own
// keys matched nothing, so no transform ran and every field was reported as a
// missing required value.
func TestProcess_RefusesAFieldWithNoColumn(t *testing.T) {
	t.Parallel()

	t.Run("a field naming no column is refused, and the error says which", func(t *testing.T) {
		t.Parallel()

		type user struct {
			Name   string `prep:"trim" validate:"required"`
			Emails string `validate:"required"` // the column is "email", singular
		}

		var out []user
		_, _, err := NewProcessor(FileTypeCSV).Process(strings.NewReader("name,email\nAlice,a@b.com\n"), &out)
		if err == nil {
			t.Fatal("Process accepted a struct whose field matches no column")
		}
		if !errors.Is(err, ErrUnknownColumn) {
			t.Fatalf("err = %v, want ErrUnknownColumn", err)
		}
		for _, want := range []string{"Emails", "emails", "email"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("err = %q, want it to name %q so the caller can see the mistake", err, want)
			}
		}
	})

	t.Run("every missing field is named at once", func(t *testing.T) {
		t.Parallel()

		type user struct {
			Name    string `prep:"trim"`
			Emails  string
			Missing string
		}

		var out []user
		_, _, err := NewProcessor(FileTypeCSV).Process(strings.NewReader("name,email\nAlice,a@b.com\n"), &out)
		if err == nil {
			t.Fatal("Process accepted a struct with two fields matching no column")
		}
		for _, want := range []string{"Emails", "Missing"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("err = %q, want it to name %q; one error listing all of them beats one per row", err, want)
			}
		}
	})

	t.Run("a struct covering a subset of the columns is still accepted", func(t *testing.T) {
		t.Parallel()

		type user struct {
			Name string `prep:"trim"`
		}

		var out []user
		_, result, err := NewProcessor(FileTypeCSV).Process(strings.NewReader("name,address\n  Alice  ,1 Main St\n"), &out)
		if err != nil {
			t.Fatalf("narrowing the struct is legitimate and must not be refused: %v", err)
		}
		if len(out) != 1 || out[0].Name != "Alice" {
			t.Fatalf("out = %+v, want the trimmed name", out)
		}
		if result.HasErrors() {
			t.Fatalf("unexpected validation errors: %v", result.ValidationErrors())
		}
	})

	t.Run("JSON says the only column is data rather than reporting empty values", func(t *testing.T) {
		t.Parallel()

		type user struct {
			Name  string `prep:"trim" validate:"required"`
			Email string `prep:"trim,lowercase" validate:"required"`
		}

		var out []user
		_, _, err := NewProcessor(FileTypeJSON).Process(strings.NewReader(`[{"name":"  Alice  ","email":"A@B.COM"}]`), &out)
		if err == nil {
			t.Fatal("Process accepted a struct written against the JSON object's keys")
		}
		if !strings.Contains(err.Error(), "data") {
			t.Errorf("err = %q, want it to name the \"data\" column so the caller learns the shape", err)
		}
	})

	t.Run("a JSON struct naming the data column works", func(t *testing.T) {
		t.Parallel()

		type record struct {
			Data string `name:"data" prep:"trim"`
		}

		var out []record
		_, _, err := NewProcessor(FileTypeJSON).Process(strings.NewReader(`[{"id":1}]`), &out)
		if err != nil {
			t.Fatalf("the documented way to use JSON must keep working: %v", err)
		}
		if len(out) != 1 || !strings.Contains(out[0].Data, `"id"`) {
			t.Fatalf("out = %+v, want the raw JSON element", out)
		}
	})
}

// bomHeaderRecord maps the two columns of a CSV whose header carries a UTF-8
// byte-order mark, which is what a spreadsheet writes.
type bomHeaderRecord struct {
	Name string `prep:"trim"`
	Memo string
}

// TestProcessStripsByteOrderMark requires prep to read a file the loader reads.
// A UTF-8 BOM belongs to the encoding, not to the first column's name, so a
// struct field naming that column has to match.
func TestProcessStripsByteOrderMark(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		fileType   parser.FileType
		input      string
		wantOutput string
	}{
		{
			name:       "CSV with a leading BOM",
			fileType:   parser.CSV,
			input:      "\ufeffname,memo\na,b\n",
			wantOutput: "name,memo\na,b\n",
		},
		{
			name:       "TSV with a leading BOM",
			fileType:   parser.TSV,
			input:      "\ufeffname\tmemo\na\tb\n",
			wantOutput: "name\tmemo\na\tb\n",
		},
		{
			name:       "CSV without a BOM is untouched",
			fileType:   parser.CSV,
			input:      "name,memo\na,b\n",
			wantOutput: "name,memo\na,b\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var records []bomHeaderRecord
			processor := NewProcessor(tt.fileType)
			reader, result, err := processor.Process(strings.NewReader(tt.input), &records)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			if diff := cmp.Diff([]string{"name", "memo"}, result.Columns); diff != "" {
				t.Errorf("columns mismatch (-want +got):\n%s", diff)
			}
			output, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("read output: %v", err)
			}
			if got := string(output); got != tt.wantOutput {
				t.Errorf("output = %q, want %q", got, tt.wantOutput)
			}
		})
	}
}

// minMaxStringRecord measures a string field, which is what min and max mean for
// a string in the validator dialect prep documents.
type minMaxStringRecord struct {
	Name string `validate:"min=3,max=5"`
}

// minMaxNumberRecord measures a magnitude, which is what the same tags mean for
// a numeric field.
type minMaxNumberRecord struct {
	Age int `validate:"min=3,max=5"`
}

// lenStringRecord counts characters, which min and max have to agree with.
type lenStringRecord struct {
	Name string `validate:"len=3"`
}

func TestMinAndMaxMeasureAStringByItsLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		input         string
		records       any
		wantValidRows int
	}{
		{
			name:          "a string within the length bounds passes",
			input:         "name\nalice\nabc\n",
			records:       &[]minMaxStringRecord{},
			wantValidRows: 2,
		},
		{
			name:          "a string outside the length bounds fails",
			input:         "name\nab\nabcdef\n",
			records:       &[]minMaxStringRecord{},
			wantValidRows: 0,
		},
		{
			name:          "a number within the bounds passes",
			input:         "age\n3\n5\n",
			records:       &[]minMaxNumberRecord{},
			wantValidRows: 2,
		},
		{
			name:          "a number outside the bounds fails",
			input:         "age\n2\n6\n",
			records:       &[]minMaxNumberRecord{},
			wantValidRows: 0,
		},
		{
			name:          "min and len agree, and both count runes",
			input:         "name\n日本語\n",
			records:       &[]lenStringRecord{},
			wantValidRows: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			processor := NewProcessor(parser.CSV)
			_, result, err := processor.Process(strings.NewReader(tt.input), tt.records)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			if result.ValidRowCount != tt.wantValidRows {
				t.Errorf("ValidRowCount = %d, want %d (errors: %v)", result.ValidRowCount, tt.wantValidRows, result.Errors)
			}
		})
	}
}

func TestMinCountsRunesForAMultibyteString(t *testing.T) {
	t.Parallel()

	var records []minMaxStringRecord
	processor := NewProcessor(parser.CSV)
	_, result, err := processor.Process(strings.NewReader("name\n日本語\n"), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if result.ValidRowCount != 1 {
		t.Errorf("ValidRowCount = %d, want 1: three runes satisfy min=3 (errors: %v)", result.ValidRowCount, result.Errors)
	}
}

// fractionalMinRecord asks for a length no whole number of characters reaches
// exactly, which the comparison has to keep rather than truncate.
type fractionalMinRecord struct {
	Name string `validate:"min=3.5"`
}

// fractionalMaxRecord is the other half of the same threshold.
type fractionalMaxRecord struct {
	Name string `validate:"max=3.5"`
}

func TestMinAndMaxKeepAFractionalLengthThreshold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		records   any
		input     string
		wantValid []string
		wantTag   string
	}{
		{
			name:      "min=3.5 takes four runes and refuses three",
			records:   &[]fractionalMinRecord{},
			input:     "name\nabcd\nabc\n",
			wantValid: []string{"abcd"},
			wantTag:   "min",
		},
		{
			name:      "max=3.5 takes three runes and refuses four",
			records:   &[]fractionalMaxRecord{},
			input:     "name\nabc\nabcd\n",
			wantValid: []string{"abc"},
			wantTag:   "max",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			processor := NewProcessor(parser.CSV, WithValidRowsOnly())
			reader, result, err := processor.Process(strings.NewReader(tt.input), tt.records)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			output, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("read output: %v", err)
			}
			gotValid := strings.Split(strings.TrimRight(string(output), "\n"), "\n")[1:]
			if diff := cmp.Diff(tt.wantValid, gotValid); diff != "" {
				t.Errorf("valid rows mismatch (-want +got):\n%s", diff)
			}

			if len(result.Errors) != 1 {
				t.Fatalf("errors = %v, want exactly one", result.Errors)
			}
			var validationErr *ValidationError
			if !errors.As(result.Errors[0], &validationErr) {
				t.Fatalf("error %v is not a *ValidationError", result.Errors[0])
			}
			if validationErr.Tag != tt.wantTag {
				t.Errorf("tag = %q, want %q", validationErr.Tag, tt.wantTag)
			}
			if !strings.Contains(validationErr.Message, "3.5") {
				t.Errorf("message = %q, want it to name the threshold 3.5", validationErr.Message)
			}
		})
	}
}

// TestProcess_MatchesAColumnWhateverItsCase pins the rule the loader already
// follows: a header and a field name are the same column when they differ only
// in case. SQLite compares the identifiers this package creates from these
// headers that way, and prep's own duplicate check folds them, so a struct
// written for "name" has to accept the "Name" a spreadsheet writes.
func TestProcess_MatchesAColumnWhateverItsCase(t *testing.T) {
	t.Parallel()

	type user struct {
		Name string `prep:"trim" validate:"required"`
		Age  int    `validate:"gte=0"`
	}

	for _, header := range []string{"name,age", "Name,Age", "NAME,AGE", "nAmE,aGe"} {
		t.Run(header, func(t *testing.T) {
			t.Parallel()

			var out []user
			reader, result, err := NewProcessor(FileTypeCSV).
				Process(strings.NewReader(header+"\n Alice ,30\n"), &out)
			if err != nil {
				t.Fatalf("Process(%q) error: %v", header, err)
			}
			if result.ValidRowCount != 1 {
				t.Errorf("valid rows = %d, want 1", result.ValidRowCount)
			}
			if len(out) != 1 || out[0].Name != "Alice" || out[0].Age != 30 {
				t.Errorf("out = %#v, want the trimmed name and the age", out)
			}
			cleaned, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			// The output keeps the header the file had; only the matching folds.
			if want := header + "\nAlice,30\n"; string(cleaned) != want {
				t.Errorf("cleaned = %q, want %q", string(cleaned), want)
			}
		})
	}

	t.Run("a field of more than one word", func(t *testing.T) {
		t.Parallel()

		type person struct {
			FirstName string `prep:"trim"`
		}
		for _, header := range []string{"first_name", "First_Name", "FIRST_NAME"} {
			var out []person
			if _, _, err := NewProcessor(FileTypeCSV).
				Process(strings.NewReader(header+"\n Bob \n"), &out); err != nil {
				t.Errorf("Process(%q) error: %v", header, err)
				continue
			}
			if len(out) != 1 || out[0].FirstName != "Bob" {
				t.Errorf("header %q: out = %#v", header, out)
			}
		}
	})

	t.Run("an explicit name tag folds too", func(t *testing.T) {
		t.Parallel()

		type tagged struct {
			Value string `name:"given name" prep:"trim"`
		}
		var out []tagged
		if _, _, err := NewProcessor(FileTypeCSV).
			Process(strings.NewReader("Given Name\n Carol \n"), &out); err != nil {
			t.Fatalf("Process error: %v", err)
		}
		if len(out) != 1 || out[0].Value != "Carol" {
			t.Errorf("out = %#v", out)
		}
	})

	t.Run("a field naming no column is still refused", func(t *testing.T) {
		t.Parallel()

		type user struct {
			Name   string
			Emails string
		}
		var out []user
		_, _, err := NewProcessor(FileTypeCSV).Process(strings.NewReader("Name,Email\nAlice,a@b.com\n"), &out)
		if !errors.Is(err, ErrUnknownColumn) {
			t.Fatalf("err = %v, want ErrUnknownColumn", err)
		}
	})
}
