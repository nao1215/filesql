package prep

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/nao1215/filesql/parser"
	"github.com/parquet-go/parquet-go"
)

// TestIntegration_AllPrepTags tests all prep tags in an integrated manner
func TestIntegration_AllPrepTags(t *testing.T) {
	t.Parallel()

	// Test struct using all prep tags
	type TestRecord struct {
		// Basic preprocessors
		TrimField      string `prep:"trim"`
		LtrimField     string `prep:"ltrim"`
		RtrimField     string `prep:"rtrim"`
		LowercaseField string `prep:"lowercase"`
		UppercaseField string `prep:"uppercase"`
		DefaultField   string `prep:"default=default_value"`

		// String transformation preprocessors
		ReplaceField       string `prep:"replace=foo:bar"`
		PrefixField        string `prep:"prefix=pre_"`
		SuffixField        string `prep:"suffix=_suf"`
		TruncateField      string `prep:"truncate=5"`
		StripHTMLField     string `prep:"strip_html"`
		StripNewlineField  string `prep:"strip_newline"`
		CollapseSpaceField string `prep:"collapse_space"`

		// Character filtering preprocessors
		RemoveDigitsField string `prep:"remove_digits"`
		RemoveAlphaField  string `prep:"remove_alpha"`
		KeepDigitsField   string `prep:"keep_digits"`
		KeepAlphaField    string `prep:"keep_alpha"`
		TrimSetField      string `prep:"trim_set=[]"`

		// Padding preprocessors
		PadLeftField  string `prep:"pad_left=5:0"`
		PadRightField string `prep:"pad_right=5:0"`

		// Advanced preprocessors
		NormalizeUnicodeField string `prep:"normalize_unicode"`
		NullifyField          string `prep:"nullify=NA"`
		CoerceIntField        string `prep:"coerce=int"`
		CoerceBoolField       string `prep:"coerce=bool"`
		FixSchemeField        string `prep:"fix_scheme=https"`
		RegexReplaceField     string `prep:"regex_replace=\\d+:X"`
	}

	// Create test CSV data
	// Note: StripNewlineField uses quoted string with embedded newline
	csvData := "trim_field,ltrim_field,rtrim_field,lowercase_field,uppercase_field,default_field,replace_field,prefix_field,suffix_field,truncate_field,strip_html_field,strip_newline_field,collapse_space_field,remove_digits_field,remove_alpha_field,keep_digits_field,keep_alpha_field,trim_set_field,pad_left_field,pad_right_field,normalize_unicode_field,nullify_field,coerce_int_field,coerce_bool_field,fix_scheme_field,regex_replace_field\n" +
		"  hello  ,  hello  ,  hello  ,HELLO,hello,,foo test,value,value,hello world,<p>text</p>,\"line1\nline2\",a  b  c,abc123,abc123,abc123,abc123,[value],42,42,hello,NA,123.5,yes,example.com,abc123"

	reader := strings.NewReader(csvData)
	var records []TestRecord

	processor := NewProcessor(parser.CSV)
	pipeReader, result, err := processor.Process(reader, &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	// Consume the pipe reader
	go func() {
		_, _ = io.Copy(io.Discard, pipeReader) //nolint:errcheck // discarding output in test
	}()

	if result.HasErrors() {
		for _, e := range result.Errors {
			t.Logf("Error: %v", e)
		}
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	r := records[0]

	// Verify basic preprocessors
	if r.TrimField != "hello" {
		t.Errorf("TrimField = %q, want %q", r.TrimField, "hello")
	}
	if r.LtrimField != "hello  " {
		t.Errorf("LtrimField = %q, want %q", r.LtrimField, "hello  ")
	}
	if r.RtrimField != "  hello" {
		t.Errorf("RtrimField = %q, want %q", r.RtrimField, "  hello")
	}
	if r.LowercaseField != "hello" {
		t.Errorf("LowercaseField = %q, want %q", r.LowercaseField, "hello")
	}
	if r.UppercaseField != "HELLO" {
		t.Errorf("UppercaseField = %q, want %q", r.UppercaseField, "HELLO")
	}
	if r.DefaultField != "default_value" {
		t.Errorf("DefaultField = %q, want %q", r.DefaultField, "default_value")
	}

	// Verify string transformation preprocessors
	if r.ReplaceField != "bar test" {
		t.Errorf("ReplaceField = %q, want %q", r.ReplaceField, "bar test")
	}
	if r.PrefixField != "pre_value" {
		t.Errorf("PrefixField = %q, want %q", r.PrefixField, "pre_value")
	}
	if r.SuffixField != "value_suf" {
		t.Errorf("SuffixField = %q, want %q", r.SuffixField, "value_suf")
	}
	if r.TruncateField != "hello" {
		t.Errorf("TruncateField = %q, want %q", r.TruncateField, "hello")
	}
	if r.StripHTMLField != "text" {
		t.Errorf("StripHTMLField = %q, want %q", r.StripHTMLField, "text")
	}
	if r.StripNewlineField != "line1line2" {
		t.Errorf("StripNewlineField = %q, want %q", r.StripNewlineField, "line1line2")
	}
	if r.CollapseSpaceField != "a b c" {
		t.Errorf("CollapseSpaceField = %q, want %q", r.CollapseSpaceField, "a b c")
	}

	// Verify character filtering preprocessors
	if r.RemoveDigitsField != "abc" {
		t.Errorf("RemoveDigitsField = %q, want %q", r.RemoveDigitsField, "abc")
	}
	if r.RemoveAlphaField != "123" {
		t.Errorf("RemoveAlphaField = %q, want %q", r.RemoveAlphaField, "123")
	}
	if r.KeepDigitsField != "123" {
		t.Errorf("KeepDigitsField = %q, want %q", r.KeepDigitsField, "123")
	}
	if r.KeepAlphaField != "abc" {
		t.Errorf("KeepAlphaField = %q, want %q", r.KeepAlphaField, "abc")
	}
	if r.TrimSetField != "value" {
		t.Errorf("TrimSetField = %q, want %q", r.TrimSetField, "value")
	}

	// Verify padding preprocessors
	if r.PadLeftField != "00042" {
		t.Errorf("PadLeftField = %q, want %q", r.PadLeftField, "00042")
	}
	if r.PadRightField != "42000" {
		t.Errorf("PadRightField = %q, want %q", r.PadRightField, "42000")
	}

	// Verify advanced preprocessors
	if r.NormalizeUnicodeField != "hello" {
		t.Errorf("NormalizeUnicodeField = %q, want %q", r.NormalizeUnicodeField, "hello")
	}
	if r.NullifyField != "" {
		t.Errorf("NullifyField = %q, want %q", r.NullifyField, "")
	}
	if r.CoerceIntField != "123" {
		t.Errorf("CoerceIntField = %q, want %q", r.CoerceIntField, "123")
	}
	if r.CoerceBoolField != "true" {
		t.Errorf("CoerceBoolField = %q, want %q", r.CoerceBoolField, "true")
	}
	if r.FixSchemeField != "https://example.com" {
		t.Errorf("FixSchemeField = %q, want %q", r.FixSchemeField, "https://example.com")
	}
	if r.RegexReplaceField != "abcX" {
		t.Errorf("RegexReplaceField = %q, want %q", r.RegexReplaceField, "abcX")
	}
}

// TestIntegration_CombinedPrepTags tests multiple prep tags on a single field
func TestIntegration_CombinedPrepTags(t *testing.T) {
	t.Parallel()

	type TestRecord struct {
		Combined string `prep:"trim,lowercase,prefix=pre_"`
	}

	csvData := `combined
  HELLO WORLD  `

	reader := strings.NewReader(csvData)
	var records []TestRecord

	processor := NewProcessor(parser.CSV)
	pipeReader, result, err := processor.Process(reader, &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	go func() {
		_, _ = io.Copy(io.Discard, pipeReader) //nolint:errcheck // discarding output in test
	}()

	if result.HasErrors() {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	expected := "pre_hello world"
	if records[0].Combined != expected {
		t.Errorf("Combined = %q, want %q", records[0].Combined, expected)
	}
}

// TestIntegration_PrepWithValidation tests prep tags working together with validation
func TestIntegration_PrepWithValidation(t *testing.T) {
	t.Parallel()

	type TestRecord struct {
		Email string `prep:"trim,lowercase" validate:"email"`
	}

	csvData := `email
  JOHN@EXAMPLE.COM
invalid_email`

	reader := strings.NewReader(csvData)
	var records []TestRecord

	processor := NewProcessor(parser.CSV)
	pipeReader, result, err := processor.Process(reader, &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	go func() {
		_, _ = io.Copy(io.Discard, pipeReader) //nolint:errcheck // discarding output in test
	}()

	// Should have validation error for second row
	if !result.HasErrors() {
		t.Error("expected validation errors")
	}

	if len(result.ValidationErrors()) != 1 {
		t.Errorf("expected 1 validation error, got %d", len(result.ValidationErrors()))
	}

	// First record should be processed correctly
	if len(records) < 1 {
		t.Fatal("expected at least 1 record")
	}

	expected := "john@example.com"
	if records[0].Email != expected {
		t.Errorf("Email = %q, want %q", records[0].Email, expected)
	}
}

// TestIntegration_CompressedCSV tests processing compressed CSV files
func TestIntegration_CompressedCSV(t *testing.T) {
	t.Parallel()

	type TestRecord struct {
		Name  string `prep:"trim"`
		Email string `prep:"trim,lowercase"`
		Age   string
	}

	tests := []struct {
		name     string
		filePath string
		fileType parser.FileType
	}{
		{"gzip CSV", filepath.Join("testdata", "sample.csv.gz"), parser.CSVGZ},
		{"bzip2 CSV", filepath.Join("testdata", "sample.csv.bz2"), parser.CSVBZ2},
		{"xz CSV", filepath.Join("testdata", "sample.csv.xz"), parser.CSVXZ},
		{"zstd CSV", filepath.Join("testdata", "sample.csv.zst"), parser.CSVZSTD},
		{"zlib CSV", filepath.Join("testdata", "sample.csv.z"), parser.CSVZLIB},
		{"snappy CSV", filepath.Join("testdata", "sample.csv.snappy"), parser.CSVSNAPPY},
		{"s2 CSV", filepath.Join("testdata", "sample.csv.s2"), parser.CSVS2},
		{"lz4 CSV", filepath.Join("testdata", "sample.csv.lz4"), parser.CSVLZ4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			file, err := os.Open(tt.filePath)
			if err != nil {
				t.Fatalf("os.Open() error = %v", err)
			}
			defer file.Close()

			var records []TestRecord

			processor := NewProcessor(tt.fileType)
			pipeReader, result, err := processor.Process(file, &records)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			go func() {
				_, _ = io.Copy(io.Discard, pipeReader) //nolint:errcheck // discarding output in test
			}()

			if result.OriginalFormat != tt.fileType {
				t.Errorf("OriginalFormat = %v, want %v", result.OriginalFormat, tt.fileType)
			}

			// Verify at least one record was processed
			if len(records) == 0 {
				t.Error("expected at least one record")
			}

			// Verify first record has trimmed name
			if records[0].Name != "John Doe" {
				t.Errorf("Name = %q, want %q", records[0].Name, "John Doe")
			}
		})
	}
}

// TestIntegration_CompressedTSV tests processing compressed TSV files
func TestIntegration_CompressedTSV(t *testing.T) {
	t.Parallel()

	type TestRecord struct {
		Name  string `prep:"trim"`
		Email string `prep:"trim,lowercase"`
		Age   string
	}

	tests := []struct {
		name     string
		filePath string
		fileType parser.FileType
	}{
		{"zlib TSV", filepath.Join("testdata", "sample.tsv.z"), parser.TSVZLIB},
		{"snappy TSV", filepath.Join("testdata", "sample.tsv.snappy"), parser.TSVSNAPPY},
		{"s2 TSV", filepath.Join("testdata", "sample.tsv.s2"), parser.TSVS2},
		{"lz4 TSV", filepath.Join("testdata", "sample.tsv.lz4"), parser.TSVLZ4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			file, err := os.Open(tt.filePath)
			if err != nil {
				t.Fatalf("os.Open() error = %v", err)
			}
			defer file.Close()

			var records []TestRecord

			processor := NewProcessor(tt.fileType)
			pipeReader, result, err := processor.Process(file, &records)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			go func() {
				_, _ = io.Copy(io.Discard, pipeReader) //nolint:errcheck // discarding output in test
			}()

			if result.OriginalFormat != tt.fileType {
				t.Errorf("OriginalFormat = %v, want %v", result.OriginalFormat, tt.fileType)
			}

			// Verify at least one record was processed
			if len(records) == 0 {
				t.Error("expected at least one record")
			}

			// Verify first record has trimmed name
			if records[0].Name != "Alice" {
				t.Errorf("Name = %q, want %q", records[0].Name, "Alice")
			}
		})
	}
}

// TestIntegration_CompressedLTSV tests processing compressed LTSV files
func TestIntegration_CompressedLTSV(t *testing.T) {
	t.Parallel()

	type TestRecord struct {
		Name  string `prep:"trim"`
		Email string `prep:"trim,lowercase"`
		Age   string
	}

	tests := []struct {
		name     string
		filePath string
		fileType parser.FileType
	}{
		{"zlib LTSV", filepath.Join("testdata", "sample.ltsv.z"), parser.LTSVZLIB},
		{"snappy LTSV", filepath.Join("testdata", "sample.ltsv.snappy"), parser.LTSVSNAPPY},
		{"s2 LTSV", filepath.Join("testdata", "sample.ltsv.s2"), parser.LTSVS2},
		{"lz4 LTSV", filepath.Join("testdata", "sample.ltsv.lz4"), parser.LTSVLZ4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			file, err := os.Open(tt.filePath)
			if err != nil {
				t.Fatalf("os.Open() error = %v", err)
			}
			defer file.Close()

			var records []TestRecord

			processor := NewProcessor(tt.fileType)
			pipeReader, result, err := processor.Process(file, &records)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			go func() {
				_, _ = io.Copy(io.Discard, pipeReader) //nolint:errcheck // discarding output in test
			}()

			if result.OriginalFormat != tt.fileType {
				t.Errorf("OriginalFormat = %v, want %v", result.OriginalFormat, tt.fileType)
			}

			// Verify at least one record was processed
			if len(records) == 0 {
				t.Error("expected at least one record")
			}

			// Verify first record has correct name
			if records[0].Name != "Charlie" {
				t.Errorf("Name = %q, want %q", records[0].Name, "Charlie")
			}
		})
	}
}

// TestIntegration_TSVProcessing tests TSV file processing with prep tags
func TestIntegration_TSVProcessing(t *testing.T) {
	t.Parallel()

	type TestRecord struct {
		Name  string `prep:"trim"`
		Email string `prep:"trim,lowercase"`
		Age   string
	}

	file, err := os.Open(filepath.Join("testdata", "sample.tsv"))
	if err != nil {
		t.Fatalf("os.Open() error = %v", err)
	}
	defer file.Close()

	var records []TestRecord

	processor := NewProcessor(parser.TSV)
	pipeReader, result, err := processor.Process(file, &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	go func() {
		_, _ = io.Copy(io.Discard, pipeReader) //nolint:errcheck // discarding output in test
	}()

	if result.OriginalFormat != parser.TSV {
		t.Errorf("OriginalFormat = %v, want %v", result.OriginalFormat, parser.TSV)
	}

	if len(records) == 0 {
		t.Error("expected at least one record")
	}
}

// TestIntegration_LTSVProcessing tests LTSV file processing with prep tags
func TestIntegration_LTSVProcessing(t *testing.T) {
	t.Parallel()

	type TestRecord struct {
		Name  string `prep:"trim"`
		Email string `prep:"trim,lowercase"`
		Age   string
	}

	file, err := os.Open(filepath.Join("testdata", "sample.ltsv"))
	if err != nil {
		t.Fatalf("os.Open() error = %v", err)
	}
	defer file.Close()

	var records []TestRecord

	processor := NewProcessor(parser.LTSV)
	pipeReader, result, err := processor.Process(file, &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	go func() {
		_, _ = io.Copy(io.Discard, pipeReader) //nolint:errcheck // discarding output in test
	}()

	if result.OriginalFormat != parser.LTSV {
		t.Errorf("OriginalFormat = %v, want %v", result.OriginalFormat, parser.LTSV)
	}

	if len(records) == 0 {
		t.Error("expected at least one record")
	}
}

// TestIntegration_ErrorReporting tests detailed error reporting
func TestIntegration_ErrorReporting(t *testing.T) {
	t.Parallel()

	type TestRecord struct {
		Name  string `validate:"required"`
		Email string `validate:"email"`
		Age   string `validate:"numeric"`
	}

	csvData := `name,email,age
,invalid-email,abc
John,john@example.com,25`

	reader := strings.NewReader(csvData)
	var records []TestRecord

	processor := NewProcessor(parser.CSV)
	pipeReader, result, err := processor.Process(reader, &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	go func() {
		_, _ = io.Copy(io.Discard, pipeReader) //nolint:errcheck // discarding output in test
	}()

	// Should have 3 validation errors for the first row
	if !result.HasErrors() {
		t.Error("expected validation errors")
	}

	errors := result.ValidationErrors()
	if len(errors) != 3 {
		t.Errorf("expected 3 validation errors, got %d", len(errors))
	}

	// Check that error details are correct
	for _, e := range errors {
		if e.Row != 1 {
			t.Errorf("expected row 1, got %d", e.Row)
		}
	}

	// Verify row counts
	if result.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", result.RowCount)
	}
	if result.ValidRowCount != 1 {
		t.Errorf("ValidRowCount = %d, want 1", result.ValidRowCount)
	}
}

// TestIntegration_CrossFieldValidation tests cross-field validation
func TestIntegration_CrossFieldValidation(t *testing.T) {
	t.Parallel()

	type TestRecord struct {
		Password        string
		ConfirmPassword string `validate:"eqfield=Password"`
	}

	csvData := `password,confirm_password
secret123,secret123
password,different`

	reader := strings.NewReader(csvData)
	var records []TestRecord

	processor := NewProcessor(parser.CSV)
	pipeReader, result, err := processor.Process(reader, &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	go func() {
		_, _ = io.Copy(io.Discard, pipeReader) //nolint:errcheck // discarding output in test
	}()

	// Should have 1 validation error for the second row
	if !result.HasErrors() {
		t.Error("expected validation errors")
	}

	errors := result.ValidationErrors()
	if len(errors) != 1 {
		t.Errorf("expected 1 validation error, got %d", len(errors))
	}

	if len(errors) > 0 && errors[0].Row != 2 {
		t.Errorf("expected error on row 2, got row %d", errors[0].Row)
	}
}

// TestIntegration_ColumnOrderIndependent tests that column order doesn't matter
func TestIntegration_ColumnOrderIndependent(t *testing.T) {
	t.Parallel()

	type TestRecord struct {
		Name  string `prep:"trim"`
		Email string `prep:"lowercase"`
		Age   string
	}

	// CSV with columns in different order than struct fields
	csvData := `email,age,name
ALICE@EXAMPLE.COM,30,  Alice
BOB@EXAMPLE.COM,25,  Bob  `

	reader := strings.NewReader(csvData)
	var records []TestRecord

	processor := NewProcessor(parser.CSV)
	_, result, err := processor.Process(reader, &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if result.HasErrors() {
		for _, e := range result.Errors {
			t.Errorf("Unexpected error: %v", e)
		}
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	// Verify first record - fields should be correctly mapped by column name
	if records[0].Name != "Alice" {
		t.Errorf("records[0].Name = %q, want %q (trim should be applied)", records[0].Name, "Alice")
	}
	if records[0].Email != "alice@example.com" {
		t.Errorf("records[0].Email = %q, want %q (lowercase should be applied)", records[0].Email, "alice@example.com")
	}
	if records[0].Age != "30" {
		t.Errorf("records[0].Age = %q, want %q", records[0].Age, "30")
	}

	// Verify second record
	if records[1].Name != "Bob" {
		t.Errorf("records[1].Name = %q, want %q", records[1].Name, "Bob")
	}
	if records[1].Email != "bob@example.com" {
		t.Errorf("records[1].Email = %q, want %q", records[1].Email, "bob@example.com")
	}
}

// TestIntegration_NameTagOverride tests that name tag overrides default column name
func TestIntegration_NameTagOverride(t *testing.T) {
	t.Parallel()

	type TestRecord struct {
		UserName    string `name:"user" prep:"trim"`
		EmailAddr   string `name:"mail" prep:"lowercase"`
		PhoneNumber string `name:"phone"`
	}

	// CSV uses custom column names defined by name tag
	csvData := `user,mail,phone
  John  ,JOHN@EXAMPLE.COM,123-456-7890
  Jane  ,JANE@EXAMPLE.COM,098-765-4321`

	reader := strings.NewReader(csvData)
	var records []TestRecord

	processor := NewProcessor(parser.CSV)
	_, result, err := processor.Process(reader, &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if result.HasErrors() {
		for _, e := range result.Errors {
			t.Errorf("Unexpected error: %v", e)
		}
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	// Verify mapping by name tag
	if records[0].UserName != "John" {
		t.Errorf("records[0].UserName = %q, want %q", records[0].UserName, "John")
	}
	if records[0].EmailAddr != "john@example.com" {
		t.Errorf("records[0].EmailAddr = %q, want %q", records[0].EmailAddr, "john@example.com")
	}
	if records[0].PhoneNumber != "123-456-7890" {
		t.Errorf("records[0].PhoneNumber = %q, want %q", records[0].PhoneNumber, "123-456-7890")
	}
}

// TestIntegration_MixedNameTagAndAutoConvert tests mixed name tag and auto snake_case
func TestIntegration_MixedNameTagAndAutoConvert(t *testing.T) {
	t.Parallel()

	type TestRecord struct {
		FirstName string // auto: "first_name"
		LastName  string `name:"family"` // override: "family"
		Age       string // auto: "age"
	}

	csvData := `first_name,family,age
John,Doe,30
Jane,Smith,25`

	reader := strings.NewReader(csvData)
	var records []TestRecord

	processor := NewProcessor(parser.CSV)
	_, result, err := processor.Process(reader, &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if result.HasErrors() {
		for _, e := range result.Errors {
			t.Errorf("Unexpected error: %v", e)
		}
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	if records[0].FirstName != "John" {
		t.Errorf("records[0].FirstName = %q, want %q", records[0].FirstName, "John")
	}
	if records[0].LastName != "Doe" {
		t.Errorf("records[0].LastName = %q, want %q", records[0].LastName, "Doe")
	}
	if records[1].FirstName != "Jane" {
		t.Errorf("records[1].FirstName = %q, want %q", records[1].FirstName, "Jane")
	}
	if records[1].LastName != "Smith" {
		t.Errorf("records[1].LastName = %q, want %q", records[1].LastName, "Smith")
	}
}

// TestIntegration_StreamOutput tests that the output reader contains preprocessed data
func TestIntegration_StreamOutput(t *testing.T) {
	t.Parallel()

	type TestRecord struct {
		Name  string `prep:"uppercase"`
		Email string `prep:"lowercase"`
	}

	csvData := `name,email
john,JOHN@EXAMPLE.COM`

	reader := strings.NewReader(csvData)
	var records []TestRecord

	processor := NewProcessor(parser.CSV)
	pipeReader, _, err := processor.Process(reader, &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	// Read the output stream
	output, err := io.ReadAll(pipeReader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	outputStr := string(output)

	// Verify the output contains preprocessed values
	if !strings.Contains(outputStr, "JOHN") {
		t.Error("expected uppercase JOHN in output")
	}
	if !strings.Contains(outputStr, "john@example.com") {
		t.Error("expected lowercase email in output")
	}
}

// verifyJSONLOutput reads all output from the reader and verifies each non-empty line is valid JSON.
func verifyJSONLOutput(t *testing.T, reader io.Reader) {
	t.Helper()

	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	// Filter out empty lines (writeJSONL skips empty records)
	var nonEmpty []string
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			nonEmpty = append(nonEmpty, line)
		}
	}

	if len(nonEmpty) == 0 {
		t.Error("JSONL output has no non-empty lines")
		return
	}

	for i, line := range nonEmpty {
		if !json.Valid([]byte(line)) {
			t.Errorf("JSONL output line %d is not valid JSON: %q", i+1, line)
		}
	}
}

// TestIntegration_JSONProcessing tests JSON file processing with prep tags
func TestIntegration_JSONProcessing(t *testing.T) {
	t.Parallel()

	type JSONRecord struct {
		Data string `name:"data" prep:"trim" validate:"required"`
	}

	file, err := os.Open(filepath.Join("testdata", "sample.json"))
	if err != nil {
		t.Fatalf("os.Open() error = %v", err)
	}
	defer file.Close()

	var records []JSONRecord

	processor := NewProcessor(parser.JSON)
	pipeReader, result, err := processor.Process(file, &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if result.OriginalFormat != parser.JSON {
		t.Errorf("OriginalFormat = %v, want %v", result.OriginalFormat, parser.JSON)
	}

	if result.RowCount != 4 {
		t.Errorf("RowCount = %d, want 4", result.RowCount)
	}

	if len(records) != 4 {
		t.Errorf("len(records) = %d, want 4", len(records))
	}

	// Verify JSONL output is re-parseable
	verifyJSONLOutput(t, pipeReader)
}

// TestIntegration_JSONLProcessing tests JSONL file processing with prep tags
func TestIntegration_JSONLProcessing(t *testing.T) {
	t.Parallel()

	type JSONRecord struct {
		Data string `name:"data" prep:"trim" validate:"required"`
	}

	file, err := os.Open(filepath.Join("testdata", "sample.jsonl"))
	if err != nil {
		t.Fatalf("os.Open() error = %v", err)
	}
	defer file.Close()

	var records []JSONRecord

	processor := NewProcessor(parser.JSONL)
	pipeReader, result, err := processor.Process(file, &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if result.OriginalFormat != parser.JSONL {
		t.Errorf("OriginalFormat = %v, want %v", result.OriginalFormat, parser.JSONL)
	}

	if result.RowCount != 4 {
		t.Errorf("RowCount = %d, want 4", result.RowCount)
	}

	if len(records) != 4 {
		t.Errorf("len(records) = %d, want 4", len(records))
	}

	// Verify JSONL output is re-parseable
	verifyJSONLOutput(t, pipeReader)
}

// TestIntegration_CompressedJSON tests processing compressed JSON files
func TestIntegration_CompressedJSON(t *testing.T) {
	t.Parallel()

	type JSONRecord struct {
		Data string `name:"data" prep:"trim" validate:"required"`
	}

	tests := []struct {
		name     string
		filePath string
		fileType parser.FileType
	}{
		{"gzip JSON", filepath.Join("testdata", "sample.json.gz"), parser.JSONGZ},
		{"bzip2 JSON", filepath.Join("testdata", "sample.json.bz2"), parser.JSONBZ2},
		{"xz JSON", filepath.Join("testdata", "sample.json.xz"), parser.JSONXZ},
		{"zstd JSON", filepath.Join("testdata", "sample.json.zst"), parser.JSONZSTD},
		{"zlib JSON", filepath.Join("testdata", "sample.json.z"), parser.JSONZLIB},
		{"snappy JSON", filepath.Join("testdata", "sample.json.snappy"), parser.JSONSNAPPY},
		{"s2 JSON", filepath.Join("testdata", "sample.json.s2"), parser.JSONS2},
		{"lz4 JSON", filepath.Join("testdata", "sample.json.lz4"), parser.JSONLZ4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			file, err := os.Open(tt.filePath)
			if err != nil {
				t.Fatalf("os.Open() error = %v", err)
			}
			defer file.Close()

			var records []JSONRecord

			processor := NewProcessor(tt.fileType)
			pipeReader, result, err := processor.Process(file, &records)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			if result.OriginalFormat != tt.fileType {
				t.Errorf("OriginalFormat = %v, want %v", result.OriginalFormat, tt.fileType)
			}

			if len(records) == 0 {
				t.Error("expected at least one record")
			}

			if result.RowCount != 4 {
				t.Errorf("RowCount = %d, want 4", result.RowCount)
			}

			// Verify JSONL output is re-parseable
			verifyJSONLOutput(t, pipeReader)
		})
	}
}

// TestIntegration_CompressedJSONL tests processing compressed JSONL files
func TestIntegration_CompressedJSONL(t *testing.T) {
	t.Parallel()

	type JSONRecord struct {
		Data string `name:"data" prep:"trim" validate:"required"`
	}

	tests := []struct {
		name     string
		filePath string
		fileType parser.FileType
	}{
		{"gzip JSONL", filepath.Join("testdata", "sample.jsonl.gz"), parser.JSONLGZ},
		{"bzip2 JSONL", filepath.Join("testdata", "sample.jsonl.bz2"), parser.JSONLBZ2},
		{"xz JSONL", filepath.Join("testdata", "sample.jsonl.xz"), parser.JSONLXZ},
		{"zstd JSONL", filepath.Join("testdata", "sample.jsonl.zst"), parser.JSONLZSTD},
		{"zlib JSONL", filepath.Join("testdata", "sample.jsonl.z"), parser.JSONLZLIB},
		{"snappy JSONL", filepath.Join("testdata", "sample.jsonl.snappy"), parser.JSONLSNAPPY},
		{"s2 JSONL", filepath.Join("testdata", "sample.jsonl.s2"), parser.JSONLS2},
		{"lz4 JSONL", filepath.Join("testdata", "sample.jsonl.lz4"), parser.JSONLLZ4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			file, err := os.Open(tt.filePath)
			if err != nil {
				t.Fatalf("os.Open() error = %v", err)
			}
			defer file.Close()

			var records []JSONRecord

			processor := NewProcessor(tt.fileType)
			pipeReader, result, err := processor.Process(file, &records)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			if result.OriginalFormat != tt.fileType {
				t.Errorf("OriginalFormat = %v, want %v", result.OriginalFormat, tt.fileType)
			}

			if len(records) == 0 {
				t.Error("expected at least one record")
			}

			if result.RowCount != 4 {
				t.Errorf("RowCount = %d, want 4", result.RowCount)
			}

			// Verify JSONL output is re-parseable
			verifyJSONLOutput(t, pipeReader)
		})
	}
}

// TestIntegration_XLSXProcessing tests XLSX file processing with prep and validation.
// sample.xlsx has headers [id, name] and rows: [1,Gina], [2,Yulia], [3,Vika].
func TestIntegration_XLSXProcessing(t *testing.T) {
	t.Parallel()

	type TestRecord struct {
		ID   string `prep:"trim" validate:"required,numeric"`
		Name string `prep:"trim" validate:"required"`
	}

	file, err := os.Open(filepath.Join("testdata", "sample.xlsx"))
	if err != nil {
		t.Fatalf("os.Open() error = %v", err)
	}
	defer file.Close()

	var records []TestRecord

	processor := NewProcessor(parser.XLSX)
	pipeReader, result, err := processor.Process(file, &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	// Drain output
	go func() {
		_, _ = io.Copy(io.Discard, pipeReader) //nolint:errcheck // discarding output in test
	}()

	if result.OriginalFormat != parser.XLSX {
		t.Errorf("OriginalFormat = %v, want %v", result.OriginalFormat, parser.XLSX)
	}

	// Verify exact record contents after preprocessing
	want := []TestRecord{
		{ID: "1", Name: "Gina"},
		{ID: "2", Name: "Yulia"},
		{ID: "3", Name: "Vika"},
	}
	if diff := cmp.Diff(want, records); diff != "" {
		t.Errorf("records mismatch (-want +got):\n%s", diff)
	}

	if result.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", result.RowCount)
	}
	if result.ValidRowCount != 3 {
		t.Errorf("ValidRowCount = %d, want 3", result.ValidRowCount)
	}
}

// TestIntegration_XLSXWithValidationErrors tests XLSX processing detects validation errors.
// sample.xlsx has headers [id, name] and rows: [1,Gina], [2,Yulia], [3,Vika].
// The struct asks for a numeric name, which none of the three rows is, so every
// row fails a rule the data really breaks.
//
// It used to ask for an "email" column the workbook does not have and count the
// resulting empty values as failures. That reads as a validation test and was
// really testing the zero-filling of an unmatched field, which is now refused:
// a column that is not there and a cell that is empty were indistinguishable,
// so the errors said the data was bad when the struct was wrong.
func TestIntegration_XLSXWithValidationErrors(t *testing.T) {
	t.Parallel()

	type StrictRecord struct {
		ID   string `validate:"required,numeric"`
		Name string `validate:"required,numeric"`
	}

	file, err := os.Open(filepath.Join("testdata", "sample.xlsx"))
	if err != nil {
		t.Fatalf("os.Open() error = %v", err)
	}
	defer file.Close()

	var records []StrictRecord

	processor := NewProcessor(parser.XLSX)
	pipeReader, result, err := processor.Process(file, &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	// Drain output
	go func() {
		_, _ = io.Copy(io.Discard, pipeReader) //nolint:errcheck // discarding output in test
	}()

	if result.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", result.RowCount)
	}

	valErrors := result.ValidationErrors()
	if len(valErrors) == 0 {
		t.Fatal("expected validation errors from XLSX data whose names are not numeric")
	}

	foundNameNumeric := false
	for _, ve := range valErrors {
		if ve.Column == "name" && ve.Tag == "numeric" {
			foundNameNumeric = true
			break
		}
	}
	if !foundNameNumeric {
		t.Error("expected a numeric validation error on the name column")
	}

	// All three names (Gina, Yulia, Vika) fail the numeric rule.
	emailErrors := 0
	for _, ve := range valErrors {
		if ve.Column == "name" {
			emailErrors++
		}
	}
	// Each row should have at least required + email validation errors
	if emailErrors < 3 {
		t.Errorf("expected at least 3 email validation errors (one per row), got %d", emailErrors)
	}
}

// TestIntegration_Parquet_FullPipeline tests Parquet end-to-end with prep, validation, and cmp.Diff
func TestIntegration_Parquet_FullPipeline(t *testing.T) {
	t.Parallel()

	type ParquetRow struct {
		Name  string `parquet:"name"`
		Email string `parquet:"email"`
		Age   string `parquet:"age"`
	}

	type ResultRecord struct {
		Name  string `prep:"trim,uppercase" validate:"required"`
		Email string `prep:"trim,lowercase" validate:"email"`
		Age   string `validate:"numeric"`
	}

	rows := []ParquetRow{
		{Name: "  alice  ", Email: "  ALICE@EXAMPLE.COM  ", Age: "30"},
		{Name: "  bob  ", Email: "  BOB@EXAMPLE.COM  ", Age: "25"},
		{Name: "", Email: "nobody@example.com", Age: "99"},
	}

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[ParquetRow](&buf)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write parquet data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close parquet writer: %v", err)
	}

	var records []ResultRecord
	processor := NewProcessor(parser.Parquet)
	pipeReader, result, err := processor.Process(bytes.NewReader(buf.Bytes()), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	// Drain output to avoid blocking
	go func() {
		_, _ = io.Copy(io.Discard, pipeReader) //nolint:errcheck // discarding output in test
	}()

	// Row counts
	if result.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", result.RowCount)
	}
	// Third row has empty name → required fails, so valid = 2
	if result.ValidRowCount != 2 {
		t.Errorf("ValidRowCount = %d, want 2", result.ValidRowCount)
	}

	// Verify preprocessed records using cmp.Diff
	want := []ResultRecord{
		{Name: "ALICE", Email: "alice@example.com", Age: "30"},
		{Name: "BOB", Email: "bob@example.com", Age: "25"},
		{Name: "", Email: "nobody@example.com", Age: "99"},
	}
	if diff := cmp.Diff(want, records); diff != "" {
		t.Errorf("records mismatch (-want +got):\n%s", diff)
	}

	// Verify validation errors
	valErrors := result.ValidationErrors()
	if len(valErrors) != 1 {
		t.Errorf("expected 1 validation error, got %d", len(valErrors))
	}
	if len(valErrors) > 0 && valErrors[0].Row != 3 {
		t.Errorf("expected error on row 3, got row %d", valErrors[0].Row)
	}

	// Verify original format
	if result.OriginalFormat != parser.Parquet {
		t.Errorf("OriginalFormat = %v, want %v", result.OriginalFormat, parser.Parquet)
	}

	// Verify columns
	wantCols := []string{"name", "email", "age"}
	if diff := cmp.Diff(wantCols, result.Columns); diff != "" {
		t.Errorf("Columns mismatch (-want +got):\n%s", diff)
	}
}

// TestIntegration_Parquet_OutputAsCSV verifies Parquet output is valid CSV
func TestIntegration_Parquet_OutputAsCSV(t *testing.T) {
	t.Parallel()

	type ParquetRow struct {
		Name string `parquet:"name"`
		City string `parquet:"city"`
	}

	type ResultRecord struct {
		Name string `prep:"trim"`
		City string `prep:"uppercase"`
	}

	rows := []ParquetRow{
		{Name: "  Alice  ", City: "tokyo"},
		{Name: "Bob", City: "osaka"},
	}

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[ParquetRow](&buf)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write parquet data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close parquet writer: %v", err)
	}

	var records []ResultRecord
	processor := NewProcessor(parser.Parquet)
	pipeReader, _, err := processor.Process(bytes.NewReader(buf.Bytes()), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	// Read output CSV
	outputBytes, err := io.ReadAll(pipeReader)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}
	output := string(outputBytes)

	// Should contain CSV header
	if !strings.Contains(output, "name") || !strings.Contains(output, "city") {
		t.Errorf("output missing CSV header columns: %q", output)
	}

	// Should contain preprocessed values
	if !strings.Contains(output, "Alice") {
		t.Errorf("output missing trimmed name 'Alice': %q", output)
	}
	if !strings.Contains(output, "TOKYO") {
		t.Errorf("output missing uppercased city 'TOKYO': %q", output)
	}
}
