package model

import (
	"testing"
)

func TestNewHeader(t *testing.T) {
	t.Parallel()

	t.Run("Create header from slice", func(t *testing.T) {
		t.Parallel()

		headerSlice := []string{"col1", "col2", "col3"}
		header := NewHeader(headerSlice)

		if len(header) != 3 {
			t.Errorf("expected length 3, got %d", len(header))
		}

		for i, expected := range headerSlice {
			if header[i] != expected {
				t.Errorf("expected %s at index %d, got %s", expected, i, header[i])
			}
		}
	})
}

func TestHeader_Equal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		header1  Header
		header2  Header
		expected bool
	}{
		{
			name:     "Equal headers",
			header1:  NewHeader([]string{"col1", "col2"}),
			header2:  NewHeader([]string{"col1", "col2"}),
			expected: true,
		},
		{
			name:     "Different length headers",
			header1:  NewHeader([]string{"col1", "col2"}),
			header2:  NewHeader([]string{"col1"}),
			expected: false,
		},
		{
			name:     "Different content headers",
			header1:  NewHeader([]string{"col1", "col2"}),
			header2:  NewHeader([]string{"col1", "col3"}),
			expected: false,
		},
		{
			name:     "Empty headers",
			header1:  NewHeader([]string{}),
			header2:  NewHeader([]string{}),
			expected: true,
		},
		{
			name:     "One empty one not",
			header1:  NewHeader([]string{}),
			header2:  NewHeader([]string{"col1"}),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.header1.Equal(tt.header2)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestNewRecord(t *testing.T) {
	t.Parallel()

	t.Run("Create record from slice", func(t *testing.T) {
		t.Parallel()

		recordSlice := []string{"val1", "val2", "val3"}
		record := NewRecord(recordSlice)

		if len(record) != 3 {
			t.Errorf("expected length 3, got %d", len(record))
		}

		for i, expected := range recordSlice {
			if record[i] != expected {
				t.Errorf("expected %s at index %d, got %s", expected, i, record[i])
			}
		}
	})
}

func TestRecord_Equal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		record1  Record
		record2  Record
		expected bool
	}{
		{
			name:     "Equal records",
			record1:  NewRecord([]string{"val1", "val2"}),
			record2:  NewRecord([]string{"val1", "val2"}),
			expected: true,
		},
		{
			name:     "Different length records",
			record1:  NewRecord([]string{"val1", "val2"}),
			record2:  NewRecord([]string{"val1"}),
			expected: false,
		},
		{
			name:     "Different content records",
			record1:  NewRecord([]string{"val1", "val2"}),
			record2:  NewRecord([]string{"val1", "val3"}),
			expected: false,
		},
		{
			name:     "Empty records",
			record1:  NewRecord([]string{}),
			record2:  NewRecord([]string{}),
			expected: true,
		},
		{
			name:     "One empty one not",
			record1:  NewRecord([]string{}),
			record2:  NewRecord([]string{"val1"}),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.record1.Equal(tt.record2)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestOutputFormat_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format OutputFormat
		want   string
	}{
		{
			name:   "CSV format",
			format: OutputFormatCSV,
			want:   "csv",
		},
		{
			name:   "TSV format",
			format: OutputFormatTSV,
			want:   "tsv",
		},
		{
			name:   "LTSV format",
			format: OutputFormatLTSV,
			want:   "ltsv",
		},
		{
			name:   "Unknown format defaults to csv",
			format: OutputFormat(999),
			want:   "csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.format.String(); got != tt.want {
				t.Errorf("OutputFormat.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOutputFormat_Extension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format OutputFormat
		want   string
	}{
		{
			name:   "CSV extension",
			format: OutputFormatCSV,
			want:   ".csv",
		},
		{
			name:   "TSV extension",
			format: OutputFormatTSV,
			want:   ".tsv",
		},
		{
			name:   "LTSV extension",
			format: OutputFormatLTSV,
			want:   ".ltsv",
		},
		{
			name:   "Unknown format defaults to csv",
			format: OutputFormat(999),
			want:   ".csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.format.Extension(); got != tt.want {
				t.Errorf("OutputFormat.Extension() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompressionType_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		compression CompressionType
		want        string
	}{
		{
			name:        "No compression",
			compression: CompressionNone,
			want:        "none",
		},
		{
			name:        "GZ compression",
			compression: CompressionGZ,
			want:        "gz",
		},
		{
			name:        "BZ2 compression",
			compression: CompressionBZ2,
			want:        "bz2",
		},
		{
			name:        "XZ compression",
			compression: CompressionXZ,
			want:        "xz",
		},
		{
			name:        "ZSTD compression",
			compression: CompressionZSTD,
			want:        "zstd",
		},
		{
			name:        "Unknown compression defaults to none",
			compression: CompressionType(999),
			want:        "none",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.compression.String(); got != tt.want {
				t.Errorf("CompressionType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompressionType_Extension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		compression CompressionType
		want        string
	}{
		{
			name:        "No compression",
			compression: CompressionNone,
			want:        "",
		},
		{
			name:        "GZ compression",
			compression: CompressionGZ,
			want:        ".gz",
		},
		{
			name:        "BZ2 compression",
			compression: CompressionBZ2,
			want:        ".bz2",
		},
		{
			name:        "XZ compression",
			compression: CompressionXZ,
			want:        ".xz",
		},
		{
			name:        "ZSTD compression",
			compression: CompressionZSTD,
			want:        ".zst",
		},
		{
			name:        "Unknown compression defaults to empty",
			compression: CompressionType(999),
			want:        "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.compression.Extension(); got != tt.want {
				t.Errorf("CompressionType.Extension() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewDumpOptions(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions()

	if options.Format != OutputFormatCSV {
		t.Errorf("NewDumpOptions().Format = %v, want %v", options.Format, OutputFormatCSV)
	}

	if options.Compression != CompressionNone {
		t.Errorf("NewDumpOptions().Compression = %v, want %v", options.Compression, CompressionNone)
	}
}

func TestDumpOptions_WithFormat(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions()
	newOptions := options.WithFormat(OutputFormatTSV)

	// Original options should not be modified
	if options.Format != OutputFormatCSV {
		t.Errorf("Original options modified: Format = %v, want %v", options.Format, OutputFormatCSV)
	}

	// New options should have the updated format
	if newOptions.Format != OutputFormatTSV {
		t.Errorf("WithFormat().Format = %v, want %v", newOptions.Format, OutputFormatTSV)
	}

	// Other fields should remain unchanged
	if newOptions.Compression != CompressionNone {
		t.Errorf("WithFormat().Compression = %v, want %v", newOptions.Compression, CompressionNone)
	}
}

func TestDumpOptions_WithCompression(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions()
	newOptions := options.WithCompression(CompressionGZ)

	// Original options should not be modified
	if options.Compression != CompressionNone {
		t.Errorf("Original options modified: Compression = %v, want %v", options.Compression, CompressionNone)
	}

	// New options should have the updated compression
	if newOptions.Compression != CompressionGZ {
		t.Errorf("WithCompression().Compression = %v, want %v", newOptions.Compression, CompressionGZ)
	}

	// Other fields should remain unchanged
	if newOptions.Format != OutputFormatCSV {
		t.Errorf("WithCompression().Format = %v, want %v", newOptions.Format, OutputFormatCSV)
	}
}

func TestDumpOptions_FileExtension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		format      OutputFormat
		compression CompressionType
		want        string
	}{
		{
			name:        "CSV with no compression",
			format:      OutputFormatCSV,
			compression: CompressionNone,
			want:        ".csv",
		},
		{
			name:        "CSV with gzip compression",
			format:      OutputFormatCSV,
			compression: CompressionGZ,
			want:        ".csv.gz",
		},
		{
			name:        "TSV with bzip2 compression",
			format:      OutputFormatTSV,
			compression: CompressionBZ2,
			want:        ".tsv.bz2",
		},
		{
			name:        "LTSV with xz compression",
			format:      OutputFormatLTSV,
			compression: CompressionXZ,
			want:        ".ltsv.xz",
		},
		{
			name:        "TSV with zstd compression",
			format:      OutputFormatTSV,
			compression: CompressionZSTD,
			want:        ".tsv.zst",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := DumpOptions{
				Format:      tt.format,
				Compression: tt.compression,
			}
			if got := options.FileExtension(); got != tt.want {
				t.Errorf("DumpOptions.FileExtension() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDumpOptions_ChainedMethods(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions().
		WithFormat(OutputFormatLTSV).
		WithCompression(CompressionZSTD)

	if options.Format != OutputFormatLTSV {
		t.Errorf("Chained WithFormat().Format = %v, want %v", options.Format, OutputFormatLTSV)
	}

	if options.Compression != CompressionZSTD {
		t.Errorf("Chained WithCompression().Compression = %v, want %v", options.Compression, CompressionZSTD)
	}

	expectedExt := ".ltsv.zst"
	if got := options.FileExtension(); got != expectedExt {
		t.Errorf("Chained options FileExtension() = %v, want %v", got, expectedExt)
	}
}

func TestInferColumnType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		values   []string
		expected ColumnType
	}{
		{
			name:     "all integers",
			values:   []string{"123", "456", "789"},
			expected: ColumnTypeInteger,
		},
		{
			name:     "mixed integers and floats",
			values:   []string{"123", "45.6", "789"},
			expected: ColumnTypeReal,
		},
		{
			name:     "all floats",
			values:   []string{"12.3", "45.6", "78.9"},
			expected: ColumnTypeReal,
		},
		{
			name:     "mixed numbers and text",
			values:   []string{"123", "hello", "789"},
			expected: ColumnTypeText,
		},
		{
			name:     "all text",
			values:   []string{"hello", "world", "test"},
			expected: ColumnTypeText,
		},
		{
			name:     "empty values",
			values:   []string{"", "", ""},
			expected: ColumnTypeText,
		},
		{
			name:     "integers with empty values",
			values:   []string{"123", "", "789"},
			expected: ColumnTypeInteger,
		},
		{
			name:     "negative integers",
			values:   []string{"-123", "456", "-789"},
			expected: ColumnTypeInteger,
		},
		{
			name:     "negative floats",
			values:   []string{"-12.3", "45.6", "-78.9"},
			expected: ColumnTypeReal,
		},
		{
			name:     "scientific notation",
			values:   []string{"1e10", "2.5e-3", "3.14e2"},
			expected: ColumnTypeReal,
		},
		{
			name:     "zero values",
			values:   []string{"0", "0.0", "000"},
			expected: ColumnTypeReal,
		},
		{
			name:     "ISO8601 dates",
			values:   []string{"2023-01-15", "2023-02-20", "2023-03-10"},
			expected: ColumnTypeDatetime,
		},
		{
			name:     "ISO8601 datetime",
			values:   []string{"2023-01-15T10:30:00", "2023-02-20T14:45:30", "2023-03-10T09:15:45"},
			expected: ColumnTypeDatetime,
		},
		{
			name:     "US date format",
			values:   []string{"1/15/2023", "2/20/2023", "3/10/2023"},
			expected: ColumnTypeDatetime,
		},
		{
			name:     "European date format",
			values:   []string{"15.1.2023", "20.2.2023", "10.3.2023"},
			expected: ColumnTypeDatetime,
		},
		{
			name:     "time only",
			values:   []string{"10:30:00", "14:45:30", "09:15:45"},
			expected: ColumnTypeDatetime,
		},
		{
			name:     "mixed datetime and text",
			values:   []string{"2023-01-15", "not a date", "2023-03-10"},
			expected: ColumnTypeText,
		},
		{
			name:     "datetime with timezone",
			values:   []string{"2023-01-15T10:30:00Z", "2023-02-20T14:45:30+09:00"},
			expected: ColumnTypeDatetime,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InferColumnType(tt.values)
			if result != tt.expected {
				t.Errorf("InferColumnType(%v) = %v, want %v", tt.values, result, tt.expected)
			}
		})
	}
}

func TestInferColumnsInfo(t *testing.T) {
	t.Parallel()

	t.Run("mixed column types", func(t *testing.T) {
		header := NewHeader([]string{"id", "name", "age", "salary", "hire_date"})
		records := []Record{
			NewRecord([]string{"1", "Alice", "30", "95000", "2023-01-15"}),
			NewRecord([]string{"2", "Bob", "25", "78000", "2023-02-20"}),
			NewRecord([]string{"3", "Charlie", "35", "102000", "2023-03-10"}),
		}

		result := InferColumnsInfo(header, records)

		expected := []ColumnInfo{
			{Name: "id", Type: ColumnTypeInteger},
			{Name: "name", Type: ColumnTypeText},
			{Name: "age", Type: ColumnTypeInteger},
			{Name: "salary", Type: ColumnTypeInteger},
			{Name: "hire_date", Type: ColumnTypeDatetime},
		}

		if len(result) != len(expected) {
			t.Fatalf("Expected %d columns, got %d", len(expected), len(result))
		}

		for i, exp := range expected {
			if result[i].Name != exp.Name {
				t.Errorf("Column %d: expected name %s, got %s", i, exp.Name, result[i].Name)
			}
			if result[i].Type != exp.Type {
				t.Errorf("Column %d: expected type %s, got %s", i, exp.Type, result[i].Type)
			}
		}
	})

	t.Run("empty records", func(t *testing.T) {
		header := NewHeader([]string{"col1", "col2"})
		records := []Record{}

		result := InferColumnsInfo(header, records)

		if len(result) != 2 {
			t.Fatalf("Expected 2 columns, got %d", len(result))
		}

		for i, col := range result {
			if col.Type != ColumnTypeText {
				t.Errorf("Column %d: expected TEXT type for empty records, got %s", i, col.Type)
			}
		}
	})

	t.Run("datetime column inference", func(t *testing.T) {
		header := NewHeader([]string{"event_date", "event_time", "timestamp"})
		records := []Record{
			NewRecord([]string{"2023-01-15", "10:30:00", "2023-01-15T10:30:00Z"}),
			NewRecord([]string{"2023-02-20", "14:45:30", "2023-02-20T14:45:30Z"}),
			NewRecord([]string{"2023-03-10", "09:15:45", "2023-03-10T09:15:45Z"}),
		}

		result := InferColumnsInfo(header, records)

		expected := []ColumnInfo{
			{Name: "event_date", Type: ColumnTypeDatetime},
			{Name: "event_time", Type: ColumnTypeDatetime},
			{Name: "timestamp", Type: ColumnTypeDatetime},
		}

		if len(result) != len(expected) {
			t.Fatalf("Expected %d columns, got %d", len(expected), len(result))
		}

		for i, exp := range expected {
			if result[i].Name != exp.Name {
				t.Errorf("Column %d: expected name %s, got %s", i, exp.Name, result[i].Name)
			}
			if result[i].Type != exp.Type {
				t.Errorf("Column %d: expected type %s, got %s", i, exp.Type, result[i].Type)
			}
		}
	})
}

func TestColumnType_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		columnType ColumnType
		expected   string
	}{
		{ColumnTypeText, "TEXT"},
		{ColumnTypeInteger, "INTEGER"},
		{ColumnTypeReal, "REAL"},
		{ColumnTypeDatetime, "TEXT"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.columnType.String()
			if result != tt.expected {
				t.Errorf("ColumnType.String() = %s, want %s", result, tt.expected)
			}
		})
	}
}

func TestIsDatetime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    string
		expected bool
	}{
		// ISO8601 formats
		{"ISO date", "2023-01-15", true},
		{"ISO datetime", "2023-01-15T10:30:00", true},
		{"ISO datetime with timezone Z", "2023-01-15T10:30:00Z", true},
		{"ISO datetime with timezone offset", "2023-01-15T10:30:00+09:00", true},
		{"ISO datetime with milliseconds", "2023-01-15T10:30:00.123", true},

		// US formats
		{"US date", "1/15/2023", true},
		{"US date padded", "01/15/2023", true},
		{"US datetime", "1/15/2023 10:30:00", true},

		// European formats
		{"European date", "15.1.2023", true},
		{"European datetime", "15.1.2023 10:30:00", true},

		// Time only
		{"Time HH:MM:SS", "10:30:00", true},
		{"Time HH:MM", "10:30", true},
		{"Time with milliseconds", "10:30:00.123", true},

		// Invalid cases
		{"Plain text", "hello world", false},
		{"Number", "123", false},
		{"Invalid date", "2023-13-45", false},
		{"Invalid time", "25:70:90", false},
		{"Empty string", "", false},
		{"Partial date", "2023-01", false},
		{"Wrong format", "Jan 15, 2023", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDatetime(tt.value)
			if result != tt.expected {
				t.Errorf("isDatetime(%q) = %v, want %v", tt.value, result, tt.expected)
			}
		})
	}
}
