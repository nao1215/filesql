package driver

import (
	"errors"
	"strings"
	"testing"
)

func TestValidatePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		wantErr  bool
		expected error
	}{
		{
			name:    "Valid relative path",
			path:    "testdata/sample.csv",
			wantErr: false,
		},
		{
			name:     "Empty path",
			path:     "",
			wantErr:  true,
			expected: ErrInvalidPath,
		},
		{
			name:     "Whitespace only path",
			path:     "   ",
			wantErr:  true,
			expected: ErrInvalidPath,
		},
		{
			name:     "Path with null byte",
			path:     "test\x00.csv",
			wantErr:  true,
			expected: ErrInvalidPath,
		},
		{
			name:     "Path traversal attempt",
			path:     "../../../../../../../etc/passwd",
			wantErr:  true,
			expected: ErrInvalidPath,
		},
		{
			name:     "Unix system directory",
			path:     "/etc/passwd",
			wantErr:  true,
			expected: ErrInvalidPath,
		},
		{
			name:     "Windows system directory",
			path:     "C:\\Windows\\System32\\config",
			wantErr:  true,
			expected: ErrInvalidPath,
		},
		{
			name:     "Windows reserved name",
			path:     "con.csv",
			wantErr:  true,
			expected: ErrInvalidPath,
		},
		{
			name:    "Valid filename",
			path:    "data.csv",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidatePath(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidatePath() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.expected != nil && !errors.Is(err, tt.expected) {
				t.Errorf("ValidatePath() error = %v, expected %v", err, tt.expected)
			}
		})
	}
}

func TestValidateColumnCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		columnCount int
		wantErr     bool
	}{
		{
			name:        "Valid column count",
			columnCount: 10,
			wantErr:     false,
		},
		{
			name:        "Maximum allowed columns",
			columnCount: MaxColumnCount,
			wantErr:     false,
		},
		{
			name:        "Too many columns",
			columnCount: MaxColumnCount + 1,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateColumnCount(tt.columnCount)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateColumnCount() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateFileCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fileCount int
		wantErr   bool
	}{
		{
			name:      "Valid file count",
			fileCount: 10,
			wantErr:   false,
		},
		{
			name:      "Maximum allowed files",
			fileCount: MaxFilesPerDirectory,
			wantErr:   false,
		},
		{
			name:      "Too many files",
			fileCount: MaxFilesPerDirectory + 1,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateFileCount(tt.fileCount)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateFileCount() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateFieldValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Normal value",
			input:    "hello world",
			expected: "hello world",
		},
		{
			name:     "Value with null bytes",
			input:    "hello\x00world",
			expected: "helloworld",
		},
		{
			name:     "Very long value",
			input:    strings.Repeat("a", MaxValueLength+100),
			expected: strings.Repeat("a", MaxValueLength),
		},
		{
			name:     "Empty value",
			input:    "",
			expected: "",
		},
		{
			name:     "Maximum length value",
			input:    strings.Repeat("b", MaxValueLength),
			expected: strings.Repeat("b", MaxValueLength),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ValidateFieldValue(tt.input)
			if result != tt.expected {
				t.Errorf("ValidateFieldValue() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestIsValidFileName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fileName string
		expected bool
	}{
		{
			name:     "Valid filename",
			fileName: "data.csv",
			expected: true,
		},
		{
			name:     "Hidden file",
			fileName: ".hidden.csv",
			expected: false,
		},
		{
			name:     "Filename with null byte",
			fileName: "data\x00.csv",
			expected: false,
		},
		{
			name:     "Filename with angle brackets",
			fileName: "data<test>.csv",
			expected: false,
		},
		{
			name:     "Filename with colon",
			fileName: "data:test.csv",
			expected: false,
		},
		{
			name:     "Filename with pipe",
			fileName: "data|test.csv",
			expected: false,
		},
		{
			name:     "Normal filename with underscore",
			fileName: "data_file.csv",
			expected: true,
		},
		{
			name:     "Normal filename with hyphen",
			fileName: "data-file.csv",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := IsValidFileName(tt.fileName)
			if result != tt.expected {
				t.Errorf("IsValidFileName() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestSanitizeForLog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Normal text",
			input:    "loading file data.csv",
			expected: "loading file data.csv",
		},
		{
			name:     "Text with password",
			input:    "failed to access password.txt",
			expected: "[REDACTED]",
		},
		{
			name:     "Text with secret",
			input:    "processing secret_data.csv",
			expected: "[REDACTED]",
		},
		{
			name:     "Very long text",
			input:    strings.Repeat("a", 300),
			expected: strings.Repeat("a", 200) + "...",
		},
		{
			name:     "Text with SSH key",
			input:    "reading id_rsa file",
			expected: "[REDACTED]",
		},
		{
			name:     "Empty input",
			input:    "",
			expected: "",
		},
		{
			name:     "Text with credential",
			input:    "credential file not found",
			expected: "[REDACTED]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := SanitizeForLog(tt.input)
			if result != tt.expected {
				t.Errorf("SanitizeForLog() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// Benchmark tests
func BenchmarkValidatePath(b *testing.B) {
	testPaths := []string{
		"normal/path/to/file.csv",
		"../../../etc/passwd",
		"c:\\windows\\system32\\config",
		"data.csv",
	}

	for _, path := range testPaths {
		b.Run(path, func(b *testing.B) {
			for range b.N {
				err := ValidatePath(path)
				// In benchmark, we don't want to fail on errors as some paths are expected to be invalid
				// We're measuring performance of validation, not correctness
				_ = err
			}
		})
	}
}

func BenchmarkValidateFieldValue(b *testing.B) {
	testValues := []string{
		"normal value",
		"value\x00with\x00nulls",
		strings.Repeat("long value ", 1000),
	}

	for _, value := range testValues {
		b.Run("len_"+strings.Repeat("x", len(value)/100), func(b *testing.B) {
			for range b.N {
				_ = ValidateFieldValue(value)
			}
		})
	}
}

func BenchmarkIsValidFileName(b *testing.B) {
	testFiles := []string{
		"normal.csv",
		".hidden.csv",
		"file<with>brackets.csv",
		"file|with|pipes.csv",
	}

	for _, file := range testFiles {
		b.Run(file, func(b *testing.B) {
			for range b.N {
				result := IsValidFileName(file)
				// Use result to prevent compiler optimizations
				_ = result
			}
		})
	}
}
