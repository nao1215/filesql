package driver

import (
	"runtime"
	"testing"
)

// TestWindowsPathHandling tests Windows-specific path handling across all platforms
// These tests ensure that Windows-style paths are handled correctly even on Unix systems
func TestWindowsPathHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		wantErr  bool
		expected error
	}{
		{
			name:    "Windows absolute path with backslashes",
			path:    "C:\\Users\\test\\data.csv",
			wantErr: false,
		},
		{
			name:    "Windows absolute path with forward slashes",
			path:    "C:/Users/test/data.csv",
			wantErr: false,
		},
		{
			name:     "Windows system directory (Windows folder)",
			path:     "C:\\Windows\\System32\\config",
			wantErr:  true,
			expected: ErrInvalidPath,
		},
		{
			name:     "Windows system directory (forward slashes)",
			path:     "C:/Windows/System32/config",
			wantErr:  true,
			expected: ErrInvalidPath,
		},
		{
			name:     "Windows Program Files directory",
			path:     "C:\\Program Files\\test\\data.csv",
			wantErr:  true,
			expected: ErrInvalidPath,
		},
		{
			name:     "Windows UNC path",
			path:     "\\\\server\\share\\data.csv",
			wantErr:  true,
			expected: ErrInvalidPath,
		},
		{
			name:     "Windows UNC device path",
			path:     "\\\\?\\C:\\data.csv",
			wantErr:  true,
			expected: ErrInvalidPath,
		},
		{
			name:    "Windows relative path with backslashes",
			path:    "..\\testdata\\sample.csv",
			wantErr: false,
		},
		{
			name:     "Windows path traversal attempt",
			path:     "..\\..\\..\\..\\Windows\\System32\\config",
			wantErr:  true,
			expected: ErrInvalidPath,
		},
		{
			name:    "Mixed separators",
			path:    "..\\testdata/sample.csv",
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

			if tt.expected != nil {
				if err == nil {
					t.Errorf("Expected error %v, got nil", tt.expected)
				} else if err.Error() != tt.expected.Error() {
					t.Errorf("Expected error %v, got %v", tt.expected, err)
				}
			}
		})
	}
}

// TestWindowsReservedNames tests handling of Windows reserved file names
func TestWindowsReservedNames(t *testing.T) {
	t.Parallel()

	// These tests are relevant on all platforms but especially important on Windows
	reservedNames := []string{
		"con.csv", "CON.csv",
		"prn.csv", "PRN.csv",
		"aux.csv", "AUX.csv",
		"nul.csv", "NUL.csv",
		"com1.csv", "COM1.csv",
		"com2.csv", "COM2.csv",
		"lpt1.csv", "LPT1.csv",
		"lpt2.csv", "LPT2.csv",
	}

	for _, name := range reservedNames {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := ValidatePath(name)
			if err == nil {
				t.Errorf("Expected error for Windows reserved name %s", name)
			}
		})
	}
}

// TestCrossPlatformPathSeparators tests that both / and \ separators work correctly
func TestCrossPlatformPathSeparators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		path        string
		expectValid bool
	}{
		{
			name:        "Unix-style path",
			path:        "testdata/sample.csv",
			expectValid: true,
		},
		{
			name:        "Windows-style path",
			path:        "testdata\\sample.csv",
			expectValid: true,
		},
		{
			name:        "Mixed separators",
			path:        "testdata/subdir\\sample.csv",
			expectValid: true,
		},
		{
			name:        "Unix-style relative path",
			path:        "../testdata/sample.csv",
			expectValid: true,
		},
		{
			name:        "Windows-style relative path",
			path:        "..\\testdata\\sample.csv",
			expectValid: true,
		},
		{
			name:        "Mixed separators in relative path",
			path:        "../testdata\\sample.csv",
			expectValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidatePath(tt.path)
			isValid := err == nil

			if isValid != tt.expectValid {
				t.Errorf("ValidatePath(%q) valid = %v, expected %v (error: %v)",
					tt.path, isValid, tt.expectValid, err)
			}
		})
	}
}

// TestWindowsDriveLetterHandling tests various Windows drive letter formats
func TestWindowsDriveLetterHandling(t *testing.T) {
	// Skip this test on non-Windows platforms for drive-specific tests
	if runtime.GOOS != "windows" {
		t.Skip("Skipping Windows-specific drive letter tests on non-Windows platform")
	}

	t.Parallel()

	tests := []struct {
		name        string
		path        string
		expectValid bool
	}{
		{
			name:        "C: drive absolute path",
			path:        "C:\\data\\sample.csv",
			expectValid: true,
		},
		{
			name:        "D: drive absolute path",
			path:        "D:/data/sample.csv",
			expectValid: true,
		},
		{
			name:        "Network drive",
			path:        "Z:\\network\\data.csv",
			expectValid: true,
		},
		{
			name:        "Invalid drive format",
			path:        "1:\\data.csv",
			expectValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidatePath(tt.path)
			isValid := err == nil

			if isValid != tt.expectValid {
				t.Errorf("ValidatePath(%q) valid = %v, expected %v (error: %v)",
					tt.path, isValid, tt.expectValid, err)
			}
		})
	}
}

// TestPathNormalization tests that paths are properly normalized across platforms
func TestPathNormalization(t *testing.T) {
	t.Parallel()

	d := NewDriver()

	tests := []struct {
		name string
		dsn  string
		// We don't test actual connection since files might not exist,
		// but we test that the connector creation works
		expectConnectorError bool
	}{
		{
			name:                 "Unix-style path",
			dsn:                  "testdata/sample.csv",
			expectConnectorError: false,
		},
		{
			name:                 "Windows-style path",
			dsn:                  "testdata\\sample.csv",
			expectConnectorError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			connector, err := d.OpenConnector(tt.dsn)
			if (err != nil) != tt.expectConnectorError {
				t.Errorf("OpenConnector() error = %v, expectError %v", err, tt.expectConnectorError)
			}

			if connector != nil {
				// Test that we can create a connector, which validates the DSN format
				_ = connector.Driver()
			}
		})
	}
}
