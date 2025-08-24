package driver

import (
	"errors"
	"path/filepath"
	"strings"
)

// MaxFileSize defines the maximum file size allowed for processing (1GB)
const MaxFileSize = 1024 * 1024 * 1024

// MaxFilesPerDirectory defines the maximum number of files allowed per directory
const MaxFilesPerDirectory = 1000

// MaxColumnCount defines the maximum number of columns allowed in a table
const MaxColumnCount = 2000

// MaxValueLength defines the maximum length of a single field value
const MaxValueLength = 65536

var (
	// ErrFileTooLarge is returned when a file exceeds the maximum size limit
	ErrFileTooLarge = errors.New("file too large")

	// ErrTooManyFiles is returned when a directory contains too many files
	ErrTooManyFiles = errors.New("too many files in directory")

	// ErrTooManyColumns is returned when a file has too many columns
	ErrTooManyColumns = errors.New("too many columns")

	// ErrInvalidPath is returned when a path is invalid or potentially dangerous
	ErrInvalidPath = errors.New("invalid or dangerous path")

	// ErrInvalidIdentifier is returned when an SQL identifier is invalid
	ErrInvalidIdentifier = errors.New("invalid SQL identifier")
)

// ValidatePath performs comprehensive path validation for security
func ValidatePath(path string) error {
	// Check for empty or whitespace-only paths
	if strings.TrimSpace(path) == "" {
		return ErrInvalidPath
	}

	// Check for null byte injection
	if strings.Contains(path, "\x00") {
		return ErrInvalidPath
	}

	// Check for path traversal attempts - but allow legitimate relative paths
	cleanPath := filepath.Clean(path)
	if strings.Contains(cleanPath, "..") && !isLegitimateRelativePath(path) {
		return ErrInvalidPath
	}

	// Check for absolute paths to system directories (Unix-like systems)
	systemDirs := []string{"/etc/", "/proc/", "/sys/", "/dev/", "/root/", "/boot/"}
	lowerPath := strings.ToLower(path)
	for _, sysDir := range systemDirs {
		if strings.HasPrefix(lowerPath, sysDir) {
			return ErrInvalidPath
		}
	}

	// Check for Windows system directories (handle both forward and backward slashes)
	windowsDirs := []string{
		"c:\\windows\\", "c:/windows/",
		"c:\\program files", "c:/program files",
		"c:\\users\\administrator", "c:/users/administrator",
		"\\\\?\\", // UNC paths
		"\\\\",    // Network paths
	}
	for _, winDir := range windowsDirs {
		if strings.HasPrefix(lowerPath, winDir) {
			return ErrInvalidPath
		}
	}

	// Check for Windows reserved names
	reservedNames := []string{"con", "prn", "aux", "nul", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9"}
	baseName := strings.ToLower(strings.TrimSuffix(path, ".csv"))
	baseName = strings.TrimSuffix(baseName, ".tsv")
	baseName = strings.TrimSuffix(baseName, ".ltsv")
	for _, reserved := range reservedNames {
		if baseName == reserved {
			return ErrInvalidPath
		}
	}

	return nil
}

// ValidateColumnCount checks if the number of columns is within acceptable limits
func ValidateColumnCount(columnCount int) error {
	if columnCount > MaxColumnCount {
		return ErrTooManyColumns
	}
	return nil
}

// ValidateFileCount checks if the number of files is within acceptable limits
func ValidateFileCount(fileCount int) error {
	if fileCount > MaxFilesPerDirectory {
		return ErrTooManyFiles
	}
	return nil
}

// ValidateFieldValue validates and sanitizes field values
func ValidateFieldValue(value string) string {
	// Truncate extremely long values
	if len(value) > MaxValueLength {
		value = value[:MaxValueLength]
	}

	// Remove null bytes
	value = strings.ReplaceAll(value, "\x00", "")

	return value
}

// IsValidFileName checks if a filename is safe to process
func IsValidFileName(fileName string) bool {
	// Skip hidden files
	if strings.HasPrefix(fileName, ".") {
		return false
	}

	// Check for null bytes
	if strings.Contains(fileName, "\x00") {
		return false
	}

	// Check for suspicious characters
	suspiciousChars := []string{"<", ">", ":", "\"", "|", "?", "*"}
	for _, char := range suspiciousChars {
		if strings.Contains(fileName, char) {
			return false
		}
	}

	return true
}

// SanitizeForLog removes sensitive information from strings before logging
func SanitizeForLog(input string) string {
	// Remove common sensitive patterns
	sensitive := []string{
		"password", "passwd", "secret", "key", "token",
		"credential", "auth", "private", "ssh", "rsa",
	}

	result := input
	for _, pattern := range sensitive {
		if strings.Contains(strings.ToLower(result), pattern) {
			return "[REDACTED]"
		}
	}

	// Limit length to prevent log flooding
	const maxLogLength = 200
	if len(result) > maxLogLength {
		result = result[:maxLogLength] + "..."
	}

	return result
}

// isLegitimateRelativePath checks if a path containing ".." is a legitimate relative path
func isLegitimateRelativePath(path string) bool {
	// Clean the path and check if it's trying to escape the current directory structure
	cleanPath := filepath.Clean(path)

	// If the cleaned path starts with ".." or contains multiple consecutive "..", it's suspicious
	// Handle both Unix and Windows path separators
	if strings.HasPrefix(cleanPath, "../") || strings.HasPrefix(cleanPath, "..\\") {
		// Count how many levels up it goes
		// Use proper cross-platform path splitting
		parts := strings.FieldsFunc(cleanPath, func(c rune) bool {
			return c == '/' || c == '\\'
		})
		upLevels := 0
		for _, part := range parts {
			if part == ".." {
				upLevels++
			} else if part != "." && part != "" {
				break
			}
		}
		// Allow only reasonable number of parent directory references (e.g., max 3 levels up)
		return upLevels <= 3
	}

	return true
}
