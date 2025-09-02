package filesql

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// validator handles validation logic for DBBuilder
type validator struct {
	// No configuration needed for now, but keeping struct for future extensibility
}

// newValidator creates a new validator instance
func newValidator() *validator {
	return &validator{}
}

// validatePath validates a single file or directory path
func (v *validator) validatePath(path string) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("path cannot be empty")
	}

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("failed to load file: path does not exist: %s", path)
		}
		return fmt.Errorf("failed to stat path %s: %w", path, err)
	}

	// For files, check if they are supported
	if !info.IsDir() {
		if !isSupportedFile(path) {
			return fmt.Errorf("unsupported file type: %s", path)
		}
	}

	return nil
}

// validateReader validates a reader input
func (v *validator) validateReader(reader any, tableName string, fileType FileType) error {
	if reader == nil {
		return errors.New("reader cannot be nil")
	}
	if tableName == "" {
		return errors.New("table name must be specified for reader input")
	}
	if fileType == FileTypeUnsupported {
		return errors.New("file type must be specified for reader input")
	}

	// For specific readers where we can safely peek without consuming, validate empty content
	// This provides format-specific error messages at Build time
	if stringReader, ok := reader.(*strings.Reader); ok && stringReader.Len() == 0 {
		switch fileType.baseType() {
		case FileTypeCSV:
			return errors.New("empty CSV data")
		case FileTypeTSV:
			return errors.New("empty TSV data")
		case FileTypeLTSV:
			return errors.New("empty LTSV data")
		default:
			return errors.New("reader contains no data")
		}
	}

	// Skip other reader content validation at Build time to avoid consuming reader
	// Content validation will be done during streaming phase
	// This prevents issues with readers being consumed before Open()

	return nil
}

// validateAutoSaveConfig validates auto-save configuration
func (v *validator) validateAutoSaveConfig(config *autoSaveConfig) error {
	if config == nil {
		return nil // Auto-save is optional
	}

	if !config.enabled {
		return nil // Disabled config is valid
	}

	// Validate output directory if specified
	if config.outputDir != "" {
		// Check if parent directory exists for non-empty output directory
		if err := v.validateOutputDirectory(config.outputDir); err != nil {
			return fmt.Errorf("invalid auto-save output directory: %w", err)
		}
	}

	return nil
}

// validateOutputDirectory validates that the output directory can be created/accessed
func (v *validator) validateOutputDirectory(outputDir string) error {
	// For overwrite mode (empty outputDir), no validation needed
	if outputDir == "" {
		return nil
	}

	// Check if directory already exists
	if info, err := os.Stat(outputDir); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("output path exists but is not a directory: %s", outputDir)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to check output directory: %w", err)
	}

	// Directory doesn't exist, that's fine - it will be created later
	return nil
}

// validateFinalState performs final validation to ensure we have valid inputs
func (v *validator) validateFinalState(collectedPaths []string, readers []readerInput, originalPaths []string) error {
	if len(collectedPaths) == 0 && len(readers) == 0 {
		hasDirectories := false
		for _, path := range originalPaths {
			if info, err := os.Stat(path); err == nil && info.IsDir() {
				hasDirectories = true
				break
			}
		}

		if hasDirectories {
			return errors.New("no supported files found in directory")
		}
		return errors.New("no valid input files found")
	}

	return nil
}

// validateInputsAvailable checks if any valid inputs are available for database creation
func (v *validator) validateInputsAvailable(collectedPaths []string, readers []readerInput) error {
	if len(collectedPaths) == 0 && len(readers) == 0 {
		return errors.New("no valid input files found, did you call Build()?")
	}
	return nil
}
