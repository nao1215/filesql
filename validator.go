package filesql

import (
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
		return ErrEmptyPath
	}

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: %s", ErrFileNotFound, path)
		}
		return fmt.Errorf("%w: failed to stat path %s: %w", ErrIOOperation, path, err)
	}

	// For files, check if they are supported
	if !info.IsDir() {
		if !isSupportedFile(path) {
			return fmt.Errorf("%w: %s", ErrUnsupportedFormat, path)
		}
	}

	return nil
}

// validateReader validates a reader input
func (v *validator) validateReader(reader any, tableName string, fileType FileType) error {
	if reader == nil {
		return fmt.Errorf("%w: reader cannot be nil", ErrNilInput)
	}
	if tableName == "" {
		return fmt.Errorf("%w: table name must be specified for reader input", ErrInvalidData)
	}
	if fileType == FileTypeUnsupported {
		return fmt.Errorf("%w: file type must be specified for reader input", ErrUnsupportedFormat)
	}

	// Whether the stream holds anything is not asked here. Reading it would
	// consume it, and the reader that reads it for real already answers the
	// question in the words of the format it read: this used to peek when the
	// reader happened to be a *strings.Reader and say so in its own words, so
	// the same empty bytes were refused through one reader type and loaded
	// through another, which is not something a caller can see coming.
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

	// A directory of spaces is a name the operating system takes, so a save
	// used to write every table into one in the working directory. Only the
	// empty string names the original files; a blank one names nothing, and
	// this package refuses the same string as an input path.
	if strings.TrimSpace(outputDir) == "" {
		return ErrEmptyPath
	}

	// Check if directory already exists
	if info, err := os.Stat(outputDir); err == nil {
		if !info.IsDir() {
			// The same condition an export reports, reported with the same
			// sentinel, so a caller reads one answer whichever writes it.
			return fmt.Errorf("%w: output path exists but is not a directory: %s", ErrIOOperation, outputDir)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("%w: failed to check output directory: %w", ErrIOOperation, err)
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
			return fmt.Errorf("%w: no supported files found in directory", ErrNoFiles)
		}
		return fmt.Errorf("%w: no valid input files found", ErrNoFiles)
	}

	return nil
}
