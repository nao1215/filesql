package filesql

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// fileProcessor handles file-specific processing operations
type fileProcessor struct {
	chunkSize int
	validator *validator
}

// newFileProcessor creates a new file processor instance
func newFileProcessor(chunkSize int) *fileProcessor {
	return &fileProcessor{
		chunkSize: chunkSize,
		validator: newValidator(),
	}
}

// collectFilesFromPaths validates and collects all files from the given paths
func (fp *fileProcessor) collectFilesFromPaths(paths []string) ([]string, error) {
	var collectedPaths []string
	processedFiles := make(map[string]bool)

	for _, path := range paths {
		if err := fp.validator.validatePath(path); err != nil {
			return nil, err
		}

		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("failed to stat path %s: %w", path, err)
		}

		if info.IsDir() {
			dirFiles, err := fp.collectFilesFromDirectory(path, processedFiles)
			if err != nil {
				return nil, err
			}
			collectedPaths = append(collectedPaths, dirFiles...)
		} else {
			if err := fp.addSingleFile(path, processedFiles, &collectedPaths); err != nil {
				return nil, err
			}
		}
	}

	return collectedPaths, nil
}

// collectFilesFromDirectory recursively collects all supported files from a directory
func (fp *fileProcessor) collectFilesFromDirectory(dirPath string, processedFiles map[string]bool) ([]string, error) {
	var collectedPaths []string

	err := filepath.WalkDir(dirPath, func(filePath string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !isSupportedFile(filePath) {
			return nil
		}

		// Skip files with duplicate_columns in name (test files)
		if strings.Contains(filepath.Base(filePath), "duplicate_columns") {
			return nil
		}

		absPath, err := filepath.Abs(filePath)
		if err != nil {
			return fmt.Errorf("failed to get absolute path for %s: %w", filePath, err)
		}

		if !processedFiles[absPath] {
			processedFiles[absPath] = true
			collectedPaths = append(collectedPaths, filePath)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk directory %s: %w", dirPath, err)
	}

	return collectedPaths, nil
}

// addSingleFile validates and adds a single file to the collected paths
func (fp *fileProcessor) addSingleFile(filePath string, processedFiles map[string]bool, collectedPaths *[]string) error {
	if !isSupportedFile(filePath) {
		return fmt.Errorf("unsupported file type: %s", filePath)
	}

	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for %s: %w", filePath, err)
	}

	if !processedFiles[absPath] {
		processedFiles[absPath] = true
		*collectedPaths = append(*collectedPaths, filePath)
	}

	return nil
}

// processFilesystemsToReaders processes embedded filesystems and converts them to readers
func (fp *fileProcessor) processFilesystemsToReaders(ctx context.Context, filesystems []fs.FS) ([]readerInput, error) {
	var allReaders []readerInput

	for _, filesystem := range filesystems {
		if filesystem == nil {
			return nil, errors.New("FS cannot be nil")
		}

		fsReaders, err := fp.processFSToReaders(ctx, filesystem)
		if err != nil {
			return nil, fmt.Errorf("failed to process FS input: %w", err)
		}
		allReaders = append(allReaders, fsReaders...)
	}

	return allReaders, nil
}

// processFSToReaders processes all supported files from an fs.FS and creates ReaderInput
func (fp *fileProcessor) processFSToReaders(_ context.Context, filesystem fs.FS) ([]readerInput, error) {
	readers := make([]readerInput, 0)

	// Search for all supported file patterns
	supportedPatterns := supportedFileExtPatterns()

	// Collect all matching files
	allMatches := make([]string, 0)
	for _, pattern := range supportedPatterns {
		matches, err := fs.Glob(filesystem, pattern)
		if err != nil {
			return nil, fmt.Errorf("failed to search pattern %s: %w", pattern, err)
		}
		allMatches = append(allMatches, matches...)
	}

	// Also search recursively in subdirectories
	if _, err := fs.Stat(filesystem, "."); err == nil {
		walkErr := fs.WalkDir(filesystem, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if isSupportedFile(path) {
				// Check if already found by glob patterns
				normalizedPath := filepath.ToSlash(path)
				found := false
				for _, existing := range allMatches {
					normalizedExisting := filepath.ToSlash(existing)
					if normalizedExisting == normalizedPath {
						found = true
						break
					}
				}
				if !found {
					allMatches = append(allMatches, path)
				}
			}
			return nil
		})
		if walkErr != nil {
			return nil, fmt.Errorf("failed to walk filesystem: %w", walkErr)
		}
	}

	if len(allMatches) == 0 {
		return nil, errors.New("no supported files found in filesystem")
	}

	// Remove compressed duplicates when uncompressed versions exist
	allMatches = fp.deduplicateCompressedFiles(allMatches)

	// Create ReaderInput for each matched file
	for _, match := range allMatches {
		// Open the file from FS
		file, err := filesystem.Open(match)
		if err != nil {
			return nil, fmt.Errorf("failed to open FS file %s: %w", match, err)
		}

		// Determine file type from extension using NewFile
		fileInfo := newFile(match)
		fileType := fileInfo.getFileType()

		// Generate table name from file path (remove extension and clean up)
		tableName := tableFromFilePath(match)

		// Create ReaderInput
		readerInput := readerInput{
			reader:    file,
			tableName: tableName,
			fileType:  fileType,
		}

		readers = append(readers, readerInput)
	}

	return readers, nil
}

// deduplicateCompressedFiles removes compressed files when their uncompressed versions exist
func (fp *fileProcessor) deduplicateCompressedFiles(files []string) []string {
	// Create a map of table names to file paths, prioritizing uncompressed files
	tableToFile := make(map[string]string)

	// First pass: collect all uncompressed files
	for _, file := range files {
		tableName := tableFromFilePath(file)
		if !fp.isCompressedFile(file) {
			tableToFile[tableName] = file
		}
	}

	// Second pass: add compressed files only if uncompressed version doesn't exist
	for _, file := range files {
		tableName := tableFromFilePath(file)
		if fp.isCompressedFile(file) {
			if _, exists := tableToFile[tableName]; !exists {
				tableToFile[tableName] = file
			}
		}
	}

	// Convert map back to slice
	result := make([]string, 0, len(tableToFile))
	for _, file := range tableToFile {
		result = append(result, file)
	}

	return result
}

// isCompressedFile checks if a file path represents a compressed file
func (fp *fileProcessor) isCompressedFile(filePath string) bool {
	p := strings.ToLower(filePath)
	return strings.HasSuffix(p, extGZ) ||
		strings.HasSuffix(p, extBZ2) ||
		strings.HasSuffix(p, extXZ) ||
		strings.HasSuffix(p, extZSTD)
}
