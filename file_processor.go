package filesql

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// fileProcessor handles file-specific processing operations
type fileProcessor struct {
	validator *validator
	logger    Logger
}

// newFileProcessor creates a new file processor instance
func newFileProcessor() *fileProcessor {
	return &fileProcessor{
		validator: newValidator(),
		logger:    newNopLogger(),
	}
}

// setLogger sets the logger for the file processor
func (fp *fileProcessor) setLogger(logger Logger) {
	if logger != nil {
		fp.logger = logger
	}
}

// collectFilesFromPaths validates and collects all files from the given paths
func (fp *fileProcessor) collectFilesFromPaths(paths []string) ([]string, error) {
	fp.logger.Debug("collecting files from paths", "path_count", len(paths))
	var collectedPaths []string
	processedFiles := make(map[string]bool)

	for _, path := range paths {
		if err := fp.validator.validatePath(path); err != nil {
			fp.logger.Error("path validation failed", "path", path, "error", err)
			return nil, err
		}

		info, err := os.Stat(path)
		if err != nil {
			fp.logger.Error("failed to stat path", "path", path, "error", err)
			return nil, fmt.Errorf("%w: failed to stat path %s: %w", ErrIOOperation, path, err)
		}

		if info.IsDir() {
			fp.logger.Debug("collecting files from directory", "path", path)
			dirFiles, err := fp.collectFilesFromDirectory(path, processedFiles)
			if err != nil {
				return nil, err
			}
			fp.logger.Debug("collected files from directory", "path", path, "file_count", len(dirFiles))
			collectedPaths = append(collectedPaths, dirFiles...)
		} else {
			if err := fp.addSingleFile(path, processedFiles, &collectedPaths); err != nil {
				return nil, err
			}
		}
	}

	fp.logger.Info("file collection completed", "total_files", len(collectedPaths))
	return collectedPaths, nil
}

// collectFilesFromDirectory recursively collects all supported files from a directory
func (fp *fileProcessor) collectFilesFromDirectory(dirPath string, processedFiles map[string]bool) ([]string, error) {
	var collectedPaths []string

	// os.Stat, which is what said this path is a directory, follows a symbolic
	// link; filepath.WalkDir lstats its root, so a linked directory was visited
	// as the one unsupported entry the link itself is and the load failed with
	// "no supported files found in directory" for a directory full of them. The
	// root is resolved before the walk, and what is found under it is reported back
	// under the name the caller gave: that is the path an in-place save writes
	// to and the one an error message names. Links found inside the walk are
	// still not followed, which is what keeps a cycle from running forever.
	walkRoot := dirPath
	if resolved, resolveErr := filepath.EvalSymlinks(dirPath); resolveErr == nil {
		walkRoot = resolved
	}

	err := filepath.WalkDir(walkRoot, func(filePath string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !isSupportedFile(filePath) {
			return nil
		}

		if walkRoot != dirPath {
			rel, relErr := filepath.Rel(walkRoot, filePath)
			if relErr != nil {
				return fmt.Errorf("%w: failed to place %s under %s: %w", ErrIOOperation, filePath, dirPath, relErr)
			}
			filePath = filepath.Join(dirPath, rel)
		}

		absPath, err := filepath.Abs(filePath)
		if err != nil {
			return fmt.Errorf("%w: failed to get absolute path for %s: %w", ErrIOOperation, filePath, err)
		}

		if !processedFiles[absPath] {
			processedFiles[absPath] = true
			collectedPaths = append(collectedPaths, filePath)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("%w: failed to walk directory %s: %w", ErrIOOperation, dirPath, err)
	}

	return collectedPaths, nil
}

// addSingleFile validates and adds a single file to the collected paths
func (fp *fileProcessor) addSingleFile(filePath string, processedFiles map[string]bool, collectedPaths *[]string) error {
	if !isSupportedFile(filePath) {
		return fmt.Errorf("%w: %s", ErrUnsupportedFormat, filePath)
	}

	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("%w: failed to get absolute path for %s: %w", ErrIOOperation, filePath, err)
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
			return nil, fmt.Errorf("%w: filesystem cannot be nil", ErrNilInput)
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
			return nil, fmt.Errorf("%w: failed to search pattern %s: %w", ErrIOOperation, pattern, err)
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
			return nil, fmt.Errorf("%w: failed to walk filesystem: %w", ErrIOOperation, walkErr)
		}
	}

	if len(allMatches) == 0 {
		return nil, fmt.Errorf("%w: no supported files found in filesystem", ErrNoFiles)
	}

	// Remove compressed duplicates when uncompressed versions exist
	allMatches = fp.deduplicateCompressedFiles(allMatches)

	// Create ReaderInput for each matched file
	for _, match := range allMatches {
		// Open the file from FS
		file, err := filesystem.Open(match)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to open FS file %s: %w", ErrIOOperation, match, err)
		}

		// Determine format and codec from the extension. The file is handed over
		// still compressed, so the codec has to travel with it.
		fileInfo := newFile(match)
		fileType := fileInfo.getFileType()
		compression := NewCompressionFactory().DetectCompressionType(match)

		// Generate table name from file path (remove extension and clean up)
		tableName := sanitizeTableName(tableFromFilePath(match))

		// Create ReaderInput with closer so the file is released after streaming
		readerInput := readerInput{
			reader:      file,
			tableName:   tableName,
			fileType:    fileType,
			compression: compression,
			closer:      file,
			reopen: func() (io.Reader, func() error, error) {
				again, err := filesystem.Open(match)
				if err != nil {
					return nil, nil, fmt.Errorf("%w: failed to open FS file %s: %w", ErrIOOperation, match, err)
				}
				return again, again.Close, nil
			},
		}

		readers = append(readers, readerInput)
	}

	return readers, nil
}

// sourceIdentity is what makes two inputs the same source: the path with any
// compression suffix removed.
//
// It is the path, and deliberately not the table name. A table name comes from
// the base name alone, so "a/users.csv" and "b/users.csv" produce the same one
// while naming entirely different files. Keying deduplication on it dropped one
// of them without a word, and which one it dropped depended on Go's map
// iteration order — so a load could lose a different file on every run. Two
// inputs are the same source only when they are in the same place.
//
// Separators are normalized to "/" so a local path and an fs.FS path (always
// slash-separated) compare the same way on every platform.
//
// Case is significant, including on Windows, where the filesystem's is not.
// That errs toward keeping both: two paths that really are one file are then
// loaded twice and reported as a table collision, rather than one of them
// vanishing. Losing an input in silence is the failure this function exists to
// prevent, so the tie is broken away from it.
func sourceIdentity(filePath string) string {
	stripped := NewCompressionFactory().RemoveCompressionExtension(filePath)
	return path.Clean(filepath.ToSlash(stripped))
}

// deduplicateCompressedFiles drops an input that is another input's compressed
// twin, and an input listed twice, keeping the order the caller gave.
//
// Two things it must not do. It must not treat inputs from different places as
// the same file: a directory holding "users.csv" and another holding
// "users.csv.gz" are two datasets, and both belong in the load. And it must not
// decide the surviving order from a map, because everything downstream depends
// on it — which input a last-wins load leaves in place, which malformed file a
// failing load reports, and the order the collision check sees.
func (fp *fileProcessor) deduplicateCompressedFiles(files []string) []string {
	// The sources that arrived uncompressed. A compressed input matching one of
	// these is the same data behind a codec, so reading it would build the same
	// table twice from the same place.
	plain := make(map[string]struct{}, len(files))
	for _, file := range files {
		if !fp.isCompressedFile(file) {
			plain[sourceIdentity(file)] = struct{}{}
		}
	}

	result := make([]string, 0, len(files))
	emitted := make(map[string]struct{}, len(files))
	for _, file := range files {
		identity := sourceIdentity(file)
		if _, already := emitted[identity]; already {
			// The same source named twice — "./users.csv" and "users.csv", or a
			// path repeated outright. Loading it twice would build one table from
			// one file two times over.
			continue
		}
		if fp.isCompressedFile(file) {
			if _, uncompressed := plain[identity]; uncompressed {
				continue
			}
		}
		emitted[identity] = struct{}{}
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
		strings.HasSuffix(p, extZSTD) ||
		strings.HasSuffix(p, extZLIB) ||
		strings.HasSuffix(p, extSNAPPY) ||
		strings.HasSuffix(p, extS2) ||
		strings.HasSuffix(p, extLZ4)
}
