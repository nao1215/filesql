// Package filesql provides file-based SQL driver implementation.
package filesql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/nao1215/filesql/domain/model"
)

// DBBuilder is a builder for creating database connections from files and embedded filesystems.
// It provides a flexible way to configure input sources before creating a database connection.
// Use NewBuilder to create a new instance, then chain method calls to configure it.
//
// The typical usage pattern is:
//
//	builder := filesql.NewBuilder().AddPath("data.csv").AddFS(embeddedFS)
//	validatedBuilder, err := builder.Build(ctx)
//	if err != nil {
//		return err
//	}
//	db, err := validatedBuilder.Open(ctx)
//	defer db.Close()
//	defer validatedBuilder.Cleanup() // Clean up temporary files
type DBBuilder struct {
	// paths contains regular file paths
	paths []string
	// filesystems contains fs.FS instances
	filesystems []fs.FS
	// collectedPaths contains all paths after Build validation
	collectedPaths []string
	// tempFiles tracks temporary files created for cleanup
	tempFiles []string
	// autoSaveConfig contains auto-save settings
	autoSaveConfig *AutoSaveConfig
}

// AutoSaveTiming specifies when automatic saving should occur
type AutoSaveTiming int

const (
	// AutoSaveOnClose saves data when db.Close() is called (default)
	AutoSaveOnClose AutoSaveTiming = iota
	// AutoSaveOnCommit saves data when transaction is committed
	AutoSaveOnCommit
)

// AutoSaveConfig holds configuration for automatic saving
type AutoSaveConfig struct {
	// Enabled indicates whether auto-save is enabled
	Enabled bool
	// Timing specifies when to save (on close or on commit)
	Timing AutoSaveTiming
	// OutputDir is the directory where files will be saved (overwrites original files)
	OutputDir string
	// Options contains dump options for formatting
	Options DumpOptions
}

// NewBuilder creates a new database builder for configuring file inputs.
// The returned builder can be used to add file paths and embedded filesystems
// before building and opening a database connection.
//
// Example:
//
//	builder := filesql.NewBuilder()
//	builder.AddPath("users.csv")
//	builder.AddPath("orders.tsv")
//	validatedBuilder, err := builder.Build(ctx)
//	if err != nil {
//		return err
//	}
//	db, err := validatedBuilder.Open(ctx)
//	if err != nil {
//		return err
//	}
//	defer db.Close()
//	defer validatedBuilder.Cleanup()
func NewBuilder() *DBBuilder {
	return &DBBuilder{
		paths:          make([]string, 0),
		filesystems:    make([]fs.FS, 0),
		collectedPaths: make([]string, 0),
		tempFiles:      make([]string, 0),
		autoSaveConfig: nil, // Default: no auto-save
	}
}

// AddPath adds a regular file or directory path to the builder.
// The path can be:
// - A single file with supported extensions (.csv, .tsv, .ltsv, and their compressed variants)
// - A directory path (all supported files will be loaded recursively)
//
// Supported file extensions: .csv, .tsv, .ltsv
// Supported compression: .gz, .bz2, .xz, .zst
//
// Returns the builder for method chaining.
func (b *DBBuilder) AddPath(path string) *DBBuilder {
	b.paths = append(b.paths, path)
	return b
}

// AddPaths adds multiple regular file or directory paths to the builder.
// This is a convenience method for adding multiple paths at once.
// Each path can be a file or directory, following the same rules as AddPath.
//
// Returns the builder for method chaining.
func (b *DBBuilder) AddPaths(paths ...string) *DBBuilder {
	b.paths = append(b.paths, paths...)
	return b
}

// AddFS adds all supported files from an fs.FS filesystem to the builder.
// This method is particularly useful for embedded filesystems using go:embed.
// It automatically searches for all supported file types recursively:
// - Base formats: .csv, .tsv, .ltsv
// - Compressed variants: .gz, .bz2, .xz, .zst
//
// The filesystem will be processed during Build(), where matching files will be
// copied to temporary files for database access. Use Cleanup() to remove these
// temporary files when done.
//
// Example with embedded filesystem:
//
//	//go:embed data/*.csv data/*.tsv
//	var dataFS embed.FS
//
//	subFS, _ := fs.Sub(dataFS, "data")
//	builder := filesql.NewBuilder().AddFS(subFS)
//
// Returns the builder for method chaining.
func (b *DBBuilder) AddFS(filesystem fs.FS) *DBBuilder {
	b.filesystems = append(b.filesystems, filesystem)
	return b
}

// EnableAutoSave enables automatic saving when the database connection is closed.
// Files will be overwritten in their original locations with the current database content.
// This uses the same functionality as DumpDatabase() internally.
//
// The outputDir parameter specifies where to save the files. If empty, files will be
// saved to their original locations (overwrite mode).
//
// Example:
//
//	builder := filesql.NewBuilder().
//		AddPath("data.csv").
//		EnableAutoSave("./backup", NewDumpOptions()) // Save to backup directory on close
//
// Returns the builder for method chaining.
func (b *DBBuilder) EnableAutoSave(outputDir string, options ...DumpOptions) *DBBuilder {
	opts := NewDumpOptions()
	if len(options) > 0 {
		opts = options[0]
	}

	b.autoSaveConfig = &AutoSaveConfig{
		Enabled:   true,
		Timing:    AutoSaveOnClose, // Default to close-time saving
		OutputDir: outputDir,
		Options:   opts,
	}
	return b
}

// EnableAutoSaveOnCommit enables automatic saving when transactions are committed.
// Files will be overwritten in their original locations with the current database content.
// This provides more frequent saves but may impact performance for frequent commits.
//
// The outputDir parameter specifies where to save the files. If empty, files will be
// saved to their original locations (overwrite mode).
//
// Example:
//
//	builder := filesql.NewBuilder().
//		AddPath("data.csv").
//		EnableAutoSaveOnCommit("", NewDumpOptions()) // Overwrite original on each commit
//
// Returns the builder for method chaining.
func (b *DBBuilder) EnableAutoSaveOnCommit(outputDir string, options ...DumpOptions) *DBBuilder {
	opts := NewDumpOptions()
	if len(options) > 0 {
		opts = options[0]
	}

	b.autoSaveConfig = &AutoSaveConfig{
		Enabled:   true,
		Timing:    AutoSaveOnCommit,
		OutputDir: outputDir,
		Options:   opts,
	}
	return b
}

// DisableAutoSave disables automatic saving (default behavior).
// Returns the builder for method chaining.
func (b *DBBuilder) DisableAutoSave() *DBBuilder {
	b.autoSaveConfig = nil
	return b
}

// Build validates all configured inputs and prepares the builder for opening a database.
// This method must be called before Open(). It performs the following operations:
//
// 1. Validates that at least one input source is configured
// 2. Checks existence and format of all file paths
// 3. Processes embedded filesystems by copying files to temporary locations
// 4. Validates that all files have supported extensions
//
// After successful validation, the builder is ready to create database connections
// with Open(). The context is used for file operations and can be used for cancellation.
//
// Returns the same builder instance for method chaining, or an error if validation fails.
func (b *DBBuilder) Build(ctx context.Context) (*DBBuilder, error) {
	// Validate that we have at least one input
	if len(b.paths) == 0 && len(b.filesystems) == 0 {
		return nil, errors.New("at least one path must be provided")
	}

	// Reset collected paths
	b.collectedPaths = make([]string, 0)

	// Validate and collect regular paths
	for _, path := range b.paths {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("failed to load file: path does not exist: %s", path)
			}
			return nil, fmt.Errorf("failed to stat path %s: %w", path, err)
		}

		// If it's a directory, we accept it (will be processed later)
		// If it's a file, check if it has a supported extension
		if !info.IsDir() {
			if !model.IsSupportedFile(path) {
				return nil, fmt.Errorf("unsupported file type: %s", path)
			}
		}
		// Add to collected paths
		b.collectedPaths = append(b.collectedPaths, path)
	}

	// Process and validate FS inputs, converting to temporary files
	for _, filesystem := range b.filesystems {
		if filesystem == nil {
			return nil, errors.New("FS cannot be nil")
		}

		// Process FS and get temporary file paths
		paths, err := b.processFSInput(ctx, filesystem)
		if err != nil {
			return nil, fmt.Errorf("failed to process FS input: %w", err)
		}
		b.collectedPaths = append(b.collectedPaths, paths...)
	}

	if len(b.collectedPaths) == 0 {
		return nil, errors.New("no valid input files found")
	}

	return b, nil
}

// Open creates and returns a database connection using the configured and validated inputs.
// This method can only be called after Build() has been successfully executed.
// It creates an in-memory SQLite database and loads all configured files as tables.
//
// Table names are derived from file names without extensions:
// - "users.csv" becomes table "users"
// - "data.tsv.gz" becomes table "data"
//
// The returned database connection supports the full SQLite3 SQL syntax.
// The caller is responsible for closing the connection and calling Cleanup()
// to remove any temporary files created from embedded filesystems.
//
// Returns a *sql.DB connection or an error if the database cannot be created.
func (b *DBBuilder) Open(ctx context.Context) (*sql.DB, error) {
	// Use collected paths from Build
	if len(b.collectedPaths) == 0 {
		return nil, errors.New("no valid input files found, did you call Build()?")
	}

	// Create DSN with all collected paths and auto-save config
	dsn := strings.Join(b.collectedPaths, ";")

	// Append auto-save configuration to DSN if enabled
	if b.autoSaveConfig != nil && b.autoSaveConfig.Enabled {
		configJSON, err := json.Marshal(b.autoSaveConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize auto-save config: %w", err)
		}
		configEncoded := base64.StdEncoding.EncodeToString(configJSON)
		dsn += "?autosave=" + configEncoded
	}

	// Open database connection
	db, err := sql.Open(DriverName, dsn)
	if err != nil {
		cleanupErr := b.cleanup()
		if cleanupErr != nil {
			// Join the original error with cleanup error
			return nil, errors.Join(err, fmt.Errorf("cleanup failed: %w", cleanupErr))
		}
		return nil, err
	}

	// Validate connection
	if err := db.PingContext(ctx); err != nil {
		closeErr := db.Close()
		cleanupErr := b.cleanup()

		// Collect all errors that occurred during error handling
		var allErrors []error
		allErrors = append(allErrors, err) // Original error
		if closeErr != nil {
			allErrors = append(allErrors, fmt.Errorf("failed to close database: %w", closeErr))
		}
		if cleanupErr != nil {
			allErrors = append(allErrors, fmt.Errorf("cleanup failed: %w", cleanupErr))
		}

		return nil, errors.Join(allErrors...)
	}
	return db, nil
}

// processFSInput processes all supported files from an fs.FS
func (b *DBBuilder) processFSInput(ctx context.Context, filesystem fs.FS) ([]string, error) {
	paths := make([]string, 0)

	// Search for all supported file patterns
	supportedPatterns := model.SupportedFileExtPatterns()

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
	err := fs.WalkDir(filesystem, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if model.IsSupportedFile(path) {
			// Check if already found by glob patterns
			// Use path.Clean to normalize paths for comparison (fs.FS uses forward slashes)
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
	if err != nil {
		return nil, fmt.Errorf("failed to walk filesystem: %w", err)
	}

	if len(allMatches) == 0 {
		return nil, errors.New("no supported files found in filesystem")
	}

	// Copy all matched files to temporary files
	for _, match := range allMatches {
		tempPath, err := b.copyFSToTemp(ctx, filesystem, match)
		if err != nil {
			return nil, fmt.Errorf("failed to copy file %s: %w", match, err)
		}
		paths = append(paths, tempPath)
	}

	return paths, nil
}

// copyFSToTemp copies a file from fs.FS to a temporary file
func (b *DBBuilder) copyFSToTemp(_ context.Context, filesystem fs.FS, path string) (string, error) {
	// Open the file from FS
	file, err := filesystem.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open FS file: %w", err)
	}
	defer file.Close()

	// Get file extension for proper temp file naming
	ext := filepath.Ext(path)

	// Create temporary file
	tempFile, err := os.CreateTemp("", "filesql-*"+ext)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempFile.Close()

	// Copy content
	if _, err := io.Copy(tempFile, file); err != nil {
		removeErr := os.Remove(tempFile.Name())
		if removeErr != nil {
			// Join copy error with cleanup error
			return "", errors.Join(
				fmt.Errorf("failed to copy content: %w", err),
				fmt.Errorf("failed to cleanup temp file: %w", removeErr),
			)
		}
		return "", fmt.Errorf("failed to copy content: %w", err)
	}

	// Track temp file for cleanup
	b.tempFiles = append(b.tempFiles, tempFile.Name())

	return tempFile.Name(), nil
}

// cleanup removes temporary files and returns any errors
func (b *DBBuilder) cleanup() error {
	var errs []error
	for _, path := range b.tempFiles {
		if err := os.Remove(path); err != nil {
			errs = append(errs, fmt.Errorf("failed to remove temp file %s: %w", path, err))
		}
	}
	b.tempFiles = nil

	// Join all errors if any occurred
	return errors.Join(errs...)
}

// Cleanup removes all temporary files created during filesystem processing.
// This method should be called when you're done with the database to clean up
// any temporary files that were created from embedded filesystems (fs.FS).
//
// It's safe to call this multiple times - subsequent calls will have no effect.
// The method returns an error if any temporary files could not be removed.
// Multiple removal errors are joined together using errors.Join.
//
// Example usage:
//
//	builder, err := filesql.NewBuilder().AddFS(embeddedFS).Build(ctx)
//	if err != nil {
//		return err
//	}
//	defer builder.Cleanup()
//
//	db, err := builder.Open(ctx)
//	if err != nil {
//		return err
//	}
//	defer db.Close()
func (b *DBBuilder) Cleanup() error {
	return b.cleanup()
}
