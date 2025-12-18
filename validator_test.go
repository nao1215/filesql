package filesql

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidator_validatePath(t *testing.T) {
	t.Parallel()

	v := newValidator()

	t.Run("empty path returns ErrEmptyPath", func(t *testing.T) {
		t.Parallel()
		err := v.validatePath("")
		assert.True(t, errors.Is(err, ErrEmptyPath))
	})

	t.Run("whitespace only path returns ErrEmptyPath", func(t *testing.T) {
		t.Parallel()
		err := v.validatePath("   ")
		assert.True(t, errors.Is(err, ErrEmptyPath))
	})

	t.Run("non-existent path returns ErrFileNotFound", func(t *testing.T) {
		t.Parallel()
		err := v.validatePath("/nonexistent/path/file.csv")
		assert.True(t, errors.Is(err, ErrFileNotFound))
	})

	t.Run("valid directory path succeeds", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		err := v.validatePath(tempDir)
		assert.NoError(t, err)
	})

	t.Run("valid CSV file path succeeds", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		csvFile := filepath.Join(tempDir, "test.csv")
		require.NoError(t, os.WriteFile(csvFile, []byte("a,b\n1,2"), 0600))

		err := v.validatePath(csvFile)
		assert.NoError(t, err)
	})

	t.Run("unsupported file format returns ErrUnsupportedFormat", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		txtFile := filepath.Join(tempDir, "test.txt")
		require.NoError(t, os.WriteFile(txtFile, []byte("test"), 0600))

		err := v.validatePath(txtFile)
		assert.True(t, errors.Is(err, ErrUnsupportedFormat))
	})
}

func TestValidator_validateReader(t *testing.T) {
	t.Parallel()

	v := newValidator()

	t.Run("nil reader returns ErrNilInput", func(t *testing.T) {
		t.Parallel()
		err := v.validateReader(nil, "table", FileTypeCSV)
		assert.True(t, errors.Is(err, ErrNilInput))
	})

	t.Run("empty table name returns ErrInvalidData", func(t *testing.T) {
		t.Parallel()
		reader := strings.NewReader("a,b\n1,2")
		err := v.validateReader(reader, "", FileTypeCSV)
		assert.True(t, errors.Is(err, ErrInvalidData))
	})

	t.Run("unsupported file type returns ErrUnsupportedFormat", func(t *testing.T) {
		t.Parallel()
		reader := strings.NewReader("a,b\n1,2")
		err := v.validateReader(reader, "table", FileTypeUnsupported)
		assert.True(t, errors.Is(err, ErrUnsupportedFormat))
	})

	t.Run("empty strings.Reader for CSV returns ErrEmptyData", func(t *testing.T) {
		t.Parallel()
		reader := strings.NewReader("")
		err := v.validateReader(reader, "table", FileTypeCSV)
		assert.True(t, errors.Is(err, ErrEmptyData))
		assert.Contains(t, err.Error(), "empty CSV data")
	})

	t.Run("empty strings.Reader for TSV returns ErrEmptyData", func(t *testing.T) {
		t.Parallel()
		reader := strings.NewReader("")
		err := v.validateReader(reader, "table", FileTypeTSV)
		assert.True(t, errors.Is(err, ErrEmptyData))
		assert.Contains(t, err.Error(), "empty TSV data")
	})

	t.Run("empty strings.Reader for LTSV returns ErrEmptyData", func(t *testing.T) {
		t.Parallel()
		reader := strings.NewReader("")
		err := v.validateReader(reader, "table", FileTypeLTSV)
		assert.True(t, errors.Is(err, ErrEmptyData))
		assert.Contains(t, err.Error(), "empty LTSV data")
	})

	t.Run("empty strings.Reader for other type returns ErrEmptyData", func(t *testing.T) {
		t.Parallel()
		reader := strings.NewReader("")
		err := v.validateReader(reader, "table", FileTypeParquet)
		assert.True(t, errors.Is(err, ErrEmptyData))
		assert.Contains(t, err.Error(), "reader contains no data")
	})

	t.Run("valid reader succeeds", func(t *testing.T) {
		t.Parallel()
		reader := strings.NewReader("a,b\n1,2")
		err := v.validateReader(reader, "table", FileTypeCSV)
		assert.NoError(t, err)
	})
}

func TestValidator_validateAutoSaveConfig(t *testing.T) {
	t.Parallel()

	v := newValidator()

	t.Run("nil config succeeds", func(t *testing.T) {
		t.Parallel()
		err := v.validateAutoSaveConfig(nil)
		assert.NoError(t, err)
	})

	t.Run("disabled config succeeds", func(t *testing.T) {
		t.Parallel()
		config := &autoSaveConfig{enabled: false}
		err := v.validateAutoSaveConfig(config)
		assert.NoError(t, err)
	})

	t.Run("enabled config with empty outputDir succeeds", func(t *testing.T) {
		t.Parallel()
		config := &autoSaveConfig{enabled: true, outputDir: ""}
		err := v.validateAutoSaveConfig(config)
		assert.NoError(t, err)
	})

	t.Run("enabled config with existing directory succeeds", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		config := &autoSaveConfig{enabled: true, outputDir: tempDir}
		err := v.validateAutoSaveConfig(config)
		assert.NoError(t, err)
	})

	t.Run("enabled config with non-directory path fails", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		tempFile := filepath.Join(tempDir, "file.txt")
		require.NoError(t, os.WriteFile(tempFile, []byte("test"), 0600))

		config := &autoSaveConfig{enabled: true, outputDir: tempFile}
		err := v.validateAutoSaveConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not a directory")
	})
}

func TestValidator_validateOutputDirectory(t *testing.T) {
	t.Parallel()

	v := newValidator()

	t.Run("empty outputDir succeeds", func(t *testing.T) {
		t.Parallel()
		err := v.validateOutputDirectory("")
		assert.NoError(t, err)
	})

	t.Run("existing directory succeeds", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		err := v.validateOutputDirectory(tempDir)
		assert.NoError(t, err)
	})

	t.Run("non-existent directory succeeds (will be created later)", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		newDir := filepath.Join(tempDir, "newdir")
		err := v.validateOutputDirectory(newDir)
		assert.NoError(t, err)
	})

	t.Run("path is file not directory fails", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		tempFile := filepath.Join(tempDir, "file.txt")
		require.NoError(t, os.WriteFile(tempFile, []byte("test"), 0600))

		err := v.validateOutputDirectory(tempFile)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not a directory")
	})
}

func TestValidator_validateFinalState(t *testing.T) {
	t.Parallel()

	v := newValidator()

	t.Run("both empty returns ErrNoFiles", func(t *testing.T) {
		t.Parallel()
		err := v.validateFinalState(nil, nil, nil)
		assert.True(t, errors.Is(err, ErrNoFiles))
	})

	t.Run("collectedPaths not empty succeeds", func(t *testing.T) {
		t.Parallel()
		err := v.validateFinalState([]string{"/path/to/file.csv"}, nil, nil)
		assert.NoError(t, err)
	})

	t.Run("readers not empty succeeds", func(t *testing.T) {
		t.Parallel()
		readers := []readerInput{{tableName: "test"}}
		err := v.validateFinalState(nil, readers, nil)
		assert.NoError(t, err)
	})

	t.Run("directory path returns appropriate error message", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		err := v.validateFinalState(nil, nil, []string{tempDir})
		assert.True(t, errors.Is(err, ErrNoFiles))
		assert.Contains(t, err.Error(), "no supported files found in directory")
	})

	t.Run("file path returns appropriate error message", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		tempFile := filepath.Join(tempDir, "file.csv")
		require.NoError(t, os.WriteFile(tempFile, []byte("a,b\n1,2"), 0600))

		err := v.validateFinalState(nil, nil, []string{tempFile})
		assert.True(t, errors.Is(err, ErrNoFiles))
		assert.Contains(t, err.Error(), "no valid input files found")
	})
}

func TestValidator_validateInputsAvailable(t *testing.T) {
	t.Parallel()

	v := newValidator()

	t.Run("both empty returns ErrNoFiles", func(t *testing.T) {
		t.Parallel()
		err := v.validateInputsAvailable(nil, nil)
		assert.True(t, errors.Is(err, ErrNoFiles))
		assert.Contains(t, err.Error(), "did you call Build()?")
	})

	t.Run("collectedPaths not empty succeeds", func(t *testing.T) {
		t.Parallel()
		err := v.validateInputsAvailable([]string{"/path/to/file.csv"}, nil)
		assert.NoError(t, err)
	})

	t.Run("readers not empty succeeds", func(t *testing.T) {
		t.Parallel()
		readers := []readerInput{{tableName: "test"}}
		err := v.validateInputsAvailable(nil, readers)
		assert.NoError(t, err)
	})
}
