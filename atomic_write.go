package filesql

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// defaultOutputFileMode is the permission given to a file this package creates.
// An existing destination keeps its own mode instead.
const defaultOutputFileMode os.FileMode = 0o644

// writeFileAtomically hands write a writer for a temporary file in dest's
// directory and renames it over dest only after write and the close both
// succeed. See writeFileAtomicallyAtPath for why every write goes through this.
func writeFileAtomically(dest string, write func(io.Writer) error) error {
	return writeFileAtomicallyAtPath(dest, func(path string) error {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, defaultOutputFileMode) //nolint:gosec // path is the staged file this package just created
		if err != nil {
			return fmt.Errorf("%w: failed to open the staged file for %s: %s", ErrIOOperation, dest, err.Error())
		}
		if err := write(f); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("%w: failed to close the staged file for %s: %s", ErrIOOperation, dest, err.Error())
		}
		return nil
	})
}

// writeFileAtomicallyAtPath reserves a temporary path in dest's directory, hands
// it to write, and renames it over dest only when write returns nil. When write
// fails, dest is left exactly as it was and the temporary file is removed. It is
// the form for writers that need a path of their own (Parquet and XLSX open the
// output themselves); writeFileAtomically is the form for writers that take an
// io.Writer.
//
// Why every write goes through one of these: opening dest with os.Create
// truncates it before a single byte has been produced, and a write can still
// fail after that. The ACH and Fedwire encoders validate while they encode, so a
// value the format cannot represent is rejected partway through; an encoder can
// also fail on a full disk or an unwritable row. For an in-place save dest is
// the caller's own source file, so a rejected write destroyed the data it was
// saving. Staging means a failure costs nothing.
//
// The temporary file is created in dest's directory so the rename stays within
// one filesystem, where it is atomic. An existing dest keeps its permissions; a
// new file is created 0644.
func writeFileAtomicallyAtPath(dest string, write func(path string) error) error {
	dir := filepath.Dir(dest)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(dest)+".tmp*")
	if err != nil {
		return fmt.Errorf("%w: failed to create a temporary file next to %s: %s", ErrIOOperation, dest, err.Error())
	}
	tmpName := tmp.Name()
	// The handle only reserves the name; write opens the path itself.
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName) //nolint:errcheck // Best-effort cleanup of a file this package just created
		return fmt.Errorf("%w: failed to close the temporary file for %s: %s", ErrIOOperation, dest, err.Error())
	}
	// Remove the staged file unless the rename below claimed it; a no-op after a
	// successful rename.
	defer func() {
		_ = os.Remove(tmpName) //nolint:errcheck // Best-effort cleanup; the file is gone after a successful rename
	}()

	if err := write(tmpName); err != nil {
		return err
	}

	mode := defaultOutputFileMode
	if info, statErr := os.Stat(dest); statErr == nil {
		mode = info.Mode().Perm()
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("%w: failed to inspect %s: %s", ErrIOOperation, dest, statErr.Error())
	}
	if err := os.Chmod(tmpName, mode); err != nil {
		return fmt.Errorf("%w: failed to set permissions on the temporary file for %s: %s", ErrIOOperation, dest, err.Error())
	}

	if err := commitStagedFile(tmpName, dest); err != nil {
		return fmt.Errorf("%w: failed to replace %s: %s", ErrIOOperation, dest, err.Error())
	}
	return nil
}

// commitStagedFile moves the staged file onto dest.
//
// A plain rename is the goal: it is atomic, so a reader sees either the old file
// or the new one. Windows refuses to rename over a destination another handle
// still has open, which is exactly a save that overwrites a file this package is
// streaming from. When that happens the destination is renamed out of the way
// first — moving an open file is allowed where replacing one is not — and put
// back if the second rename fails, so the destination is never left missing or
// half-written.
func commitStagedFile(staged, dest string) error {
	err := os.Rename(staged, dest)
	if err == nil {
		return nil
	}
	if _, statErr := os.Stat(dest); statErr != nil {
		// Nothing was in the way, so moving something aside cannot help; report
		// the original failure.
		return err
	}
	return commitByMovingAside(staged, dest)
}

// commitByMovingAside renames dest out of the way, renames the staged file into
// its place, and discards what it moved aside. If the second rename fails, dest
// is put back exactly as it was, so a refused commit costs nothing. It is the
// fallback for a platform that will not rename over dest; see commitStagedFile.
func commitByMovingAside(staged, dest string) error {
	aside := dest + ".filesql-replaced"
	// Clear any leftover from an interrupted commit so the move below is not
	// blocked by it.
	_ = os.Remove(aside) //nolint:errcheck // Best-effort; the rename below reports it if it still matters
	if err := os.Rename(dest, aside); err != nil {
		return err
	}
	if err := os.Rename(staged, dest); err != nil {
		_ = os.Rename(aside, dest) //nolint:errcheck // Best-effort restore; err is the failure to report
		return err
	}
	_ = os.Remove(aside) //nolint:errcheck // Best-effort cleanup of the replaced file
	return nil
}
