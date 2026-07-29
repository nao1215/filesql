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
// A rename is the goal: it is atomic, so a reader either sees the old file or
// the new one. It is not always available. Windows refuses to rename over a
// destination another handle still has open, which is exactly the case for a
// save that overwrites a file this package is streaming from, and a rename
// across filesystems fails everywhere. When the rename is refused, the staged
// bytes are copied over dest instead. That still keeps the guarantee the staging
// exists for — the destination is not touched until the data is complete and
// valid — and gives up only atomicity for a reader watching during the copy.
func commitStagedFile(staged, dest string) error {
	if err := os.Rename(staged, dest); err == nil {
		return nil
	}
	return copyStagedOnto(staged, dest)
}

// copyStagedOnto writes the staged file's bytes over dest, truncating whatever
// was there. It is the fallback for a rename the platform refuses; see
// commitStagedFile.
func copyStagedOnto(staged, dest string) error {
	src, err := os.Open(staged) //nolint:gosec // staged is the file this package just created
	if err != nil {
		return err
	}
	defer src.Close()

	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, defaultOutputFileMode) //nolint:gosec // dest is the caller's chosen output
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, src); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
