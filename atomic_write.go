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
// directory and renames it over dest only when write and the close both succeed.
// When either fails, dest is left exactly as it was and the temporary file is
// removed.
//
// Why every write goes through this: opening dest with os.Create truncates it
// before a single byte has been produced, and a write can still fail after that.
// The ACH and Fedwire encoders validate while they encode, so a value the format
// cannot represent is rejected partway through; an encoder can also fail on a
// full disk or an unwritable row. For an in-place save dest is the caller's own
// source file, so a rejected write destroyed the data it was saving. Staging
// means a failure costs nothing.
//
// Why the writer and not the staged path is what write receives: the staged name
// carries a temporary suffix, so a format that reads anything out of its file
// name gets the wrong answer from it. Excel picks both its container format and
// its sheet name that way, which is how a staged XLSX dump came out unreadable.
//
// The temporary file is created in dest's directory so the rename stays within
// one filesystem, where it is atomic. An existing dest keeps its permissions; a
// new file is created 0644.
func writeFileAtomically(dest string, write func(io.Writer) error) error {
	dir := filepath.Dir(dest)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(dest)+".tmp*")
	if err != nil {
		return fmt.Errorf("%w: failed to create a temporary file next to %s: %s", ErrIOOperation, dest, err.Error())
	}
	tmpName := tmp.Name()
	// Remove the staged file unless the rename below claimed it; a no-op after a
	// successful rename.
	defer func() {
		_ = os.Remove(tmpName) //nolint:errcheck // Best-effort cleanup; the file is gone after a successful rename
	}()

	if err := write(tmp); err != nil {
		_ = tmp.Close() // The write error is the one to report
		return err
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("%w: failed to close the staged file for %s: %s", ErrIOOperation, dest, err.Error())
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
		// Nothing was in the way, so the fallback cannot help; report the original
		// failure.
		return err
	}
	return commitByCopy(staged, dest)
}

// commitByCopy writes the staged bytes over dest through the handle Windows will
// grant, after taking a copy of dest so a failure partway can put it back. It is
// the fallback for a destination that cannot be renamed at all: while another
// handle has the file open, Windows refuses both to rename over it and to rename
// it out of the way, and an in-place save overwrites exactly the file this
// package is reading from.
//
// This is not atomic — a reader watching during the copy can see a partial file
// — but it keeps the guarantee that matters here: a failure does not cost the
// data that was already there.
func commitByCopy(staged, dest string) error {
	backup, err := copyToBackup(dest)
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(backup) //nolint:errcheck // Best-effort cleanup of this package's own temporary file
	}()

	if copyErr := copyOnto(staged, dest); copyErr != nil {
		// Put back what was there. The restore is best effort: if it fails too, the
		// write error is still the one worth reporting.
		_ = copyOnto(backup, dest) //nolint:errcheck // Best-effort restore; copyErr is the failure to report
		return copyErr
	}
	return nil
}

// copyToBackup copies path to a temporary file beside it and returns that path.
func copyToBackup(path string) (string, error) {
	backup, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".bak*")
	if err != nil {
		return "", err
	}
	name := backup.Name()
	if err := backup.Close(); err != nil {
		_ = os.Remove(name) //nolint:errcheck // Best-effort cleanup of this package's own temporary file
		return "", err
	}
	if err := copyOnto(path, name); err != nil {
		_ = os.Remove(name) //nolint:errcheck // Best-effort cleanup of this package's own temporary file
		return "", err
	}
	return name, nil
}

// copyOnto replaces dest's contents with src's, truncating whatever was there.
func copyOnto(src, dest string) error {
	in, err := os.Open(src) //nolint:gosec // src is a file this package created or was given as the output
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, defaultOutputFileMode) //nolint:gosec // dest is the caller's chosen output
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
