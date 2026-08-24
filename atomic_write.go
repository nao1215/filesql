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
func writeFileAtomically(dest string, write func(io.Writer) error) (err error) {
	dir := filepath.Dir(dest)
	tmp, err := createTempBeside(dir, filepath.Base(dest), stagedSuffix)
	if err != nil {
		return fmt.Errorf("%w: failed to create a temporary file next to %s: %w", ErrIOOperation, dest, err)
	}
	tmpName := tmp.Name()
	// Remove the staged file unless the rename below claimed it; a no-op after a
	// successful rename.
	defer func() {
		// A staged file left behind is a leftover in the user's directory, so
		// its removal failure is reported rather than dropped. A successful
		// rename already consumed it, which is not a failure.
		if removeErr := os.Remove(tmpName); removeErr != nil && !os.IsNotExist(removeErr) {
			err = joinCleanup(err, removeErr, "remove the staged file "+tmpName)
		}
	}()

	if err := write(tmp); err != nil {
		_ = tmp.Close() // The write error is the one to report
		return err
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("%w: failed to close the staged file for %s: %w", ErrIOOperation, dest, err)
	}

	mode := defaultOutputFileMode
	if info, statErr := os.Stat(dest); statErr == nil {
		mode = info.Mode().Perm()
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("%w: failed to inspect %s: %w", ErrIOOperation, dest, statErr)
	}
	if err := os.Chmod(tmpName, mode); err != nil {
		return fmt.Errorf("%w: failed to set permissions on the temporary file for %s: %w", ErrIOOperation, dest, err)
	}

	if err := commitStagedFile(tmpName, dest); err != nil {
		return fmt.Errorf("%w: failed to replace %s: %w", ErrIOOperation, dest, err)
	}
	return nil
}

// stagedSuffix and backupSuffix name what a temporary file beside a destination
// is for, so one left behind by an interrupted save can be told apart.
const (
	stagedSuffix = ".tmp"
	backupSuffix = ".bak"
)

// createTempBeside creates a temporary file in dir, named after base where the
// name fits, so a file left behind says which destination it belongs to.
//
// The descriptive name is up to fifteen bytes longer than base, which is more
// than the filesystem accepts when base is itself close to the limit: a file
// this package could load and query could not be saved, because the name chosen
// for the staging did not fit even though the caller's did. No portable constant
// gives that limit, so the descriptive name is tried first and a short fixed one
// takes over when it is refused, which needs no length calculation.
func createTempBeside(dir, base, suffix string) (*os.File, error) {
	if file, err := os.CreateTemp(dir, "."+base+suffix+"*"); err == nil {
		return file, nil
	}
	return os.CreateTemp(dir, ".filesql"+suffix+"*")
}

// commitStagedFile moves the staged file onto dest.
//
// A plain rename is the goal: it is atomic, so a reader sees either the old file
// or the new one. Windows refuses to rename over a destination another handle
// still has open, which is exactly a save that overwrites a file this package is
// streaming from. When that happens the bytes are copied over the destination
// instead, through the handle Windows will grant. That copy is not atomic, so
// the guarantee drops from "a reader sees one file or the other" to "a failure
// does not cost the data that was already there"; commitByCopy is where the
// second one is kept.
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
	backup, err := createTempBeside(filepath.Dir(path), filepath.Base(path), backupSuffix)
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
