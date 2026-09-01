// Package atomicwrite stages a file's new contents beside it and puts them in
// place only once the whole write has succeeded.
//
// Every write in this module goes through it, because opening a destination
// with os.Create truncates it before a single byte has been produced and a
// write can still fail after that. The ACH and Fedwire encoders validate while
// they encode, so a value the format cannot represent is rejected partway
// through; the TSV writer refuses a value holding a tab or a newline for the
// same reason, since TSV has no quoting; and any encoder can fail on a full
// disk. For an in-place save the destination is the caller's own source file,
// so a rejected write destroyed the data it was saving. Staging means a failure
// costs nothing.
package atomicwrite

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// DefaultFileMode is the permission given to a file this package creates.
// An existing destination keeps its own mode instead.
const DefaultFileMode os.FileMode = 0o644

// Options say how the calling package words a failure of the staging itself.
// Both fields are optional: a zero Options reports plain errors, which is what
// a caller with no sentinel of its own would get. No caller takes that path
// today.
type Options struct {
	// FailIO words a failure of the staging machinery. what is a formatted
	// phrase naming the step ("failed to replace /tmp/out.csv"); err is what
	// the operating system reported. The caller's own write error never
	// reaches it, so a package that marks I/O failures with a sentinel does
	// not put that sentinel on an error the encoder produced.
	FailIO func(what string, err error) error

	// FailCleanup attaches a failure to remove the staged file to the error
	// the write is already returning. primary may be nil, which is the case
	// where the write succeeded and only the cleanup did not.
	FailCleanup func(primary error, what string, err error) error
}

func (o Options) failIO(what string, err error) error {
	if o.FailIO != nil {
		return o.FailIO(what, err)
	}
	return fmt.Errorf("%s: %w", what, err)
}

func (o Options) failCleanup(primary error, what string, err error) error {
	if err == nil {
		return primary
	}
	if o.FailCleanup != nil {
		return o.FailCleanup(primary, what, err)
	}
	return errors.Join(primary, fmt.Errorf("%s: %w", what, err))
}

// Write hands write a writer for a temporary file in dest's directory and
// renames it over dest only when write and the close both succeed. When either
// fails, dest is left exactly as it was and the temporary file is removed.
//
// It is Stage followed by Commit, which is the whole of what a single file
// needs. A caller replacing several files together stages them all first.
func Write(dest string, write func(io.Writer) error, opt Options) error {
	staged, err := Stage(dest, write, opt)
	if err != nil {
		return err
	}
	return staged.Commit()
}

// Staged is a destination's new contents, written and waiting beside it. It is
// what lets a group of files be produced before any of them is put in place, so
// a write refused partway through a set costs what it costs for one file:
// nothing.
//
// Either Commit or Discard has to be called. Until one of them is, the staged
// file sits in the destination's directory.
type Staged struct {
	// dest is the path the caller named, which is what its errors say.
	dest string
	// target is the file to replace, which is dest with its links followed.
	target string
	// tmp is the staged file beside target.
	tmp string
	opt Options
}

// Stage writes dest's new contents into a temporary file beside it and stops
// there. Nothing at dest has been touched when Stage returns.
//
// Why the writer and not the staged path is what write receives: the staged
// name carries a temporary suffix, so a format that reads anything out of its
// file name gets the wrong answer from it. Excel picks both its container
// format and its sheet name that way, which is how a staged XLSX dump came out
// unreadable.
//
// The temporary file is created in dest's directory so the rename stays within
// one filesystem, where it is atomic. An existing dest keeps its permissions; a
// new file is created with DefaultFileMode.
//
// A dest that is a symbolic link names the file to replace rather than being
// it, so the staging and the rename both move to the file it points at and the
// link stays a link.
func Stage(dest string, write func(io.Writer) error, opt Options) (_ *Staged, err error) {
	// The caller's own dest stays in the error wording, which is the path they
	// named; only the file being replaced follows the link.
	target := resolveLinks(dest)
	dir := filepath.Dir(target)
	tmp, err := createTempBeside(dir, filepath.Base(target), stagedSuffix)
	if err != nil {
		return nil, opt.failIO("failed to create a temporary file next to "+dest, err)
	}
	staged := &Staged{dest: dest, target: target, tmp: tmp.Name(), opt: opt}
	// Remove the staged file for any failure below; a caller that gets a *Staged
	// back owns it from then on.
	defer func() {
		if err != nil {
			err = staged.cleanup(err)
		}
	}()

	if err := write(tmp); err != nil {
		_ = tmp.Close() // The write error is the one to report
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, opt.failIO("failed to close the staged file for "+dest, err)
	}

	mode := DefaultFileMode
	if info, statErr := os.Stat(target); statErr == nil {
		// A write replaces a file and nothing else. Renaming onto a named pipe,
		// a socket or a device turns it into a regular file with nothing said,
		// which is not what a caller asking for a file at that path meant and
		// cannot be undone; a directory in the way fails from inside the backup
		// step instead, reporting the name of a file this package invented. The
		// stat is here anyway, for the permissions to carry over.
		if !info.Mode().IsRegular() {
			return nil, opt.failIO("cannot write "+dest,
				fmt.Errorf("it is %s and a write replaces a file", DescribeFileMode(info.Mode())))
		}
		mode = info.Mode().Perm()
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, opt.failIO("failed to inspect "+dest, statErr)
	}
	if err := os.Chmod(staged.tmp, mode); err != nil {
		return nil, opt.failIO("failed to set permissions on the temporary file for "+dest, err)
	}
	return staged, nil
}

// Commit puts the staged contents at the destination. A failure leaves the
// destination as it was and removes the staged file.
func (s *Staged) Commit() (err error) {
	defer func() { err = s.cleanup(err) }()

	if err := commitStagedFile(s.tmp, s.target); err != nil {
		return s.opt.failIO("failed to replace "+s.dest, err)
	}
	return nil
}

// Discard throws the staged contents away, leaving the destination as it was.
func (s *Staged) Discard() error {
	return s.cleanup(nil)
}

// cleanup removes the staged file and attaches a failure to remove it to
// primary. A successful commit already consumed the file, which is not a
// failure, and so is calling this twice.
//
// A staged file left behind is a leftover in the user's directory, so its
// removal failure is reported rather than dropped.
func (s *Staged) cleanup(primary error) error {
	if err := os.Remove(s.tmp); err != nil && !os.IsNotExist(err) {
		return s.opt.failCleanup(primary, "remove the staged file "+s.tmp, err)
	}
	return primary
}

// DescribeFileMode names the kind of file a mode says it is, for an error a
// caller reads without knowing Go's mode bits. It is exported so that a write
// refusing a destination and a load refusing a source name a kind the same way.
func DescribeFileMode(mode os.FileMode) string {
	switch {
	case mode&os.ModeDir != 0:
		return "a directory"
	case mode&os.ModeNamedPipe != 0:
		return "a named pipe"
	case mode&os.ModeSocket != 0:
		return "a socket"
	case mode&os.ModeCharDevice != 0:
		return "a character device"
	case mode&os.ModeDevice != 0:
		return "a block device"
	default:
		// A caller passes a mode a symbolic link resolves to, never a link's
		// own, so what is left is os.ModeIrregular: a file the operating system
		// says is not a regular one without saying what it is instead.
		return "not a regular file"
	}
}

// resolveLinks returns the file a write to path has to replace: the file path
// names when it is a symbolic link, and path itself otherwise.
//
// A broken link and a path that does not exist yet both fail to resolve and
// keep path, so the contents go where the caller asked rather than to a file
// this package invented a name for.
func resolveLinks(path string) string {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return path
	}
	return resolved
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
// instead, through the handle Windows will grant. That copy is not atomic and it
// truncates the destination before it can fail, so what stands in for atomicity
// is a backup taken first and put back after a failed copy — best effort, since
// putting it back is itself a copy. See commitByCopy.
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
// — and the copy truncates dest before it can fail, so the backup is what stands
// between a refused write and the caller's data. Putting it back is itself a
// copy and can fail in turn, which is why the write error is the one reported:
// it is the failure the caller can act on.
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

	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, DefaultFileMode) //nolint:gosec // dest is the caller's chosen output
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
