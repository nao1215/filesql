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
// succeed. When either fails, dest is left exactly as it was and the temporary
// file is removed.
//
// Why this matters: the ACH and Fedwire encoders validate while they encode, so
// a value the format cannot represent is rejected partway through the write.
// Opening dest directly with os.Create truncated it first, which for an in-place
// save is the caller's own source file — a rejected save destroyed the data it
// was saving. Staging the write means a failure costs nothing.
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
	// Remove the staged file unless the rename below claimed it. Both calls are
	// no-ops after a successful rename.
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName) //nolint:errcheck // Best-effort cleanup; the file is gone after a successful rename
	}()

	if err := write(tmp); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("%w: failed to close the temporary file for %s: %s", ErrIOOperation, dest, err.Error())
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

	if err := os.Rename(tmpName, dest); err != nil {
		return fmt.Errorf("%w: failed to replace %s: %s", ErrIOOperation, dest, err.Error())
	}
	return nil
}
