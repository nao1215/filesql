package filesql

import (
	"fmt"
	"io"
	"os"

	"github.com/nao1215/filesql/internal/atomicwrite"
)

// defaultOutputFileMode is the permission given to a file this package creates.
// An existing destination keeps its own mode instead.
const defaultOutputFileMode os.FileMode = atomicwrite.DefaultFileMode

// writeFileAtomically hands write a writer for a temporary file in dest's
// directory and renames it over dest only when write and the close both succeed.
// When either fails, dest is left exactly as it was and the temporary file is
// removed.
//
// The staging itself lives in internal/atomicwrite, where it can be tested
// against a filesystem with no database in front of it. This package is its only
// caller. What stays here is the vocabulary: a failure of the staging is an
// ErrIOOperation, a staged file that could not be removed afterwards is joined
// on as an ErrCleanup, and the error write itself returned is passed through
// untouched — an encoder refusing a value it cannot represent is not an I/O
// failure.
func writeFileAtomically(dest string, write func(io.Writer) error) error {
	return atomicwrite.Write(dest, write, atomicwrite.Options{
		FailIO: func(what string, err error) error {
			return fmt.Errorf("%w: %s: %w", ErrIOOperation, what, err)
		},
		FailCleanup: func(primary error, what string, err error) error {
			return joinCleanup(primary, err, what)
		},
	})
}
