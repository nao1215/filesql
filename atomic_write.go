package filesql

import (
	"errors"
	"fmt"
	"io"

	"github.com/nao1215/filesql/internal/atomicwrite"
)

// stagingOptions is how this package words a failure of the staging itself: a
// failure of the staging machinery is an ErrIOOperation, a staged file that
// could not be removed afterwards is joined on as an ErrCleanup, and the error
// write itself returned is passed through untouched — an encoder refusing a
// value it cannot represent is not an I/O failure.
func stagingOptions() atomicwrite.Options {
	return atomicwrite.Options{
		FailIO: func(what string, err error) error {
			return fmt.Errorf("%w: %s: %w", ErrIOOperation, what, err)
		},
		FailCleanup: func(primary error, what string, err error) error {
			return joinCleanup(primary, err, what)
		},
	}
}

// writeFileAtomically hands write a writer for a temporary file in dest's
// directory and renames it over dest only when write and the close both succeed.
// When either fails, dest is left exactly as it was and the temporary file is
// removed.
//
// The staging itself lives in internal/atomicwrite, where it can be tested
// against a filesystem with no database in front of it. This package is its only
// caller.
func writeFileAtomically(dest string, write func(io.Writer) error) error {
	return atomicwrite.Write(dest, write, stagingOptions())
}

// writeSet is a group of files that are replaced together. Each write is staged
// beside its destination and nothing is put in place until every one of them has
// been produced, so a set costs what one file costs when a write is refused:
// nothing.
//
// It exists because a save of several files wrote them one at a time. A refusal
// that needs the rows — a tab in a TSV value, an LTSV table a session emptied —
// therefore arrived after the earlier files already held the new rows, and an
// in-place save left the caller a directory holding two states of one session
// with nothing saying which file was which.
type writeSet struct {
	staged []*atomicwrite.Staged
}

// write stages dest's new contents. Nothing at dest is touched until commit.
//
// A nil set is a caller replacing one file rather than a set, and the write goes
// straight through: staged and put in place on its own, which is all a single
// file ever needed. Writing it this way lets one function serve both a dump of
// one file and a save of several.
func (s *writeSet) write(dest string, write func(io.Writer) error) error {
	if s == nil {
		return writeFileAtomically(dest, write)
	}
	staged, err := atomicwrite.Stage(dest, write, stagingOptions())
	if err != nil {
		return err
	}
	s.staged = append(s.staged, staged)
	return nil
}

// commit puts every staged file in place, in the order they were staged. A
// failure here is the operating system refusing a rename rather than an encoder
// refusing a value, so the files already committed stay committed and the rest
// are discarded; the caller hears both.
func (s *writeSet) commit() error {
	staged := s.staged
	s.staged = nil
	for i, one := range staged {
		if err := one.Commit(); err != nil {
			return errors.Join(err, discardStaged(staged[i+1:]))
		}
	}
	return nil
}

// discard throws away every staged file, leaving each destination as it was.
func (s *writeSet) discard() error {
	staged := s.staged
	s.staged = nil
	return discardStaged(staged)
}

func discardStaged(staged []*atomicwrite.Staged) error {
	var errs []error
	for _, one := range staged {
		if err := one.Discard(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// writeTogether runs produce against a set of its own and puts the files it
// staged in place once it has finished. A produce that fails replaces nothing.
func writeTogether(produce func(*writeSet) error) error {
	set := &writeSet{}
	if err := produce(set); err != nil {
		return errors.Join(err, set.discard())
	}
	return set.commit()
}
