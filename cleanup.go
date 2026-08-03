package filesql

import (
	"errors"
	"fmt"
)

// ErrCleanup marks a failure that happened while releasing or undoing something
// after an operation finished: rolling back a transaction, closing a prepared
// statement, detaching a database.
//
// It is joined onto the error that the operation itself returned rather than
// replacing it, because the two answer different questions — "did the load do
// what I asked" and "is anything left over". A caller can test for either with
// errors.Is.
//
// The rule this replaces was written by hand at each site as `_ = tx.Rollback()`
// or "assign the cleanup error only when the primary error is nil". Both drop
// the cleanup failure exactly when it matters most: a rollback runs because the
// load already failed, so its own failure — the one that says the database is
// now in a state neither the caller's intent nor the starting point describes —
// was the one guaranteed to be discarded.
var ErrCleanup = errors.New("filesql: cleanup failed")

// joinCleanup attaches a cleanup failure to the primary error of an operation.
// A nil cleanupErr returns primary unchanged, so callers can call it
// unconditionally. what names the step for the message ("rollback import
// transaction").
func joinCleanup(primary, cleanupErr error, what string) error {
	if cleanupErr == nil {
		return primary
	}
	return errors.Join(primary, fmt.Errorf("%w: %s: %w", ErrCleanup, what, cleanupErr))
}
