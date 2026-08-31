package filesql

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// This file holds what a load does when another connection holds the lock it
// needs. It is a concern of its own: the rest of stream_processor.go is about
// turning a file into a table, and this is about waiting for the database to
// be free enough to take it.

// How long an input waits for another load to let go, and how the wait grows.
//
// SQLite does not queue a second writer; it refuses one. Creating a table takes
// a lock on the schema, so two loads into the same database at the same time
// left one of them reporting `database schema is locked` on a shared-cache
// database or `database is locked` on a file, with its table not created and
// nothing having queued behind anything. Waiting is what every SQLite
// application does about that, and five seconds is the budget the drivers'
// busy_timeout defaults sit around; past it the error the database gave is
// returned as it stands.
//
// The budget is spent on waiting alone. It used to be wall clock over the whole
// attempt, and an attempt is a whole input. For a CSV that costs nothing, since
// the drop and the create come before the rows are read, but a workbook has to
// be opened and its sheets listed before the load knows what tables to make: a
// 40000-row XLSX waiting on a lock held for eight seconds gave up at five with
// the parse having eaten the budget three attempts in, where counting the
// waiting alone gets it in as soon as the lock is free.
//
// Waiting on something is not the same as retrying it, so the wall clock is
// bounded too. Each attempt at an input this expensive costs its parse again,
// and a hundred attempts of a workbook that takes a second to read is a load
// that looks hung. Half a minute is long enough for any queue of loads worth
// waiting for and short enough to be an answer rather than a hang.
const (
	loadLockBudget     = 5 * time.Second
	loadLockWallBudget = 30 * time.Second
	loadLockFloor      = time.Millisecond
	loadLockCeiling    = 50 * time.Millisecond
)

// retryWhileLocked runs step until it succeeds, fails for a reason other than
// another connection's lock, runs out of budget, or the context ends.
//
// The wait doubles and carries jitter, because the loads that collide are the
// loads that would otherwise retry in step with one another.
func retryWhileLocked(ctx context.Context, step func() error) error {
	return retryWhileLockedFor(ctx, loadLockBudget, loadLockWallBudget, step)
}

// retryWhileLockedFor is retryWhileLocked with the two budgets named, so a test
// can pin the shape of the waiting without waiting the whole of it.
//
// What is counted down is the sleeping, not the clock: an attempt may take as
// long as its input is large without spending anything, so an input that has to
// be parsed before it reaches its first write statement still gets the waiting
// it was promised. The clock bounds the whole thing separately, so retrying
// something expensive cannot run away with it.
func retryWhileLockedFor(ctx context.Context, budget, wallBudget time.Duration, step func() error) error {
	remaining := budget
	wallDeadline := time.Now().Add(wallBudget)
	for wait := loadLockFloor; ; {
		err := step()
		if err == nil || !lockedByAnotherConnection(err) {
			return err
		}
		if remaining <= 0 || !time.Now().Before(wallDeadline) {
			return err
		}
		// The wait is cut to what is left of the budget, so the last attempt
		// happens inside it rather than just past it.
		delay := wait/2 + rand.N(wait/2+1) //nolint:gosec // Jitter, not a secret
		if delay > remaining {
			delay = remaining
		}
		remaining -= delay
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
		// What ended the wait is asked here rather than in the select, because
		// both of its cases can be ready at once and it picks either: asking
		// there let a canceled load pay for one more attempt before it answered.
		if ctx.Err() != nil {
			return withContextError(ctx, err)
		}
		if wait *= 2; wait > loadLockCeiling {
			wait = loadLockCeiling
		}
	}
}

// lockedByAnotherConnection reports the two answers SQLite gives when someone
// else holds what this load needs. It reads the driver's code rather than the
// message, which differs between a file database and a shared-cache one and is
// not this package's to depend on: SQLITE_BUSY for a file, SQLITE_LOCKED for a
// shared-cache table, each with extended codes above them that carry the same
// primary code in their low byte.
func lockedByAnotherConnection(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	return isLockCode(sqliteErr.Code())
}

// isLockCode reads a result code's low byte, which is where an extended code
// carries the primary one it refines.
func isLockCode(code int) bool {
	switch code & 0xFF {
	case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED:
		return true
	default:
		return false
	}
}
