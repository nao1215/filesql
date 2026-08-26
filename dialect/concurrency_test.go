package dialect

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	_ "modernc.org/sqlite"
)

// TestTranslateIsSafeForConcurrentUse exercises the two caches a translation
// reads -- the scanned TO_CHAR template and its parsed numeric form -- from
// several goroutines at once, filling them as it goes. Both are process-wide,
// so nothing else in this package's tests reaches them concurrently, and -race
// only reports a race something actually runs into.
func TestTranslateIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := range 40 {
				// Distinct templates per iteration, so the caches are written
				// as well as read, and repeated across workers, so two
				// goroutines store the same key.
				query := fmt.Sprintf(
					`SELECT TO_CHAR(d, 'FMDay %d'), TO_CHAR(n, 'FM999D9%d'), GREATEST(a, b) FROM t`,
					(worker+i)%13, (worker+i)%10)
				if _, err := Translate(PostgreSQL, query); err != nil {
					t.Errorf("translate: %v", err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}

// TestRegisteredHelpersAreSafeForConcurrentUse runs the helpers themselves from
// several connections at once. They are registered into a process-wide driver
// and read package-level tables, so a table written after registration would
// show up here.
func TestRegisteredHelpersAreSafeForConcurrentUse(t *testing.T) {
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions(): %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := range 25 {
				query := fmt.Sprintf(
					`SELECT TO_CHAR(TIMESTAMP '2024-03-05 13:04:05', 'FMDay %d'), TO_CHAR(%d.5, 'FM999D99'), SAFE.PARSE_DATE('%%Y-%%m-%%d', 'nope')`,
					(worker+i)%13, worker+i)
				translated, err := Translate(GoogleSQL, query)
				if err != nil {
					t.Errorf("translate: %v", err)
					return
				}
				var day, number string
				var parsed sql.NullString
				if err := db.QueryRowContext(context.Background(), translated).Scan(&day, &number, &parsed); err != nil {
					t.Errorf("query: %v", err)
					return
				}
				if parsed.Valid {
					t.Errorf("SAFE.PARSE_DATE of an unparseable date = %q, want NULL", parsed.String)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}
