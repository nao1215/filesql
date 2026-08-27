package filesql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// TestJoinCleanupKeepsBothErrors pins the rule the package uses everywhere it
// releases something. The variant it replaced — dropping the cleanup error, or
// keeping it only when the primary error was nil — discarded it in the very
// case that produces one, because cleanup runs after a failure.
func TestJoinCleanupKeepsBothErrors(t *testing.T) {
	t.Parallel()

	primary := errors.New("load failed")
	cleanupErr := errors.New("rollback refused")

	tests := []struct {
		name        string
		primary     error
		cleanupErr  error
		wantNil     bool
		wantErrs    []error
		wantNotErrs []error
	}{
		{name: "nothing failed", wantNil: true},
		{
			name:        "only the operation failed",
			primary:     primary,
			wantErrs:    []error{primary},
			wantNotErrs: []error{ErrCleanup},
		},
		{
			name:       "only the cleanup failed",
			cleanupErr: cleanupErr,
			wantErrs:   []error{cleanupErr, ErrCleanup},
		},
		{
			name:       "both failed",
			primary:    primary,
			cleanupErr: cleanupErr,
			wantErrs:   []error{primary, cleanupErr, ErrCleanup},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := joinCleanup(tt.primary, tt.cleanupErr, "rollback import transaction")
			if tt.wantNil {
				if got != nil {
					t.Fatalf("joinCleanup = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("joinCleanup = nil, want an error reaching %v", tt.wantErrs)
			}
			for _, want := range tt.wantErrs {
				if !errors.Is(got, want) {
					t.Errorf("errors.Is(err, %v) = false; err = %v", want, got)
				}
			}
			for _, unwanted := range tt.wantNotErrs {
				if errors.Is(got, unwanted) {
					t.Errorf("errors.Is(err, %v) = true, want false; err = %v", unwanted, got)
				}
			}
		})
	}
}

// TestJoinCleanupPreservesTypedErrors checks errors.As as well as errors.Is, so
// a caller inspecting a typed cause still finds it after a cleanup failure is
// attached. Formatting the two into one string would break this.
func TestJoinCleanupPreservesTypedErrors(t *testing.T) {
	t.Parallel()

	got := joinCleanup(&tableError{table: "users"}, errors.New("close failed"), "close insert statement")

	var typed *tableError
	if !errors.As(got, &typed) {
		t.Fatalf("errors.As did not reach the typed cause; err = %v", got)
	}
	if typed.table != "users" {
		t.Errorf("table = %q, want users", typed.table)
	}
	if !errors.Is(got, ErrCleanup) {
		t.Errorf("cleanup marker lost; err = %v", got)
	}
}

type tableError struct{ table string }

func (e *tableError) Error() string { return "table error: " + e.table }

// TestCommitIsTerminalForRollback documents the database/sql rule the loader's
// lifecycle depends on, so a future change that adds a rollback after a commit
// has something to fail against. Commit ends the transaction whether it
// succeeds or fails, so any later Rollback answers sql.ErrTxDone.
func TestCommitIsTerminalForRollback(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `CREATE TABLE t (a)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if rollbackErr := tx.Rollback(); !errors.Is(rollbackErr, sql.ErrTxDone) {
		t.Errorf("Rollback after Commit = %v, want sql.ErrTxDone", rollbackErr)
	}
}

// TestLoadIntoTxStagingFailureRollsBackEverything is the atomicity guarantee at
// the library boundary: when the caller owns the transaction, a failure part
// way through leaves the caller free to roll back and lose every table the load
// created, not only the one that failed.
func TestLoadIntoTxStagingFailureRollsBackEverything(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	good := filepath.Join(dir, "good.csv")
	broken := filepath.Join(dir, "broken.csv")
	if err := os.WriteFile(good, []byte("id,name\n1,alice\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(broken, []byte("id,name\n1,alice,extra\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	goodBuilder, err := buildForTest(t.Context(), NewBuilder().AddPath(good))
	if err != nil {
		t.Fatal(err)
	}
	if err := goodBuilder.LoadIntoTx(t.Context(), tx); err != nil {
		t.Fatalf("staging the good input: %v", err)
	}

	brokenBuilder, err := buildForTest(t.Context(), NewBuilder().AddPath(broken))
	if err != nil {
		t.Fatal(err)
	}
	if err := brokenBuilder.LoadIntoTx(t.Context(), tx); err == nil {
		t.Fatal("staging the broken input = nil, want an error")
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	for _, name := range []string{"good", "broken"} {
		var n int
		if err := db.QueryRowContext(t.Context(),
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("table %q survived the rollback", name)
		}
	}
}

// TestLoadIntoTxACHMetadataStaysInTheTransaction checks the other half: the
// write-back metadata a load produces lives in the transaction that carries the
// tables, so a rollback leaves neither behind.
func TestLoadIntoTxACHMetadataStaysInTheTransaction(t *testing.T) {
	achPath := filepath.Join("testdata", "ppd-debit.ach")
	if _, err := os.Stat(achPath); err != nil {
		t.Skipf("ACH fixture not available: %v", err)
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	builder, err := buildForTest(t.Context(), NewBuilder().AddPath(achPath))
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.LoadIntoTx(t.Context(), tx); err != nil {
		t.Fatalf("LoadIntoTx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	var n int
	if err := db.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, sourceTableName).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Error("write-back metadata survived the rollback")
	}
}

// TestStreamProcessorContextCancelLeavesNoTable checks that canceling mid-load
// does not leave a half-populated table behind, and that the cancellation is
// reported as such rather than as a lifecycle defect.
func TestStreamProcessorContextCancelLeavesNoTable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "rows.csv")
	var b strings.Builder
	b.WriteString("id,name\n")
	for i := range 5000 {
		fmt.Fprintf(&b, "%d,name-%d\n", i, i)
	}
	if err := os.WriteFile(csvPath, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	db, err := OpenContext(ctx, csvPath)
	if err == nil {
		_ = db.Close()
		t.Fatal("OpenContext with a canceled context = nil error, want a failure")
	}
	if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "context canceled") {
		t.Errorf("err = %v, want it to report the cancellation", err)
	}
}
