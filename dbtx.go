package filesql

import (
	"context"
	"database/sql"
)

// dbtx is the common execution surface implemented by *sql.DB and *sql.Tx.
// It lets a load run on a caller-owned transaction as readily as on the
// database, without the loader starting a nested one of its own.
type dbtx interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	PrepareContext(context.Context, string) (*sql.Stmt, error)
}
