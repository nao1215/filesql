package filesql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

// ErrReadOnly is returned when a write operation is attempted on a read-only database.
var ErrReadOnly = errors.New("database is read-only: write operations are not allowed")

// ReadOnlyDB wraps a *sql.DB to prevent write operations.
// All SELECT queries work normally, but INSERT, UPDATE, DELETE, DROP, ALTER, and CREATE
// statements are rejected with ErrReadOnly.
//
// This is useful for audit scenarios where you want to view data without risk of modification.
//
// Example:
//
//	db, err := filesql.Open("payment.ach")
//	if err != nil {
//		return err
//	}
//	defer db.Close()
//
//	rodb := filesql.NewReadOnlyDB(db)
//
//	// SELECT works fine
//	rows, err := rodb.Query("SELECT * FROM payment_entries")
//
//	// UPDATE/DELETE/INSERT are rejected
//	_, err = rodb.Exec("DELETE FROM payment_entries") // returns ErrReadOnly
type ReadOnlyDB struct {
	db *sql.DB
}

// NewReadOnlyDB creates a read-only wrapper around an existing database connection.
// The underlying database is not modified; write operations are simply rejected at the API level.
func NewReadOnlyDB(db *sql.DB) *ReadOnlyDB {
	return &ReadOnlyDB{db: db}
}

// writeKeywords are the SQL verbs that mutate data or schema. A statement is
// treated as a write if any of these appears as a bare word outside of string
// literals and parentheses (i.e. at the top level of the statement). Scanning
// the whole statement rather than only its first keyword is what blocks writes
// hidden behind comments (/*x*/ DELETE ...) or a CTE (WITH ... DELETE ...).
const (
	kwInsert   = "INSERT"
	kwUpdate   = "UPDATE"
	kwDelete   = "DELETE"
	kwDrop     = "DROP"
	kwAlter    = "ALTER"
	kwCreate   = "CREATE"
	kwTruncate = "TRUNCATE"
	kwReplace  = "REPLACE"
	kwUpsert   = "UPSERT"
)

var writeKeywords = map[string]struct{}{
	kwInsert:   {},
	kwUpdate:   {},
	kwDelete:   {},
	kwDrop:     {},
	kwAlter:    {},
	kwCreate:   {},
	kwTruncate: {},
	kwReplace:  {},
	kwUpsert:   {},
}

// isWriteStatement reports whether the SQL statement performs a write.
//
// It is intentionally conservative: a statement is rejected if a write keyword
// appears anywhere at the top level, so writes cannot be smuggled past the
// read-only API through SQL comments, common table expressions (WITH ...
// DELETE) or a RETURNING clause executed via Query/QueryRow. Keywords inside
// string literals, quoted identifiers, comments or parenthesised subqueries are
// ignored to avoid rejecting legitimate SELECTs.
func isWriteStatement(query string) bool {
	for _, word := range topLevelWords(query) {
		if _, ok := writeKeywords[word]; ok {
			return true
		}
	}
	return false
}

// topLevelWords scans an SQL statement and returns the uppercased keywords that
// appear at parenthesis depth zero, skipping comments, string literals and
// quoted identifiers. CTE subqueries live inside parentheses, so the main
// statement verb (SELECT / INSERT / UPDATE / DELETE) is always reported while
// the inner verbs of the WITH clause are not.
func topLevelWords(query string) []string {
	var words []string
	var word strings.Builder
	depth := 0

	flush := func() {
		if word.Len() > 0 {
			words = append(words, strings.ToUpper(word.String()))
			word.Reset()
		}
	}

	runes := []rune(query)
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		switch {
		case c == '-' && i+1 < len(runes) && runes[i+1] == '-':
			// Line comment: skip to end of line.
			flush()
			for i < len(runes) && runes[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(runes) && runes[i+1] == '*':
			// Block comment: skip to closing */.
			flush()
			i += 2
			for i+1 < len(runes) && (runes[i] != '*' || runes[i+1] != '/') {
				i++
			}
			i++ // position on '/'; loop's i++ moves past it
		case c == '\'' || c == '"' || c == '`':
			// String literal or quoted identifier: skip to the matching quote,
			// honouring doubled-quote escapes ('' "" ``).
			flush()
			quote := c
			i++
			for i < len(runes) {
				if runes[i] == quote {
					if i+1 < len(runes) && runes[i+1] == quote {
						i++ // escaped quote, stay inside
					} else {
						break
					}
				}
				i++
			}
		case c == '(':
			flush()
			depth++
		case c == ')':
			flush()
			if depth > 0 {
				depth--
			}
		case isWordChar(c):
			if depth == 0 {
				word.WriteRune(c)
			}
		default:
			flush()
		}
	}
	flush()
	return words
}

// isWordChar reports whether c can be part of an SQL identifier/keyword.
func isWordChar(c rune) bool {
	return c == '_' ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9')
}

// readOnlyViolationQuery is an intentionally failing query used to surface a
// read-only error through *sql.Row, which has no exported error constructor.
// Selecting from a non-existent table named with the violation message makes
// the deferred Scan error mention "read-only" without performing any write.
const readOnlyViolationQuery = `SELECT 1 FROM "filesql read-only: write operations are not allowed"`

// QueryContext executes a query that returns rows with context.
// Write statements (including DELETE/UPDATE ... RETURNING) are rejected with
// ErrReadOnly because they mutate data even when invoked through Query.
func (r *ReadOnlyDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if isWriteStatement(query) {
		return nil, ErrReadOnly
	}
	return r.db.QueryContext(ctx, query, args...)
}

// Query executes a query that returns rows (SELECT statements).
// Deprecated: Use QueryContext instead.
func (r *ReadOnlyDB) Query(query string, args ...any) (*sql.Rows, error) {
	return r.QueryContext(context.Background(), query, args...)
}

// QueryRowContext executes a query that returns at most one row with context.
// Write statements are rejected: the returned row's Scan reports a read-only
// error instead of executing the statement.
func (r *ReadOnlyDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if isWriteStatement(query) {
		return r.db.QueryRowContext(ctx, readOnlyViolationQuery)
	}
	return r.db.QueryRowContext(ctx, query, args...)
}

// QueryRow executes a query that returns at most one row.
// Deprecated: Use QueryRowContext instead.
func (r *ReadOnlyDB) QueryRow(query string, args ...any) *sql.Row {
	return r.QueryRowContext(context.Background(), query, args...)
}

// ExecContext rejects write operations and returns ErrReadOnly.
// For read-only databases, use Query methods instead.
func (r *ReadOnlyDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if isWriteStatement(query) {
		return nil, ErrReadOnly
	}
	return r.db.ExecContext(ctx, query, args...)
}

// Exec rejects write operations and returns ErrReadOnly.
// For read-only databases, use Query methods instead.
// Deprecated: Use ExecContext instead.
func (r *ReadOnlyDB) Exec(query string, args ...any) (sql.Result, error) {
	if isWriteStatement(query) {
		return nil, ErrReadOnly
	}
	return r.db.ExecContext(context.Background(), query, args...)
}

// PrepareContext creates a prepared statement with context.
func (r *ReadOnlyDB) PrepareContext(ctx context.Context, query string) (*ReadOnlyStmt, error) {
	if isWriteStatement(query) {
		return nil, ErrReadOnly
	}
	stmt, err := r.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &ReadOnlyStmt{stmt: stmt, isWrite: isWriteStatement(query)}, nil
}

// Prepare creates a prepared statement.
// Deprecated: Use PrepareContext instead.
func (r *ReadOnlyDB) Prepare(query string) (*ReadOnlyStmt, error) {
	return r.PrepareContext(context.Background(), query)
}

// BeginTx starts a read-only transaction with context and options.
func (r *ReadOnlyDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*ReadOnlyTx, error) {
	tx, err := r.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &ReadOnlyTx{tx: tx}, nil
}

// Begin starts a read-only transaction.
// Deprecated: Use BeginTx instead.
func (r *ReadOnlyDB) Begin() (*ReadOnlyTx, error) {
	return r.BeginTx(context.Background(), nil)
}

// Close closes the underlying database connection.
func (r *ReadOnlyDB) Close() error {
	return r.db.Close()
}

// PingContext verifies the connection to the database with context.
func (r *ReadOnlyDB) PingContext(ctx context.Context) error {
	return r.db.PingContext(ctx)
}

// Ping verifies the connection to the database.
// Deprecated: Use PingContext instead.
func (r *ReadOnlyDB) Ping() error {
	return r.db.PingContext(context.Background())
}

// DB returns the underlying *sql.DB.
// Use with caution as this bypasses read-only protection.
func (r *ReadOnlyDB) DB() *sql.DB {
	return r.db
}

// ReadOnlyStmt wraps a *sql.Stmt to enforce read-only operations.
type ReadOnlyStmt struct {
	stmt    *sql.Stmt
	isWrite bool
}

// Query executes a prepared query statement.
// Deprecated: Use QueryContext instead.
func (s *ReadOnlyStmt) Query(args ...any) (*sql.Rows, error) {
	return s.stmt.QueryContext(context.Background(), args...)
}

// QueryContext executes a prepared query statement with context.
func (s *ReadOnlyStmt) QueryContext(ctx context.Context, args ...any) (*sql.Rows, error) {
	return s.stmt.QueryContext(ctx, args...)
}

// QueryRow executes a prepared query statement that returns at most one row.
// Deprecated: Use QueryRowContext instead.
func (s *ReadOnlyStmt) QueryRow(args ...any) *sql.Row {
	return s.stmt.QueryRowContext(context.Background(), args...)
}

// QueryRowContext executes a prepared query statement that returns at most one row with context.
func (s *ReadOnlyStmt) QueryRowContext(ctx context.Context, args ...any) *sql.Row {
	return s.stmt.QueryRowContext(ctx, args...)
}

// Exec is not allowed for read-only statements.
// Deprecated: Use ExecContext instead.
func (s *ReadOnlyStmt) Exec(args ...any) (sql.Result, error) {
	if s.isWrite {
		return nil, ErrReadOnly
	}
	return s.stmt.ExecContext(context.Background(), args...)
}

// ExecContext is not allowed for read-only statements.
func (s *ReadOnlyStmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	if s.isWrite {
		return nil, ErrReadOnly
	}
	return s.stmt.ExecContext(ctx, args...)
}

// Close closes the statement.
func (s *ReadOnlyStmt) Close() error {
	return s.stmt.Close()
}

// ReadOnlyTx wraps a *sql.Tx to enforce read-only operations.
type ReadOnlyTx struct {
	tx *sql.Tx
}

// Query executes a query that returns rows.
// Deprecated: Use QueryContext instead.
func (t *ReadOnlyTx) Query(query string, args ...any) (*sql.Rows, error) {
	return t.QueryContext(context.Background(), query, args...)
}

// QueryContext executes a query that returns rows with context.
// Write statements (including DELETE/UPDATE ... RETURNING) are rejected with
// ErrReadOnly.
func (t *ReadOnlyTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if isWriteStatement(query) {
		return nil, ErrReadOnly
	}
	return t.tx.QueryContext(ctx, query, args...)
}

// QueryRow executes a query that returns at most one row.
// Deprecated: Use QueryRowContext instead.
func (t *ReadOnlyTx) QueryRow(query string, args ...any) *sql.Row {
	return t.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext executes a query that returns at most one row with context.
// Write statements are rejected: the returned row's Scan reports a read-only
// error instead of executing the statement.
func (t *ReadOnlyTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if isWriteStatement(query) {
		return t.tx.QueryRowContext(ctx, readOnlyViolationQuery)
	}
	return t.tx.QueryRowContext(ctx, query, args...)
}

// Exec rejects write operations.
// Deprecated: Use ExecContext instead.
func (t *ReadOnlyTx) Exec(query string, args ...any) (sql.Result, error) {
	if isWriteStatement(query) {
		return nil, ErrReadOnly
	}
	return t.tx.ExecContext(context.Background(), query, args...)
}

// ExecContext rejects write operations.
func (t *ReadOnlyTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if isWriteStatement(query) {
		return nil, ErrReadOnly
	}
	return t.tx.ExecContext(ctx, query, args...)
}

// Commit commits the transaction.
func (t *ReadOnlyTx) Commit() error {
	return t.tx.Commit()
}

// Rollback aborts the transaction.
func (t *ReadOnlyTx) Rollback() error {
	return t.tx.Rollback()
}

// Prepare creates a prepared statement within the transaction.
// Deprecated: Use PrepareContext instead.
func (t *ReadOnlyTx) Prepare(query string) (*ReadOnlyStmt, error) {
	return t.PrepareContext(context.Background(), query)
}

// PrepareContext creates a prepared statement within the transaction with context.
func (t *ReadOnlyTx) PrepareContext(ctx context.Context, query string) (*ReadOnlyStmt, error) {
	if isWriteStatement(query) {
		return nil, ErrReadOnly
	}
	stmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &ReadOnlyStmt{stmt: stmt, isWrite: isWriteStatement(query)}, nil
}
