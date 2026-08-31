package filesql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"
)

// txTracker is told when a connection enters and leaves a transaction, whatever
// spelling began it, and when one has committed. autoSaveConnector is the only
// implementation: it counts open transactions so a close can refuse to save
// rows the caller has neither committed nor rolled back, and runs the
// commit-time save.
type txTracker interface {
	transactionBegan()
	transactionEnded()
	transactionCommitted() error
}

// guardedConnector opens the connections a database this package returns hands
// out. Every one of them is a real connection to the shared-cache in-memory
// database, wrapped so the transaction options the caller passed are honored
// rather than dropped.
type guardedConnector struct {
	drv      driver.Driver
	dsn      string
	readOnly bool
	tracker  txTracker
}

// Connect implements driver.Connector.
func (c *guardedConnector) Connect(_ context.Context) (driver.Conn, error) {
	conn, err := c.drv.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &guardedConn{conn: conn, readOnly: c.readOnly, tracker: c.tracker}, nil
}

// Driver implements driver.Connector.
func (c *guardedConnector) Driver() driver.Driver {
	return c.drv
}

// sqliteDriver returns the driver registered under the "sqlite" name. It is the
// instance the dialect helper functions are registered on, so connections have
// to be opened through it rather than through a fresh one.
func sqliteDriver() (driver.Driver, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("%w: failed to reach the sqlite driver: %w", ErrDatabaseOperation, err)
	}
	drv := db.Driver()
	if err := db.Close(); err != nil {
		return nil, fmt.Errorf("%w: failed to reach the sqlite driver: %w", ErrDatabaseOperation, err)
	}
	return drv, nil
}

// guardedConn wraps one pooled connection.
//
// SQLite provides serializable transactions and has no read-only transaction of
// its own, and the driver takes a database/sql TxOptions asking for either
// without saying it cannot give it: a transaction begun with ReadOnly set went
// on to accept writes, and an isolation level SQLite does not implement was
// accepted and silently downgraded. This wrapper answers for both, refusing a
// level it cannot give and holding the query_only pragma for the life of a
// read-only transaction.
type guardedConn struct {
	conn driver.Conn
	// readOnly reports whether the whole handle is read-only already, in which
	// case a read-only transaction has nothing left to set.
	readOnly bool
	tracker  txTracker
	// rawTx reports whether a BEGIN this connection ran as a statement is still
	// open. A transaction begun that way never reaches BeginTx, so without this
	// the tracker would not know it exists.
	rawTx bool
	// spent reports that this connection could not be put back the way it was
	// found, which is what takes it out of the pool rather than handing it to
	// the next caller in a state they did not ask for.
	spent bool
}

// Close implements driver.Conn. A transaction this connection began as a
// statement is deliberately left in the tracker's count: the driver rolls it
// back here, so the rows it held are gone, and a close that saved anyway would
// write a file the caller never asked for.
func (c *guardedConn) Close() error {
	return c.conn.Close()
}

// Prepare implements driver.Conn.
func (c *guardedConn) Prepare(query string) (driver.Stmt, error) {
	return c.conn.Prepare(query)
}

// PrepareContext implements driver.ConnPrepareContext.
func (c *guardedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if p, ok := c.conn.(driver.ConnPrepareContext); ok {
		return p.PrepareContext(ctx, query)
	}
	return c.conn.Prepare(query)
}

// Ping implements driver.Pinger.
func (c *guardedConn) Ping(ctx context.Context) error {
	if p, ok := c.conn.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

// ResetSession implements driver.SessionResetter.
func (c *guardedConn) ResetSession(ctx context.Context) error {
	if r, ok := c.conn.(driver.SessionResetter); ok {
		return r.ResetSession(ctx)
	}
	return nil
}

// IsValid implements driver.Validator.
func (c *guardedConn) IsValid() bool {
	if c.spent {
		return false
	}
	if v, ok := c.conn.(driver.Validator); ok {
		return v.IsValid()
	}
	return true
}

// allowWrites gives the connection its write permission back after a read-only
// transaction. A connection that cannot be restored is spent: leaving it in the
// pool would hand the next caller a handle that refuses to write for a reason
// nothing in their code names.
func (c *guardedConn) allowWrites(ctx context.Context) {
	if err := c.setQueryOnly(ctx, false); err != nil {
		c.spent = true
	}
}

// Begin implements driver.Conn.
//
//nolint:staticcheck // database/sql calls BeginTx; this is here for the interface.
func (c *guardedConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx implements driver.ConnBeginTx.
func (c *guardedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if err := checkIsolation(opts.Isolation); err != nil {
		return nil, err
	}
	// The pragma has to be set before the transaction starts: SQLite refuses to
	// change query_only inside one.
	restore := false
	if opts.ReadOnly && !c.readOnly {
		if err := c.setQueryOnly(ctx, true); err != nil {
			return nil, err
		}
		restore = true
	}
	tx, err := c.begin(ctx, opts)
	if err != nil {
		if restore {
			c.allowWrites(ctx)
		}
		return nil, err
	}
	if c.tracker != nil {
		c.tracker.transactionBegan()
	}
	return &guardedTx{tx: tx, conn: c, restoreWrites: restore}, nil
}

// begin starts the transaction on the wrapped connection, through whichever of
// the two interfaces it has.
func (c *guardedConn) begin(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if b, ok := c.conn.(driver.ConnBeginTx); ok {
		// The options are already checked, and the driver reads only ReadOnly,
		// to pick the BEGIN it writes.
		return b.BeginTx(ctx, opts)
	}
	//nolint:staticcheck // Backward compatibility with drivers that only implement the legacy interface.
	return c.conn.Begin()
}

// ExecContext implements driver.ExecerContext.
func (c *guardedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	res, err := c.exec(ctx, query, args)
	if err == nil {
		c.noteStatement(query)
	}
	return res, err
}

func (c *guardedConn) exec(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if execer, ok := c.conn.(driver.ExecerContext); ok {
		return execer.ExecContext(ctx, query, args)
	}
	// Fallback to the deprecated Execer for backward compatibility.
	//nolint:staticcheck // Backward compatibility with drivers that only implement the legacy interface.
	if execer, ok := c.conn.(driver.Execer); ok {
		return execer.Exec(query, plainValues(args))
	}
	return nil, driver.ErrSkip
}

// QueryContext implements driver.QueryerContext.
func (c *guardedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.query(ctx, query, args)
	if err == nil {
		c.noteStatement(query)
	}
	return rows, err
}

func (c *guardedConn) query(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if queryer, ok := c.conn.(driver.QueryerContext); ok {
		return queryer.QueryContext(ctx, query, args)
	}
	// Fallback to the deprecated Queryer for backward compatibility.
	//nolint:staticcheck // Backward compatibility with drivers that only implement the legacy interface.
	if queryer, ok := c.conn.(driver.Queryer); ok {
		return queryer.Query(query, plainValues(args))
	}
	return nil, driver.ErrSkip
}

// plainValues drops the names database/sql carries, for a driver that predates
// them.
func plainValues(args []driver.NamedValue) []driver.Value {
	out := make([]driver.Value, len(args))
	for i, arg := range args {
		out[i] = arg.Value
	}
	return out
}

// noteStatement keeps the tracker's count right for a transaction the caller
// began by running BEGIN rather than by asking database/sql for one. Savepoints
// are not counted: a RELEASE of a nested one does not end the transaction
// around it, so reading it as an end would clear the count while work is still
// open, which is the direction that loses rows.
func (c *guardedConn) noteStatement(query string) {
	if c.tracker == nil {
		return
	}
	switch statementTxEffect(query) {
	case txEffectBegin:
		if !c.rawTx {
			c.rawTx = true
			c.tracker.transactionBegan()
		}
	case txEffectEnd:
		if c.rawTx {
			c.rawTx = false
			c.tracker.transactionEnded()
		}
	case txEffectNone:
	}
}

// setQueryOnly turns SQLite's query_only pragma on or off for this connection.
func (c *guardedConn) setQueryOnly(ctx context.Context, on bool) error {
	value := "false"
	if on {
		value = "true"
	}
	if _, err := c.exec(ctx, "PRAGMA query_only = "+value, nil); err != nil {
		return fmt.Errorf("%w: failed to set the read-only pragma: %w", ErrDatabaseOperation, err)
	}
	return nil
}

// checkIsolation refuses an isolation level SQLite does not provide. SQLite runs
// serializable transactions and has nothing weaker or stronger to offer, so
// taking a level it cannot give would mean answering a query under rules the
// caller did not ask for.
func checkIsolation(level driver.IsolationLevel) error {
	switch sql.IsolationLevel(level) {
	case sql.LevelDefault, sql.LevelSerializable:
		return nil
	case sql.LevelReadUncommitted, sql.LevelReadCommitted, sql.LevelWriteCommitted,
		sql.LevelRepeatableRead, sql.LevelSnapshot, sql.LevelLinearizable:
		return fmt.Errorf("%w: isolation level %s is not available; SQLite runs serializable transactions",
			ErrDatabaseOperation, sql.IsolationLevel(level))
	default:
		return fmt.Errorf("%w: isolation level %d is not available; SQLite runs serializable transactions",
			ErrDatabaseOperation, level)
	}
}

// guardedTx is one transaction on a guarded connection. It gives back what
// beginning the transaction took: the tracker's count, and the write permission
// a read-only transaction gave up.
type guardedTx struct {
	tx   driver.Tx
	conn *guardedConn
	// restoreWrites reports whether this transaction is the one that set the
	// query_only pragma, and so the one that has to clear it.
	restoreWrites bool
	// finished keeps the count right if a driver ever calls both Commit and
	// Rollback on the same transaction.
	finished sync.Once
}

// finish takes this transaction out of the tracker's count and gives the
// connection its write permission back.
func (t *guardedTx) finish() {
	t.finished.Do(func() {
		if t.restoreWrites {
			t.conn.allowWrites(context.Background())
		}
		if t.conn.tracker != nil {
			t.conn.tracker.transactionEnded()
		}
	})
}

// Commit implements driver.Tx.
func (t *guardedTx) Commit() error {
	commitErr := t.tx.Commit()
	// Whether it committed or not, this transaction is over: database/sql does
	// not call Rollback after a failed Commit, and the driver already rolled
	// the connection back itself, so leaving it in the count would make every
	// later close refuse a save it should have run. Dropping it comes before
	// the save below, which reads the same database.
	t.finish()
	if commitErr != nil {
		return commitErr
	}
	if t.conn.tracker != nil {
		return t.conn.tracker.transactionCommitted()
	}
	return nil
}

// Rollback implements driver.Tx.
func (t *guardedTx) Rollback() error {
	defer t.finish()
	return t.tx.Rollback()
}

// txEffect is what a statement does to the transaction a connection is in.
type txEffect int

const (
	// txEffectNone is a statement that leaves the transaction state alone.
	txEffectNone txEffect = iota
	// txEffectBegin is a statement that opens a transaction.
	txEffectBegin
	// txEffectEnd is a statement that closes the transaction around it.
	txEffectEnd
)

// statementTxEffect reads the leading keywords of a statement to see whether it
// opens or closes a transaction. Only the spellings SQLite gives an explicit
// transaction are read: BEGIN with any of its qualifiers opens one, and COMMIT,
// END and a bare ROLLBACK close one. ROLLBACK TO a savepoint keeps the
// transaction around it open and so counts as neither.
func statementTxEffect(query string) txEffect {
	first, rest := leadingWord(query)
	switch strings.ToUpper(first) {
	case "BEGIN":
		return txEffectBegin
	case "COMMIT", "END":
		return txEffectEnd
	case "ROLLBACK":
		if next, _ := leadingWord(rest); strings.EqualFold(next, "TO") {
			return txEffectNone
		}
		return txEffectEnd
	default:
		return txEffectNone
	}
}

// leadingWord returns the first run of letters in s and what follows it. A
// statement that starts with anything else -- a comment, a parenthesis, a
// number -- has no leading word, which is what makes it none of the three.
func leadingWord(s string) (string, string) {
	s = strings.TrimLeft(s, " \t\r\n")
	end := 0
	for end < len(s) && isASCIILetter(s[end]) {
		end++
	}
	return s[:end], s[end:]
}

func isASCIILetter(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}
