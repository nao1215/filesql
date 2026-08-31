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
	gate     *txGate
}

// Connect implements driver.Connector.
func (c *guardedConnector) Connect(_ context.Context) (driver.Conn, error) {
	conn, err := c.drv.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &guardedConn{conn: conn, readOnly: c.readOnly, tracker: c.tracker, gate: c.gate}, nil
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
	// gate is the queue this connection's statements and transactions wait in.
	gate *txGate
	// inTx reports whether a transaction of this connection is open, whether it
	// was begun through BeginTx or by running BEGIN as a statement. It is what
	// keeps a statement inside a transaction from queueing behind the
	// transaction it belongs to, and what lets the tracker see both spellings.
	inTx bool
	// savepoint names the savepoint that opened the transaction, empty when a
	// BEGIN or BeginTx opened it. Releasing that savepoint is what ends the
	// transaction; releasing one taken inside it leaves the transaction open.
	savepoint string
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
	stmt, err := c.conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return c.wrapStmt(stmt, query), nil
}

// PrepareContext implements driver.ConnPrepareContext.
func (c *guardedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := c.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return c.wrapStmt(stmt, query), nil
}

func (c *guardedConn) prepare(ctx context.Context, query string) (driver.Stmt, error) {
	if p, ok := c.conn.(driver.ConnPrepareContext); ok {
		return p.PrepareContext(ctx, query)
	}
	return c.conn.Prepare(query)
}

// wrapStmt puts a prepared statement under the same rules as one run directly.
// A statement is left as the driver's own only when there is nothing to apply:
// no gate to queue in and no tracker to tell.
func (c *guardedConn) wrapStmt(stmt driver.Stmt, query string) driver.Stmt {
	if c.gate == nil && c.tracker == nil {
		return stmt
	}
	return &guardedStmt{stmt: stmt, conn: c, sql: query}
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
	if err := c.openTx(ctx); err != nil {
		if restore {
			c.allowWrites(ctx)
		}
		return nil, err
	}
	tx, err := c.begin(ctx, opts)
	if err != nil {
		c.closeTx()
		if restore {
			c.allowWrites(ctx)
		}
		return nil, err
	}
	return &guardedTx{tx: tx, conn: c, restoreWrites: restore}, nil
}

// openTx takes the gate for a transaction of this connection and tells the
// tracker about it. Both spellings of BEGIN come through here, so a transaction
// counts the same however it was begun.
func (c *guardedConn) openTx(ctx context.Context) error {
	if c.inTx {
		// SQLite has no nested transaction; let the driver say so rather than
		// taking the gate a second time.
		return nil
	}
	if c.gate != nil {
		if err := c.gate.acquire(ctx); err != nil {
			return err
		}
	}
	c.inTx = true
	if c.tracker != nil {
		c.tracker.transactionBegan()
	}
	return nil
}

// closeTx gives back what openTx took. A connection closed with a transaction
// still open does not come through here: the driver rolls that transaction back
// rather than finishing it, so the tracker keeps counting it and a save is
// refused rather than writing rows the caller lost.
func (c *guardedConn) closeTx() {
	if !c.inTx {
		return
	}
	c.inTx = false
	c.savepoint = ""
	if c.gate != nil {
		c.gate.release()
	}
	if c.tracker != nil {
		c.tracker.transactionEnded()
	}
}

// runExec runs one statement, taking the gate when its keyword opens a
// transaction and giving it back when one closes it. Everything else runs
// without touching the gate: a statement outside a transaction is not what the
// gate queues, and making it wait would deadlock the ordinary shape of holding
// a transaction open while querying the same database beside it.
//
// Only the statement that opened the transaction may close it. A BEGIN run
// inside a transaction is a mistake SQLite refuses, and a savepoint taken
// inside one is not a transaction of its own; treating either as an opener made
// the failure of the first drop the count of the transaction still running
// underneath it.
func (c *guardedConn) runExec(ctx context.Context, query string, run func(context.Context) (driver.Result, error)) (driver.Result, error) {
	stmt := c.effectOf(query)
	switch stmt.effect {
	case txEffectBegin:
		if c.inTx {
			break
		}
		if err := c.openTx(ctx); err != nil {
			return nil, err
		}
		res, err := run(ctx)
		if err != nil {
			c.closeTx()
			return res, err
		}
		c.savepoint = stmt.savepoint
		return res, nil
	case txEffectCommit, txEffectRollback:
		if !c.endsTransaction(stmt) {
			break
		}
		res, err := run(ctx)
		if err != nil {
			return res, err
		}
		c.closeTx()
		if stmt.effect == txEffectCommit && c.tracker != nil {
			if saveErr := c.tracker.transactionCommitted(); saveErr != nil {
				return res, saveErr
			}
		}
		return res, nil
	case txEffectNone:
	}
	return run(ctx)
}

// endsTransaction reports whether a statement that closes a transaction closes
// the one this connection is in. A COMMIT, END or ROLLBACK closes whatever is
// open; a RELEASE closes it only when it names the savepoint that opened it,
// since releasing a savepoint taken inside a transaction leaves that
// transaction open. SQLite compares savepoint names without regard to case, so
// this does too.
func (c *guardedConn) endsTransaction(stmt txStatement) bool {
	if !c.inTx {
		return false
	}
	if stmt.savepoint == "" {
		return true
	}
	return c.savepoint != "" && strings.EqualFold(c.savepoint, stmt.savepoint)
}

// runQuery runs one query. A transaction keyword run as a query is odd but
// legal, so it is read the same way a statement is.
func (c *guardedConn) runQuery(ctx context.Context, query string, run func(context.Context) (driver.Rows, error)) (driver.Rows, error) {
	if c.effectOf(query).effect == txEffectNone {
		return run(ctx)
	}
	var rows driver.Rows
	_, err := c.runExec(ctx, query, func(ctx context.Context) (driver.Result, error) {
		var runErr error
		rows, runErr = run(ctx)
		return driver.RowsAffected(0), runErr
	})
	return rows, err
}

// effectOf reads a statement only when something depends on the answer.
func (c *guardedConn) effectOf(query string) txStatement {
	if c.gate == nil && c.tracker == nil {
		return txStatement{}
	}
	return readTxStatement(query)
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
	return c.runExec(ctx, query, func(ctx context.Context) (driver.Result, error) {
		return c.exec(ctx, query, args)
	})
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
	return c.runQuery(ctx, query, func(ctx context.Context) (driver.Rows, error) {
		return c.query(ctx, query, args)
	})
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
		t.conn.closeTx()
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
	// txEffectCommit is a statement that closes the transaction around it and
	// keeps what it wrote.
	txEffectCommit
	// txEffectRollback is a statement that closes the transaction around it and
	// discards what it wrote.
	txEffectRollback
)

// txStatement is a statement read for what it does to a transaction.
type txStatement struct {
	// effect is what the statement does.
	effect txEffect
	// savepoint is the savepoint a SAVEPOINT takes or a RELEASE releases, and
	// is empty for BEGIN, COMMIT, END and ROLLBACK. A savepoint bounds a
	// transaction only when it is the outermost one, which the connection
	// running the statement knows and this reading does not.
	savepoint string
}

// readTxStatement reads the leading keywords of a statement to see whether it
// opens or closes a transaction. The spellings SQLite gives an explicit
// transaction are all read: BEGIN with any of its qualifiers opens one, COMMIT
// and END keep one, a bare ROLLBACK discards one, and SAVEPOINT and RELEASE do
// the same for the savepoint they name. ROLLBACK TO a savepoint keeps the
// transaction around it open and so counts as none of them.
func readTxStatement(query string) txStatement {
	first, rest := leadingWord(query)
	switch strings.ToUpper(first) {
	case "BEGIN":
		return txStatement{effect: txEffectBegin}
	case "COMMIT", "END":
		return txStatement{effect: txEffectCommit}
	case "ROLLBACK":
		// SQLite writes the keyword as ROLLBACK [TRANSACTION] TO [SAVEPOINT]
		// name, so the optional TRANSACTION stands between the two words that
		// decide this.
		next, after := leadingWord(rest)
		if strings.EqualFold(next, "TRANSACTION") {
			next, _ = leadingWord(after)
		}
		if strings.EqualFold(next, "TO") {
			return txStatement{}
		}
		return txStatement{effect: txEffectRollback}
	case "SAVEPOINT":
		if name := leadingIdentifier(rest); name != "" {
			return txStatement{effect: txEffectBegin, savepoint: name}
		}
	case "RELEASE":
		if name := leadingIdentifier(releaseTarget(rest)); name != "" {
			return txStatement{effect: txEffectCommit, savepoint: name}
		}
	}
	return txStatement{}
}

// releaseTarget drops the optional SAVEPOINT keyword of RELEASE [SAVEPOINT]
// name. It is dropped only when a name follows it, because "savepoint" is
// itself a name a caller may have taken: RELEASE savepoint releases that one.
func releaseTarget(rest string) string {
	next, after := leadingWord(rest)
	if strings.EqualFold(next, "SAVEPOINT") && leadingIdentifier(after) != "" {
		return after
	}
	return rest
}

// leadingWord returns the first run of letters in s and what follows it. A
// statement whose first thing is not a letter -- a parenthesis, a number, a
// string -- has no leading word, which is what makes it none of the effects.
func leadingWord(s string) (string, string) {
	s = skipToStatement(s)
	end := 0
	for end < len(s) && isASCIILetter(s[end]) {
		end++
	}
	return s[:end], s[end:]
}

// leadingIdentifier returns the identifier at the front of s, in the spelling a
// comparison uses rather than the one it was written in, or "" when there is
// none. SQLite writes an identifier bare or wrapped in double quotes,
// backticks, or brackets, and a savepoint name is an identifier like any other,
// so the name in RELEASE "the batch" has to be read back as the one SAVEPOINT
// "the batch" took.
func leadingIdentifier(s string) string {
	s = skipToStatement(s)
	if s == "" {
		return ""
	}
	if closing, quoted := identifierQuotes[s[0]]; quoted {
		return quotedIdentifier(s[1:], s[0], closing)
	}
	end := 0
	for end < len(s) && isIdentifierByte(s[end]) {
		end++
	}
	return s[:end]
}

// identifierQuotes maps the character that opens a quoted identifier to the one
// that closes it.
//
//nolint:gochecknoglobals // constant-like lookup table
var identifierQuotes = map[byte]byte{'"': '"', '`': '`', '[': ']'}

// quotedIdentifier reads the body of a quoted identifier, stopping at the
// closing quote and reading a doubled one as a single character of the name.
// Brackets have no doubled form, and opening and closing differ there, so the
// first closing bracket ends the name.
func quotedIdentifier(s string, opening, closing byte) string {
	var name strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != closing {
			name.WriteByte(s[i])
			continue
		}
		if opening == closing && i+1 < len(s) && s[i+1] == closing {
			name.WriteByte(closing)
			i++
			continue
		}
		return name.String()
	}
	// An identifier that is never closed is not one.
	return ""
}

// skipToStatement drops the whitespace and the comments at the front of s, so
// what is left begins with the first thing the statement says. A comment is not
// part of the statement: reading from the first non-space character alone left
// a commented BEGIN looking like no transaction at all, and a commented COMMIT
// leaving one counted open that SQLite had already ended.
func skipToStatement(s string) string {
	for {
		s = strings.TrimLeft(s, " \t\r\n\v\f")
		switch {
		case strings.HasPrefix(s, "--"):
			end := strings.IndexByte(s, '\n')
			if end < 0 {
				// A line comment with no line after it is the whole statement.
				return ""
			}
			s = s[end+1:]
		case strings.HasPrefix(s, "/*"):
			end := strings.Index(s[2:], "*/")
			if end < 0 {
				// SQLite reads an unterminated block comment to the end.
				return ""
			}
			s = s[2+end+2:]
		default:
			return s
		}
	}
}

func isASCIILetter(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// isIdentifierByte reports whether b may appear in an identifier written
// without quotes. SQLite takes a letter, a digit, an underscore, a dollar sign,
// and any byte outside ASCII, which is what lets a name be written in another
// script.
func isIdentifierByte(b byte) bool {
	return isASCIILetter(b) || (b >= '0' && b <= '9') || b == '_' || b == '$' || b >= 0x80
}

// guardedStmt is a prepared statement running under the same rules as one run
// directly. database/sql runs a prepared statement against the driver statement
// rather than against the connection, so without this a prepared BEGIN would
// open a transaction the connection knows nothing about and a prepared query
// would hold a cursor the gate never queued behind.
type guardedStmt struct {
	stmt driver.Stmt
	conn *guardedConn
	sql  string
}

// Close implements driver.Stmt.
func (s *guardedStmt) Close() error { return s.stmt.Close() }

// NumInput implements driver.Stmt.
func (s *guardedStmt) NumInput() int { return s.stmt.NumInput() }

// Exec implements driver.Stmt.
//
//nolint:staticcheck // database/sql calls ExecContext; this is here for the interface.
func (s *guardedStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.conn.runExec(context.Background(), s.sql, func(context.Context) (driver.Result, error) {
		return s.stmt.Exec(args)
	})
}

// Query implements driver.Stmt.
//
//nolint:staticcheck // database/sql calls QueryContext; this is here for the interface.
func (s *guardedStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.conn.runQuery(context.Background(), s.sql, func(context.Context) (driver.Rows, error) {
		return s.stmt.Query(args)
	})
}

// ExecContext implements driver.StmtExecContext.
func (s *guardedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.runExec(ctx, s.sql, func(ctx context.Context) (driver.Result, error) {
		return s.exec(ctx, args)
	})
}

func (s *guardedStmt) exec(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if e, ok := s.stmt.(driver.StmtExecContext); ok {
		return e.ExecContext(ctx, args)
	}
	//nolint:staticcheck // Backward compatibility with statements that only implement the legacy interface.
	return s.stmt.Exec(plainValues(args))
}

// QueryContext implements driver.StmtQueryContext.
func (s *guardedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.runQuery(ctx, s.sql, func(ctx context.Context) (driver.Rows, error) {
		return s.query(ctx, args)
	})
}

func (s *guardedStmt) query(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if q, ok := s.stmt.(driver.StmtQueryContext); ok {
		return q.QueryContext(ctx, args)
	}
	//nolint:staticcheck // Backward compatibility with statements that only implement the legacy interface.
	return s.stmt.Query(plainValues(args))
}

// txGate is the queue a database's transactions wait in.
//
// SQLite runs one write transaction at a time, and the driver waits for its turn
// inside itself: it registers an sqlite3_unlock_notify callback and blocks on a
// mutex of its own, with no deadline and no context in the path. A second
// transaction therefore waited without a bound, and a caller who had put a
// deadline on the work could not fail it -- the goroutine simply stayed there
// until whatever held the lock let go.
//
// The gate is how this package stays out of that wait. A transaction takes it
// for as long as it runs, so the second transaction queues here, where waiting
// is a channel receive and the caller's context ends it.
//
// Only transactions queue. A statement outside one is left alone on purpose:
// this package cannot tell a transaction that has written from one that has only
// read, so making statements wait would block the ordinary shape of holding a
// transaction open and querying the same database beside it, which works today.
type txGate struct {
	// held carries one token. Taking it is taking the gate.
	held chan struct{}
}

func newTxGate() *txGate {
	return &txGate{held: make(chan struct{}, 1)}
}

// acquire takes the gate, or returns the context's error if the wait outlives
// the context. It returns nothing else: a wait this long is a wait on another
// caller's transaction, not a failure of the database.
func (g *txGate) acquire(ctx context.Context) error {
	// A caller who has already given up does not get the gate, even a free one:
	// taking it would open a transaction nobody is waiting for the result of.
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case g.held <- struct{}{}:
		return nil
	default:
	}
	select {
	case g.held <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release gives the gate back.
func (g *txGate) release() {
	select {
	case <-g.held:
	default:
	}
}
