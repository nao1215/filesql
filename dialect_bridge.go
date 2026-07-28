package filesql

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/nao1215/filesql/dialect"
	"modernc.org/sqlite"
)

// dialectConnector is a driver.Connector that opens connections to a
// shared-cache in-memory SQLite database and wraps each so queries written in a
// non-SQLite dialect are translated to SQLite before execution.
//
// drv is the SQLite driver instance the dialect helper functions were registered
// on (the driver registered under the "sqlite" name). Using it — rather than a
// fresh &sqlite.Driver{} — is what makes the registered UDFs visible on the
// connections opened here.
type dialectConnector struct {
	drv        driver.Driver
	dsn        string
	sqlDialect dialect.Dialect
}

// Connect opens a new connection to the shared-cache database and wraps it with
// dialect translation.
func (c *dialectConnector) Connect(_ context.Context) (driver.Conn, error) {
	conn, err := c.driver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &dialectConnection{conn: conn, sqlDialect: c.sqlDialect}, nil
}

// Driver returns the underlying SQLite driver.
func (c *dialectConnector) Driver() driver.Driver {
	return c.driver()
}

// driver returns the configured driver, falling back to a fresh SQLite driver if
// none was supplied.
func (c *dialectConnector) driver() driver.Driver {
	if c.drv != nil {
		return c.drv
	}
	return &sqlite.Driver{}
}

// dialectConnection wraps a SQLite driver connection and translates query text
// from the configured dialect to SQLite at prepare time.
//
// It deliberately implements only Prepare/PrepareContext (plus transaction and
// close plumbing) and not the Execer/Queryer fast paths, so every statement is
// translated exactly once as it is prepared. Translation output is already
// SQLite, so translating it a second time (as an Execer fallback could) must not
// happen.
type dialectConnection struct {
	conn       driver.Conn
	sqlDialect dialect.Dialect
}

// translate converts a query from the connection's dialect into SQLite.
func (c *dialectConnection) translate(query string) (string, error) {
	translated, err := dialect.Translate(c.sqlDialect, query)
	if err != nil {
		return "", fmt.Errorf("filesql: translate %s query: %w", c.sqlDialect, err)
	}
	return translated, nil
}

// Prepare translates query and prepares it on the underlying connection.
func (c *dialectConnection) Prepare(query string) (driver.Stmt, error) {
	translated, err := c.translate(query)
	if err != nil {
		return nil, err
	}
	return c.conn.Prepare(translated)
}

// PrepareContext translates query and prepares it with context support.
func (c *dialectConnection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	translated, err := c.translate(query)
	if err != nil {
		return nil, err
	}
	if p, ok := c.conn.(driver.ConnPrepareContext); ok {
		return p.PrepareContext(ctx, translated)
	}
	return c.conn.Prepare(translated)
}

// Begin starts a transaction on the underlying connection.
func (c *dialectConnection) Begin() (driver.Tx, error) {
	//nolint:staticcheck // Backward compatibility with the legacy driver interface.
	return c.conn.Begin()
}

// BeginTx starts a transaction with options on the underlying connection.
func (c *dialectConnection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if b, ok := c.conn.(driver.ConnBeginTx); ok {
		return b.BeginTx(ctx, opts)
	}
	//nolint:staticcheck // Backward compatibility with the legacy driver interface.
	return c.conn.Begin()
}

// Close closes the underlying connection.
func (c *dialectConnection) Close() error {
	return c.conn.Close()
}
