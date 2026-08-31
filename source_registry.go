package filesql

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	achconv "github.com/nao1215/filesql/parser/ach"
	wireconv "github.com/nao1215/filesql/parser/wire"
)

// sourceTableName is the reserved table recording where a write-back format was
// loaded from.
//
// ACH and Fedwire cannot be rebuilt from their SQL tables alone: the writer
// copies the original file and applies the edits to it, because fields no table
// exposes exist only there. The database therefore has to remember its own
// sources. Holding that in the database rather than in a process-global map is
// what keeps two databases loaded from same-named files apart, and lets a
// rolled-back load discard the metadata with the tables.
const sourceTableName = "_filesql_sources"

// sourceTablePrefix is reserved for this package's own bookkeeping tables.
// A caller's table cannot occupy it; see validateTableName.
const sourceTablePrefix = "_filesql_"

// sqliteTablePrefix is reserved by SQLite for its own tables. A caller's table
// cannot occupy it either, and refusing it here is what turns that library's
// "object name reserved for internal use" into an answer a caller can match.
const sqliteTablePrefix = "sqlite_"

// sourceTableLikePattern matches sourceTablePrefix in a LIKE clause, so those
// tables stay hidden from callers and from dumps. The underscores are escaped
// because LIKE reads a bare underscore as a wildcard, which would also hide a
// caller's table named, say, xfilesqly_totals.
const sourceTableLikePattern = `\_filesql\_%`

// sourceFormat names the reader that can rebuild a file's structure.
type sourceFormat string

const (
	sourceFormatACH     sourceFormat = "ach"
	sourceFormatFedWire sourceFormat = "fedwire"
)

// recordFileSource remembers that baseTableName was loaded from sourcePath.
//
// It runs on the same dbtx that created the tables, so a caller-owned
// transaction commits or discards both together. An empty sourcePath records
// nothing: a load from an io.Reader has no file to go back to.
func recordFileSource(ctx context.Context, db dbtx, baseTableName, sourcePath string, format sourceFormat) error {
	if sourcePath == "" {
		return nil
	}

	// The path is resolved now because the process may change directory between
	// load and dump, and a relative path would then name a different file.
	absPath, err := filepath.Abs(sourcePath)
	if err != nil {
		return fmt.Errorf("%w: failed to resolve source path %s: %w", ErrIOOperation, sourcePath, err)
	}

	// The key is the pair, not the name alone: payment.ach and payment.fed make
	// tables that do not collide, so both can be loaded into one database, and
	// keying by name alone would let the second erase the first's source.
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS "`+sourceTableName+`" (
		base_table_name TEXT NOT NULL,
		source_path TEXT NOT NULL,
		format TEXT NOT NULL,
		PRIMARY KEY (base_table_name, format)
	)`); err != nil {
		return fmt.Errorf("%w: failed to create %s: %w", ErrDatabaseOperation, sourceTableName, err)
	}

	// Reloading a file replaces its tables, so its row is replaced too.
	if _, err := db.ExecContext(ctx,
		`INSERT OR REPLACE INTO "`+sourceTableName+`" (base_table_name, source_path, format) VALUES (?, ?, ?)`,
		baseTableName, absPath, string(format),
	); err != nil {
		return fmt.Errorf("%w: failed to record source of %s: %w", ErrDatabaseOperation, baseTableName, err)
	}

	return nil
}

// fileSourcePath returns the file baseTableName was loaded from, or false when
// the database holds no such record.
func fileSourcePath(ctx context.Context, db *sql.DB, baseTableName string, format sourceFormat) (string, bool) {
	var path string
	err := db.QueryRowContext(ctx,
		`SELECT source_path FROM "`+sourceTableName+`" WHERE base_table_name = ? AND format = ?`,
		baseTableName, string(format),
	).Scan(&path)
	if err != nil {
		// A database that loaded no write-back format has no such table, so a
		// missing table is not distinguished from a missing row.
		return "", false
	}
	return path, true
}

// fileSourceBaseNames lists the base table names loaded from files of format,
// in a stable order so repeated dumps write the same set of files.
func fileSourceBaseNames(ctx context.Context, db *sql.DB, format sourceFormat) []string {
	rows, err := db.QueryContext(ctx,
		`SELECT base_table_name FROM "`+sourceTableName+`" WHERE format = ? ORDER BY base_table_name`,
		string(format),
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil
		}
		names = append(names, name)
	}
	if rows.Err() != nil {
		return nil
	}
	return names
}

// achTableSetForDump rebuilds the ACH structure of baseTableName by reading its
// source file again.
func achTableSetForDump(ctx context.Context, db *sql.DB, baseTableName string) (*achconv.TableSet, error) {
	path, ok := fileSourcePath(ctx, db, baseTableName, sourceFormatACH)
	if !ok {
		return nil, fmt.Errorf("%w: no ACH source recorded for base table name %q; load the file with Open() or Builder from a path, or pass the structure to DumpACHWithTableSet", ErrSourceUnavailable, baseTableName)
	}

	file, err := openRegularFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot read the ACH file %s that %q was loaded from: %w", ErrSourceUnavailable, path, baseTableName, err)
	}
	defer file.Close()

	tableSet, err := achconv.ParseReader(file)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to parse the ACH file %s that %q was loaded from: %w", ErrACH, path, baseTableName, err)
	}
	return tableSet, nil
}

// wireTableSetForDump rebuilds the Fedwire structure of baseTableName by
// reading its source file again.
func wireTableSetForDump(ctx context.Context, db *sql.DB, baseTableName string) (*wireconv.TableSet, error) {
	path, ok := fileSourcePath(ctx, db, baseTableName, sourceFormatFedWire)
	if !ok {
		return nil, fmt.Errorf("%w: no Fedwire source recorded for base table name %q; load the file with Open() or Builder from a path, or pass the structure to DumpFedWireWithTableSet", ErrSourceUnavailable, baseTableName)
	}

	file, err := openRegularFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot read the Fedwire file %s that %q was loaded from: %w", ErrSourceUnavailable, path, baseTableName, err)
	}
	defer file.Close()

	tableSet, err := wireconv.ParseReader(file)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to parse the Fedwire file %s that %q was loaded from: %w", ErrWire, path, baseTableName, err)
	}
	return tableSet, nil
}

// validateTableName refuses a table name in this package's reserved namespace.
//
// The prefix is only reserved if nothing else can occupy it. Hiding _filesql_
// tables from dumps and listings while still loading a file named
// _filesql_report.csv into one would make that file's table exist and be
// queryable but absent from everything that enumerates tables — the kind of
// half-present table a caller cannot debug. SQLite answers the same way for its
// own sqlite_ prefix, so the rule and its message follow that precedent.
//
// The comparison ignores ASCII case because the LIKE that hides these tables
// does: without that, _FILESQL_report loaded and then vanished from every
// listing, which is the state this check exists to prevent.
//
// SQLite's own sqlite_ prefix is refused here for the same reason it is cited
// above. Left to the database, it surfaced as a raw "object name reserved for
// internal use" wrapped in a database-operation error, after the file had
// already been read, and no caller could tell it from a broken database.
func validateTableName(tableName string) error {
	if hasPrefixFold(tableName, sourceTablePrefix) {
		return fmt.Errorf("%w: %q begins with %s, which this package keeps for its own tables; a table under it would be hidden from dumps and from table listings",
			ErrReservedTableName, tableName, sourceTablePrefix)
	}
	if hasPrefixFold(tableName, sqliteTablePrefix) {
		return fmt.Errorf("%w: %q begins with %s, which SQLite keeps for its own tables; creating it fails there whatever this package does with it",
			ErrReservedTableName, tableName, sqliteTablePrefix)
	}
	return nil
}

// hasPrefixFold reports whether tableName starts with prefix, folding ASCII case
// only. SQLite's LIKE and its identifier matching both fold exactly that much,
// so matching it here keeps the set of refused names equal to the set the
// database treats as taken.
func hasPrefixFold(tableName, prefix string) bool {
	if len(tableName) < len(prefix) {
		return false
	}
	return strings.EqualFold(tableName[:len(prefix)], prefix)
}
