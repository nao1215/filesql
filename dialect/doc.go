// Package dialect translates SQL written in a non-SQLite dialect (MySQL,
// PostgreSQL, or GoogleSQL) into SQLite SQL so it can run against the SQLite
// engine that backs filesql. Storage is always SQLite; only the query text a
// caller supplies is translated.
//
// The translation is best-effort compatibility, not a full emulator. It handles
// three classes of input:
//
//   - Known incompatibilities that have a SQLite equivalent are rewritten (for
//     example MySQL's backtick identifiers become double-quoted identifiers, and
//     PostgreSQL's "expr::type" becomes "CAST(expr AS type)").
//   - Known constructs with no SQLite equivalent (QUALIFY, ARRAY/STRUCT types,
//     LATERAL, DISTINCT ON, ...) are rejected with ErrUnsupportedSyntax so the
//     caller sees a clear error instead of a confusing engine message.
//   - Anything else is passed through unchanged and left to SQLite to accept or
//     reject.
//
// Function gaps (NOW, DATE_FORMAT, TO_CHAR, SPLIT_PART, SAFE_DIVIDE, ...) are
// filled by user-defined functions registered into the SQLite driver via
// RegisterFunctions rather than by rewriting the SQL.
//
// The SQLite dialect is the identity translation: Translate returns the input
// unchanged.
package dialect
