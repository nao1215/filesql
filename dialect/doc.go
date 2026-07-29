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
// Casts go through the same mechanism for a different reason. SQLite's own CAST
// applies type affinity, which is close enough to look right and different
// enough to be wrong: it truncates where the dialects round, and it coerces a
// value the target type cannot represent instead of rejecting it. Each dialect
// therefore rewrites CAST (and PostgreSQL's "::", and GoogleSQL's SAFE_CAST)
// into a helper that converts with that dialect's rules and returns
// ErrInvalidCast for a value it cannot represent.
//
// The SQLite dialect is the identity translation: Translate returns the input
// unchanged.
package dialect
