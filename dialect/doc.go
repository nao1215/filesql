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
//   - Known constructs with no SQLite equivalent (QUALIFY, arrays, STRUCT
//     types, LATERAL, DISTINCT ON, PostgreSQL's set-returning functions,
//     MySQL's XOR, ...) are rejected with ErrUnsupportedSyntax so the caller
//     sees a clear error instead of a confusing engine message: an array
//     literal reached SQLite as identifier quoting and came back as "no such
//     column: 1,2,3", and generate_series as "no such table", each naming
//     something the query never wrote.
//   - Anything else is passed through unchanged and left to SQLite to accept or
//     reject.
//
// Function gaps (NOW, DATE_FORMAT, TO_CHAR, SPLIT_PART, SAFE_DIVIDE, DIV,
// WIDTH_BUCKET, ...) are
// filled by user-defined functions registered into the SQLite driver via
// RegisterFunctions rather than by rewriting the SQL. The MySQL-only ones are
// in mysql_functions.go and mysql_time.go, registered under the names MySQL
// gives them: a name
// that is not a SQLite keyword and that no other dialect means something else
// by needs no rewrite, and keeping the call text as the query wrote it keeps
// the result column's name with it. STRCMP is the one whose answer is an
// approximation rather than a match: it folds case, which MySQL's default
// collation does, and not accents, which that collation also does.
//
// The TIME functions have a file of their own because a MySQL TIME is not a
// point on a clock. It is a signed span running from -838:59:59 to 838:59:59,
// so SEC_TO_TIME answers 100:00:00 for 360000 and TIME_FORMAT prints an hour
// field of three digits with a sign in front of the whole result, none of which
// a time.Time holds. What that file cannot carry is MySQL's fractional-seconds
// precision, which comes from the type of each argument rather than its value:
// MySQL prints SEC_TO_TIME('3661') as 01:01:01.000000 and this prints
// 01:01:01, because SQLite has no type to take the six from.
//
// Arithmetic reaches the same helpers by the other route, rewriting the
// operator into a call, where a dialect disagrees with SQLite about the answer.
// Division disagrees twice over: MySQL and GoogleSQL divide two integers into a
// real where SQLite and PostgreSQL answer an integer, and a zero divisor raises
// in PostgreSQL and GoogleSQL where SQLite and MySQL answer NULL. So "/" is
// rewritten for all three dialects and the remainder for the two that raise,
// with GoogleSQL's SAFE_DIVIDE left as the way to ask for the NULL.
//
// Casts go through the same mechanism for a different reason. SQLite's own CAST
// applies type affinity, which is close enough to look right and different
// enough to be wrong: it truncates where the dialects round, and it coerces a
// value the target type cannot represent instead of rejecting it. Each dialect
// therefore rewrites CAST (and PostgreSQL's "::", and GoogleSQL's SAFE_CAST)
// into a helper that converts with that dialect's rules and returns
// ErrInvalidCast for a value it cannot represent.
//
// A name SQLite shares with a dialect needs the same treatment when the two mean
// different things by it, since the call runs and answers a plausible value.
// LOG is the natural logarithm in MySQL and GoogleSQL and the base-ten one in
// SQLite, MySQL's FORMAT groups a number where SQLite's is printf,
// PostgreSQL's to_hex converts an integer where GoogleSQL's hexes bytes,
// MySQL's QUOTE escapes with a backslash where SQLite doubles the quote, its
// ASCII answers a byte where PostgreSQL's answers a code point, and LEFT, RIGHT
// and REGEXP_REPLACE each read a negative length or a fourth argument their own
// way. Those calls are rewritten to a helper named for the dialect rather than
// left to the name SQLite already has. GoogleSQL's FORMAT prints its own %t and
// %T verbs there; a boolean reaches the helper as the integer SQLite stores, so
// it prints as 0 or 1.
//
// Collation is part of what a call means. MySQL's default collation folds case,
// so its LIKE and its REGEXP both match a letter in either case, and both are
// routed to helpers that do. The operators are left alone: "=", IN, BETWEEN,
// ORDER BY, DISTINCT and GROUP BY compare inside the engine, where a token
// rewrite cannot reach every one of them, so under the MySQL dialect they
// compare the way SQLite does and 'abc' = 'ABC' is false here and true in MySQL.
//
// Bit operations have a ceiling this package cannot lift. MySQL computes them on
// an unsigned 64-bit value, and SQLite has no unsigned 64-bit integer to answer
// with. The shifts are rewritten because their bits genuinely differ: SQLite's
// ">>" copies the sign bit where MySQL brings in zeros, and SQLite reads a
// negative shift count as a shift the other way. What stays different is only
// how a result with its top bit set is spelled: MySQL prints ~0 as
// 18446744073709551615 and this prints -1, the same bits under the only reading
// SQLite has for them.
//
// Lexing is per dialect, because what counts as a string, an identifier, a
// comment, or an escape differs between them: a double-quoted literal is a
// string in MySQL and an identifier in PostgreSQL, block comments nest in
// PostgreSQL and not elsewhere, and only some dialects decode an escape that
// names a character by its number. Reading a literal by the wrong dialect's
// rules is the worst kind of failure this package can have, because the
// translation succeeds and the query answers from a value the caller never
// wrote, so each of those rules is configured rather than assumed.
//
// The SQLite dialect is the identity translation: Translate returns the input
// unchanged.
package dialect
