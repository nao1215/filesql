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
// in mysql_functions.go and mysql_time.go and the PostgreSQL-only ones in
// postgresql_functions.go, registered under the names each engine
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
// GoogleSQL's own file holds the two kinds of name it needs. Some are BigQuery
// functions SQLite has nothing like -- CONTAINS_SUBSTR, EDIT_DISTANCE,
// IEEE_DIVIDE, the base32 pair. The rest are names another dialect here already
// means something else by: BigQuery's MD5 and SHA1 answer bytes where
// PostgreSQL's MD5 and MySQL's SHA1 answer hexadecimal text, so TO_HEX(MD5(x))
// -- the spelling BigQuery's documentation uses to print a digest -- hexed the
// hex; and DATE, DATETIME and TIME are constructors where SQLite has functions
// of those names that read a time value and modifiers, so DATE(2024, 3, 5)
// found nothing it could read and answered NULL. Its TIME family needs
// arithmetic of its own for the same reason MySQL's does: a BigQuery TIME is a
// time of day, so TIME_ADD wraps around midnight rather than moving to another
// day.
//
// BigQuery's SAFE. prefix is a wrapper rather than a name: SAFE.f(args) becomes
// safe_call('f', args), which runs f and answers NULL where f could not compute
// a value from what it was given. It runs after the call pass, so what it wraps
// is whatever that pass turned the call into. Two kinds of failure are still
// reported rather than answered with a NULL, because neither is about the data:
// a name nothing here defines, and an argument count the function does not
// have. A function SQLite computes itself is out of reach from a user-defined
// function, so the prefix is dropped there and the call runs as written --
// those are the ones that do not raise in the first place.
//
// TO_CHAR has a file of its own for the same kind of reason: its template is a
// language rather than a set of names, and it cannot be translated into a Go
// layout string. A Go layout has one spelling per field, so MONTH, Month and
// month would be one answer where PostgreSQL gives three, it has no fixed-width
// form for the nine columns PostgreSQL pads a day or a month name to, and a
// pattern with no Go equivalent has to be copied out as literal text, which is
// how TO_CHAR(d, 'DDD') answered "05D". postgresql_template.go scans the
// template into its elements and renders one at a time, for the numeric
// templates as well as the date ones, and TO_DATE and TO_TIMESTAMP read their
// templates through the same scanner.
//
// What the translation cannot reach is SQLite's type system. SQLite has five
// storage classes -- NULL, INTEGER, REAL, TEXT and BLOB -- and so no boolean,
// no interval, no array and no arbitrary-precision numeric, and a construct
// whose answer is one of those has nowhere to land. A comparison answers 1 or 0 rather than true or false; an
// INTERVAL literal is translatable only as the right operand of date
// arithmetic, and anywhere else it is refused with ErrUnsupportedSyntax rather
// than passed on to fail as a syntax error naming something else; an array
// literal and the set-returning functions are refused for the same reason; and
// the functions whose answer is one of those types -- PostgreSQL's AGE,
// JUSTIFY_DAYS, REGEXP_MATCH, SCALE and TRIM_SCALE, and BigQuery's ARRAY_AGG,
// APPROX_QUANTILES and the rest of its array-returning aggregates -- are not
// implemented, since there would be no value to return. A time zone is the
// other thing there is no type for, so BigQuery's forms that take one are
// refused rather than answered unshifted, which would be a different instant.
//
// The clock follows from the same absence. NOW, CURDATE, CURTIME,
// UNIX_TIMESTAMP, PostgreSQL's now and its two fixed siblings, and BigQuery's
// CURRENT_DATETIME all read UTC, which is what SQLite's own CURRENT_TIMESTAMP
// answers and what BigQuery answers when no zone is named; MySQL and
// PostgreSQL answer a session zone that has no counterpart here. Each of them
// is fixed at the start of the statement, so every row of one result carries
// the same reading. PostgreSQL's clock_timestamp and timeofday are the
// exception and advance while the statement runs, which is what they are for.
//
// One consequence of that has no error to report it: subtracting one timestamp
// from another is an interval in PostgreSQL and an ordinary subtraction to
// SQLite, so "ts2 - ts1" answers a number rather than a span, and for two
// dates in the same year it answers 0. The operands are columns as often as
// literals and a token rewrite cannot see their types, so there is no shape to
// match on; DATE_PART and DATE_DIFF are the ways to ask this question here.
//
// Comparison is the other place the engine has the last word. "=", IN, BETWEEN,
// ORDER BY, DISTINCT and GROUP BY compare inside SQLite, so a string and a
// number are different values here where PostgreSQL coerces them, and '5' = 5
// is false.
//
// The SQLite dialect is the identity translation: Translate returns the input
// unchanged.
package dialect
