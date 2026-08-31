// Package dialect translates SQL written in a non-SQLite dialect (MySQL,
// PostgreSQL, or GoogleSQL) into SQLite SQL so it can run against the SQLite
// engine that backs filesql. Storage is always SQLite; only the query text a
// caller supplies is translated.
//
// # Translation model
//
// A query is read into a syntax tree, the tree is rewritten into one SQLite can
// execute, and the result is written back out:
//
//	source SQL -> lexer -> parser -> syntax tree
//	           -> dialect lowering -> SQLite syntax tree
//	           -> renderer -> SQLite SQL
//
// Operator precedence is decided once, in the parser, from one table per
// dialect. The lowering rules work on the tree rather than on text, so no rule
// depends on another having run first, and every diagnostic carries the line
// and column of the construct it is about.
//
// A construct becomes one of three things:
//
//   - What SQLite spells differently is rewritten into SQLite's spelling:
//     MySQL's backtick identifiers become double-quoted identifiers, and
//     PostgreSQL's "expr::type" becomes a call to a conversion helper.
//   - What SQLite cannot spell but a function can compute becomes a call to a
//     helper this package registers with the driver through RegisterFunctions.
//   - What is neither is refused. ErrUnsupportedSyntax says the construct has
//     no SQLite form; ErrUnsupportedFeature says it is outside the subset below;
//     ErrInvalidSyntax says the query could not be read at all.
//
// Nothing is forwarded to SQLite untranslated. What this package accepts is
// what it has been taught, not what SQLite happens to tolerate: a construct
// outside the subset is refused with a message naming it, rather than reaching
// the engine to fail there under a name the caller never wrote.
//
// # Supported SQL
//
// The subset is the whole of the contract. Queries:
//
//   - SELECT with DISTINCT, a select list with aliases, FROM, WHERE, GROUP BY,
//     HAVING, WINDOW, ORDER BY (with ASC, DESC, NULLS FIRST and NULLS LAST) and
//     LIMIT with OFFSET, in either the LIMIT or the FETCH FIRST spelling.
//   - WITH and WITH RECURSIVE, and VALUES standing as a query.
//   - UNION, INTERSECT and EXCEPT, with or without ALL.
//   - Table references: a name, a subquery, a table-valued call, and the joins
//     -- inner, left, right, full, cross and natural -- with ON or USING.
//
// Expressions: literals, qualified names, bind parameters, the arithmetic,
// comparison, logical, bitwise, string and pattern operators of each dialect,
// IS and IS NOT, IS DISTINCT FROM, BETWEEN, IN, EXISTS, scalar subqueries, row
// constructors, CASE, CAST and each dialect's own cast spelling, COLLATE,
// INTERVAL, typed literals such as DATE '2024-01-01', function and aggregate
// calls with DISTINCT, ORDER BY, FILTER and OVER, and the SQL-standard calls
// whose arguments keywords separate: EXTRACT, SUBSTRING, POSITION, TRIM and
// OVERLAY.
//
// Statements: INSERT (with VALUES, a query, DEFAULT VALUES, ON CONFLICT and
// RETURNING), UPDATE, DELETE, CREATE TABLE, CREATE VIEW, CREATE INDEX, DROP,
// the four ALTER TABLE forms SQLite has (RENAME TO, RENAME COLUMN, ADD COLUMN
// and DROP COLUMN), BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, EXPLAIN,
// PRAGMA and ANALYZE.
//
// Everything else is refused: the statements that address a server rather than
// a database (GRANT, LOCK TABLES, SHOW, USE, SET, FLUSH), the objects SQLite
// does not have (sequences, materialized views, functions, procedures,
// triggers, databases), and the DDL that changes a column's type.
//
// # What is dropped
//
// Comments do not reach the output. They carry nothing SQLite acts on, and the
// tree does not model them, so a translated query is the statement without
// them. The clauses that ask for a physical layout rather than an answer are
// dropped the same way: MySQL's ENGINE, CHARSET and COMMENT table options and
// its index hints, GoogleSQL's OPTIONS and CLUSTER BY, PostgreSQL's UNLOGGED
// and CONCURRENTLY, and the CASCADE or RESTRICT of a DROP. The rows are the
// same without them.
//
// # Column names
//
// SQLite names an unaliased result column after the text of the expression that
// produced it, so lowering an expression would rename the caller's column. A
// select item whose text changed therefore carries its original text as an
// alias: "SELECT amt::text" answers a column called "amt::text", not one called
// "postgresql_cast(amt, 'text')".
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
// The TIME functions need rules of their own because a MySQL TIME is not a
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
// rewritten for all three dialects, with GoogleSQL's SAFE_DIVIDE left as the
// way to ask for the NULL. The remainder is rewritten for all three as well,
// because SQLite truncates both operands to integers before taking it and
// answers 1 for 7.5 % 2 where every dialect here answers 1.5. MySQL spells the
// same operation three ways -- "%", the MOD operator and MOD() -- and all three
// reach one helper so they cannot disagree.
//
// Rounding is the other arithmetic that differs. MySQL and PostgreSQL break a
// tie toward the even neighbor for a floating-point argument, so ROUND(2.5) is
// 2 and ROUND(3.5) is 4, where SQLite and BigQuery round away from zero. Every
// non-integer SQLite holds is a floating-point value, so the even rule is the
// one a REAL column loaded from a file gets in either engine; what it cannot
// reproduce is a decimal literal written in the query, which MySQL reads as an
// exact decimal and rounds away from zero.
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
// How a REAL is written as text belongs to a dialect too, because every
// non-integer SQLite holds is a double and the engines do not spell one the
// same way. Under the MySQL dialect a string function writes MySQL's spelling,
// so CONCAT of a column holding 1e308 with an empty string answers 1e308 where
// PostgreSQL writes 1e+308. The calls that stay on a function SQLite answers
// itself, where only the conversion in front of it differs, have their argument
// wrapped in mysql_text so that they read the same value: TRIM, LTRIM, RTRIM,
// LOCATE, INSTR, CONCAT_WS, GROUP_CONCAT, LENGTH and CHAR_LENGTH.
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
// wrote, so each of those rules is configured rather than assumed. Two of them
// are easy to read as shared and are not: MySQL opens a line comment on a
// double dash only when a blank or a control character follows it, so SELECT
// 1--1 there is one minus negative one rather than a statement with its tail
// dropped, and a backtick-quoted identifier takes the string escape sequences
// in GoogleSQL, an escaped backtick among them, where MySQL has none and
// doubles the backtick instead.
//
// One byte a literal may hold cannot appear in SQL text. SQLite reads a
// statement up to the first NUL, so a statement carrying one ends there, and
// the MySQL and GoogleSQL escape \0 decodes to that byte; a string literal or
// a quoted name holding it is refused with ErrUnsupportedSyntax. The one
// construct that would parse, a cast from a blob, does not answer the same
// thing, since SQLite's length stops at a NUL where MySQL's does not. Every
// other control character those escapes produce is written out unchanged.
//
// GoogleSQL needs two kinds of name. Some are BigQuery
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
// TO_CHAR needs a scanner of its own for the same kind of reason: its template
// is a language rather than a set of names, and it cannot be translated into a Go
// layout string. A Go layout has one spelling per field, so MONTH, Month and
// month would be one answer where PostgreSQL gives three, it has no fixed-width
// form for the nine columns PostgreSQL pads a day or a month name to, and a
// pattern with no Go equivalent has to be copied out as literal text, which is
// how TO_CHAR(d, 'DDD') answered "05D". The template is scanned into its
// elements and rendered one at a time, for the numeric
// templates as well as the date ones, and TO_DATE and TO_TIMESTAMP read their
// templates through the same scanner.
//
// What the translation cannot reach is SQLite's type system. SQLite has five
// storage classes -- NULL, INTEGER, REAL, TEXT and BLOB -- and so no boolean,
// no interval, no array and no arbitrary-precision numeric, and a construct
// whose answer is one of those has nowhere to land. A comparison answers 1 or 0 rather than true or false; an
// INTERVAL literal is translatable only as the right operand of date
// arithmetic, and anywhere else it is refused with ErrUnsupportedSyntax; an array
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
// the same reading and so does every place the statement names one of them.
// PostgreSQL's clock_timestamp and timeofday are the exception and advance
// while the statement runs, which is what they are for.
//
// One consequence of that has no error to report it: subtracting one timestamp
// from another is an interval in PostgreSQL and an ordinary subtraction to
// SQLite, so "ts2 - ts1" answers a number rather than a span, and for two
// dates in the same year it answers 0. The operands are columns as often as
// literals and nothing in the query text says what type a column holds, so
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
