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
// the engine to fail there under a name the caller never wrote. A call whose
// argument count no form of the function accepts is refused here too, since it
// would otherwise fail under the helper's name.
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
// triggers, databases), and the DDL that changes a column's type. A value or a
// clause SQLite has no form for is refused the same way and by name: DEFAULT
// standing where a value goes, since SQLite has no way to write a column's own
// default into a row; AT TIME ZONE, since it converts between zones and SQLite
// keeps no zone with a timestamp; AND CHAIN on a COMMIT or a ROLLBACK;
// INHERITS on a CREATE TABLE; the LIKE that copies a table, in either
// dialect's spelling of it; and an ORDER BY or a LIMIT on an UPDATE or a
// DELETE, since the SQLite build behind this package takes neither there. An
// ALTER TABLE is refused the same way when it asks for what SQLite cannot
// make: more than one change in one statement, a column placed with FIRST or AFTER, a
// change skipped with IF EXISTS or IF NOT EXISTS. Each is refused by name and
// as unsupported, since the statement was read: ErrInvalidSyntax is for one that
// could not be read.
//
// # Casts
//
// A cast reaches the helper that converts to its target, and a target this
// package does not convert to is refused by name. Leaving one to SQLite's own
// CAST is not leaving it alone: SQLite applies numeric affinity to a type it
// has never heard of, so the value comes back as the number its leading digits
// spell, and every engine here raises for a type it does not have. The targets
// each dialect converts to are its own type names, including the long
// spellings -- character varying, double precision, timestamp without time
// zone -- and the types SQLite holds as text, such as PostgreSQL's inet, cidr,
// macaddr and xml. A length written on the target applies, so a cast to
// character varying(2) is two characters.
//
// # Functions a dialect has and SQLite has not
//
// A function is translated where SQLite has a form for it and refused by name
// where it has none, rather than handed to SQLite under a name only the source
// dialect knows: "no such function" tells a caller that a name they did write
// does not exist, which is a worse answer than saying the construct has no
// SQLite form. What is refused is a result that is an array or a range, a JSON
// operation SQLite's own functions have no shape for, an encoding conversion
// where SQLite holds only UTF-8, a geography, a fact about the connection or
// the server -- in the parenthesized spelling and in the bare one the standard
// writes without parentheses -- and an effect rather than a value. A value that
// is not the same twice is answered where SQLite has a form for it -- RAND,
// random and GENERATE_UUID -- and refused where it has none, as RANDOM_BYTES,
// UUID, UUID_SHORT and RANDOM_NORMAL are.
//
// The names with neither a translation nor a refusal are written down rather
// than left to be discovered: dialect/testdata/untranslated_postgresql.txt
// lists the PostgreSQL functions that still reach SQLite, and a test fails both
// when a name joins them and when a name in it gains an answer, so the list can
// only shrink. MySQL has no name left without an answer among the names its own
// help tables report.
//
// A name SQLite itself provides still reaches SQLite under every dialect: what
// is refused is the names one engine has and SQLite has not, rather than every
// name this package does not rewrite.
//
// Which names those are is checked against the engines rather than collected by
// hand. A test reads what MySQL's help tables and PostgreSQL's pg_proc say each
// of them defines, and requires that every one of those names is translated
// here or refused here. The lists were swept by hand once and a sweep is only
// as complete as the person running it: the one behind these tables walked the
// scalar functions and never reached the aggregates. BigQuery publishes no such
// catalog, so GoogleSQL's table is hand-written and nothing checks it this way.
//
// # Booleans
//
// SQLite has no boolean, and every dialect here has one. TRUE is the integer 1
// once it reaches SQLite, so a boolean written as a literal is read while the
// query is translated and one that is computed is not: a value returned from a
// registered function reaches the next one as an integer, and a comparison, a
// column and a nested cast are all computed. What the literal buys is the word
// where each dialect writes a word. PostgreSQL casts a boolean to text as
// "true" or "false", GoogleSQL casts a BOOL to STRING the same way and prints
// it so under FORMAT's %t and %T, and MySQL and GoogleSQL both write the JSON
// boolean into a document, so JSON_ARRAY(TRUE) is [true] and
// TO_JSON_STRING(TRUE) is true. MySQL's cast to CHAR is the one that stays a
// number, which is what MySQL answers. A boolean that is not a literal answers
// 1 or 0 in all of these, and FORMAT reads a boolean literal only under a
// literal format string with no argument-supplied width.
//
// # Byte strings
//
// SQLite tells a BLOB from text, and that storage class is what each dialect's
// byte string becomes: GoogleSQL's BYTES, MySQL's BINARY, VARBINARY and BLOB,
// and PostgreSQL's bytea. The bitwise operators follow it. MySQL and GoogleSQL
// both apply "&", "|", "^", "<<", ">>" and "~" to a byte string bytewise and
// answer one of the operand's length, and both refuse two operands of
// different lengths; they part company over a negative shift count, which
// GoogleSQL refuses and MySQL reads as unsigned, shifting past the width and
// clearing the operand. A NULL operand answers NULL; any other pair that is
// not two byte strings takes the unsigned 64-bit reading, except under
// GoogleSQL, which refuses a byte string beside an integer because it takes the
// second operand as the same type as the first.
//
// A literal that names bytes is read the way the dialect that wrote it reads
// it. MySQL writes one hexadecimal literal three ways -- 0x41, x'41' and X'41'
// -- and means the number its digits spell where the literal stands beside an
// arithmetic or a bitwise operator or a cast to a number, and the bytes they
// name everywhere else;
// an odd number of digits is padded on the left for the 0x spelling, so 0x4 is
// the byte 0x04, while the quoted spellings take whole bytes as MySQL requires;
// one too long to be an unsigned 64-bit number is refused where a number is
// wanted. The prefix is case sensitive, so 0X41 is refused: MySQL reads that
// spelling as an identifier rather than as a literal. MySQL's bit literal,
// 0b1010 or b'1010', is refused in all its
// spellings: the same two readings apply to it and nothing else here writes
// one. PostgreSQL has no byte-string literal: B'1010' and X'41' are bit
// strings, carried as the text of their binary digits -- X'41' is 01000001 --
// which is what PostgreSQL compares, concatenates and measures, and a cast to
// int, integer, int4, bigint or int8 reads those digits as a base-2 number at
// that target's width.
// PostgreSQL's bytea is written as text and read in both of its input formats:
// a leading backslash-x is hexadecimal, with whitespace allowed between digit
// pairs, and anything else is the escape format, where a backslash and three
// octal digits stand for one byte.
//
// Two limits are worth knowing. A byte string compared with a value of another
// storage class compares by SQLite's rules rather than the source dialect's,
// so a MySQL hexadecimal literal tested against a text column does not match
// the way MySQL matches it; write UNHEX or CAST to bring the two together. And
// a result whose top bit is set comes back as the negative integer carrying
// those bits, because SQLite has no unsigned integer to answer with.
//
// # What is dropped
//
// Comments do not reach the output. They carry nothing SQLite acts on, and the
// tree does not model them, so a translated query is the statement without
// them. The clauses that ask for a physical layout rather than an answer are
// dropped the same way: MySQL's ENGINE, CHARSET and COMMENT table options and
// its index hints, GoogleSQL's OPTIONS and CLUSTER BY, PostgreSQL's UNLOGGED
// and CONCURRENTLY, and the CASCADE or RESTRICT of a dropped table or column.
// The rows are the same without them.
//
// The words that say how to run an ANALYZE rather than what to run it on are
// dropped -- PostgreSQL's VERBOSE and its parenthesized option list, MySQL's
// TABLE, LOCAL and NO_WRITE_TO_BINLOG -- so the object the caller named is the
// one analyzed. So are the INCLUDE columns of an index, which change how it is
// stored and not what a query answers, and the WITH CHECK OPTION of a view,
// which constrains writes SQLite refuses through a view anyway.
//
// PostgreSQL's "SELECT ... INTO name" is the CREATE TABLE ... AS SELECT SQLite
// spells and becomes it, where the query is the whole statement; MySQL's INTO,
// which fills a file or a session variable, is refused.
//
// PostgreSQL's OVERRIDING SYSTEM VALUE and OVERRIDING USER VALUE are dropped,
// since a table here has no identity column for a given value to override, as
// are the ALL and DISTINCT that stand in front of PostgreSQL's grouping
// elements, which choose between grouping sets a plain list produces one of
// either way, and the AND NO CHAIN that says what a COMMIT already does.
//
// Two spellings are moved rather than dropped: Cloud Spanner writes a returning
// clause as THEN RETURN and its primary key after the table body, and both
// become the form SQLite takes, RETURNING and a table constraint. PostgreSQL's
// IS JSON becomes json_valid, which asks what the predicate asks; the forms
// that narrow it to a kind of value, or ask about repeated keys, are refused.
//
// PostgreSQL's ONLY in front of a table name, and the star after one, say
// whether the tables inheriting from that one are reached as well. Nothing
// inherits from a table here, so each names the table it stands beside and is
// dropped: "SELECT * FROM ONLY t" is a query about t. The word is PostgreSQL's
// alone -- MySQL and GoogleSQL have no ONLY, where "FROM ONLY t" names a table
// called ONLY with the alias t.
//
// # Column names
//
// SQLite names an unaliased result column after the text of the expression that
// produced it, so lowering an expression would rename the caller's column. A
// select item whose text changed therefore carries its original text as an
// alias: "SELECT amt::text" answers a column called "amt::text", not one called
// "postgresql_cast(amt, 'text')". A RETURNING list is a list of result columns
// too and is named the same way.
//
// A column list on a table reference names the columns the reference answers.
// SQLite takes those names from a select list, so a derived table's list is
// moved onto the query it stands in front of: "(SELECT a, b FROM t) s(x, y)"
// becomes "(SELECT a AS x, b AS y FROM t) AS s". Where there is no select list
// to move it onto -- a base table, a table-valued call, a VALUES list, a select
// list that ends in a star, or a count that does not match -- it is refused,
// since dropping it would answer the old names in silence. WITH name (columns)
// AS (...) is the spelling SQLite takes for all of them.
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
// the result column's name with it. DATE is where the second half of that test
// bites: SQLite has a date() of its own that rolls an impossible day forward,
// reads the word "now" and takes modifier arguments, so each dialect's DATE
// goes to a helper rather than to SQLite's. STRCMP is the one whose answer is
// an approximation rather than a match: it folds case, which MySQL's default
// collation does, and not accents, which that collation also does.
//
// The TIME functions need rules of their own because a MySQL TIME is not a
// point on a clock. It is a signed span running from -838:59:59 to 838:59:59,
// so SEC_TO_TIME answers 100:00:00 for 360000 and TIME_FORMAT prints an hour
// field of three digits with a sign in front of the whole result, none of which
// a time.Time holds. What that file cannot carry is a fraction of a second
// MySQL prints from a type rather than from a value: MySQL prints
// SEC_TO_TIME('3661') as 01:01:01.000000 and this prints 01:01:01, because
// SQLite has no type to take the six from. A fraction that is in the value is
// kept. Arithmetic on one answers all six digits, so DATE_ADD with a zero
// interval gives back what it was given, and a helper that converts a value
// rather than moving it keeps the width the value was written with, so
// TIMESTAMP('...59.1') keeps one digit and TIMESTAMP('...59.100000') keeps six.
// PostgreSQL trims the trailing zeros of a fraction and its own helpers do the
// same.
//
// A date the arithmetic moves outside the year range 1 to 9999 is answered as
// NULL rather than as a year of five digits or a negative one. Nothing here can
// read such a value back, so returning it moved the failure to the next
// function in the query, where nothing was left to say why the row went
// missing.
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
// A value with a decimal point is a binary floating-point value here, where
// MySQL and PostgreSQL hold an exact decimal, so arithmetic over such values
// can differ from the engine's in the last bits: 0.1 + 0.2 is
// 0.30000000000000004 rather than 0.3. SQLite has no decimal type and a number
// loaded from a file is a REAL, so this is the arithmetic there is. A scale
// written on a type is applied by rounding rather than carried, so a cast to
// two decimal places answers 1.5 rather than 1.50, and rounding a value whose
// nearest float falls on the other side of a tie goes with the float: 1.005 to
// two places is 1 here and 1.01 in PostgreSQL. A caller who needs exact decimal
// arithmetic should hold the value as an integer count of the smallest unit.
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
// so its LIKE, its REGEXP and the four spellings of "where does this substring
// start" -- INSTR, LOCATE, POSITION and FIND_IN_SET -- all match a letter in
// either case, and each is routed to a helper that does. The operators are left
// alone: "=", IN, BETWEEN, CASE, IF, ORDER BY, DISTINCT and GROUP BY compare
// inside the engine, so under the MySQL dialect they compare the way SQLite
// does. 'abc' = 'ABC' is false here and true in MySQL, and so is 'a' = 'a ',
// since that collation ignores a trailing space as well. Reaching them is not
// the difficulty -- the tree holds every one of them -- the cost is: a helper
// call for each row costs about two and a half times the query, measured
// against SQLite's own operators over two hundred thousand rows, and a
// comparison sits in every WHERE clause.
//
// How a call reads a value is part of what it means too. MySQL reads a string
// where a number is wanted as the number its leading run spells, or zero, so
// ROUND('abc') is 0 there and not an absence; the calls that stay on a function
// SQLite or this package answers for every dialect have their numeric arguments
// wrapped in mysql_number so they read the same value.
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
// ">>" copies the sign bit where MySQL and BigQuery bring in zeros, and SQLite
// reads a negative shift count as a shift the other way, which MySQL reads as a
// count past the width and BigQuery refuses. What stays different is only how a
// result with its top bit set is spelled: MySQL prints ~0 as 18446744073709551615
// and this prints -1, the same bits under the only reading SQLite has for them.
//
// Where "^" binds differs by dialect and is decided in one table. MySQL puts it
// above multiplication, BigQuery between bitwise AND and bitwise OR, and
// PostgreSQL spells exponentiation with it and its bitwise XOR with "#", at the
// level its manual calls "any other operator".
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
// arithmetic, and anywhere else it is refused with ErrUnsupportedSyntax. There
// it takes every unit word EXTRACT and DATE_TRUNC take, in PostgreSQL's
// abbreviations as well as in full, down to the microsecond and up to the
// millennium, and an amount with a decimal point for the units whose length is
// fixed; a fraction of a month or of anything longer is refused rather than
// spent as thirty-day months, which is a length nothing else here assumes. The
// fields are applied in the order PostgreSQL holds them, months and then days
// and then the clock, which is not the order the literal wrote them in
// whenever a month lands on a month end. an array
// literal and the set-returning functions are refused for the same reason; and
// the functions whose answer is one of those types -- PostgreSQL's
// REGEXP_MATCH and REGEXP_MATCHES, and BigQuery's ARRAY_AGG, APPROX_QUANTILES
// and the rest of its array-returning aggregates -- are not implemented, since
// there would be no value to return. SCALE and TRIM_SCALE answer a number
// rather than an array and are implemented. The two that build an
// interval, AGE and MAKE_INTERVAL, answer the text PostgreSQL prints for one,
// which is a value a caller can read even though nothing can compute with it;
// JUSTIFY_DAYS and its siblings take an interval rather than answering one, so
// there is no argument to give them. A time zone is the
// other thing there is no type for, so BigQuery's forms that take one are
// refused rather than answered unshifted, which would be a different instant.
//
// The clock follows from the same absence. NOW, CURDATE, CURTIME,
// UNIX_TIMESTAMP, MySQL's UTC_TIMESTAMP, UTC_DATE and UTC_TIME, the bare
// CURRENT_TIMESTAMP, CURRENT_DATE and CURRENT_TIME, LOCALTIME and
// LOCALTIMESTAMP, PostgreSQL's now and its two fixed siblings, and BigQuery's
// CURRENT_DATETIME all read UTC, which is what SQLite's own CURRENT_TIMESTAMP
// answers and what BigQuery answers when no zone is named; MySQL and
// PostgreSQL answer a session zone that has no counterpart here. Each of them
// is fixed at the start of the statement, so every row of one result carries
// the same reading and so does every place the statement names one of them.
// PostgreSQL's clock_timestamp and timeofday are the exception and advance
// while the statement runs, which is what they are for, and MySQL's SYSDATE is
// the same exception under its own name.
//
// The reading is whole seconds. MySQL and PostgreSQL let these names take a
// fractional-seconds precision -- CURRENT_TIMESTAMP(3) -- and a precision this
// clock cannot answer is refused rather than dropped.
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
//
// # Integers that leave their range
//
// SQLite computes integer arithmetic in 64 bits and turns to floating point
// when the answer will not fit, so "9223372036854775807 + 1" answers
// 9.223372036854776e+18 where MySQL and PostgreSQL both stop the query and say
// the value is out of range. The same holds for a sum: SUM over values whose
// total leaves the range fails with SQLite's own "integer overflow", where
// MySQL answers it exactly in DECIMAL and PostgreSQL in numeric.
//
// Neither is translated. Every arithmetic operator would have to become a
// helper call to catch it, and a call for each row of each operator costs about
// two and a half times the query, measured against SQLite's own operators over
// two hundred thousand rows. The functions that raise -- POW past the range of
// a double, PostgreSQL's logarithms and roots outside their domain, division by
// zero -- are already calls, so they are answered for; it is the operators that
// are left as SQLite computes them. A column of values near the range of a
// 64-bit integer is worth reading as a string or a real rather than an integer.
//
// # What a translation promises
//
// A translated query answers what the engine the dialect names answers, within
// the subset above and under the type system SQLite has. That is the contract,
// and it is a contract about the engine rather than about this package's
// previous answer: where a translation is found to answer something the engine
// does not, the translation changes, and the change is a fix rather than a
// breaking change even for a caller who had pinned the old answer. What this
// package keeps stable across releases is its exported names and their
// signatures, the three error sentinels and what each means, and the rules
// this documentation states -- the clock, the comparison, the floating-point
// arithmetic, the columns a translation names. A rule it does not state is
// not a promise, and the fix for one that mattered is to state it.
package dialect
