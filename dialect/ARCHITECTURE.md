# dialect architecture

This note records why the dialect package is built as a compiler frontend and
what each layer is responsible for. It is written for someone changing the
package, not for someone using it; the user-facing contract is in `doc.go`.

## The problem with the previous design

The package used to tokenize a query and then rewrite the token slice in place,
one pass per construct. Nothing between the lexer and the renderer knew what an
expression was, so every pass that needed an operand rediscovered it by walking
tokens outward and guessing where the operand began and ended. The package
carried `primaryStartBack`, `primaryEndForward`, `operandChainStartBack`,
`chainStartBack` and `adjacentCallEnd` for that purpose, and each of them
encoded a partial, private theory of SQL's grammar.

Three consequences followed, and all three were paid for in bugs.

Operator precedence lived in the passes rather than in one place, so the order
the passes ran in became part of the language's meaning: MOD had to be rewritten
before DIV, `!` before `^`, the typed-literal pass before the interval pass.
Every new construct had to be inserted at the right point in a sequence whose
constraints were recorded only in comments.

An operand boundary found by scanning is a guess, and a guess is wrong at the
edges. `SOUNDS LIKE`, `LIKE ANY`, the PostgreSQL bit-string literal and the
`#-` operator each produced SQL that could not parse, because a pass took a
neighbouring token for an operand that was not one.

Anything no pass recognized was handed to SQLite unchanged. That made the
supported language accidental: a construct "worked" whenever SQLite happened to
accept the same text, with whatever meaning SQLite gives it. `CREATE TABLE x
LIKE y` was accepted and built a table with no columns.

## The pipeline

```
source SQL
  -> token.Lex          lexical structure only
  -> parser.Parse       typed AST, one precedence table
  -> ast                what the query says, in the source dialect's terms
  -> lower.Lower        dialect semantics -> SQLite semantics
  -> ast                restricted to what SQLite can express
  -> render.Render      SQLite SQL text
```

A construct that SQLite cannot express is either lowered into a call to a
runtime helper this package registers with the driver, or refused. It is never
passed through in the hope that SQLite will accept it.

## Layers and dependency direction

    token  <- ast  <- parser
                  <- lower
                  <- render
    runtime (independent)

- `internal/token` turns bytes into tokens and knows nothing about grammar. It
  owns the lexical differences between dialects: identifier quoting, string
  quoting and escapes, comment leaders, dollar quoting, raw and byte strings.
- `internal/ast` is the node set, with a source span on every node. It holds no
  rendered SQL and no dialect logic.
- `internal/parser` builds the AST. Expression parsing is a Pratt parser with a
  single precedence table; statement parsing is recursive descent. Dialect
  differences enter through a small set of explicit hooks, not through
  `if dialect ==` scattered through the grammar.
- `internal/lower` is where a source dialect's meaning becomes SQLite's. It
  walks the tree and rewrites nodes; it never looks at text positions or token
  order. This is where the old rewrite passes went.
- `internal/render` writes SQLite SQL and nothing else. It decides quoting and
  parenthesization from the tree, so a rendered query reparses to the same
  shape.
- `internal/runtime` holds the scalar functions registered into the SQLite
  driver for semantics SQL alone cannot express. It is reachable from lowering
  only by name, so the two can be read separately.

`internal/dialects` holds the `Dialect` type and `internal/sqlerr` the sentinel
errors, because parser, lower and the public package all need them and none of
them may import the public package.

## Fail closed

The parser implements a stated subset. Anything outside it is
`ErrUnsupportedFeature`; anything inside it that SQLite cannot express is
`ErrUnsupportedSyntax`. Both carry the line and column of the construct. The
subset is listed in `doc.go`, and that list is the contract: a query outside it
is refused with a message naming what was not understood, rather than forwarded
to SQLite.

## What this is not

It is not a MySQL, PostgreSQL or BigQuery implementation, and it is not a type
checker, an optimizer or an execution engine. SQLite executes; this package
decides what SQLite is asked to execute.
