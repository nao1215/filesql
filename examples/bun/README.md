# Bun Integration Example

This example demonstrates how to use filesql with [Bun](https://bun.uptrace.dev/), a SQL-first ORM for Go.

## What is Bun?

Bun is a modern SQL-first ORM for Go that provides:

- SQL-first approach - write SQL, not learn a new query language
- Struct mapping with tags
- Query builder with type safety
- Support for complex queries (CTEs, window functions, etc.)
- PostgreSQL, MySQL, SQLite support

## How It Works

1. Open CSV file with filesql to get `*sql.DB`
2. Create Bun instance with `bun.NewDB(sqlDB, sqlitedialect.New())`
3. Use Bun's fluent query builder

## Key Points

- Embed `bun.BaseModel` and use `bun:"table:xxx"` to specify the table name
- Use `bun:"column,pk"` tags to define primary keys and column mappings
- Bun's query builder provides type-safe SQL construction

## Running the Example

```bash
cd examples/bun
go run main.go
```

## Expected Output

```
=== All Users ===
ID: 1, Name: Alice, Email: alice@example.com, Age: 28
ID: 2, Name: Bob, Email: bob@example.com, Age: 35
ID: 3, Name: Charlie, Email: charlie@example.com, Age: 22
ID: 4, Name: Diana, Email: diana@example.com, Age: 31
ID: 5, Name: Eve, Email: eve@example.com, Age: 27

=== Get User (ID=1) ===
Found: Alice <alice@example.com>

=== Users Age >= 30 ===
Diana (age 31)
Bob (age 35)

=== Statistics ===
Total users: 5

=== Names Only ===
- Alice
- Bob
- Charlie
- Diana
- Eve

=== Users by Age Group ===
30 and over: 2 users
Under 30: 3 users
```

## Dependencies

```bash
go get github.com/uptrace/bun
go get github.com/uptrace/bun/dialect/sqlitedialect
```
