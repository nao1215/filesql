# sqlx Integration Example

This example demonstrates how to use filesql with [sqlx](https://github.com/jmoiron/sqlx), a popular extension of Go's standard database/sql package.

## What is sqlx?

sqlx provides a set of extensions on Go's standard database/sql library:

- Marshal rows into structs, maps, and slices
- Named parameter support
- Get and Select for convenient querying
- IN query support

sqlx is NOT an ORM - it's a thin wrapper that makes working with SQL more convenient while keeping you in control of your queries.

## How It Works

1. Open CSV file with filesql to get `*sql.DB`
2. Wrap it with `sqlx.NewDb(sqlDB, "sqlite3")`
3. Use sqlx's convenient methods (`Get`, `Select`, `NamedQuery`, etc.)

## Key Points

- Use `db:"xxx"` tags to map struct fields to CSV columns
- sqlx wraps the existing `*sql.DB` without creating a new connection
- Full access to raw SQL with the convenience of struct scanning

## Running the Example

```bash
cd examples/sqlx
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

=== Named Query (age >= 25) ===
Alice (age 28)
Bob (age 35)
Diana (age 31)
Eve (age 27)

=== Statistics ===
Total users: 5
Average age: 28.6
```

## Dependencies

```bash
go get github.com/jmoiron/sqlx
```
