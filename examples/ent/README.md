# Ent Integration Example

This example demonstrates how to use filesql with [Ent](https://entgo.io/), a powerful entity framework for Go developed by Facebook.

## What is Ent?

Ent is an entity framework that provides:

- Type-safe, schema-driven API
- Code generation from schema definitions
- Graph traversal and complex queries
- Database migrations
- Hooks and privacy policies

## Project Structure

```
ent/
├── main.go           # Application code
├── users.csv         # Sample data
├── go.mod
└── ent/
    ├── schema/
    │   └── user.go   # Schema definition
    ├── client.go     # Generated client
    ├── user.go       # Generated user entity
    └── ...           # Other generated files
```

## How It Works

1. Define schema in `ent/schema/user.go`
2. Generate code with `go generate ./ent`
3. Open CSV file with filesql to get `*sql.DB`
4. Create Ent driver with `entsql.OpenDB(dialect.SQLite, sqlDB)`
5. Use Ent's type-safe query API

## Key Points

- Column names in CSV must match field names in the schema
- Ent provides compile-time type safety
- Full support for complex queries and graph traversals
- Works seamlessly with filesql's `*sql.DB`

## Running the Example

```bash
cd examples/ent
go mod tidy
go run main.go
```

## Regenerating Ent Code

If you modify the schema:

```bash
go generate ./ent
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

=== Users (age 25-35) ===
Alice (age 28)
Bob (age 35)
Diana (age 31)
Eve (age 27)

=== Statistics ===
Total users: 5

=== Names Only ===
- Alice
- Bob
- Charlie
- Diana
- Eve
```

## Dependencies

```bash
go get entgo.io/ent
```
