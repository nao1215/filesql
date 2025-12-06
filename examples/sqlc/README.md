# sqlc Integration Example

This example demonstrates how to use filesql with [sqlc](https://sqlc.dev/), a type-safe SQL code generator.

## What is sqlc?

sqlc generates type-safe Go code from SQL queries. It compiles SQL queries to Go code, providing:

- Type-safe database access
- No runtime reflection
- Compile-time query validation
- IDE autocompletion support

## Project Structure

```
sqlc/
├── main.go           # Application code
├── sqlc.yaml         # sqlc configuration
├── schema.sql        # Table schema definition
├── query.sql         # SQL queries for code generation
├── users.csv         # Sample data
└── db/               # Generated code by sqlc
    ├── db.go
    ├── models.go
    └── query.sql.go
```

## How It Works

1. **Define schema** (`schema.sql`): Describe your table structure that matches your CSV columns
2. **Write queries** (`query.sql`): Write SQL queries with sqlc annotations
3. **Generate code**: Run `sqlc generate` to create type-safe Go code
4. **Use with filesql**: Pass `*sql.DB` from filesql to sqlc's `New()` function

## Running the Example

```bash
cd examples/sqlc
go run main.go
```

## Regenerating sqlc Code

If you modify the schema or queries:

```bash
# Install sqlc if not already installed
go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest

# Generate code
sqlc generate
```

## Key Points

- The CSV column names must match the schema definition
- filesql returns a standard `*sql.DB`, which is fully compatible with sqlc
- You get type-safe queries on CSV data!

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
Average age: 28.6
```
