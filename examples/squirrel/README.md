# Squirrel Integration Example

This example demonstrates how to use filesql with [Squirrel](https://github.com/Masterminds/squirrel), a fluent SQL query builder for Go.

## What is Squirrel?

Squirrel is NOT an ORM - it's a lightweight SQL query builder that provides:

- Fluent API for building SQL queries
- Composable query fragments
- Support for complex WHERE clauses
- Subquery support
- No reflection or struct mapping overhead

## How It Works

1. Open CSV file with filesql to get `*sql.DB`
2. Build queries using Squirrel's fluent API
3. Call `ToSql()` to get the SQL string and arguments
4. Execute with standard `db.QueryContext()`

## Key Points

- Squirrel generates SQL strings, not executing queries directly
- You maintain full control over query execution
- Works with any `*sql.DB` compatible database
- Great for dynamic query building

## Running the Example

```bash
cd examples/squirrel
go mod tidy
go run main.go
```

## Expected Output

```
=== All Users ===
id	name	email	age
1	Alice	alice@example.com	28
2	Bob	bob@example.com	35
3	Charlie	charlie@example.com	22
4	Diana	diana@example.com	31
5	Eve	eve@example.com	27

=== Users Age >= 30 ===
id	name	age
2	Bob	35
4	Diana	31

=== Users (age 25-35) ===
id	name	email	age
1	Alice	alice@example.com	28
2	Bob	bob@example.com	35
4	Diana	diana@example.com	31
5	Eve	eve@example.com	27

=== Users with 'a' in name ===
id	name
3	Charlie
4	Diana

=== Statistics ===
count	avg_age
5	28.6

=== Users older than average ===
name	age
Bob	35
Diana	31
```

## Dependencies

```bash
go get github.com/Masterminds/squirrel
```
