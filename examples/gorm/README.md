# GORM Integration Example

This example demonstrates how to use filesql with [GORM](https://gorm.io/), the most popular ORM library for Go.

## What is GORM?

GORM is a full-featured ORM library for Go that provides:

- Full-Featured ORM (associations, hooks, preloading, etc.)
- Auto Migrations
- CRUD operations with method chaining
- SQL Builder

## How It Works

1. Open CSV file with filesql to get `*sql.DB`
2. Pass the connection to GORM's SQLite dialector using `sqlite.Dialector{Conn: sqlDB}`
3. Use GORM's fluent API to query CSV data

## Key Points

- Use `gorm:"column:xxx"` tags to map struct fields to CSV columns
- Implement `TableName()` method to specify the CSV filename (without extension)
- GORM works seamlessly with filesql's `*sql.DB`

## Running the Example

```bash
cd examples/gorm
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

=== Find User (ID=1) ===
Found: Alice <alice@example.com>

=== Users Age >= 30 ===
Diana (age 31)
Bob (age 35)

=== Statistics ===
Total users: 5

=== All Names ===
- Alice
- Bob
- Charlie
- Diana
- Eve
```

## Dependencies

```bash
go get gorm.io/gorm
go get gorm.io/driver/sqlite
```
