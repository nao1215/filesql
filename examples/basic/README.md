# Basic Example

This example demonstrates basic usage of filesql with CSV files.

## Running the Example

```bash
cd examples/basic
go run main.go
```

## What This Example Shows

- Opening a CSV file as a SQL database
- Simple SELECT queries
- WHERE clause filtering
- ORDER BY sorting
- Aggregate functions (COUNT, GROUP BY)

## Expected Output

```
=== All Users ===
id	name	role
1	Alice	admin
2	Bob	user
3	Charlie	user
4	Diana	moderator
5	Eve	user

=== Admin Users ===
id	name
1	Alice

=== Users Ordered by Name ===
id	name	role
1	Alice	admin
2	Bob	user
3	Charlie	user
4	Diana	moderator
5	Eve	user

=== User Count by Role ===
role	count
admin	1
moderator	1
user	3
```
