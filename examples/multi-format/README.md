# Multi-Format Example

This example demonstrates filesql's ability to work with multiple file formats simultaneously.

## Formats Used Here

- CSV - Comma-separated values
- TSV - Tab-separated values
- LTSV - Labeled Tab-separated values

filesql reads more than these three; see the root README for the full list.

## Running the Example

```bash
cd examples/multi-format
go run main.go
```

## What This Example Shows

- Opening multiple files of different formats
- Each file becomes a separate SQL table
- Listing available tables
- Querying each format independently
- JOIN queries across different file formats

## File Structure

- `products.csv` - Product catalog (CSV format)
- `orders.tsv` - Order records (TSV format)
- `logs.ltsv` - Application logs (LTSV format)

## Expected Output

```
=== Available Tables ===
- logs
- orders
- products

=== Products (from CSV) ===
id	name	price	category
1	Laptop	999.99	Electronics
2	Mouse	29.99	Electronics
3	Keyboard	79.99	Electronics
4	Desk	199.99	Furniture
5	Chair	149.99	Furniture

=== Orders (from TSV) ===
order_id	product_id	quantity	order_date
1	1	2	2024-01-15
2	2	5	2024-01-16
3	3	3	2024-01-17
4	4	1	2024-01-18
5	5	2	2024-01-19

=== Logs (from LTSV) ===
time	level	message	user_id
2024-01-15T10:00:00Z	INFO	User logged in	101
2024-01-15T10:05:00Z	INFO	Order placed	101
2024-01-15T10:10:00Z	WARN	Payment retry	102
2024-01-15T10:15:00Z	ERROR	Payment failed	102
2024-01-15T10:20:00Z	INFO	Order completed	101

=== JOIN: Orders with Product Names ===
order_id	product_name	quantity	price	total
1	Laptop	2	999.99	1999.98
2	Mouse	5	29.99	149.95
3	Keyboard	3	79.99	239.96999999999997
4	Desk	1	199.99	199.99
5	Chair	2	149.99	299.98
```
