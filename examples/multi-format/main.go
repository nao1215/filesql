// Package main demonstrates filesql's multi-format support.
// This example shows how to work with CSV, TSV, LTSV, and Parquet files.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nao1215/filesql"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open multiple files of different formats at once
	// Each file becomes a separate table
	db, err := filesql.OpenContext(ctx,
		"products.csv", // CSV format
		"orders.tsv",   // TSV format
		"logs.ltsv",    // LTSV format
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// List all available tables
	fmt.Println("=== Available Tables ===")
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("- %s\n", name)
	}
	rows.Close()

	// Query CSV data (products)
	fmt.Println("\n=== Products (from CSV) ===")
	rows, err = db.QueryContext(ctx, "SELECT * FROM products")
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)

	// Query TSV data (orders)
	fmt.Println("\n=== Orders (from TSV) ===")
	rows, err = db.QueryContext(ctx, "SELECT * FROM orders")
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)

	// Query LTSV data (logs)
	fmt.Println("\n=== Logs (from LTSV) ===")
	rows, err = db.QueryContext(ctx, "SELECT * FROM logs")
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)

	// JOIN across different file formats
	fmt.Println("\n=== JOIN: Orders with Product Names ===")
	rows, err = db.QueryContext(ctx, `
		SELECT o.order_id, p.name as product_name, o.quantity, p.price,
		       (o.quantity * p.price) as total
		FROM orders o
		JOIN products p ON o.product_id = p.id
		ORDER BY o.order_id
	`)
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)
}

// printRows prints all rows from the query result.
func printRows(rows interface {
	Next() bool
	Columns() ([]string, error)
	Scan(...interface{}) error
	Close() error
}) {
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}

	// Print header
	for i, col := range cols {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print(col)
	}
	fmt.Println()

	// Print rows
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Fatal(err)
		}
		for i, val := range values {
			if i > 0 {
				fmt.Print("\t")
			}
			fmt.Printf("%v", val)
		}
		fmt.Println()
	}
}
