// Package main demonstrates basic usage of filesql with CSV files.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nao1215/filesql"
)

func main() {
	// Create context with timeout for large file operations
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open a CSV file as a database
	// The table name will be "users" (derived from the filename)
	db, err := filesql.OpenContext(ctx, "users.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Query all users
	fmt.Println("=== All Users ===")
	rows, err := db.QueryContext(ctx, "SELECT * FROM users")
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)

	// Query with WHERE clause
	fmt.Println("\n=== Admin Users ===")
	rows, err = db.QueryContext(ctx, "SELECT id, name FROM users WHERE role = 'admin'")
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)

	// Query with ORDER BY
	fmt.Println("\n=== Users Ordered by Name ===")
	rows, err = db.QueryContext(ctx, "SELECT * FROM users ORDER BY name ASC")
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)

	// Aggregate query
	fmt.Println("\n=== User Count by Role ===")
	rows, err = db.QueryContext(ctx, "SELECT role, COUNT(*) as count FROM users GROUP BY role")
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
