// Package main demonstrates integration of filesql with Squirrel.
// Squirrel is a fluent SQL query builder for Go.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/nao1215/filesql"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open CSV file with filesql
	db, err := filesql.OpenContext(ctx, "users.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Simple SELECT query
	fmt.Println("=== All Users ===")
	sql, args, err := sq.Select("*").From("users").OrderBy("id").ToSql()
	if err != nil {
		log.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, sql, args...)
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)

	// SELECT with WHERE clause
	fmt.Println("\n=== Users Age >= 30 ===")
	sql, args, err = sq.Select("id", "name", "age").
		From("users").
		Where(sq.GtOrEq{"age": 30}).
		OrderBy("age").
		ToSql()
	if err != nil {
		log.Fatal(err)
	}
	rows, err = db.QueryContext(ctx, sql, args...)
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)

	// SELECT with multiple conditions
	fmt.Println("\n=== Users (age 25-35) ===")
	sql, args, err = sq.Select("*").
		From("users").
		Where(sq.And{
			sq.GtOrEq{"age": 25},
			sq.LtOrEq{"age": 35},
		}).
		OrderBy("name").
		ToSql()
	if err != nil {
		log.Fatal(err)
	}
	rows, err = db.QueryContext(ctx, sql, args...)
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)

	// SELECT with LIKE
	fmt.Println("\n=== Users with 'a' in name ===")
	sql, args, err = sq.Select("id", "name").
		From("users").
		Where(sq.Like{"name": "%a%"}).
		ToSql()
	if err != nil {
		log.Fatal(err)
	}
	rows, err = db.QueryContext(ctx, sql, args...)
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)

	// Aggregate query
	fmt.Println("\n=== Statistics ===")
	sql, args, err = sq.Select("COUNT(*) as count", "AVG(age) as avg_age").
		From("users").
		ToSql()
	if err != nil {
		log.Fatal(err)
	}
	rows, err = db.QueryContext(ctx, sql, args...)
	if err != nil {
		log.Fatal(err)
	}
	printRows(rows)

	// Subquery example (using raw SQL for subquery)
	fmt.Println("\n=== Users older than average ===")
	sql, args, err = sq.Select("name", "age").
		From("users").
		Where("age > (SELECT AVG(age) FROM users)").
		OrderBy("age DESC").
		ToSql()
	if err != nil {
		log.Fatal(err)
	}
	rows, err = db.QueryContext(ctx, sql, args...)
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
