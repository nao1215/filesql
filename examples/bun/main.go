// Package main demonstrates integration of filesql with Bun.
// Bun is a SQL-first ORM for Go.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nao1215/filesql"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
)

// User represents a user record in the CSV file.
// The bun tags define the column mappings.
type User struct {
	bun.BaseModel `bun:"table:users"`

	ID    int64  `bun:"id,pk"`
	Name  string `bun:"name"`
	Email string `bun:"email"`
	Age   int64  `bun:"age"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open CSV file with filesql
	sqlDB, err := filesql.Open(ctx, "users.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	// Create Bun instance with SQLite dialect
	db := bun.NewDB(sqlDB, sqlitedialect.New())

	// Select all users
	fmt.Println("=== All Users ===")
	var users []User
	if err := db.NewSelect().Model(&users).OrderExpr("id").Scan(ctx); err != nil {
		log.Fatal(err)
	}
	for _, u := range users {
		fmt.Printf("ID: %d, Name: %s, Email: %s, Age: %d\n",
			u.ID, u.Name, u.Email, u.Age)
	}

	// Select single user
	fmt.Println("\n=== Get User (ID=1) ===")
	var user User
	if err := db.NewSelect().Model(&user).Where("id = ?", 1).Scan(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found: %s <%s>\n", user.Name, user.Email)

	// Select with WHERE condition
	fmt.Println("\n=== Users Age >= 30 ===")
	var olderUsers []User
	if err := db.NewSelect().Model(&olderUsers).Where("age >= ?", 30).OrderExpr("age").Scan(ctx); err != nil {
		log.Fatal(err)
	}
	for _, u := range olderUsers {
		fmt.Printf("%s (age %d)\n", u.Name, u.Age)
	}

	// Count query
	fmt.Println("\n=== Statistics ===")
	count, err := db.NewSelect().Model((*User)(nil)).Count(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Total users: %d\n", count)

	// Column selection
	fmt.Println("\n=== Names Only ===")
	var names []string
	if err := db.NewSelect().Model((*User)(nil)).Column("name").OrderExpr("name").Scan(ctx, &names); err != nil {
		log.Fatal(err)
	}
	for _, name := range names {
		fmt.Printf("- %s\n", name)
	}

	// Group by with aggregate
	fmt.Println("\n=== Users by Age Group ===")
	type AgeGroup struct {
		AgeRange string `bun:"age_range"`
		Count    int    `bun:"count"`
	}
	var ageGroups []AgeGroup
	err = db.NewSelect().
		ColumnExpr("CASE WHEN age < 30 THEN 'Under 30' ELSE '30 and over' END as age_range").
		ColumnExpr("COUNT(*) as count").
		TableExpr("users").
		GroupExpr("age_range").
		Scan(ctx, &ageGroups)
	if err != nil {
		log.Fatal(err)
	}
	for _, ag := range ageGroups {
		fmt.Printf("%s: %d users\n", ag.AgeRange, ag.Count)
	}
}
