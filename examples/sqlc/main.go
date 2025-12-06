// Package main demonstrates integration of filesql with sqlc.
// sqlc generates type-safe Go code from SQL queries.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nao1215/filesql"
	"github.com/nao1215/filesql/examples/sqlc/db"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open CSV file with filesql
	sqlDB, err := filesql.OpenContext(ctx, "users.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	// Create sqlc queries instance with filesql's *sql.DB
	queries := db.New(sqlDB)

	// List all users (type-safe!)
	fmt.Println("=== All Users ===")
	users, err := queries.ListUsers(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, u := range users {
		fmt.Printf("ID: %d, Name: %s, Email: %s, Age: %d\n",
			u.ID, u.Name, u.Email, u.Age)
	}

	// Get a specific user
	fmt.Println("\n=== Get User (ID=1) ===")
	user, err := queries.GetUser(ctx, 1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found: %s <%s>\n", user.Name, user.Email)

	// List users by age
	fmt.Println("\n=== Users Age >= 30 ===")
	olderUsers, err := queries.ListUsersByAge(ctx, 30)
	if err != nil {
		log.Fatal(err)
	}
	for _, u := range olderUsers {
		fmt.Printf("%s (age %d)\n", u.Name, u.Age)
	}

	// Count users
	fmt.Println("\n=== Statistics ===")
	count, err := queries.CountUsers(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Total users: %d\n", count)

	// Get average age
	avgAge, err := queries.GetAverageAge(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Average age: %.1f\n", avgAge.(float64))
}
