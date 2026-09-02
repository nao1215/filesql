// Package main demonstrates integration of filesql with Ent.
// Ent is a powerful entity framework for Go by Facebook.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/nao1215/filesql"
	"github.com/nao1215/filesql/examples/ent/ent"
	"github.com/nao1215/filesql/examples/ent/ent/user"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open CSV file with filesql
	sqlDB, err := filesql.Open(ctx, "users.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	// Create Ent driver from existing *sql.DB
	drv := entsql.OpenDB(dialect.SQLite, sqlDB)
	client := ent.NewClient(ent.Driver(drv))
	defer client.Close()

	// Query all users
	fmt.Println("=== All Users ===")
	users, err := client.User.Query().Order(ent.Asc(user.FieldID)).All(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, u := range users {
		fmt.Printf("ID: %d, Name: %s, Email: %s, Age: %d\n",
			u.ID, u.Name, u.Email, u.Age)
	}

	// Query single user by ID
	fmt.Println("\n=== Get User (ID=1) ===")
	u, err := client.User.Query().Where(user.ID(1)).Only(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found: %s <%s>\n", u.Name, u.Email)

	// Query with WHERE condition
	fmt.Println("\n=== Users Age >= 30 ===")
	olderUsers, err := client.User.Query().
		Where(user.AgeGTE(30)).
		Order(ent.Asc(user.FieldAge)).
		All(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, u := range olderUsers {
		fmt.Printf("%s (age %d)\n", u.Name, u.Age)
	}

	// Query with multiple conditions
	fmt.Println("\n=== Users (age 25-35) ===")
	filteredUsers, err := client.User.Query().
		Where(
			user.AgeGTE(25),
			user.AgeLTE(35),
		).
		Order(ent.Asc(user.FieldName)).
		All(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, u := range filteredUsers {
		fmt.Printf("%s (age %d)\n", u.Name, u.Age)
	}

	// Count query
	fmt.Println("\n=== Statistics ===")
	count, err := client.User.Query().Count(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Total users: %d\n", count)

	// Select specific fields
	fmt.Println("\n=== Names Only ===")
	names, err := client.User.Query().
		Order(ent.Asc(user.FieldName)).
		Select(user.FieldName).
		Strings(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, name := range names {
		fmt.Printf("- %s\n", name)
	}
}
