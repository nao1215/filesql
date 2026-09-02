// Package main demonstrates integration of filesql with sqlx.
// sqlx is an extension of the standard database/sql package.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nao1215/filesql"
)

// User represents a user record in the CSV file.
// The db tags define the column mappings.
type User struct {
	ID    int64  `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
	Age   int64  `db:"age"`
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

	// Wrap *sql.DB with sqlx
	db := sqlx.NewDb(sqlDB, "sqlite3")

	// Select all users using Get/Select
	fmt.Println("=== All Users ===")
	var users []User
	if err := db.SelectContext(ctx, &users, "SELECT * FROM users ORDER BY id"); err != nil {
		log.Fatal(err)
	}
	for _, u := range users {
		fmt.Printf("ID: %d, Name: %s, Email: %s, Age: %d\n",
			u.ID, u.Name, u.Email, u.Age)
	}

	// Get single user
	fmt.Println("\n=== Get User (ID=1) ===")
	var user User
	if err := db.GetContext(ctx, &user, "SELECT * FROM users WHERE id = ?", 1); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found: %s <%s>\n", user.Name, user.Email)

	// Select with WHERE condition
	fmt.Println("\n=== Users Age >= 30 ===")
	var olderUsers []User
	if err := db.SelectContext(ctx, &olderUsers, "SELECT * FROM users WHERE age >= ? ORDER BY age", 30); err != nil {
		log.Fatal(err)
	}
	for _, u := range olderUsers {
		fmt.Printf("%s (age %d)\n", u.Name, u.Age)
	}

	// Named query
	fmt.Println("\n=== Named Query (age >= 25) ===")
	query, args, err := sqlx.Named("SELECT * FROM users WHERE age >= :min_age ORDER BY name", map[string]interface{}{
		"min_age": 25,
	})
	if err != nil {
		log.Fatal(err)
	}
	query = db.Rebind(query)

	var filteredUsers []User
	if err := db.SelectContext(ctx, &filteredUsers, query, args...); err != nil {
		log.Fatal(err)
	}
	for _, u := range filteredUsers {
		fmt.Printf("%s (age %d)\n", u.Name, u.Age)
	}

	// NamedExec example (demonstrating the API, though changes won't persist)
	fmt.Println("\n=== Statistics ===")
	var count int
	if err := db.GetContext(ctx, &count, "SELECT COUNT(*) FROM users"); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Total users: %d\n", count)

	var avgAge float64
	if err := db.GetContext(ctx, &avgAge, "SELECT AVG(age) FROM users"); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Average age: %.1f\n", avgAge)
}
