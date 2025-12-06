// Package main demonstrates integration of filesql with GORM.
// GORM is a popular ORM library for Go.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nao1215/filesql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// User represents a user record in the CSV file.
// The struct tags define the column mappings.
type User struct {
	ID    int64  `gorm:"column:id;primaryKey"`
	Name  string `gorm:"column:name"`
	Email string `gorm:"column:email"`
	Age   int64  `gorm:"column:age"`
}

// TableName specifies the table name for GORM.
// This must match the CSV filename (without extension).
func (User) TableName() string {
	return "users"
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open CSV file with filesql
	sqlDB, err := filesql.OpenContext(ctx, "users.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	// Create GORM instance using the existing *sql.DB connection
	gormDB, err := gorm.Open(sqlite.Dialector{Conn: sqlDB}, &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	// Find all users
	fmt.Println("=== All Users ===")
	var users []User
	if err := gormDB.Find(&users).Error; err != nil {
		log.Fatal(err)
	}
	for _, u := range users {
		fmt.Printf("ID: %d, Name: %s, Email: %s, Age: %d\n",
			u.ID, u.Name, u.Email, u.Age)
	}

	// Find user by ID
	fmt.Println("\n=== Find User (ID=1) ===")
	var user User
	if err := gormDB.First(&user, 1).Error; err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found: %s <%s>\n", user.Name, user.Email)

	// Find users with WHERE condition
	fmt.Println("\n=== Users Age >= 30 ===")
	var olderUsers []User
	if err := gormDB.Where("age >= ?", 30).Order("age").Find(&olderUsers).Error; err != nil {
		log.Fatal(err)
	}
	for _, u := range olderUsers {
		fmt.Printf("%s (age %d)\n", u.Name, u.Age)
	}

	// Count users
	fmt.Println("\n=== Statistics ===")
	var count int64
	if err := gormDB.Model(&User{}).Count(&count).Error; err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Total users: %d\n", count)

	// Pluck specific column
	fmt.Println("\n=== All Names ===")
	var names []string
	if err := gormDB.Model(&User{}).Pluck("name", &names).Error; err != nil {
		log.Fatal(err)
	}
	for _, name := range names {
		fmt.Printf("- %s\n", name)
	}
}
