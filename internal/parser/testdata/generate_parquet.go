//go:build ignore

// Generates products.parquet, the Parquet fixture the tests read. The
// committed fixture is not regenerated on every run — a new writer writes
// different bytes for the same rows — so run this only when the fixture's
// contents have to change, and expect the byte-level tests that pin damage
// offsets to need new offsets afterwards.
package main

import (
	"log"
	"os"

	"github.com/parquet-go/parquet-go"
)

func main() {
	type product struct {
		ID    int64   `parquet:"id"`
		Name  string  `parquet:"name"`
		Price float64 `parquet:"price"`
	}

	f, err := os.Create("products.parquet")
	if err != nil {
		log.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	w := parquet.NewGenericWriter[product](f)
	if _, err := w.Write([]product{
		{ID: 1, Name: "Laptop", Price: 999.99},
		{ID: 2, Name: "Mouse", Price: 29.99},
		{ID: 3, Name: "Keyboard", Price: 79.99},
	}); err != nil {
		log.Fatalf("failed to write parquet: %v", err)
	}
	if err := w.Close(); err != nil {
		log.Fatalf("failed to close parquet writer: %v", err)
	}

	log.Println("Created products.parquet")
}
