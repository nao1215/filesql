//go:build ignore

package main

import (
	"log"
	"os"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
)

func main() {
	// Define schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "price", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	// Create record batch
	pool := memory.NewGoAllocator()

	idBuilder := array.NewInt64Builder(pool)
	defer idBuilder.Release()
	idBuilder.AppendValues([]int64{1, 2, 3}, nil)

	nameBuilder := array.NewStringBuilder(pool)
	defer nameBuilder.Release()
	nameBuilder.AppendValues([]string{"Laptop", "Mouse", "Keyboard"}, nil)

	priceBuilder := array.NewFloat64Builder(pool)
	defer priceBuilder.Release()
	priceBuilder.AppendValues([]float64{999.99, 29.99, 79.99}, nil)

	// Build arrays
	idArr := idBuilder.NewArray()
	defer idArr.Release()
	nameArr := nameBuilder.NewArray()
	defer nameArr.Release()
	priceArr := priceBuilder.NewArray()
	defer priceArr.Release()

	// Create record batch
	record := array.NewRecord(schema, []arrow.Array{idArr, nameArr, priceArr}, 3)
	defer record.Release()

	// Create table from record
	table := array.NewTableFromRecords(schema, []arrow.Record{record})
	defer table.Release()

	// Write to parquet file
	f, err := os.Create("products.parquet")
	if err != nil {
		log.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	props := parquet.NewWriterProperties()
	arrProps := pqarrow.DefaultWriterProps()

	err = pqarrow.WriteTable(table, f, 1024, props, arrProps)
	if err != nil {
		log.Fatalf("failed to write parquet: %v", err)
	}

	log.Println("Created products.parquet")
}
