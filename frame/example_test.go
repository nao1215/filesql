package frame_test

import (
	"fmt"
	"slices"
	"strings"

	"github.com/nao1215/filesql/frame"
)

// Example demonstrates basic DataFrame operations: reading CSV,
// filtering rows, grouping, and aggregating data.
func Example() {
	// Sample sales data
	csvData := `product,amount,category
Apple,100,Fruit
Banana,150,Fruit
Carrot,80,Vegetable
Orange,120,Fruit
Broccoli,90,Vegetable`

	// Create DataFrame from CSV
	df, err := frame.NewDataFrame(strings.NewReader(csvData), frame.CSV)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Printf("Total rows: %d\n", df.Len())
	fmt.Printf("Columns: %v\n", df.Columns())

	// Filter: only items with amount > 100
	filtered := df.Filter(func(row map[string]any) bool {
		amount, ok := row["amount"].(int64)
		return ok && amount > 100
	})
	fmt.Printf("Rows with amount > 100: %d\n", filtered.Len())

	// GroupBy category and sum amounts
	groupedDf, err := df.GroupBy("category")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	grouped, err := groupedDf.Sum("amount")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Printf("Grouped columns: %v\n", grouped.Columns())

	// Show grouped results
	for _, row := range grouped.ToRecords() {
		fmt.Printf("  %s: %.0f\n", row["category"], row["sum_amount"])
	}

	// Output:
	// Total rows: 5
	// Columns: [product amount category]
	// Rows with amount > 100: 2
	// Grouped columns: [category sum_amount]
	//   Fruit: 370
	//   Vegetable: 170
}

// Example_complexOperations demonstrates advanced DataFrame operations
// including multiple aggregations, data transformation with Mutate,
// and combining results from different DataFrames.
func Example_complexOperations() {
	// Sales data
	salesCSV := `order_id,product_id,quantity,unit_price
1,P001,2,100
2,P002,1,200
3,P001,3,100
4,P003,5,50
5,P002,2,200`

	// Product master data
	productsCSV := `product_id,name,category
P001,Laptop Stand,Electronics
P002,Mechanical Keyboard,Electronics
P003,Notebook,Stationery`

	// Create DataFrames
	sales, _ := frame.NewDataFrame(strings.NewReader(salesCSV), frame.CSV)       //nolint:errcheck // example code
	products, _ := frame.NewDataFrame(strings.NewReader(productsCSV), frame.CSV) //nolint:errcheck // example code

	// Add calculated column: total_amount = quantity * unit_price
	salesWithTotal := sales.Mutate("total_amount", func(row map[string]any) any {
		qty, _ := row["quantity"].(int64)     //nolint:errcheck // example code
		price, _ := row["unit_price"].(int64) //nolint:errcheck // example code
		return qty * price
	})

	fmt.Println("=== Sales with Total Amount ===")
	for _, row := range salesWithTotal.ToRecords() {
		fmt.Printf("Order %v: %v x %v = %v\n",
			row["order_id"], row["quantity"], row["unit_price"], row["total_amount"])
	}

	// Aggregate sales by product_id
	salesByProductGrp, _ := salesWithTotal.GroupBy("product_id") //nolint:errcheck // example code
	salesByProduct, _ := salesByProductGrp.Sum("total_amount")   //nolint:errcheck // example code

	fmt.Println("\n=== Sales by Product ===")
	for _, row := range salesByProduct.ToRecords() {
		fmt.Printf("%s: %.0f\n", row["product_id"], row["sum_total_amount"])
	}

	// Create a lookup map from products DataFrame
	productLookup := make(map[string]map[string]any)
	for _, row := range products.ToRecords() {
		pid, _ := row["product_id"].(string) //nolint:errcheck // example code
		productLookup[pid] = row
	}

	// Combine sales summary with product info (manual join)
	combinedRecords := make([]map[string]any, 0)
	for _, salesRow := range salesByProduct.ToRecords() {
		pid, _ := salesRow["product_id"].(string) //nolint:errcheck // example code
		if productInfo, exists := productLookup[pid]; exists {
			combined := map[string]any{
				"product_id":  pid,
				"name":        productInfo["name"],
				"category":    productInfo["category"],
				"total_sales": salesRow["sum_total_amount"],
			}
			combinedRecords = append(combinedRecords, combined)
		}
	}
	combined := frame.NewDataFrameFromRecords(combinedRecords)

	fmt.Println("\n=== Combined Sales Report ===")
	fmt.Printf("Columns: %v\n", combined.Columns())
	for _, row := range combined.ToRecords() {
		fmt.Printf("%s (%s): %.0f\n",
			row["name"], row["category"], row["total_sales"])
	}

	// Group combined data by category
	byCategoryGrp, _ := combined.GroupBy("category")  //nolint:errcheck // example code
	byCategory, _ := byCategoryGrp.Sum("total_sales") //nolint:errcheck // example code

	fmt.Println("\n=== Total Sales by Category ===")
	for _, row := range byCategory.ToRecords() {
		fmt.Printf("%s: %.0f\n", row["category"], row["sum_total_sales"])
	}

	// Calculate statistics
	fmt.Println("\n=== Sales Statistics ===")
	statsGrp, _ := salesWithTotal.GroupBy()  //nolint:errcheck // example code
	stats, _ := statsGrp.Sum("total_amount") //nolint:errcheck // example code
	for _, row := range stats.ToRecords() {
		fmt.Printf("Total Revenue: %.0f\n", row["sum_total_amount"])
	}

	meanSalesGrp, _ := salesWithTotal.GroupBy()       //nolint:errcheck // example code
	meanSales, _ := meanSalesGrp.Mean("total_amount") //nolint:errcheck // example code
	for _, row := range meanSales.ToRecords() {
		fmt.Printf("Average Order Value: %.0f\n", row["mean_total_amount"])
	}

	minSalesGrp, _ := salesWithTotal.GroupBy()     //nolint:errcheck // example code
	minSales, _ := minSalesGrp.Min("total_amount") //nolint:errcheck // example code
	for _, row := range minSales.ToRecords() {
		fmt.Printf("Min Order: %.0f\n", row["min_total_amount"])
	}

	maxSalesGrp, _ := salesWithTotal.GroupBy()     //nolint:errcheck // example code
	maxSales, _ := maxSalesGrp.Max("total_amount") //nolint:errcheck // example code
	for _, row := range maxSales.ToRecords() {
		fmt.Printf("Max Order: %.0f\n", row["max_total_amount"])
	}

	// Output:
	// === Sales with Total Amount ===
	// Order 1: 2 x 100 = 200
	// Order 2: 1 x 200 = 200
	// Order 3: 3 x 100 = 300
	// Order 4: 5 x 50 = 250
	// Order 5: 2 x 200 = 400
	//
	// === Sales by Product ===
	// P001: 500
	// P002: 600
	// P003: 250
	//
	// === Combined Sales Report ===
	// Columns: [category name product_id total_sales]
	// Laptop Stand (Electronics): 500
	// Mechanical Keyboard (Electronics): 600
	// Notebook (Stationery): 250
	//
	// === Total Sales by Category ===
	// Electronics: 1100
	// Stationery: 250
	//
	// === Sales Statistics ===
	// Total Revenue: 1350
	// Average Order Value: 270
	// Min Order: 200
	// Max Order: 400
}

// Example_globalAggregation demonstrates how to calculate global statistics
// without grouping by any column. Call GroupBy() with no arguments to
// aggregate the entire DataFrame into a single result.
func Example_globalAggregation() {
	csvData := `product,price,quantity
Laptop,1000,5
Mouse,25,50
Keyboard,75,30
Monitor,300,10`

	df, err := frame.NewDataFrame(strings.NewReader(csvData), frame.CSV)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// GroupBy() with no arguments = global aggregation (entire DataFrame as one group)
	grouped, err := df.GroupBy()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Calculate various statistics for the entire dataset
	count := grouped.Count()
	fmt.Printf("Total products: %d\n", count.ToRecords()[0]["count"])

	sumResult, _ := grouped.Sum("price") //nolint:errcheck // example code
	fmt.Printf("Sum of prices: %.0f\n", sumResult.ToRecords()[0]["sum_price"])

	meanResult, _ := grouped.Mean("quantity") //nolint:errcheck // example code
	fmt.Printf("Average quantity: %.2f\n", meanResult.ToRecords()[0]["mean_quantity"])

	minResult, _ := grouped.Min("price") //nolint:errcheck // example code
	fmt.Printf("Min price: %.0f\n", minResult.ToRecords()[0]["min_price"])

	maxResult, _ := grouped.Max("price") //nolint:errcheck // example code
	fmt.Printf("Max price: %.0f\n", maxResult.ToRecords()[0]["max_price"])

	// Output:
	// Total products: 4
	// Sum of prices: 1400
	// Average quantity: 23.75
	// Min price: 25
	// Max price: 1000
}

// Example_customAggregation demonstrates how to use the Agg function
// to implement custom aggregation logic such as median calculation.
func Example_customAggregation() {
	csvData := `category,value
A,10
A,20
A,30
A,40
A,50
B,5
B,15
B,25`

	df, err := frame.NewDataFrame(strings.NewReader(csvData), frame.CSV)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	grouped, err := df.GroupBy("category")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Custom aggregation: calculate median
	median := func(values []any) any {
		// Filter and convert to float64
		nums := make([]float64, 0, len(values))
		for _, v := range values {
			switch n := v.(type) {
			case int64:
				nums = append(nums, float64(n))
			case float64:
				nums = append(nums, n)
			}
		}
		if len(nums) == 0 {
			return nil
		}

		// Sort values
		slices.Sort(nums)

		// Calculate median
		mid := len(nums) / 2
		if len(nums)%2 == 0 {
			return (nums[mid-1] + nums[mid]) / 2
		}
		return nums[mid]
	}

	result, _ := grouped.Agg("value", median) //nolint:errcheck // example code

	fmt.Println("Median by category:")
	for _, row := range result.ToRecords() {
		fmt.Printf("  %s: %.1f\n", row["category"], row["agg_value"])
	}

	// Custom aggregation: calculate range (max - min)
	rangeFunc := func(values []any) any {
		var minVal, maxVal float64
		first := true
		for _, v := range values {
			var n float64
			switch val := v.(type) {
			case int64:
				n = float64(val)
			case float64:
				n = val
			default:
				continue
			}
			if first {
				minVal, maxVal = n, n
				first = false
			} else {
				if n < minVal {
					minVal = n
				}
				if n > maxVal {
					maxVal = n
				}
			}
		}
		if first {
			return nil
		}
		return maxVal - minVal
	}

	rangeResult, _ := grouped.Agg("value", rangeFunc) //nolint:errcheck // example code

	fmt.Println("Range by category:")
	for _, row := range rangeResult.ToRecords() {
		fmt.Printf("  %s: %.0f\n", row["category"], row["agg_value"])
	}

	// Output:
	// Median by category:
	//   A: 30.0
	//   B: 15.0
	// Range by category:
	//   A: 40
	//   B: 20
}

// Example_fileFormats shows the various file formats supported by frame.
// NewDataFrameFromPath automatically detects file type and handles compression.
//
// Supported formats:
//   - CSV, TSV, LTSV, XLSX, Parquet
//   - Compressed variants: .gz, .bz2, .xz, .zst
//
// Usage:
//
//	// Auto-detect CSV
//	df, err := frame.NewDataFrameFromPath("data.csv")
//
//	// Auto-detect compressed CSV (gzip)
//	df, err := frame.NewDataFrameFromPath("data.csv.gz")
//
//	// Auto-detect TSV with zstd compression
//	df, err := frame.NewDataFrameFromPath("data.tsv.zst")
//
//	// Auto-detect Excel file
//	df, err := frame.NewDataFrameFromPath("spreadsheet.xlsx")
//
//	// Auto-detect Parquet file
//	df, err := frame.NewDataFrameFromPath("data.parquet")
func Example_fileFormats() {
	// This example demonstrates the API for reading various file formats.
	// Since Example functions require deterministic output, we show
	// equivalent operations using NewDataFrame with explicit file types.

	// TSV (Tab-Separated Values)
	tsvData := "name\tage\tcity\nAlice\t30\tTokyo\nBob\t25\tOsaka"
	dfTSV, _ := frame.NewDataFrame(strings.NewReader(tsvData), frame.TSV) //nolint:errcheck
	fmt.Printf("TSV columns: %v, rows: %d\n", dfTSV.Columns(), dfTSV.Len())

	// LTSV (Labeled Tab-Separated Values)
	ltsvData := "name:Alice\tage:30\tcity:Tokyo\nname:Bob\tage:25\tcity:Osaka"
	dfLTSV, _ := frame.NewDataFrame(strings.NewReader(ltsvData), frame.LTSV) //nolint:errcheck
	fmt.Printf("LTSV columns: %v, rows: %d\n", dfLTSV.Columns(), dfLTSV.Len())

	// For file-based operations with compression, use NewDataFrameFromPath:
	//
	//   df, err := frame.NewDataFrameFromPath("logs.csv.gz")    // gzip
	//   df, err := frame.NewDataFrameFromPath("data.tsv.bz2")   // bzip2
	//   df, err := frame.NewDataFrameFromPath("export.csv.xz")  // xz
	//   df, err := frame.NewDataFrameFromPath("archive.csv.zst") // zstd

	// Output:
	// TSV columns: [name age city], rows: 2
	// LTSV columns: [name age city], rows: 2
}

// Example_join demonstrates how to combine two DataFrames using Join.
// This is similar to SQL JOIN operations and supports inner, left, right, and outer joins.
func Example_join() {
	// Users table
	usersCSV := `id,name,department
1,Alice,Engineering
2,Bob,Marketing
3,Charlie,Engineering
4,Diana,Sales`

	// Orders table - note that user_id references users.id
	ordersCSV := `order_id,user_id,product,amount
101,1,Laptop,1200
102,1,Mouse,50
103,2,Monitor,400
104,5,Keyboard,100`

	users, _ := frame.NewDataFrame(strings.NewReader(usersCSV), frame.CSV)   //nolint:errcheck
	orders, _ := frame.NewDataFrame(strings.NewReader(ordersCSV), frame.CSV) //nolint:errcheck

	// Inner Join: Only users who have orders
	inner, _ := users.Join(orders, frame.JoinOption{ //nolint:errcheck
		On:  []string{"id", "user_id"}, // Left column, Right column
		How: frame.InnerJoin,
	})
	fmt.Println("=== Inner Join (users with orders) ===")
	fmt.Printf("Rows: %d\n", inner.Len())
	for _, row := range inner.ToRecords() {
		fmt.Printf("  %s ordered %s ($%v)\n", row["name"], row["product"], row["amount"])
	}

	// Left Join: All users, with order info if available
	left, _ := users.Join(orders, frame.JoinOption{ //nolint:errcheck
		On:  []string{"id", "user_id"},
		How: frame.LeftJoin,
	})
	fmt.Println("\n=== Left Join (all users) ===")
	fmt.Printf("Rows: %d\n", left.Len())

	// Count users without orders
	noOrders := 0
	for _, row := range left.ToRecords() {
		if row["order_id"] == nil {
			noOrders++
		}
	}
	fmt.Printf("Users without orders: %d\n", noOrders)

	// Output:
	// === Inner Join (users with orders) ===
	// Rows: 3
	//   Alice ordered Laptop ($1200)
	//   Alice ordered Mouse ($50)
	//   Bob ordered Monitor ($400)
	//
	// === Left Join (all users) ===
	// Rows: 5
	// Users without orders: 2
}

// Example_joinTypes demonstrates all four join types:
// InnerJoin, LeftJoin, RightJoin, and OuterJoin.
func Example_joinTypes() {
	// Left DataFrame: Products
	productsCSV := `product_id,name
P1,Laptop
P2,Mouse
P3,Keyboard`

	// Right DataFrame: Inventory
	inventoryCSV := `item_id,quantity,warehouse
P1,50,Tokyo
P2,200,Osaka
P4,30,Tokyo`

	products, _ := frame.NewDataFrame(strings.NewReader(productsCSV), frame.CSV)   //nolint:errcheck
	inventory, _ := frame.NewDataFrame(strings.NewReader(inventoryCSV), frame.CSV) //nolint:errcheck

	// Inner Join: Only products in inventory
	inner, _ := products.Join(inventory, frame.JoinOption{ //nolint:errcheck
		On:  []string{"product_id", "item_id"},
		How: frame.InnerJoin,
	})
	fmt.Printf("Inner Join: %d rows (products in inventory)\n", inner.Len())

	// Left Join: All products, with inventory if exists
	left, _ := products.Join(inventory, frame.JoinOption{ //nolint:errcheck
		On:  []string{"product_id", "item_id"},
		How: frame.LeftJoin,
	})
	fmt.Printf("Left Join: %d rows (all products)\n", left.Len())

	// Right Join: All inventory items, with product info if exists
	right, _ := products.Join(inventory, frame.JoinOption{ //nolint:errcheck
		On:  []string{"product_id", "item_id"},
		How: frame.RightJoin,
	})
	fmt.Printf("Right Join: %d rows (all inventory)\n", right.Len())

	// Outer Join: Everything from both
	outer, _ := products.Join(inventory, frame.JoinOption{ //nolint:errcheck
		On:  []string{"product_id", "item_id"},
		How: frame.OuterJoin,
	})
	fmt.Printf("Outer Join: %d rows (all products + all inventory)\n", outer.Len())

	// Output:
	// Inner Join: 2 rows (products in inventory)
	// Left Join: 3 rows (all products)
	// Right Join: 3 rows (all inventory)
	// Outer Join: 4 rows (all products + all inventory)
}

// Example_concat demonstrates vertical concatenation of DataFrames with the same schema.
// Use Concat when combining data from multiple sources with identical columns.
func Example_concat() {
	// Sales data from different regions (same schema)
	tokyoCSV := `region,product,sales
Tokyo,Laptop,100
Tokyo,Mouse,300`

	osakaCSV := `region,product,sales
Osaka,Laptop,80
Osaka,Mouse,250
Osaka,Keyboard,120`

	tokyo, _ := frame.NewDataFrame(strings.NewReader(tokyoCSV), frame.CSV) //nolint:errcheck
	osaka, _ := frame.NewDataFrame(strings.NewReader(osakaCSV), frame.CSV) //nolint:errcheck

	// Concat requires identical columns
	combined, err := tokyo.Concat(osaka)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Printf("Tokyo rows: %d\n", tokyo.Len())
	fmt.Printf("Osaka rows: %d\n", osaka.Len())
	fmt.Printf("Combined rows: %d\n", combined.Len())

	// Now we can analyze the combined data
	grouped, _ := combined.GroupBy("product") //nolint:errcheck
	totals, _ := grouped.Sum("sales")         //nolint:errcheck

	// Sort by product name for deterministic output
	sortedTotals, _ := totals.Sort("product", frame.Ascending) //nolint:errcheck

	fmt.Println("\nTotal sales by product:")
	for _, row := range sortedTotals.ToRecords() {
		fmt.Printf("  %s: %.0f\n", row["product"], row["sum_sales"])
	}

	// Output:
	// Tokyo rows: 2
	// Osaka rows: 3
	// Combined rows: 5
	//
	// Total sales by product:
	//   Keyboard: 120
	//   Laptop: 180
	//   Mouse: 550
}

// Example_concatAll demonstrates flexible concatenation of DataFrames with different schemas.
// ConcatAll automatically handles different column sets by creating a union of all columns.
func Example_concatAll() {
	// Data from 2023 - basic schema
	data2023CSV := `year,product,sales
2023,Laptop,1000
2023,Mouse,500`

	// Data from 2024 - added "region" column
	data2024CSV := `year,product,sales,region
2024,Laptop,1200,Tokyo
2024,Mouse,600,Osaka
2024,Keyboard,300,Tokyo`

	df2023, _ := frame.NewDataFrame(strings.NewReader(data2023CSV), frame.CSV) //nolint:errcheck
	df2024, _ := frame.NewDataFrame(strings.NewReader(data2024CSV), frame.CSV) //nolint:errcheck

	fmt.Printf("2023 columns: %v\n", df2023.Columns())
	fmt.Printf("2024 columns: %v\n", df2024.Columns())

	// ConcatAll handles different schemas
	combined, err := frame.ConcatAll(df2023, df2024)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Printf("Combined columns: %v\n", combined.Columns())
	fmt.Printf("Combined rows: %d\n", combined.Len())

	// 2023 data will have nil for "region"
	fmt.Println("\nCombined data:")
	for _, row := range combined.ToRecords() {
		region := row["region"]
		if region == nil {
			region = "(no region)"
		}
		// Handle int64 vs float64 for sales column
		var sales float64
		switch v := row["sales"].(type) {
		case int64:
			sales = float64(v)
		case float64:
			sales = v
		}
		fmt.Printf("  %v %s: %.0f - %v\n",
			row["year"], row["product"], sales, region)
	}

	// Output:
	// 2023 columns: [year product sales]
	// 2024 columns: [year product sales region]
	// Combined columns: [product region sales year]
	// Combined rows: 5
	//
	// Combined data:
	//   2023 Laptop: 1000 - (no region)
	//   2023 Mouse: 500 - (no region)
	//   2024 Laptop: 1200 - Tokyo
	//   2024 Mouse: 600 - Osaka
	//   2024 Keyboard: 300 - Tokyo
}

// Example_sortAndDistinct demonstrates sorting and deduplication operations.
func Example_sortAndDistinct() {
	csvData := `name,category,score
Alice,A,85
Bob,B,90
Charlie,A,85
Alice,A,85
Diana,B,75
Eve,A,95`

	df, _ := frame.NewDataFrame(strings.NewReader(csvData), frame.CSV) //nolint:errcheck

	fmt.Printf("Original rows: %d\n", df.Len())

	// Remove duplicate rows
	unique := df.Distinct()
	fmt.Printf("After Distinct: %d rows\n", unique.Len())

	// Sort by score descending
	sorted, _ := unique.Sort("score", frame.Descending) //nolint:errcheck
	fmt.Println("\nTop scores:")
	for _, row := range sorted.Head(3).ToRecords() {
		fmt.Printf("  %s: %v\n", row["name"], row["score"])
	}

	// Sort by multiple columns: category ascending, then score descending
	multiSorted, _ := unique.SortBy( //nolint:errcheck
		frame.SortOption{Column: "category", Order: frame.Ascending},
		frame.SortOption{Column: "score", Order: frame.Descending},
	)
	fmt.Println("\nBy category, then score:")
	for _, row := range multiSorted.ToRecords() {
		fmt.Printf("  [%s] %s: %v\n", row["category"], row["name"], row["score"])
	}

	// Output:
	// Original rows: 6
	// After Distinct: 5 rows
	//
	// Top scores:
	//   Eve: 95
	//   Bob: 90
	//   Alice: 85
	//
	// By category, then score:
	//   [A] Eve: 95
	//   [A] Alice: 85
	//   [A] Charlie: 85
	//   [B] Bob: 90
	//   [B] Diana: 75
}

// Example_headTailLimit demonstrates row selection operations.
func Example_headTailLimit() {
	csvData := `id,value
1,100
2,200
3,300
4,400
5,500
6,600
7,700`

	df, _ := frame.NewDataFrame(strings.NewReader(csvData), frame.CSV) //nolint:errcheck

	fmt.Printf("Total rows: %d\n", df.Len())

	// Get first 3 rows
	head := df.Head(3)
	fmt.Printf("\nHead(3) - first 3 rows:\n")
	for _, row := range head.ToRecords() {
		fmt.Printf("  id=%v, value=%v\n", row["id"], row["value"])
	}

	// Get last 2 rows
	tail := df.Tail(2)
	fmt.Printf("\nTail(2) - last 2 rows:\n")
	for _, row := range tail.ToRecords() {
		fmt.Printf("  id=%v, value=%v\n", row["id"], row["value"])
	}

	// Limit is alias for Head - useful for SQL-like syntax
	limited := df.Limit(2)
	fmt.Printf("\nLimit(2) rows: %d\n", limited.Len())

	// Output:
	// Total rows: 7
	//
	// Head(3) - first 3 rows:
	//   id=1, value=100
	//   id=2, value=200
	//   id=3, value=300
	//
	// Tail(2) - last 2 rows:
	//   id=6, value=600
	//   id=7, value=700
	//
	// Limit(2) rows: 2
}

// Example_dropRename demonstrates column manipulation operations.
func Example_dropRename() {
	csvData := `user_id,first_name,last_name,internal_code,email
1,Alice,Smith,X123,alice@example.com
2,Bob,Jones,X456,bob@example.com`

	df, _ := frame.NewDataFrame(strings.NewReader(csvData), frame.CSV) //nolint:errcheck

	fmt.Printf("Original columns: %v\n", df.Columns())

	// Drop internal column
	cleaned, _ := df.Drop("internal_code") //nolint:errcheck // the column exists
	fmt.Printf("After Drop: %v\n", cleaned.Columns())

	// Rename columns for clarity
	renamed, _ := cleaned.RenameColumns(map[string]string{ //nolint:errcheck
		"first_name": "first",
		"last_name":  "last",
	})
	fmt.Printf("After Rename: %v\n", renamed.Columns())

	// Single column rename
	final, _ := renamed.Rename("user_id", "id") //nolint:errcheck
	fmt.Printf("Final columns: %v\n", final.Columns())

	// Output:
	// Original columns: [user_id first_name last_name internal_code email]
	// After Drop: [user_id first_name last_name email]
	// After Rename: [user_id first last email]
	// Final columns: [id first last email]
}

// Example_handleMissingValues demonstrates DropNA and FillNA operations.
func Example_handleMissingValues() {
	// Create DataFrame with nil values
	records := []map[string]any{
		{"name": "Alice", "age": int64(30), "city": "Tokyo"},
		{"name": "Bob", "age": nil, "city": "Osaka"},
		{"name": nil, "age": int64(25), "city": nil},
		{"name": "Diana", "age": int64(35), "city": "Kyoto"},
	}
	df := frame.NewDataFrameFromRecords(records)

	fmt.Printf("Original rows: %d\n", df.Len())

	// DropNA: Remove rows with any nil values
	cleaned := df.DropNA()
	fmt.Printf("After DropNA: %d rows\n", cleaned.Len())

	// DropNASubset: Remove rows with nil only in specific columns
	partialClean := df.DropNASubset("name")
	fmt.Printf("After DropNASubset(name): %d rows\n", partialClean.Len())

	// FillNA: Replace all nil values with a default
	filled := df.FillNA("Unknown")
	fmt.Println("\nAfter FillNA('Unknown'):")
	for _, row := range filled.ToRecords() {
		fmt.Printf("  %v, %v, %v\n", row["name"], row["age"], row["city"])
	}

	// FillNAByColumn: Different defaults per column
	smartFilled := df.FillNAByColumn(map[string]any{
		"name": "Anonymous",
		"age":  int64(0),
		"city": "Unknown",
	})
	fmt.Println("\nAfter FillNAByColumn:")
	for _, row := range smartFilled.ToRecords() {
		fmt.Printf("  %v, %v, %v\n", row["name"], row["age"], row["city"])
	}

	// Output:
	// Original rows: 4
	// After DropNA: 2 rows
	// After DropNASubset(name): 3 rows
	//
	// After FillNA('Unknown'):
	//   Alice, 30, Tokyo
	//   Bob, Unknown, Osaka
	//   Unknown, 25, Unknown
	//   Diana, 35, Kyoto
	//
	// After FillNAByColumn:
	//   Alice, 30, Tokyo
	//   Bob, 0, Osaka
	//   Anonymous, 25, Unknown
	//   Diana, 35, Kyoto
}

// Example_dataframePipeline demonstrates chaining multiple DataFrame operations
// to build a complete data processing pipeline.
func Example_dataframePipeline() {
	// Raw sales data with some issues
	salesCSV := `date,region,product,quantity,price,salesperson
2024-01-15,Tokyo,Laptop,2,1000,Alice
2024-01-15,Tokyo,Mouse,10,25,Alice
2024-01-16,Osaka,Laptop,1,1000,Bob
2024-01-16,Osaka,Keyboard,5,75,
2024-01-17,Tokyo,Monitor,3,300,Charlie
2024-01-17,Nagoya,Mouse,8,25,Diana`

	df, _ := frame.NewDataFrame(strings.NewReader(salesCSV), frame.CSV) //nolint:errcheck

	// Pipeline: Clean -> Transform -> Aggregate -> Sort -> Limit
	transformed := df.
		// 1. Fill missing salesperson
		FillNAByColumn(map[string]any{"salesperson": "Unknown"}).
		// 2. Add calculated column
		Mutate("revenue", func(row map[string]any) any {
			qty, _ := row["quantity"].(int64) //nolint:errcheck
			price, _ := row["price"].(int64)  //nolint:errcheck
			return float64(qty) * float64(price)
		})
	// 3. Select relevant columns
	result, _ := transformed.Select("region", "product", "revenue", "salesperson") //nolint:errcheck // the columns exist

	// Group by region and sum revenue
	grouped, _ := result.GroupBy("region") //nolint:errcheck
	byRegion, _ := grouped.Sum("revenue")  //nolint:errcheck

	// Sort by revenue descending
	sorted, _ := byRegion.Sort("sum_revenue", frame.Descending) //nolint:errcheck

	fmt.Println("Revenue by Region (Top to Bottom):")
	for _, row := range sorted.ToRecords() {
		fmt.Printf("  %s: $%.0f\n", row["region"], row["sum_revenue"])
	}

	// Also get top 3 individual sales
	topSales, _ := result.Sort("revenue", frame.Descending) //nolint:errcheck
	fmt.Println("\nTop 3 Sales:")
	for _, row := range topSales.Head(3).ToRecords() {
		fmt.Printf("  %s in %s: $%.0f (by %s)\n",
			row["product"], row["region"], row["revenue"], row["salesperson"])
	}

	// Output:
	// Revenue by Region (Top to Bottom):
	//   Tokyo: $3150
	//   Osaka: $1375
	//   Nagoya: $200
	//
	// Top 3 Sales:
	//   Laptop in Tokyo: $2000 (by Alice)
	//   Laptop in Osaka: $1000 (by Bob)
	//   Monitor in Tokyo: $900 (by Charlie)
}
