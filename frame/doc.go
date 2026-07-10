// Package frame provides a lightweight table utility that bridges prep and filesql.
//
// frame is not a degraded copy of Pandas, but a practical tabular data manipulation
// tool that is idiomatic to Go. It follows the UNIX philosophy of doing one thing well.
//
// # Design Philosophy
//
//   - Small: Do one thing well (UNIX philosophy)
//   - Practical: Only features used in real data analysis
//   - Simple and clear: API is self-explanatory
//   - Intuitive: Natural Go-like coding style
//   - Extensible: Complex features delegated to filesql
//
// # Basic Usage
//
//	// Create DataFrame from CSV
//	f, _ := os.Open("sales.csv")
//	defer f.Close()
//
//	df, err := frame.NewDataFrame(f, frame.CSV)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Select columns and filter rows
//	result := df.
//	    Select("product", "amount", "category").
//	    Filter(func(row map[string]any) bool {
//	        amount, ok := row["amount"].(float64)
//	        return ok && amount > 1000
//	    })
//
//	// Group by and aggregate
//	grouped := result.
//	    GroupBy("category").
//	    Sum("amount")
//
//	// Output to CSV
//	grouped.ToCSV("summary.csv")
//
// # Architecture
//
// frame sits between prep (preprocessing) and filesql (persistence/SQL):
//
//   - Receives output from prep (io.Reader -> DataFrame)
//   - Performs basic transformations (Select, Filter, Mutate, GroupBy)
//   - Outputs to CSV or passes to filesql
//
// For complex operations like Window functions, subqueries, or large-scale data processing,
// use filesql directly.
//
// # Important Notes
//
//   - All operations execute immediately (no lazy evaluation)
//   - Target scale: Small to medium data (under 100,000 rows)
//   - Row-oriented design with []map[string]any
//   - All methods return new DataFrames (immutable operations)
package frame
