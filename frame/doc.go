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
//	f, err := os.Open("sales.csv")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer func() {
//	    if err := f.Close(); err != nil {
//	        log.Fatal(err)
//	    }
//	}()
//
//	df, err := frame.NewDataFrame(f, frame.CSV)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Select columns and filter rows
//	selected, err := df.Select("product", "amount", "category")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	result := selected.Filter(func(row map[string]any) bool {
//	    amount, ok := frame.Row(row).Float("amount")
//	    return ok && amount > 1000
//	})
//
//	// Group by and aggregate
//	byCategory, err := result.GroupBy("category")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	grouped, err := byCategory.Sum("amount")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Output to CSV
//	if err := grouped.ToCSV("summary.csv"); err != nil {
//	    log.Fatal(err)
//	}
//
// # Value Types
//
// CSV, TSV and LTSV carry no types, so a column's type is inferred from a
// sample of its values and each cell is stored as an int64, a float64, or the
// text it was read from. A callback therefore receives a value that may not be
// the string the file held, which is what Row is for: its accessors return the
// value in the form the caller asks for.
//
// What the inference preserves is the quantity, not the way it was written.
// 1, 1.0 and 1.00 are all the real 1, so they are one value: Distinct collapses
// them to a single row and a join matches them to each other. Keeping the
// spelling instead would mean keeping the values as text, where 9.00 does not
// compare as less than 10.00 and arithmetic on a column of money stops working.
// A file whose decimal scale is meaningful is better read into a column of your
// own declared as text, and formatted on the way out.
//
// Two kinds of value are kept as text for the opposite reason, because
// converting them would change what they are rather than how they look: a
// zero-padded code, where 007 and 7 are two different codes, and an integer
// past the range of int64, which has no exact numeric form. Those stay distinct
// through Distinct and Join.
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
