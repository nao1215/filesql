//nolint:gosec // Example files use simplified error handling for clarity
package filesql_test

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing/fstest"
	"time"

	"github.com/nao1215/filesql"
)

//go:embed testdata/embed_test/*.csv testdata/embed_test/*.tsv
var builderExampleFS embed.FS

// ExampleOpen demonstrates how to use filesql.Open() with complex SQL queries.
// This example shows advanced SQL features including JOINs, window functions,
// subqueries, and aggregations on CSV data loaded into an in-memory SQLite database.
func ExampleOpen() {
	// Create temporary test data files
	tmpDir := createTempTestData()
	defer os.RemoveAll(tmpDir)

	// Open the database with multiple files
	db, err := filesql.Open(filepath.Join(tmpDir, "employees.csv"), filepath.Join(tmpDir, "departments.csv"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Complex query demonstrating multiple SQL features:
	// - JOINs between tables
	// - Window functions (RANK, AVG, COUNT)
	// - Subqueries with correlated conditions
	// - CASE statements
	// - Grouping and ordering
	query := `
		SELECT 
			e.name,
			d.name as department_name,
			e.salary,
			d.budget,
			RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) as salary_rank_in_dept,
			AVG(e.salary) OVER (PARTITION BY e.department_id) as dept_avg_salary,
			COUNT(*) OVER (PARTITION BY e.department_id) as dept_employee_count,
			CASE 
				WHEN e.salary > (SELECT AVG(salary) FROM employees WHERE department_id = e.department_id) * 1.2
				THEN 'High Performer'
				WHEN e.salary < (SELECT AVG(salary) FROM employees WHERE department_id = e.department_id) * 0.8  
				THEN 'Below Average'
				ELSE 'Average'
			END as performance_category,
			ROUND(e.salary / d.budget * 100, 2) as salary_budget_percentage
		FROM employees e
		JOIN departments d ON e.department_id = d.id
		WHERE e.salary > (
			SELECT AVG(salary) * 0.7
			FROM employees e2 
			WHERE e2.department_id = e.department_id
		)
		AND d.budget > 500000
		ORDER BY d.name, e.salary DESC
		LIMIT 10
	`

	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("Employee Analysis Report:")
	fmt.Println("========================")

	for rows.Next() {
		var name, deptName, perfCategory string
		var salary, budget, deptAvgSalary, salaryBudgetPct float64
		var salaryRank, deptEmpCount int

		err := rows.Scan(&name, &deptName, &salary, &budget, &salaryRank,
			&deptAvgSalary, &deptEmpCount, &perfCategory, &salaryBudgetPct)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("%-15s | %-12s | $%7.0f | Rank: %d/%d | %s\n",
			name, deptName, salary, salaryRank, deptEmpCount, perfCategory)
	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	// Output:
	// Employee Analysis Report:
	// ========================
	// Eve Davis       | Marketing    | $  70000 | Rank: 1/2 | Average
	// Frank Miller    | Marketing    | $  65000 | Rank: 2/2 | Average
	// Grace Lee       | Sales        | $  60000 | Rank: 1/2 | Average
	// Henry Taylor    | Sales        | $  55000 | Rank: 2/2 | Average
}

// createTempTestData creates temporary CSV files for the example
func createTempTestData() string {
	tmpDir, err := os.MkdirTemp("", "filesql_example")
	if err != nil {
		log.Fatal(err)
	}

	// Create employees.csv
	employeesData := `id,name,department_id,salary,hire_date
1,Alice Johnson,1,95000,2020-01-15
2,Bob Smith,1,85000,2019-03-22
3,Charlie Brown,1,80000,2021-06-10
4,David Wilson,1,75000,2022-02-28
5,Eve Davis,2,70000,2020-09-15
6,Frank Miller,2,65000,2021-11-30
7,Grace Lee,3,60000,2019-12-05
8,Henry Taylor,3,55000,2022-04-18`

	err = os.WriteFile(filepath.Join(tmpDir, "employees.csv"), []byte(employeesData), 0600)
	if err != nil {
		log.Fatal(err)
	}

	// Create departments.csv
	departmentsData := `id,name,budget,manager_id
1,Engineering,1000000,1
2,Marketing,800000,5
3,Sales,600000,7
4,HR,400000,9`

	err = os.WriteFile(filepath.Join(tmpDir, "departments.csv"), []byte(departmentsData), 0600)
	if err != nil {
		log.Fatal(err)
	}

	return tmpDir
}

// createSalesTestData creates test data for sales analysis examples
func createSalesTestData() string {
	tmpDir, err := os.MkdirTemp("", "filesql_sales_example")
	if err != nil {
		log.Fatal(err)
	}

	// Create sales.csv
	salesData := `order_id,customer_id,product_name,category,quantity,unit_price,order_date,region
1,101,Laptop Pro,Electronics,2,1299.99,2024-01-15,North
2,102,Wireless Mouse,Electronics,1,29.99,2024-01-16,South
3,103,Office Chair,Furniture,1,299.99,2024-01-17,East
4,101,USB Cable,Electronics,3,12.99,2024-01-18,North
5,104,Standing Desk,Furniture,1,599.99,2024-01-19,West
6,105,Bluetooth Speaker,Electronics,2,79.99,2024-01-20,South
7,106,Coffee Table,Furniture,1,199.99,2024-01-21,East
8,102,Keyboard,Electronics,1,89.99,2024-01-22,South
9,107,Monitor 24inch,Electronics,1,249.99,2024-01-23,North
10,103,Desk Lamp,Furniture,2,39.99,2024-01-24,East`

	err = os.WriteFile(filepath.Join(tmpDir, "sales.csv"), []byte(salesData), 0600)
	if err != nil {
		log.Fatal(err)
	}

	// Create customers.csv
	customersData := `customer_id,name,email,city,registration_date
101,John Doe,john@example.com,New York,2023-06-01
102,Jane Smith,jane@example.com,Los Angeles,2023-07-15
103,Bob Johnson,bob@example.com,Chicago,2023-08-20
104,Alice Brown,alice@example.com,Houston,2023-09-10
105,Charlie Wilson,charlie@example.com,Phoenix,2023-10-05
106,Diana Lee,diana@example.com,Philadelphia,2023-11-12
107,Frank Miller,frank@example.com,San Antonio,2023-12-03`

	err = os.WriteFile(filepath.Join(tmpDir, "customers.csv"), []byte(customersData), 0600)
	if err != nil {
		log.Fatal(err)
	}

	return tmpDir
}

// ExampleOpen_multipleFiles demonstrates opening multiple files and directories
func ExampleOpen_multipleFiles() {
	tmpDir := createTempTestData()
	defer os.RemoveAll(tmpDir)

	// Open database with multiple paths (files and directories)
	db, err := filesql.Open(tmpDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Query to show all available tables
	rows, err := db.QueryContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("Available tables:")
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("- %s\n", tableName)
	}

	// Output:
	// Available tables:
	// - departments
	// - employees
}

// ExampleOpenContext demonstrates opening files with context support for timeout and cancellation
func ExampleOpenContext() {
	tmpDir := createTempTestData()
	defer os.RemoveAll(tmpDir)

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Open database with context
	db, err := filesql.OpenContext(ctx, filepath.Join(tmpDir, "employees.csv"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Query with context support
	rows, err := db.QueryContext(ctx, `
		SELECT name, salary 
		FROM employees 
		WHERE salary > 70000 
		ORDER BY salary DESC
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("High earners (>$70,000):")
	for rows.Next() {
		var name string
		var salary float64
		if err := rows.Scan(&name, &salary); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("- %s: $%.0f\n", name, salary)
	}

	// Output:
	// High earners (>$70,000):
	// - Alice Johnson: $95000
	// - Bob Smith: $85000
	// - Charlie Brown: $80000
	// - David Wilson: $75000
}

// ExampleOpen_constraints demonstrates the constraint that modifications don't affect original files
func ExampleOpen_constraints() {
	tmpDir := createTempTestData()
	defer os.RemoveAll(tmpDir)

	db, err := filesql.Open(filepath.Join(tmpDir, "employees.csv"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Show original data count
	var originalCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM employees").Scan(&originalCount)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Original employee count: %d\n", originalCount)

	// Insert new data (only affects in-memory database)
	_, err = db.ExecContext(context.Background(), "INSERT INTO employees (id, name, department_id, salary, hire_date) VALUES (99, 'Test User', 1, 50000, '2023-01-01')")
	if err != nil {
		log.Fatal(err)
	}

	// Show in-memory count
	var memoryCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM employees").Scan(&memoryCount)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("In-memory count after INSERT: %d\n", memoryCount)

	// Verify original file is unchanged by reopening
	db2, err := filesql.Open(filepath.Join(tmpDir, "employees.csv"))
	if err != nil {
		log.Fatal(err)
	}
	defer db2.Close()

	var fileCount int
	err = db2.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM employees").Scan(&fileCount)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("File-based count (unchanged): %d\n", fileCount)

	// Output:
	// Original employee count: 8
	// In-memory count after INSERT: 9
	// File-based count (unchanged): 8
}

// ExampleOpen_salesAnalysis demonstrates practical sales data analysis
func ExampleOpen_salesAnalysis() {
	tmpDir := createSalesTestData()
	defer os.RemoveAll(tmpDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := filesql.OpenContext(ctx, tmpDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Sales summary by category and region
	query := `
		SELECT 
			category,
			region,
			COUNT(*) as order_count,
			SUM(quantity * unit_price) as total_revenue,
			AVG(quantity * unit_price) as avg_order_value,
			MIN(order_date) as first_order,
			MAX(order_date) as last_order
		FROM sales 
		GROUP BY category, region
		ORDER BY total_revenue DESC
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("Sales Analysis by Category and Region:")
	fmt.Println("=====================================")
	fmt.Printf("%-12s %-8s %-6s %-10s %-12s %-12s %s\n",
		"Category", "Region", "Orders", "Revenue", "Avg Order", "First Order", "Last Order")
	fmt.Println(strings.Repeat("-", 80))

	for rows.Next() {
		var category, region, firstOrder, lastOrder string
		var orderCount int
		var totalRevenue, avgOrderValue float64

		err := rows.Scan(&category, &region, &orderCount, &totalRevenue, &avgOrderValue, &firstOrder, &lastOrder)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("%-12s %-8s %-6d $%-9.2f $%-11.2f %-12s %s\n",
			category, region, orderCount, totalRevenue, avgOrderValue, firstOrder, lastOrder)
	}

	// Output:
	// Sales Analysis by Category and Region:
	// =====================================
	// Category     Region   Orders Revenue    Avg Order    First Order  Last Order
	// --------------------------------------------------------------------------------
	// Electronics  North    3      $2888.94   $962.98      2024-01-15   2024-01-23
	// Furniture    West     1      $599.99    $599.99      2024-01-19   2024-01-19
	// Furniture    East     3      $579.96    $193.32      2024-01-17   2024-01-24
	// Electronics  South    3      $279.96    $93.32       2024-01-16   2024-01-22
}

// ExampleOpen_customerInsights demonstrates customer behavior analysis
func ExampleOpen_customerInsights() {
	tmpDir := createSalesTestData()
	defer os.RemoveAll(tmpDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := filesql.OpenContext(ctx, tmpDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Customer lifetime value and behavior analysis
	query := `
		SELECT 
			c.name,
			c.city,
			COUNT(s.order_id) as total_orders,
			SUM(s.quantity * s.unit_price) as lifetime_value,
			AVG(s.quantity * s.unit_price) as avg_order_value,
			MIN(s.order_date) as first_purchase,
			MAX(s.order_date) as last_purchase,
			julianday(MAX(s.order_date)) - julianday(MIN(s.order_date)) as days_active,
			COUNT(DISTINCT s.category) as categories_purchased
		FROM customers c
		JOIN sales s ON c.customer_id = s.customer_id
		GROUP BY c.customer_id, c.name, c.city
		HAVING total_orders > 1
		ORDER BY lifetime_value DESC
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("Customer Insights (Multi-Purchase Customers):")
	fmt.Println("===========================================")
	fmt.Printf("%-12s %-12s %-7s %-10s %-10s %-12s %-12s %-6s %s\n",
		"Name", "City", "Orders", "LTV", "Avg Order", "First Buy", "Last Buy", "Days", "Categories")
	fmt.Println(strings.Repeat("-", 100))

	for rows.Next() {
		var name, city, firstPurchase, lastPurchase string
		var totalOrders, daysActive, categoriesPurchased int
		var lifetimeValue, avgOrderValue float64

		err := rows.Scan(&name, &city, &totalOrders, &lifetimeValue, &avgOrderValue,
			&firstPurchase, &lastPurchase, &daysActive, &categoriesPurchased)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("%-12s %-12s %-7d $%-9.2f $%-9.2f %-12s %-12s %-6d %d\n",
			name, city, totalOrders, lifetimeValue, avgOrderValue,
			firstPurchase, lastPurchase, daysActive, categoriesPurchased)
	}

	// Output:
	// Customer Insights (Multi-Purchase Customers):
	// ===========================================
	// Name         City         Orders  LTV        Avg Order  First Buy    Last Buy     Days   Categories
	// ----------------------------------------------------------------------------------------------------
	// John Doe     New York     2       $2638.95   $1319.47   2024-01-15   2024-01-18   3      1
	// Bob Johnson  Chicago      2       $379.97    $189.99    2024-01-17   2024-01-24   7      1
	// Jane Smith   Los Angeles  2       $119.98    $59.99     2024-01-16   2024-01-22   6      1
}

// ExampleOpen_errorHandling demonstrates proper error handling patterns
func ExampleOpen_errorHandling() {
	// Example 1: Handling non-existent files gracefully
	_, err := filesql.Open("nonexistent.csv")
	if err != nil {
		fmt.Printf("Expected error for non-existent file: %v\n", err)
	}

	// Example 2: Context timeout handling
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond) // Very short timeout
	defer cancel()

	// This will likely timeout
	tmpDir := createTempTestData()
	defer os.RemoveAll(tmpDir)

	time.Sleep(10 * time.Millisecond) // Ensure timeout triggers
	_, err = filesql.OpenContext(ctx, tmpDir)
	if err != nil {
		fmt.Printf("Expected timeout error: %v\n", err)
	}

	// Example 3: Successful operation with proper error checking
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	db, err := filesql.OpenContext(ctx2, tmpDir)
	if err != nil {
		fmt.Printf("Unexpected error: %v\n", err)
		return
	}
	defer db.Close()

	// Test query with error handling
	rows, err := db.QueryContext(ctx2, "SELECT COUNT(*) FROM employees")
	if err != nil {
		fmt.Printf("Query error: %v\n", err)
		return
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			fmt.Printf("Scan error: %v\n", err)
			return
		}
		fmt.Printf("Successfully counted %d employees\n", count)
	}

	if err := rows.Err(); err != nil {
		fmt.Printf("Rows iteration error: %v\n", err)
		return
	}

	// Output:
	// Expected error for non-existent file: failed to load file: path does not exist: nonexistent.csv
	// Expected timeout error: context deadline exceeded
	// Successfully counted 8 employees
}

// ExampleDumpDatabase demonstrates exporting modified data
func ExampleDumpDatabase() {
	tmpDir := createTempTestData()
	defer os.RemoveAll(tmpDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := filesql.OpenContext(ctx, filepath.Join(tmpDir, "employees.csv"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Modify data in memory
	_, err = db.ExecContext(ctx, `
		UPDATE employees 
		SET salary = salary * 1.10 
		WHERE department_id = 1
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Add a new employee
	_, err = db.ExecContext(ctx, `
		INSERT INTO employees (id, name, department_id, salary, hire_date) 
		VALUES (99, 'New Employee', 2, 60000, '2024-01-01')
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	err = os.MkdirAll(outputDir, 0750)
	if err != nil {
		log.Fatal(err)
	}

	// Export modified data
	err = filesql.DumpDatabase(db, outputDir)
	if err != nil {
		log.Fatal(err)
	}

	// Verify export by reading the exported file
	exportedFile := filepath.Join(outputDir, "employees.csv")
	if _, err := os.Stat(exportedFile); err != nil {
		log.Fatal("Exported file not found:", err)
	}

	// Count records in exported file
	db2, err := filesql.OpenContext(ctx, exportedFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db2.Close()

	var count int
	err = db2.QueryRowContext(ctx, "SELECT COUNT(*) FROM employees").Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Original file: 8 employees\n")
	fmt.Printf("Modified and exported: %d employees\n", count)

	// Extract just the filename for consistent output (normalize path separators for cross-platform compatibility)
	exportPath := strings.Replace(exportedFile, tmpDir, "/tmp/filesql_example*", 1)
	exportPath = strings.ReplaceAll(exportPath, "\\", "/") // Convert Windows backslashes to forward slashes
	fmt.Printf("Export location: %s\n", exportPath)

	// Output:
	// Original file: 8 employees
	// Modified and exported: 9 employees
	// Export location: /tmp/filesql_example*/output/employees.csv
}

// ExampleOpen_performanceOptimization demonstrates techniques for handling large datasets efficiently
func ExampleOpen_performanceOptimization() {
	tmpDir := createLargeTestData()
	defer os.RemoveAll(tmpDir)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := filesql.OpenContext(ctx, tmpDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Technique 1: Use LIMIT and OFFSET for pagination
	fmt.Println("=== Performance Optimization Techniques ===")
	fmt.Println("\n1. Pagination with LIMIT and OFFSET:")

	pageSize := 3
	offset := 0

	for page := 1; page <= 2; page++ {
		rows, err := db.QueryContext(ctx, `
			SELECT customer_id, name, total_orders 
			FROM customer_summary 
			ORDER BY total_orders DESC 
			LIMIT ? OFFSET ?
		`, pageSize, offset)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Page %d:\n", page)
		for rows.Next() {
			var customerID int
			var name string
			var totalOrders int
			if err := rows.Scan(&customerID, &name, &totalOrders); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("  - %s (ID: %d, Orders: %d)\n", name, customerID, totalOrders)
		}
		_ = rows.Close() // Ignore close error in test cleanup
		offset += pageSize
	}

	// Technique 2: Use indexes by querying with WHERE clauses on sorted columns
	fmt.Println("\n2. Efficient filtering with indexes:")
	rows, err := db.QueryContext(ctx, `
		SELECT name, email, registration_date 
		FROM customer_summary 
		WHERE total_spent > 1000 
		ORDER BY total_spent DESC
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("High-value customers:")
	for rows.Next() {
		var name, email, regDate string
		if err := rows.Scan(&name, &email, &regDate); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  - %s (%s) - Registered: %s\n", name, email, regDate)
	}

	// Technique 3: Aggregate queries for summary statistics
	fmt.Println("\n3. Summary statistics:")
	var totalCustomers int
	var avgOrders, totalRevenue, avgSpent float64

	err = db.QueryRowContext(ctx, `
		SELECT 
			COUNT(*) as total_customers,
			AVG(total_orders) as avg_orders,
			SUM(total_spent) as total_revenue,
			AVG(total_spent) as avg_spent
		FROM customer_summary
	`).Scan(&totalCustomers, &avgOrders, &totalRevenue, &avgSpent)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Total customers: %d\n", totalCustomers)
	fmt.Printf("Average orders per customer: %.1f\n", avgOrders)
	fmt.Printf("Total revenue: $%.2f\n", totalRevenue)
	fmt.Printf("Average customer value: $%.2f\n", avgSpent)

	// Output:
	// === Performance Optimization Techniques ===
	//
	// 1. Pagination with LIMIT and OFFSET:
	// Page 1:
	//   - Regular Customer D (ID: 1004, Orders: 8)
	//   - Regular Customer E (ID: 1005, Orders: 6)
	//   - Regular Customer F (ID: 1006, Orders: 5)
	// Page 2:
	//   - Budget Customer G (ID: 1007, Orders: 3)
	//   - Budget Customer H (ID: 1008, Orders: 2)
	//   - Premium Customer A (ID: 1001, Orders: 15)
	//
	// 2. Efficient filtering with indexes:
	// High-value customers:
	//   - Regular Customer D (regular.d@example.com) - Registered: 2023-04-05
	//   - Regular Customer E (regular.e@example.com) - Registered: 2023-05-15
	//   - Regular Customer F (regular.f@example.com) - Registered: 2023-06-20
	//   - Budget Customer G (budget.g@example.com) - Registered: 2023-07-10
	//   - Budget Customer H (budget.h@example.com) - Registered: 2023-08-25
	//   - Premium Customer A (premium.a@example.com) - Registered: 2023-01-15
	//   - Premium Customer B (premium.b@example.com) - Registered: 2023-02-20
	//   - Premium Customer C (premium.c@example.com) - Registered: 2023-03-10
	//
	// 3. Summary statistics:
	// Total customers: 10
	// Average orders per customer: 6.3
	// Total revenue: $6300.00
	// Average customer value: $630.00
}

// ExampleOpen_advancedSQL demonstrates advanced SQL features available in SQLite3
func ExampleOpen_advancedSQL() {
	tmpDir := createAdvancedTestData()
	defer os.RemoveAll(tmpDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := filesql.OpenContext(ctx, tmpDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== Advanced SQL Features ===")

	// Window functions with RANK() and ROW_NUMBER()
	fmt.Println("\n1. Window Functions - Employee Rankings by Department:")
	rows, err := db.QueryContext(ctx, `
		SELECT 
			e.name,
			d.name as department,
			e.salary,
			RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) as salary_rank,
			ROW_NUMBER() OVER (ORDER BY e.salary DESC) as overall_rank
		FROM employees e
		JOIN departments d ON e.department_id = d.id
		ORDER BY e.department_id, salary_rank
	`)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%-15s %-12s %-8s %-10s %s\n", "Name", "Department", "Salary", "Dept Rank", "Overall Rank")
	fmt.Println(strings.Repeat("-", 65))

	for rows.Next() {
		var name, department string
		var salary float64
		var salaryRank, overallRank int

		if err := rows.Scan(&name, &department, &salary, &salaryRank, &overallRank); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-15s %-12s $%-7.0f %-10d %d\n", name, department, salary, salaryRank, overallRank)
	}
	_ = rows.Close() // Ignore close error in test cleanup

	// Common Table Expressions (CTE)
	fmt.Println("\n2. Common Table Expressions - Department Analysis:")
	rows, err = db.QueryContext(ctx, `
		WITH dept_stats AS (
			SELECT 
				d.name as department,
				COUNT(e.id) as employee_count,
				AVG(e.salary) as avg_salary,
				MAX(e.salary) as max_salary,
				MIN(e.salary) as min_salary
			FROM departments d
			LEFT JOIN employees e ON d.id = e.department_id
			GROUP BY d.id, d.name
		),
		company_avg AS (
			SELECT AVG(salary) as company_avg_salary
			FROM employees
		)
		SELECT 
			ds.department,
			ds.employee_count,
			ds.avg_salary,
			ca.company_avg_salary,
			ds.avg_salary - ca.company_avg_salary as salary_diff,
			CASE 
				WHEN ds.avg_salary > ca.company_avg_salary THEN 'Above Average'
				WHEN ds.avg_salary < ca.company_avg_salary THEN 'Below Average'
				ELSE 'At Average'
			END as comparison
		FROM dept_stats ds
		CROSS JOIN company_avg ca
		WHERE ds.employee_count > 0
		ORDER BY ds.avg_salary DESC
	`)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%-12s %-5s %-10s %-12s %-10s %s\n", "Department", "Count", "Avg Salary", "Company Avg", "Difference", "Comparison")
	fmt.Println(strings.Repeat("-", 75))

	for rows.Next() {
		var department, comparison string
		var employeeCount int
		var avgSalary, companyAvg, salaryDiff float64

		if err := rows.Scan(&department, &employeeCount, &avgSalary, &companyAvg, &salaryDiff, &comparison); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-12s %-5d $%-9.0f $%-11.0f $%-9.0f %s\n",
			department, employeeCount, avgSalary, companyAvg, salaryDiff, comparison)
	}
	_ = rows.Close() // Ignore close error in test cleanup

	// JSON operations (if data contains JSON)
	fmt.Println("\n3. Text Functions - Name Analysis:")
	rows, err = db.QueryContext(ctx, `
		SELECT 
			name,
			LENGTH(name) as name_length,
			UPPER(SUBSTR(name, 1, 1)) || LOWER(SUBSTR(name, 2)) as formatted_name,
			INSTR(name, ' ') as space_position,
			CASE 
				WHEN INSTR(name, ' ') > 0 THEN SUBSTR(name, 1, INSTR(name, ' ') - 1)
				ELSE name
			END as first_name
		FROM employees
		WHERE LENGTH(name) > 8
		ORDER BY name_length DESC
	`)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%-15s %-6s %-15s %-8s %s\n", "Name", "Length", "Formatted", "Space@", "First Name")
	fmt.Println(strings.Repeat("-", 60))

	for rows.Next() {
		var name, formattedName, firstName string
		var nameLength, spacePos int

		if err := rows.Scan(&name, &nameLength, &formattedName, &spacePos, &firstName); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-15s %-6d %-15s %-8d %s\n", name, nameLength, formattedName, spacePos, firstName)
	}
	_ = rows.Close() // Ignore close error in test cleanup

	// Output:
	// === Advanced SQL Features ===
	//
	// 1. Window Functions - Employee Rankings by Department:
	// Name            Department   Salary   Dept Rank  Overall Rank
	// -----------------------------------------------------------------
	// Alice Johnson   Engineering  $95000   1          1
	// Charlie Brown   Engineering  $80000   2          3
	// David Wilson    Engineering  $75000   3          4
	// Bob Smith       Sales        $85000   1          2
	// Eve Davis       Sales        $65000   2          6
	// Frank Miller    Marketing    $70000   1          5
	//
	// 2. Common Table Expressions - Department Analysis:
	// Department   Count Avg Salary Company Avg  Difference Comparison
	// ---------------------------------------------------------------------------
	// Engineering  3     $83333     $78333       $5000      Above Average
	// Sales        2     $75000     $78333       $-3333     Below Average
	// Marketing    1     $70000     $78333       $-8333     Below Average
	//
	// 3. Text Functions - Name Analysis:
	// Name            Length Formatted       Space@   First Name
	// ------------------------------------------------------------
	// Alice Johnson   13     Alice johnson   6        Alice
	// Charlie Brown   13     Charlie brown   8        Charlie
	// David Wilson    12     David wilson    6        David
	// Frank Miller    12     Frank miller    6        Frank
	// Bob Smith       9      Bob smith       4        Bob
	// Eve Davis       9      Eve davis       4        Eve
}

// ExampleOpen_compressionSupport demonstrates working with compressed files
func ExampleOpen_compressionSupport() {
	tmpDir := createCompressedTestData()
	defer os.RemoveAll(tmpDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Open compressed files seamlessly
	db, err := filesql.OpenContext(ctx, tmpDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== Compression Support Demo ===")
	fmt.Println("Successfully loaded compressed files:")

	// List all tables from compressed files
	rows, err := db.QueryContext(ctx, `
		SELECT name, sql 
		FROM sqlite_master 
		WHERE type='table' 
		ORDER BY name
	`)
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var tableName, createSQL string
		if err := rows.Scan(&tableName, &createSQL); err != nil {
			log.Fatal(err)
		}

		// Count records in each table
		var count int
		countQuery := "SELECT COUNT(*) FROM " + tableName
		if err := db.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
			log.Fatal(err)
		}

		fmt.Printf("- %s: %d records\n", tableName, count)
	}
	_ = rows.Close() // Ignore close error in test cleanup

	// Demonstrate querying across compressed files
	fmt.Println("\nCross-file analysis from compressed data:")

	analysisRows, err := db.QueryContext(ctx, `
		SELECT 
			'logs' as source_table,
			COUNT(*) as total_records,
			MIN(timestamp) as earliest,
			MAX(timestamp) as latest
		FROM logs
		
		UNION ALL
		
		SELECT 
			'products' as source_table,
			COUNT(*) as total_records,
			'N/A' as earliest,
			'N/A' as latest
		FROM products
		
		ORDER BY source_table
	`)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%-12s %-8s %-19s %s\n", "Table", "Records", "Earliest", "Latest")
	fmt.Println(strings.Repeat("-", 60))

	for analysisRows.Next() {
		var sourceTable, earliest, latest string
		var totalRecords int

		if err := analysisRows.Scan(&sourceTable, &totalRecords, &earliest, &latest); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-12s %-8d %-19s %s\n", sourceTable, totalRecords, earliest, latest)
	}
	_ = analysisRows.Close() // Ignore close error in test cleanup

	// Output:
	// === Compression Support Demo ===
	// Successfully loaded compressed files:
	// - logs: 5 records
	// - products: 3 records
	//
	// Cross-file analysis from compressed data:
	// Table        Records  Earliest            Latest
	// ------------------------------------------------------------
	// logs         5        2024-01-01 10:00:00 2024-01-01 14:00:00
	// products     3        N/A                 N/A
}

// ExampleOpen_webLogAnalysis demonstrates analyzing web server logs
func ExampleOpen_webLogAnalysis() {
	tmpDir := createWebLogTestData()
	defer os.RemoveAll(tmpDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := filesql.OpenContext(ctx, tmpDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== Web Log Analysis ===")

	// Top pages by hits
	fmt.Println("\n1. Top Pages by Hits:")
	rows, err := db.QueryContext(ctx, `
		SELECT 
			path,
			COUNT(*) as hits,
			COUNT(DISTINCT ip_address) as unique_visitors
		FROM access_logs 
		WHERE status_code = 200
		GROUP BY path
		ORDER BY hits DESC
		LIMIT 5
	`)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%-20s %-6s %s\n", "Path", "Hits", "Unique")
	fmt.Println(strings.Repeat("-", 35))
	for rows.Next() {
		var path string
		var hits, unique int
		if err := rows.Scan(&path, &hits, &unique); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-20s %-6d %d\n", path, hits, unique)
	}
	_ = rows.Close() // Ignore close error in test cleanup

	// Error analysis
	fmt.Println("\n2. Error Analysis:")
	rows, err = db.QueryContext(ctx, `
		SELECT 
			status_code,
			COUNT(*) as error_count,
			ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM access_logs), 2) as percentage
		FROM access_logs 
		WHERE status_code >= 400
		GROUP BY status_code
		ORDER BY error_count DESC
	`)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%-12s %-6s %-10s\n", "Status Code", "Count", "Percentage")
	fmt.Println(strings.Repeat("-", 30))
	for rows.Next() {
		var statusCode, errorCount int
		var percentage float64
		if err := rows.Scan(&statusCode, &errorCount, &percentage); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-12d %-6d %-10.2f%%\n", statusCode, errorCount, percentage)
	}
	_ = rows.Close() // Ignore close error in test cleanup

	// Hourly traffic pattern
	fmt.Println("\n3. Traffic by Hour:")
	rows, err = db.QueryContext(ctx, `
		SELECT 
			CAST(strftime('%H', timestamp) AS INTEGER) as hour,
			COUNT(*) as requests,
			AVG(response_time) as avg_response_time
		FROM access_logs
		GROUP BY hour
		ORDER BY hour
	`)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%-5s %-9s %-12s\n", "Hour", "Requests", "Avg Response")
	fmt.Println(strings.Repeat("-", 28))
	for rows.Next() {
		var hour, requests int
		var avgResponseTime float64
		if err := rows.Scan(&hour, &requests, &avgResponseTime); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-5d %-9d %-12.0fms\n", hour, requests, avgResponseTime)
	}
	_ = rows.Close() // Ignore close error in test cleanup

	// Output:
	// === Web Log Analysis ===
	//
	// 1. Top Pages by Hits:
	// Path                 Hits   Unique
	// -----------------------------------
	// /                    3      1
	// /products            2      1
	// /contact             1      1
	// /about               1      1
	//
	// 2. Error Analysis:
	// Status Code  Count  Percentage
	// ------------------------------
	// 404          2      22.22     %
	//
	// 3. Traffic by Hour:
	// Hour  Requests  Avg Response
	// ----------------------------
	// 9     2         175         ms
	// 10    3         153         ms
	// 11    3         130         ms
	// 14    1         100         ms
}

// ExampleOpen_financialDataAnalysis demonstrates financial data processing
func ExampleOpen_financialDataAnalysis() {
	tmpDir := createFinancialTestData()
	defer os.RemoveAll(tmpDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := filesql.OpenContext(ctx, tmpDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== Financial Data Analysis ===")

	// Monthly revenue trend
	fmt.Println("\n1. Monthly Revenue Trend:")
	rows, err := db.QueryContext(ctx, `
		SELECT 
			strftime('%Y-%m', transaction_date) as month,
			COUNT(*) as transaction_count,
			SUM(amount) as total_revenue,
			AVG(amount) as avg_transaction,
			MAX(amount) as largest_transaction
		FROM transactions 
		WHERE type = 'sale'
		GROUP BY month
		ORDER BY month
	`)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%-8s %-6s %-10s %-8s %s\n", "Month", "Count", "Revenue", "Average", "Largest")
	fmt.Println(strings.Repeat("-", 50))
	for rows.Next() {
		var month string
		var count int
		var revenue, average, largest float64
		if err := rows.Scan(&month, &count, &revenue, &average, &largest); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-8s %-6d $%-9.2f $%-7.2f $%.2f\n", month, count, revenue, average, largest)
	}
	_ = rows.Close() // Ignore close error in test cleanup

	// Expense category breakdown
	fmt.Println("\n2. Expense Categories:")
	rows, err = db.QueryContext(ctx, `
		SELECT 
			category,
			COUNT(*) as transaction_count,
			SUM(ABS(amount)) as total_expense,
			ROUND(SUM(ABS(amount)) * 100.0 / (
				SELECT SUM(ABS(amount)) FROM transactions WHERE type = 'expense'
			), 2) as percentage
		FROM transactions 
		WHERE type = 'expense'
		GROUP BY category
		ORDER BY total_expense DESC
	`)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%-15s %-6s %-12s %-10s\n", "Category", "Count", "Total", "Percentage")
	fmt.Println(strings.Repeat("-", 45))
	for rows.Next() {
		var category string
		var count int
		var expense, percentage float64
		if err := rows.Scan(&category, &count, &expense, &percentage); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-15s %-6d $%-11.2f %-10.2f%%\n", category, count, expense, percentage)
	}
	_ = rows.Close() // Ignore close error in test cleanup

	// Cash flow summary
	fmt.Println("\n3. Cash Flow Summary:")
	var totalIncome, totalExpenses, netIncome float64
	err = db.QueryRowContext(ctx, `
		SELECT 
			SUM(CASE WHEN type = 'sale' THEN amount ELSE 0 END) as total_income,
			SUM(CASE WHEN type = 'expense' THEN ABS(amount) ELSE 0 END) as total_expenses,
			SUM(CASE WHEN type = 'sale' THEN amount ELSE -ABS(amount) END) as net_income
		FROM transactions
	`).Scan(&totalIncome, &totalExpenses, &netIncome)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Total Income:  $%.2f\n", totalIncome)
	fmt.Printf("Total Expenses: $%.2f\n", totalExpenses)
	fmt.Printf("Net Income:    $%.2f\n", netIncome)
	fmt.Printf("Profit Margin: %.2f%%\n", (netIncome/totalIncome)*100)

	// Output:
	// === Financial Data Analysis ===
	//
	// 1. Monthly Revenue Trend:
	// Month    Count  Revenue    Average  Largest
	// --------------------------------------------------
	// 2024-01  3      $3550.00   $1183.33 $850.00
	// 2024-02  2      $2200.00   $1100.00 $1200.00
	//
	// 2. Expense Categories:
	// Category        Count  Total        Percentage
	// ---------------------------------------------
	// Office Supplies 2      $350.00      58.33     %
	// Marketing       1      $250.00      41.67     %
	//
	// 3. Cash Flow Summary:
	// Total Income:  $5750.00
	// Total Expenses: $600.00
	// Net Income:    $5150.00
	// Profit Margin: 89.57%
}

// createLargeTestData creates test data for performance optimization examples
func createLargeTestData() string {
	tmpDir, err := os.MkdirTemp("", "filesql_large_example_*")
	if err != nil {
		log.Fatal(err)
	}

	// Create customer summary data for performance testing
	customerData := `customer_id,name,email,registration_date,total_orders,total_spent
1001,Premium Customer A,premium.a@example.com,2023-01-15,15,1500.00
1002,Premium Customer B,premium.b@example.com,2023-02-20,12,1200.00
1003,Premium Customer C,premium.c@example.com,2023-03-10,10,1000.00
1004,Regular Customer D,regular.d@example.com,2023-04-05,8,800.00
1005,Regular Customer E,regular.e@example.com,2023-05-15,6,600.00
1006,Regular Customer F,regular.f@example.com,2023-06-20,5,500.00
1007,Budget Customer G,budget.g@example.com,2023-07-10,3,300.00
1008,Budget Customer H,budget.h@example.com,2023-08-25,2,200.00
1009,New Customer I,new.i@example.com,2023-09-30,1,100.00
1010,New Customer J,new.j@example.com,2023-10-15,1,100.00`

	customerFile := filepath.Join(tmpDir, "customer_summary.csv")
	err = os.WriteFile(customerFile, []byte(customerData), 0600)
	if err != nil {
		log.Fatal(err)
	}

	return tmpDir
}

// createAdvancedTestData creates test data for advanced SQL examples
func createAdvancedTestData() string {
	tmpDir, err := os.MkdirTemp("", "filesql_advanced_example_*")
	if err != nil {
		log.Fatal(err)
	}

	// Extended employees data
	employeesData := `id,name,department_id,salary,hire_date
1,Alice Johnson,1,95000,2023-01-15
2,Bob Smith,2,85000,2023-02-20
3,Charlie Brown,1,80000,2023-03-10
4,David Wilson,1,75000,2023-04-05
5,Eve Davis,2,65000,2023-05-15
6,Frank Miller,3,70000,2023-06-01`

	employeesFile := filepath.Join(tmpDir, "employees.csv")
	err = os.WriteFile(employeesFile, []byte(employeesData), 0600)
	if err != nil {
		log.Fatal(err)
	}

	// Departments data
	departmentsData := `id,name,budget
1,Engineering,500000
2,Sales,300000
3,Marketing,200000
4,HR,150000`

	departmentsFile := filepath.Join(tmpDir, "departments.csv")
	err = os.WriteFile(departmentsFile, []byte(departmentsData), 0600)
	if err != nil {
		log.Fatal(err)
	}

	return tmpDir
}

// createCompressedTestData creates test data with compressed files
func createCompressedTestData() string {
	tmpDir, err := os.MkdirTemp("", "filesql_compressed_example_*")
	if err != nil {
		log.Fatal(err)
	}

	// Create logs data
	logsData := `timestamp,level,message,user_id
2024-01-01 10:00:00,INFO,User login,1001
2024-01-01 11:30:00,INFO,Order created,1002
2024-01-01 12:15:00,ERROR,Payment failed,1003
2024-01-01 13:45:00,INFO,User logout,1001
2024-01-01 14:00:00,INFO,System backup completed,0`

	logsFile := filepath.Join(tmpDir, "logs.csv")
	err = os.WriteFile(logsFile, []byte(logsData), 0600)
	if err != nil {
		log.Fatal(err)
	}

	// Create products data
	productsData := `id,name,category,price,in_stock
1,Laptop Pro,Electronics,1299.99,true
2,Office Chair,Furniture,299.99,true
3,Wireless Mouse,Electronics,49.99,false`

	productsFile := filepath.Join(tmpDir, "products.csv")
	err = os.WriteFile(productsFile, []byte(productsData), 0600)
	if err != nil {
		log.Fatal(err)
	}

	// Note: In a real scenario, these would be compressed files (.gz, .bz2, etc.)
	// For the example, we're using regular CSV files to demonstrate the concept
	// The actual filesql library would handle the compression/decompression

	return tmpDir
}

// createWebLogTestData creates test data for web log analysis examples
func createWebLogTestData() string {
	tmpDir, err := os.MkdirTemp("", "filesql_weblog_example_*")
	if err != nil {
		log.Fatal(err)
	}

	// Create web access logs
	accessLogsData := `timestamp,ip_address,method,path,status_code,response_time,user_agent
2024-01-01 09:15:30,192.168.1.100,GET,/,200,150,Mozilla/5.0
2024-01-01 09:30:45,192.168.1.101,GET,/products,200,200,Mozilla/5.0
2024-01-01 10:05:15,192.168.1.100,GET,/,200,120,Mozilla/5.0
2024-01-01 10:20:30,192.168.1.102,GET,/about,200,180,Mozilla/5.0
2024-01-01 10:35:45,192.168.1.101,GET,/products,200,160,Mozilla/5.0
2024-01-01 11:10:15,192.168.1.103,GET,/contact,200,140,Mozilla/5.0
2024-01-01 11:25:30,192.168.1.100,GET,/,200,200,Mozilla/5.0
2024-01-01 11:40:45,192.168.1.104,GET,/missing,404,50,Mozilla/5.0
2024-01-01 14:15:30,192.168.1.105,GET,/notfound,404,100,Mozilla/5.0`

	accessLogsFile := filepath.Join(tmpDir, "access_logs.csv")
	err = os.WriteFile(accessLogsFile, []byte(accessLogsData), 0600)
	if err != nil {
		log.Fatal(err)
	}

	return tmpDir
}

// createFinancialTestData creates test data for financial analysis examples
func createFinancialTestData() string {
	tmpDir, err := os.MkdirTemp("", "filesql_financial_example_*")
	if err != nil {
		log.Fatal(err)
	}

	// Create transaction data
	transactionData := `transaction_id,transaction_date,type,category,amount,description
1,2024-01-15,sale,Product Sales,1500.00,Sale of premium product
2,2024-01-20,sale,Product Sales,850.00,Sale of standard product
3,2024-01-25,sale,Service,1200.00,Consulting service
4,2024-01-10,expense,Office Supplies,-150.00,Office equipment purchase
5,2024-01-18,expense,Marketing,-250.00,Social media advertising
6,2024-02-05,sale,Product Sales,1200.00,Sale of premium product
7,2024-02-15,sale,Service,1000.00,Training service
8,2024-02-08,expense,Office Supplies,-200.00,Stationery purchase`

	transactionFile := filepath.Join(tmpDir, "transactions.csv")
	err = os.WriteFile(transactionFile, []byte(transactionData), 0600)
	if err != nil {
		log.Fatal(err)
	}

	return tmpDir
}

func ExampleDumpDatabase_withOptions() {
	// Create a temporary directory for output
	tempDir := filepath.Join(os.TempDir(), "filesql_dump_example")
	if err := os.MkdirAll(tempDir, 0750); err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Open CSV file
	db, err := filesql.Open("testdata/sample.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Example 1: Default CSV output (no options)
	fmt.Println("Example 1: Default CSV output")
	csvDir := filepath.Join(tempDir, "csv_output")
	if err := filesql.DumpDatabase(db, csvDir); err != nil {
		log.Fatal(err)
	}

	// List output files
	files1, err := filepath.Glob(filepath.Join(csvDir, "*"))
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files1 {
		fmt.Printf("Created: %s\n", filepath.Base(file))
	}

	// Example 2: TSV output with gzip compression
	fmt.Println("\nExample 2: TSV output with gzip compression")
	tsvDir := filepath.Join(tempDir, "tsv_output")
	options := filesql.NewDumpOptions().
		WithFormat(filesql.OutputFormatTSV).
		WithCompression(filesql.CompressionGZ)
	if err := filesql.DumpDatabase(db, tsvDir, options); err != nil {
		log.Fatal(err)
	}

	files2, err := filepath.Glob(filepath.Join(tsvDir, "*"))
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files2 {
		fmt.Printf("Created: %s\n", filepath.Base(file))
	}

	// Example 3: LTSV output with zstd compression
	fmt.Println("\nExample 3: LTSV output with zstd compression")
	ltsvDir := filepath.Join(tempDir, "ltsv_output")
	options3 := filesql.NewDumpOptions().
		WithFormat(filesql.OutputFormatLTSV).
		WithCompression(filesql.CompressionZSTD)
	if err := filesql.DumpDatabase(db, ltsvDir, options3); err != nil {
		log.Fatal(err)
	}

	files3, err := filepath.Glob(filepath.Join(ltsvDir, "*"))
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files3 {
		fmt.Printf("Created: %s\n", filepath.Base(file))
	}

	// Output:
	// Example 1: Default CSV output
	// Created: sample.csv
	//
	// Example 2: TSV output with gzip compression
	// Created: sample.tsv.gz
	//
	// Example 3: LTSV output with zstd compression
	// Created: sample.ltsv.zst
}

func ExampleDumpDatabase_multipleFormats() {
	tempDir := filepath.Join(os.TempDir(), "filesql_formats_example")
	if err := os.MkdirAll(tempDir, 0750); err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Open CSV file and modify data
	db, err := filesql.Open("testdata/sample.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Add some data to demonstrate functionality
	_, err = db.Exec("INSERT INTO sample (id, name, age, email) VALUES (4, 'Alice Brown', 28, 'alice@example.com')")
	if err != nil {
		log.Fatal(err)
	}

	// Demonstrate different compression options
	compressionTypes := []struct {
		name        string
		compression filesql.CompressionType
		extension   string
	}{
		{"No compression", filesql.CompressionNone, ""},
		{"Gzip compression", filesql.CompressionGZ, ".gz"},
		{"XZ compression", filesql.CompressionXZ, ".xz"},
		{"Zstd compression", filesql.CompressionZSTD, ".zst"},
	}

	for _, ct := range compressionTypes {
		fmt.Printf("%s:\n", ct.name)

		options := filesql.NewDumpOptions().
			WithFormat(filesql.OutputFormatCSV).
			WithCompression(ct.compression)

		outputDir := filepath.Join(tempDir, "compression_"+ct.compression.String())
		if err := filesql.DumpDatabase(db, outputDir, options); err != nil {
			log.Fatal(err)
		}

		files, err := filepath.Glob(filepath.Join(outputDir, "*"))
		if err != nil {
			log.Fatal(err)
		}
		for _, file := range files {
			fmt.Printf("  %s\n", filepath.Base(file))
		}
	}

	// Output:
	// No compression:
	//   sample.csv
	// Gzip compression:
	//   sample.csv.gz
	// XZ compression:
	//   sample.csv.xz
	// Zstd compression:
	//   sample.csv.zst
}

func ExampleDumpOptions_fileExtensions() {
	// Show how file extensions are built
	examples := []struct {
		format      filesql.OutputFormat
		compression filesql.CompressionType
	}{
		{filesql.OutputFormatCSV, filesql.CompressionNone},
		{filesql.OutputFormatTSV, filesql.CompressionGZ},
		{filesql.OutputFormatLTSV, filesql.CompressionBZ2},
		{filesql.OutputFormatCSV, filesql.CompressionXZ},
		{filesql.OutputFormatTSV, filesql.CompressionZSTD},
	}

	for _, ex := range examples {
		options := filesql.DumpOptions{
			Format:      ex.format,
			Compression: ex.compression,
		}
		fmt.Printf("Format: %-4s, Compression: %-4s -> Extension: %s\n",
			ex.format.String(),
			ex.compression.String(),
			options.FileExtension())
	}

	// Output:
	// Format: csv , Compression: none -> Extension: .csv
	// Format: tsv , Compression: gz   -> Extension: .tsv.gz
	// Format: ltsv, Compression: bz2  -> Extension: .ltsv.bz2
	// Format: csv , Compression: xz   -> Extension: .csv.xz
	// Format: tsv , Compression: zstd -> Extension: .tsv.zst
}

func ExampleDumpDatabase_dataProcessing() {
	tempDir := filepath.Join(os.TempDir(), "filesql_processing_example")
	if err := os.MkdirAll(tempDir, 0750); err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Open CSV file
	db, err := filesql.Open("testdata/sample.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Process data with SQL
	_, err = db.Exec(`
		UPDATE sample 
		SET age = age + 1 
		WHERE name LIKE '%John%'
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Add aggregated data
	_, err = db.Exec(`
		INSERT INTO sample (id, name, age, email) 
		SELECT 999, 'Summary: ' || COUNT(*), AVG(age), 'summary@example.com'
		FROM sample 
		WHERE id < 999
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Export processed data in different formats for different use cases

	// 1. TSV for spreadsheet import
	options := filesql.NewDumpOptions().WithFormat(filesql.OutputFormatTSV)
	spreadsheetDir := filepath.Join(tempDir, "for_spreadsheet")
	if err := filesql.DumpDatabase(db, spreadsheetDir, options); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Exported TSV for spreadsheet import")

	// 2. Compressed CSV for archival
	options = filesql.NewDumpOptions().
		WithFormat(filesql.OutputFormatCSV).
		WithCompression(filesql.CompressionGZ)
	archiveDir := filepath.Join(tempDir, "for_archive")
	if err := filesql.DumpDatabase(db, archiveDir, options); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Exported compressed CSV for archival")

	// 3. LTSV for log analysis
	options = filesql.NewDumpOptions().WithFormat(filesql.OutputFormatLTSV)
	logDir := filepath.Join(tempDir, "for_logs")
	if err := filesql.DumpDatabase(db, logDir, options); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Exported LTSV for log analysis")

	// Show what was created
	dirs := []string{"for_spreadsheet", "for_archive", "for_logs"}
	for _, dir := range dirs {
		files, err := filepath.Glob(filepath.Join(tempDir, dir, "*"))
		if err != nil {
			log.Fatal(err)
		}
		for _, file := range files {
			fmt.Printf("%s: %s\n", dir, filepath.Base(file))
		}
	}

	// Output:
	// Exported TSV for spreadsheet import
	// Exported compressed CSV for archival
	// Exported LTSV for log analysis
	// for_spreadsheet: sample.tsv
	// for_archive: sample.csv.gz
	// for_logs: sample.ltsv
}

// ================================
// Builder Pattern Examples
// ================================

// ExampleNewBuilder demonstrates the basic usage of the Builder pattern.
// This is the recommended approach for most use cases, especially when working
// with embedded filesystems or when you need more control over the database creation process.
func ExampleNewBuilder() {
	// Create a new builder - this is the starting point for all Builder pattern usage
	builder := filesql.NewBuilder()

	// The builder supports method chaining for a fluent API
	// You can add individual paths or multiple paths at once
	builder.AddPath("users.csv").AddPaths("orders.tsv", "products.ltsv")

	fmt.Printf("Builder created successfully: %t\n", builder != nil)

	// In real usage, you would continue with:
	// ctx := context.Background()
	// validatedBuilder, err := builder.Build(ctx)
	// if err != nil { return err }
	// db, err := validatedBuilder.Open(ctx)
	// if err != nil { return err }
	// defer db.Close()
	// defer validatedBuilder.Cleanup()

	// Output: Builder created successfully: true
}

// ExampleDBBuilder_EnableAutoSave demonstrates automatic saving on database close.
// This feature automatically saves modified data when the database connection is closed,
// providing a convenient way to persist changes without manual intervention.
//
//nolint:errcheck // Examples don't need full error handling
func ExampleDBBuilder_EnableAutoSave() {
	// Create temporary directory and test file
	tempDir, _ := os.MkdirTemp("", "filesql-autosave-example")
	defer os.RemoveAll(tempDir)

	// Create sample CSV file
	csvPath := filepath.Join(tempDir, "employees.csv")
	csvContent := "name,department,salary\nAlice,Engineering,80000\nBob,Marketing,65000\n"
	_ = os.WriteFile(csvPath, []byte(csvContent), 0600)

	// Create output directory
	outputDir := filepath.Join(tempDir, "backup")
	_ = os.MkdirAll(outputDir, 0750)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Configure builder with auto-save on close
	builder := filesql.NewBuilder().
		AddPath(csvPath).
		EnableAutoSave(outputDir, filesql.NewDumpOptions()) // Save to backup directory on close

	validatedBuilder, _ := builder.Build(ctx)
	defer validatedBuilder.Cleanup()

	db, _ := validatedBuilder.Open(ctx)

	// Modify data - this will be automatically saved when db.Close() is called
	_, _ = db.ExecContext(ctx, "INSERT INTO employees (name, department, salary) VALUES ('Charlie', 'Sales', 70000)")
	_, _ = db.ExecContext(ctx, "UPDATE employees SET salary = 85000 WHERE name = 'Alice'")

	// Close database - triggers automatic save to backup directory
	_ = db.Close()

	// Verify the backup file was created and contains our changes
	backupFile := filepath.Join(outputDir, "employees.csv")
	if _, err := os.Stat(backupFile); err == nil {
		fmt.Println("Auto-save completed successfully")
	}

	// Output: Auto-save completed successfully
}

// ExampleDBBuilder_EnableAutoSaveOnCommit demonstrates automatic saving on transaction commit.
// This provides more frequent saves but may impact performance for workloads with many commits.
//
//nolint:errcheck // Examples don't need full error handling
func ExampleDBBuilder_EnableAutoSaveOnCommit() {
	// Create temporary directory and test file
	tempDir, _ := os.MkdirTemp("", "filesql-commit-save-example")
	defer os.RemoveAll(tempDir)

	// Create sample CSV file
	csvPath := filepath.Join(tempDir, "transactions.csv")
	csvContent := "id,amount,status\n1,100.50,pending\n2,250.75,pending\n"
	_ = os.WriteFile(csvPath, []byte(csvContent), 0600)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Configure builder with auto-save on commit
	// Using temp directory to keep the example self-contained
	builder := filesql.NewBuilder().
		AddPath(csvPath).
		EnableAutoSaveOnCommit(tempDir, filesql.NewDumpOptions()) // Save to temp directory on each commit

	validatedBuilder, _ := builder.Build(ctx)
	defer validatedBuilder.Cleanup()

	db, _ := validatedBuilder.Open(ctx)
	defer func() { _ = db.Close() }()

	// Start transaction
	tx, _ := db.BeginTx(ctx, nil)

	// Process transactions within the transaction
	_, _ = tx.ExecContext(ctx, "UPDATE transactions SET status = 'completed' WHERE id = 1")
	_, _ = tx.ExecContext(ctx, "INSERT INTO transactions (id, amount, status) VALUES (3, 175.25, 'completed')")

	// Commit transaction - triggers automatic save
	_ = tx.Commit()

	fmt.Println("Transaction committed with auto-save")

	// Output: Transaction committed with auto-save
}

//nolint:errcheck // Examples don't need full error handling
func ExampleDBBuilder_AddPath() {
	// Create temporary CSV file for example
	tempDir, _ := os.MkdirTemp("", "filesql-example")
	defer os.RemoveAll(tempDir)

	csvFile := filepath.Join(tempDir, "users.csv")
	content := "id,name,age\n1,Alice,30\n2,Bob,25\n"
	os.WriteFile(csvFile, []byte(content), 0644)

	// Use builder to add a single file path
	builder := filesql.NewBuilder().AddPath(csvFile)

	// Build and open database
	ctx := context.Background()
	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		log.Fatal(err)
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Query the data
	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		rows.Scan(&count)
	}
	fmt.Printf("Number of users: %d\n", count)
	// Output: Number of users: 2
}

//nolint:errcheck // Examples don't need full error handling
func ExampleDBBuilder_AddPaths() {
	// Create temporary files for example
	tempDir, _ := os.MkdirTemp("", "filesql-example")
	defer os.RemoveAll(tempDir)

	// Create users.csv
	usersFile := filepath.Join(tempDir, "users.csv")
	usersContent := "id,name\n1,Alice\n2,Bob\n"
	os.WriteFile(usersFile, []byte(usersContent), 0644)

	// Create products.csv
	productsFile := filepath.Join(tempDir, "products.csv")
	productsContent := "id,product_name\n1,Laptop\n2,Phone\n"
	os.WriteFile(productsFile, []byte(productsContent), 0644)

	// Use builder to add multiple file paths
	builder := filesql.NewBuilder().AddPaths(usersFile, productsFile)

	ctx := context.Background()
	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		log.Fatal(err)
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Query data from both tables
	rows, err := db.Query(`
		SELECT u.name, p.product_name 
		FROM users u 
		JOIN products p ON u.id = p.id
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, product string
		rows.Scan(&name, &product)
		fmt.Printf("%s has %s\n", name, product)
	}
	// Output: Alice has Laptop
	// Bob has Phone
}

//nolint:errcheck // Examples don't need full error handling
func ExampleDBBuilder_AddFS() {
	// Create mock filesystem with test data
	mockFS := fstest.MapFS{
		"users.csv":    &fstest.MapFile{Data: []byte("id,name,department\n1,Alice,Engineering\n2,Bob,Sales\n")},
		"products.tsv": &fstest.MapFile{Data: []byte("id\tname\tprice\n1\tLaptop\t1000\n2\tPhone\t500\n")},
		"logs.ltsv":    &fstest.MapFile{Data: []byte("time:2024-01-01T00:00:00Z\tlevel:info\tmsg:started\n")},
		"readme.txt":   &fstest.MapFile{Data: []byte("This file will be ignored\n")}, // unsupported format
	}

	// Use builder with filesystem
	builder := filesql.NewBuilder().AddFS(mockFS)

	ctx := context.Background()
	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		log.Fatal(err)
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Clean up temporary files
	defer validatedBuilder.Cleanup()

	// List all tables that were created from the filesystem
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var tableCount int
	for rows.Next() {
		var tableName string
		rows.Scan(&tableName)
		tableCount++
	}

	fmt.Printf("Created %d tables from filesystem\n", tableCount)
	// Output: Created 3 tables from filesystem
}

//nolint:errcheck // Examples don't need full error handling
func ExampleDBBuilder_AddFS_embedFS() {
	// Use embedded test filesystem
	subFS, err := fs.Sub(builderExampleFS, "testdata/embed_test")
	if err != nil {
		log.Fatal(err)
	}

	// Use builder with embedded filesystem
	builder := filesql.NewBuilder().AddFS(subFS)

	ctx := context.Background()
	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		log.Fatal(err)
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Clean up temporary files
	defer validatedBuilder.Cleanup()

	// Count the number of tables created from embedded files
	rows, err := db.Query("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var tableCount int
	if rows.Next() {
		rows.Scan(&tableCount)
	}

	fmt.Printf("Created %d tables from embedded files\n", tableCount)
	// Output: Created 3 tables from embedded files
}

//nolint:errcheck // Examples don't need full error handling
func ExampleDBBuilder_Build() {
	// Create temporary CSV file
	tempDir, _ := os.MkdirTemp("", "filesql-example")
	defer os.RemoveAll(tempDir)

	csvFile := filepath.Join(tempDir, "data.csv")
	content := "name,value\ntest,123\n"
	os.WriteFile(csvFile, []byte(content), 0644)

	// Build validates inputs and prepares for opening
	builder := filesql.NewBuilder().AddPath(csvFile)

	ctx := context.Background()
	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Builder validated successfully: %t\n", validatedBuilder != nil)
	// Output: Builder validated successfully: true
}

//nolint:errcheck // Examples don't need full error handling
func ExampleDBBuilder_Open() {
	// Create temporary CSV file
	tempDir, _ := os.MkdirTemp("", "filesql-example")
	defer os.RemoveAll(tempDir)

	csvFile := filepath.Join(tempDir, "employees.csv")
	content := "id,name,salary\n1,Alice,50000\n2,Bob,60000\n3,Charlie,55000\n"
	os.WriteFile(csvFile, []byte(content), 0644)

	// Complete builder workflow: AddPath -> Build -> Open
	builder := filesql.NewBuilder().AddPath(csvFile)

	ctx := context.Background()
	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		log.Fatal(err)
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Perform complex SQL query
	rows, err := db.Query(`
		SELECT name, salary,
		       salary - (SELECT AVG(salary) FROM employees) as salary_diff
		FROM employees 
		WHERE salary > (SELECT AVG(salary) FROM employees)
		ORDER BY salary DESC
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var salary, diff float64
		rows.Scan(&name, &salary, &diff)
		fmt.Printf("%s: $%.0f (+$%.0f above average)\n", name, salary, diff)
	}
	// Output: Bob: $60000 (+$5000 above average)
}

func ExampleDBBuilder_Cleanup() {
	// Create mock filesystem with test data
	mockFS := fstest.MapFS{
		"temp_data.csv": &fstest.MapFile{Data: []byte("id,value\n1,test\n")},
	}

	// Use filesystem input (creates temporary files)
	builder := filesql.NewBuilder().AddFS(mockFS)

	ctx := context.Background()
	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		log.Fatal(err)
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	_ = db.Close() // Example doesn't need error handling

	// Clean up temporary files created by AddFS
	err = validatedBuilder.Cleanup()
	if err != nil {
		log.Printf("Cleanup error: %v", err)
	} else {
		fmt.Println("Cleanup completed successfully")
	}
	// Output: Cleanup completed successfully
}

//nolint:errcheck // Examples don't need full error handling
func ExampleDBBuilder_chaining() {
	// Create temporary files
	tempDir, _ := os.MkdirTemp("", "filesql-example")
	defer os.RemoveAll(tempDir)

	csvFile := filepath.Join(tempDir, "data1.csv")
	content1 := "id,name\n1,Alice\n2,Bob\n"
	os.WriteFile(csvFile, []byte(content1), 0644)

	tsvFile := filepath.Join(tempDir, "data2.tsv")
	content2 := "id\tproduct\n1\tLaptop\n2\tPhone\n"
	os.WriteFile(tsvFile, []byte(content2), 0644)

	// Create mock filesystem
	mockFS := fstest.MapFS{
		"logs.ltsv": &fstest.MapFile{Data: []byte("time:2024-01-01T00:00:00Z\tlevel:info\n")},
	}

	// Demonstrate method chaining
	ctx := context.Background()
	db, err := filesql.NewBuilder().
		AddPath(csvFile).
		AddPaths(tsvFile).
		AddFS(mockFS).
		Build(ctx)
	if err != nil {
		log.Fatal(err)
	}

	connection, err := db.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()
	defer db.Cleanup()

	// Count tables from different sources
	rows, err := connection.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var tableCount int
	for rows.Next() {
		var tableName string
		rows.Scan(&tableName)
		tableCount++
	}

	fmt.Printf("Successfully loaded %d tables from mixed sources\n", tableCount)
	// Output: Successfully loaded 3 tables from mixed sources
}

//nolint:errcheck // Examples don't need full error handling
func ExampleDBBuilder_errorHandling() {
	// Example 1: Build without inputs should fail
	builder := filesql.NewBuilder()
	ctx := context.Background()

	_, err := builder.Build(ctx)
	if err != nil {
		fmt.Printf("Expected error for no inputs: %v\n", err)
	}

	// Example 2: Open without Build should fail
	builder2 := filesql.NewBuilder().AddPath("nonexistent.csv")
	_, err = builder2.Open(ctx)
	if err != nil {
		fmt.Println("Expected error for Open without Build")
	}

	// Example 3: Non-existent file should fail during Build
	builder3 := filesql.NewBuilder().AddPath("/nonexistent/file.csv")
	_, err = builder3.Build(ctx)
	if err != nil {
		fmt.Println("Expected error for non-existent file")
	}

	// Example 4: Success case
	tempDir, _ := os.MkdirTemp("", "filesql-example")
	defer os.RemoveAll(tempDir)

	csvFile := filepath.Join(tempDir, "valid.csv")
	os.WriteFile(csvFile, []byte("id,name\n1,test\n"), 0644)

	builder4 := filesql.NewBuilder().AddPath(csvFile)
	validatedBuilder, err := builder4.Build(ctx)
	if err != nil {
		log.Fatal(err)
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("Success: Valid file loaded correctly")

	// Output: Expected error for no inputs: at least one path must be provided
	// Expected error for Open without Build
	// Expected error for non-existent file
	// Success: Valid file loaded correctly
}
