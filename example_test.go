package filesql_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/nao1215/filesql"
)

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

	err = os.WriteFile(filepath.Join(tmpDir, "employees.csv"), []byte(employeesData), 0644)
	if err != nil {
		log.Fatal(err)
	}

	// Create departments.csv
	departmentsData := `id,name,budget,manager_id
1,Engineering,1000000,1
2,Marketing,800000,5
3,Sales,600000,7
4,HR,400000,9`

	err = os.WriteFile(filepath.Join(tmpDir, "departments.csv"), []byte(departmentsData), 0644)
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
