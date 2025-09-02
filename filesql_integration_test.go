package filesql

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompanyDataIntegration(t *testing.T) {
	t.Parallel()

	companyDir := filepath.Join("testdata", "company")
	db, err := Open(companyDir)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	t.Run("Basic table existence and exact record count", func(t *testing.T) {
		tables := []struct {
			name          string
			expectedCount int
		}{
			{"user", 500},
			{"department", 20},
			{"orders", 2000},
			{"address", 500},
			{"salary", 500},
			{"project", 50},
			{"user_project", 500},
			{"attendance", 2000},
			{"performance", 500},
			{"training", 200},
			{"user_training", 1000},
			{"benefits", 500},
		}

		for _, table := range tables {
			t.Run(table.name, func(t *testing.T) {
				query := "SELECT COUNT(*) FROM " + table.name
				var count int
				err := db.QueryRowContext(ctx, query).Scan(&count)
				assert.NoError(t, err, "Failed to query %s table", table.name)
				assert.Equal(t, table.expectedCount, count,
					"Table %s should have exactly %d records", table.name, table.expectedCount)
			})
		}
	})

	t.Run("Verify specific user and department data", func(t *testing.T) {
		// Verify against known baseline data to ensure consistent test results
		query := `
			SELECT u.name, u.email, u.age, d.name as department_name, d.location
			FROM user u
			JOIN department d ON u.department_id = d.id
			WHERE u.id = 1
		`
		var userName, userEmail, deptName, location string
		var age float64
		err := db.QueryRowContext(ctx, query).Scan(&userName, &userEmail, &age, &deptName, &location)
		require.NoError(t, err)

		assert.Equal(t, "User_1", userName)
		assert.Equal(t, "user1@example.com", userEmail)
		assert.Equal(t, 36.0, age)
		assert.Equal(t, "Department_12", deptName)
		assert.Equal(t, "Tokyo", location)

		var userCount int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM user WHERE department_id = 12").Scan(&userCount)
		require.NoError(t, err)
		assert.Equal(t, 25, userCount, "Department_12 should have exactly 25 users")
	})

	t.Run("Complex JOIN with multiple tables", func(t *testing.T) {
		query := `
			SELECT 
				u.name as user_name,
				d.name as department_name,
				p.name as project_name,
				up.role
			FROM user u
			JOIN department d ON u.department_id = d.id
			JOIN user_project up ON u.id = up.user_id
			JOIN project p ON up.project_id = p.id
			WHERE up.role IS NOT NULL
			LIMIT 5
		`
		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			var userName, deptName, projectName, role string
			err := rows.Scan(&userName, &deptName, &projectName, &role)
			assert.NoError(t, err)
			assert.NotEmpty(t, userName)
			assert.NotEmpty(t, projectName)
			count++
		}
		assert.Greater(t, count, 0, "Should have results from multi-table JOIN")
		assert.NoError(t, rows.Err())
	})

	t.Run("Department aggregation with exact validation", func(t *testing.T) {
		// Department_12 is used as baseline with 25 known users
		var dept12Count int
		var dept12AvgAge sql.NullFloat64
		err := db.QueryRowContext(ctx, `
			SELECT 
				COUNT(u.id),
				AVG(CAST(u.age AS REAL))
			FROM user u
			WHERE u.department_id = 12 AND u.age IS NOT NULL AND u.age != ''
		`).Scan(&dept12Count, &dept12AvgAge)
		require.NoError(t, err)

		assert.Greater(t, dept12Count, 0, "Department 12 should have users with age data")
		if dept12AvgAge.Valid {
			assert.Greater(t, dept12AvgAge.Float64, 0.0, "Average age should be positive")
			assert.Less(t, dept12AvgAge.Float64, 100.0, "Average age should be reasonable")
		}

		query := `
			SELECT 
				d.id,
				d.name as department_name,
				COUNT(u.id) as user_count
			FROM department d
			LEFT JOIN user u ON d.id = u.department_id
			GROUP BY d.id, d.name
			ORDER BY user_count DESC
		`
		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()

		totalUsersInDepts := 0
		deptCount := 0
		for rows.Next() {
			var deptID int
			var deptName string
			var userCount int
			err := rows.Scan(&deptID, &deptName, &userCount)
			assert.NoError(t, err)
			assert.Regexp(t, `^Department_\d+$`, deptName, "Department name format")
			assert.GreaterOrEqual(t, userCount, 0)

			if deptID == 12 {
				assert.Equal(t, 25, userCount, "Department_12 should have exactly 25 users")
			}

			totalUsersInDepts += userCount
			deptCount++
		}
		assert.Equal(t, 20, deptCount, "Should have exactly 20 departments")
		assert.Equal(t, 500, totalUsersInDepts, "Total users across departments should be 500")
		assert.NoError(t, rows.Err())
	})

	t.Run("Verify order statistics with exact values", func(t *testing.T) {
		var totalOrders int
		var minAmount, maxAmount, avgAmount float64
		err := db.QueryRowContext(ctx, `
			SELECT COUNT(*), MIN(amount), MAX(amount), AVG(amount)
			FROM orders
		`).Scan(&totalOrders, &minAmount, &maxAmount, &avgAmount)
		require.NoError(t, err)

		assert.Equal(t, 2000, totalOrders, "Should have exactly 2000 orders")
		assert.InDelta(t, 1150.19, minAmount, 0.01, "Minimum order amount")
		assert.InDelta(t, 99996.86, maxAmount, 0.01, "Maximum order amount")
		assert.InDelta(t, 50119.53, avgAmount, 1.0, "Average order amount")

		query := `
			SELECT 
				u.id,
				u.name,
				COUNT(o.id) as order_count,
				ROUND(SUM(o.amount), 2) as total_amount,
				ROUND(AVG(o.amount), 2) as avg_amount
			FROM user u
			JOIN orders o ON u.id = o.user_id
			GROUP BY u.id, u.name
			HAVING COUNT(o.id) >= 10
			ORDER BY total_amount DESC
			LIMIT 3
		`
		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		var prevTotal float64 = 999999999
		for rows.Next() {
			var userID int
			var userName string
			var orderCount int
			var totalAmount, avgAmount float64
			err := rows.Scan(&userID, &userName, &orderCount, &totalAmount, &avgAmount)
			assert.NoError(t, err)

			assert.Greater(t, userID, 0)
			assert.Regexp(t, `^User_\d+$`, userName, "User name format")
			assert.GreaterOrEqual(t, orderCount, 10)
			assert.Greater(t, totalAmount, 0.0)
			assert.InDelta(t, totalAmount/float64(orderCount), avgAmount, 0.1, "Average calculation")
			assert.LessOrEqual(t, totalAmount, prevTotal, "Should be ordered by total DESC")
			prevTotal = totalAmount
			count++
		}
		assert.Equal(t, 3, count, "Should have exactly 3 results")
		assert.NoError(t, rows.Err())
	})

	t.Run("Verify specific salary data and calculations", func(t *testing.T) {
		// Use User_1 as baseline since it has known values including bonus
		var baseSalary, bonus, totalComp float64
		query := `
			SELECT 
				s.base_salary,
				COALESCE(CAST(s.bonus AS REAL), 0) as bonus,
				(s.base_salary + COALESCE(CAST(s.bonus AS REAL), 0)) as total_compensation
			FROM salary s
			WHERE s.user_id = 1
		`
		err := db.QueryRowContext(ctx, query).Scan(&baseSalary, &bonus, &totalComp)
		require.NoError(t, err)
		assert.Equal(t, 518300.0, baseSalary, "User_1 base salary should be 518300")
		assert.Equal(t, 105113.0, bonus, "User_1 bonus should be 105113")
		assert.Equal(t, 623413.0, totalComp, "User_1 total compensation should be 623413")

		highSalaryQuery := `
			SELECT u.id, u.name, s.base_salary
			FROM user u
			JOIN salary s ON u.id = s.user_id
			WHERE s.base_salary > 790000
			ORDER BY s.base_salary DESC
		`
		rows, err := db.QueryContext(ctx, highSalaryQuery)
		require.NoError(t, err)
		defer rows.Close()

		expectedHighEarners := []struct {
			id     int
			name   string
			salary float64
		}{
			{168, "User_168", 796593},
			{90, "User_90", 794750},
			{257, "User_257", 793196},
			{424, "User_424", 792701},
			{137, "User_137", 790786},
		}

		i := 0
		for rows.Next() {
			var id int
			var name string
			var salary float64
			err := rows.Scan(&id, &name, &salary)
			assert.NoError(t, err)

			if i < len(expectedHighEarners) {
				assert.Equal(t, expectedHighEarners[i].id, id)
				assert.Equal(t, expectedHighEarners[i].name, name)
				assert.Equal(t, expectedHighEarners[i].salary, salary)
			}
			i++
		}
		assert.NoError(t, rows.Err())
		assert.GreaterOrEqual(t, i, 5, "Should have at least 5 high earners")
	})

	t.Run("Multiple sequential queries with cross-validation", func(t *testing.T) {
		var user1Name, user1Email string
		var user1Age sql.NullFloat64
		var user1DeptID int
		err := db.QueryRowContext(ctx, `
			SELECT name, email, age, department_id 
			FROM user 
			WHERE id = 1
		`).Scan(&user1Name, &user1Email, &user1Age, &user1DeptID)
		require.NoError(t, err)
		assert.Equal(t, "User_1", user1Name)
		assert.Equal(t, "user1@example.com", user1Email)
		assert.Equal(t, 12, user1DeptID)

		var deptName, location string
		err = db.QueryRowContext(ctx, `
			SELECT name, location 
			FROM department 
			WHERE id = ?
		`, user1DeptID).Scan(&deptName, &location)
		require.NoError(t, err)
		assert.Equal(t, "Department_12", deptName)
		assert.Equal(t, "Tokyo", location)

		var deptUserCount int
		err = db.QueryRowContext(ctx, `
			SELECT COUNT(*) 
			FROM user 
			WHERE department_id = ?
		`, user1DeptID).Scan(&deptUserCount)
		require.NoError(t, err)
		assert.Equal(t, 25, deptUserCount)

		var baseSalary, bonus sql.NullFloat64
		err = db.QueryRowContext(ctx, `
			SELECT base_salary, bonus 
			FROM salary 
			WHERE user_id = 1
		`).Scan(&baseSalary, &bonus)
		require.NoError(t, err)
		assert.True(t, baseSalary.Valid)
		assert.Equal(t, 518300.0, baseSalary.Float64)
		assert.True(t, bonus.Valid)
		assert.Equal(t, 105113.0, bonus.Float64)

		var orderCount int
		err = db.QueryRowContext(ctx, `
			SELECT COUNT(*) 
			FROM orders 
			WHERE user_id = 1
		`).Scan(&orderCount)
		require.NoError(t, err)
		// Orders are randomly distributed, so we only check query execution
		assert.GreaterOrEqual(t, orderCount, 0)
	})

	t.Run("Attendance analysis with status distribution", func(t *testing.T) {
		query := `
			SELECT 
				status,
				COUNT(*) as count,
				COUNT(DISTINCT user_id) as unique_users
			FROM attendance
			WHERE status IS NOT NULL AND status != ''
			GROUP BY status
			ORDER BY count DESC
		`
		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()

		statusCounts := make(map[string]int)
		for rows.Next() {
			var status string
			var count, uniqueUsers int
			err := rows.Scan(&status, &count, &uniqueUsers)
			assert.NoError(t, err)
			assert.NotEmpty(t, status)
			assert.Greater(t, count, 0)
			assert.Greater(t, uniqueUsers, 0)
			statusCounts[status] = count
		}
		assert.NoError(t, rows.Err())
		assert.NotEmpty(t, statusCounts, "Should have attendance status distribution")
	})

	t.Run("Performance rating analysis", func(t *testing.T) {
		query := `
			SELECT 
				p.year,
				AVG(CAST(p.rating AS REAL)) as avg_rating,
				MIN(CAST(p.rating AS INTEGER)) as min_rating,
				MAX(CAST(p.rating AS INTEGER)) as max_rating,
				COUNT(*) as review_count
			FROM performance p
			WHERE p.rating IS NOT NULL AND p.rating != ''
			GROUP BY p.year
			ORDER BY p.year DESC
		`
		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			var year, minRating, maxRating, reviewCount int
			var avgRating float64
			err := rows.Scan(&year, &avgRating, &minRating, &maxRating, &reviewCount)
			assert.NoError(t, err)
			assert.Greater(t, year, 2000)
			assert.GreaterOrEqual(t, avgRating, 1.0)
			assert.LessOrEqual(t, avgRating, 5.0)
			assert.GreaterOrEqual(t, minRating, 1)
			assert.LessOrEqual(t, maxRating, 5)
			assert.Greater(t, reviewCount, 0)
			count++
		}
		assert.Greater(t, count, 0, "Should have performance rating results")
		assert.NoError(t, rows.Err())
	})

	t.Run("Training completion analysis", func(t *testing.T) {
		query := `
			SELECT 
				t.title,
				d.name as department_name,
				t.duration_days,
				COUNT(ut.user_id) as enrolled_users,
				SUM(CASE WHEN ut.completed = 'true' THEN 1 ELSE 0 END) as completed_users
			FROM training t
			JOIN department d ON t.department_id = d.id
			LEFT JOIN user_training ut ON t.id = ut.training_id
			GROUP BY t.id, t.title, d.name, t.duration_days
			HAVING COUNT(ut.user_id) > 0
			ORDER BY enrolled_users DESC
			LIMIT 10
		`
		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			var title, deptName string
			var durationDays, enrolledUsers, completedUsers int
			err := rows.Scan(&title, &deptName, &durationDays, &enrolledUsers, &completedUsers)
			assert.NoError(t, err)
			assert.NotEmpty(t, title)
			assert.NotEmpty(t, deptName)
			assert.Greater(t, durationDays, 0)
			assert.Greater(t, enrolledUsers, 0)
			assert.GreaterOrEqual(t, enrolledUsers, completedUsers)
			count++
		}
		assert.Greater(t, count, 0, "Should have training completion results")
		assert.NoError(t, rows.Err())
	})

	t.Run("Benefits enrollment analysis", func(t *testing.T) {
		query := `
			SELECT 
				COUNT(*) as total_users,
				SUM(CASE WHEN health_insurance = 'true' THEN 1 ELSE 0 END) as with_health,
				SUM(CASE WHEN pension_plan = 'true' THEN 1 ELSE 0 END) as with_pension,
				SUM(CASE WHEN health_insurance = 'true' AND pension_plan = 'true' THEN 1 ELSE 0 END) as with_both
			FROM benefits
		`
		var totalUsers, withHealth, withPension, withBoth int
		err := db.QueryRowContext(ctx, query).Scan(&totalUsers, &withHealth, &withPension, &withBoth)
		require.NoError(t, err)
		assert.Greater(t, totalUsers, 0)
		assert.GreaterOrEqual(t, totalUsers, withHealth)
		assert.GreaterOrEqual(t, totalUsers, withPension)
		assert.GreaterOrEqual(t, withHealth, withBoth)
		assert.GreaterOrEqual(t, withPension, withBoth)
	})

	t.Run("Complex subquery with EXISTS", func(t *testing.T) {
		query := `
			SELECT u.name, u.email
			FROM user u
			WHERE EXISTS (
				SELECT 1 
				FROM orders o 
				WHERE o.user_id = u.id 
				AND o.amount > 50000
			)
			AND EXISTS (
				SELECT 1 
				FROM salary s 
				WHERE s.user_id = u.id 
				AND s.base_salary > 60000
			)
			LIMIT 5
		`
		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			var name, email string
			err := rows.Scan(&name, &email)
			assert.NoError(t, err)
			assert.NotEmpty(t, name)
			assert.NotEmpty(t, email)
			count++
		}
		assert.Greater(t, count, 0, "Should have users matching complex criteria")
		assert.NoError(t, rows.Err())
	})

	t.Run("UNION query combining different data sources", func(t *testing.T) {
		query := `
			SELECT * FROM (
				SELECT 'High Earner' as category, u.name, s.base_salary as value
				FROM user u
				JOIN salary s ON u.id = s.user_id
				WHERE s.base_salary > 80000
				ORDER BY s.base_salary DESC
				LIMIT 3
			)
			
			UNION ALL
			
			SELECT * FROM (
				SELECT 'Big Spender' as category, u.name, SUM(o.amount) as value
				FROM user u
				JOIN orders o ON u.id = o.user_id
				GROUP BY u.id, u.name
				HAVING SUM(o.amount) > 100000
				ORDER BY value DESC
				LIMIT 3
			)
		`
		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()

		categories := make(map[string]int)
		for rows.Next() {
			var category, name string
			var value float64
			err := rows.Scan(&category, &name, &value)
			assert.NoError(t, err)
			assert.NotEmpty(t, category)
			assert.NotEmpty(t, name)
			assert.Greater(t, value, 0.0)
			categories[category]++
		}
		assert.NoError(t, rows.Err())
		assert.NotEmpty(t, categories, "Should have results from UNION query")
	})

	t.Run("Window functions simulation with self-join", func(t *testing.T) {
		// Simulate ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY base_salary DESC)
		query := `
			SELECT 
				u1.name,
				d.name as department,
				s1.base_salary,
				COUNT(s2.user_id) + 1 as salary_rank
			FROM user u1
			JOIN salary s1 ON u1.id = s1.user_id
			JOIN department d ON u1.department_id = d.id
			LEFT JOIN user u2 ON u1.department_id = u2.department_id
			LEFT JOIN salary s2 ON u2.id = s2.user_id AND s2.base_salary > s1.base_salary
			GROUP BY u1.id, u1.name, d.name, s1.base_salary
			HAVING COUNT(s2.user_id) < 3
			ORDER BY d.name, salary_rank
			LIMIT 10
		`
		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		prevDept := ""
		for rows.Next() {
			var name, dept string
			var salary float64
			var rank int
			err := rows.Scan(&name, &dept, &salary, &rank)
			assert.NoError(t, err)
			assert.NotEmpty(t, name)
			assert.NotEmpty(t, dept)
			assert.Greater(t, salary, 0.0)
			assert.GreaterOrEqual(t, rank, 1)
			assert.LessOrEqual(t, rank, 3)

			if dept != prevDept {
				prevDept = dept
			}
			count++
		}
		assert.Greater(t, count, 0, "Should have ranked salary results")
		assert.NoError(t, rows.Err())
	})

	t.Run("Transaction-like multiple queries", func(t *testing.T) {
		// Simulate a business logic flow with multiple dependent queries

		// Step 1: Find departments with most projects
		deptProjectQuery := `
			SELECT department_id, COUNT(*) as project_count
			FROM project
			GROUP BY department_id
			ORDER BY project_count DESC
			LIMIT 1
		`
		var topDeptID, projectCount int
		err := db.QueryRowContext(ctx, deptProjectQuery).Scan(&topDeptID, &projectCount)
		require.NoError(t, err)
		assert.Greater(t, projectCount, 0)

		// Step 2: Find users in that department
		userCountQuery := `
			SELECT COUNT(*) 
			FROM user 
			WHERE department_id = ?
		`
		var userCount int
		err = db.QueryRowContext(ctx, userCountQuery, topDeptID).Scan(&userCount)
		require.NoError(t, err)

		// Step 3: Calculate average salary for that department
		avgSalaryQuery := `
			SELECT AVG(s.base_salary)
			FROM user u
			JOIN salary s ON u.id = s.user_id
			WHERE u.department_id = ?
		`
		var avgSalary sql.NullFloat64
		err = db.QueryRowContext(ctx, avgSalaryQuery, topDeptID).Scan(&avgSalary)
		require.NoError(t, err)

		// Step 4: Find training programs for that department
		trainingQuery := `
			SELECT COUNT(*)
			FROM training
			WHERE department_id = ?
		`
		var trainingCount int
		err = db.QueryRowContext(ctx, trainingQuery, topDeptID).Scan(&trainingCount)
		require.NoError(t, err)

		// Validate the results make sense
		assert.Greater(t, userCount, 0, "Department should have users")
		if avgSalary.Valid {
			assert.Greater(t, avgSalary.Float64, 0.0, "Average salary should be positive")
		}
	})

	t.Run("Date-based analysis with validation", func(t *testing.T) {
		var minDate, maxDate string
		err := db.QueryRowContext(ctx, `
			SELECT MIN(created_at), MAX(created_at)
			FROM orders
			WHERE created_at IS NOT NULL
		`).Scan(&minDate, &maxDate)
		require.NoError(t, err)
		assert.NotEmpty(t, minDate)
		assert.NotEmpty(t, maxDate)
		assert.Less(t, minDate, maxDate, "Date range should be valid")

		query := `
			SELECT 
				DATE(created_at) as order_date,
				COUNT(*) as order_count,
				ROUND(SUM(amount), 2) as daily_total,
				ROUND(AVG(amount), 2) as daily_average,
				ROUND(MIN(amount), 2) as daily_min,
				ROUND(MAX(amount), 2) as daily_max
			FROM orders
			WHERE created_at IS NOT NULL
			GROUP BY DATE(created_at)
			ORDER BY order_date
			LIMIT 5
		`
		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		prevDate := ""
		for rows.Next() {
			var orderDate string
			var orderCount int
			var dailyTotal, dailyAverage, dailyMin, dailyMax float64
			err := rows.Scan(&orderDate, &orderCount, &dailyTotal, &dailyAverage, &dailyMin, &dailyMax)
			assert.NoError(t, err)

			assert.Regexp(t, `^\d{4}-\d{2}-\d{2}`, orderDate, "Date format")
			if prevDate != "" {
				assert.GreaterOrEqual(t, orderDate, prevDate, "Dates should be in order")
			}
			prevDate = orderDate

			assert.Greater(t, orderCount, 0)
			assert.Greater(t, dailyTotal, 0.0)
			assert.InDelta(t, dailyTotal/float64(orderCount), dailyAverage, 0.1)
			assert.LessOrEqual(t, dailyMin, dailyAverage)
			assert.GreaterOrEqual(t, dailyMax, dailyAverage)
			assert.Greater(t, dailyMax, 0.0)
			assert.Greater(t, dailyMin, 0.0)

			count++
		}
		assert.Equal(t, 5, count, "Should have exactly 5 daily statistics")
		assert.NoError(t, rows.Err())
	})

	t.Run("NULL handling and COALESCE", func(t *testing.T) {
		query := `
			SELECT 
				COUNT(*) as total,
				COUNT(age) as with_age,
				COUNT(*) - COUNT(age) as without_age,
				AVG(CAST(age AS REAL)) as avg_age,
				COALESCE(MAX(CAST(age AS INTEGER)), 0) as max_age
			FROM user
		`
		var total, withAge, withoutAge int
		var avgAge sql.NullFloat64
		var maxAge int
		err := db.QueryRowContext(ctx, query).Scan(&total, &withAge, &withoutAge, &avgAge, &maxAge)
		require.NoError(t, err)
		assert.Greater(t, total, 0)
		assert.Equal(t, total, withAge+withoutAge)
		if avgAge.Valid {
			assert.Greater(t, avgAge.Float64, 0.0)
			assert.LessOrEqual(t, avgAge.Float64, float64(maxAge))
		}
	})

	t.Run("Case-insensitive search with LIKE", func(t *testing.T) {
		query := `
			SELECT name, email
			FROM user
			WHERE LOWER(email) LIKE '%example.com%'
			LIMIT 10
		`
		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			var name, email string
			err := rows.Scan(&name, &email)
			assert.NoError(t, err)
			assert.NotEmpty(t, name)
			assert.Contains(t, email, "example.com")
			count++
		}
		assert.Greater(t, count, 0, "Should find users with example.com emails")
		assert.NoError(t, rows.Err())
	})
}

func TestMultipleSequentialQueries(t *testing.T) {
	t.Parallel()

	companyDir := filepath.Join("testdata", "company")
	db, err := Open(companyDir)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	t.Run("Sequential data modification simulation", func(t *testing.T) {
		// Simulates transactional workflow without actual writes since CSV files are read-only

		var initialOrderCount int
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders").Scan(&initialOrderCount)
		require.NoError(t, err)

		highVolumeUsers := make(map[int]float64)
		query := `
			SELECT user_id, SUM(amount) as total
			FROM orders
			GROUP BY user_id
			HAVING SUM(amount) > 100000
		`
		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)

		for rows.Next() {
			var userID int
			var total float64
			err := rows.Scan(&userID, &total)
			assert.NoError(t, err)
			highVolumeUsers[userID] = total
		}
		assert.NoError(t, rows.Err())
		_ = rows.Close()
		assert.NotEmpty(t, highVolumeUsers, "Should have high-volume users")

		for userID, total := range highVolumeUsers {
			var userName, userEmail string
			userQuery := "SELECT name, email FROM user WHERE id = ?"
			err := db.QueryRowContext(ctx, userQuery, userID).Scan(&userName, &userEmail)
			assert.NoError(t, err)
			assert.NotEmpty(t, userName)
			assert.NotEmpty(t, userEmail)

			var verifyTotal float64
			verifyQuery := "SELECT SUM(amount) FROM orders WHERE user_id = ?"
			err = db.QueryRowContext(ctx, verifyQuery, userID).Scan(&verifyTotal)
			assert.NoError(t, err)
			assert.InDelta(t, total, verifyTotal, 0.01, "Totals should match")

			var hasHealthInsurance, hasPension sql.NullString
			benefitsQuery := "SELECT health_insurance, pension_plan FROM benefits WHERE user_id = ?"
			err = db.QueryRowContext(ctx, benefitsQuery, userID).Scan(&hasHealthInsurance, &hasPension)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				assert.NoError(t, err)
			}

			// Limit iterations to avoid test timeout
			if len(highVolumeUsers) > 5 {
				break
			}
		}

		var finalOrderCount int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders").Scan(&finalOrderCount)
		require.NoError(t, err)
		assert.Equal(t, initialOrderCount, finalOrderCount, "Order count should remain constant")
	})

	t.Run("Complex business report generation", func(t *testing.T) {
		// Tests real-world scenario where multiple aggregations build a single report

		type DepartmentReport struct {
			ID            int
			Name          string
			EmployeeCount int
			AvgSalary     float64
			TotalOrders   float64
			ProjectCount  int
			TrainingHours int
		}

		deptQuery := "SELECT id, name FROM department WHERE name IS NOT NULL LIMIT 5"
		deptRows, err := db.QueryContext(ctx, deptQuery)
		require.NoError(t, err)
		defer deptRows.Close()

		var reports []DepartmentReport
		for deptRows.Next() {
			var report DepartmentReport
			err := deptRows.Scan(&report.ID, &report.Name)
			assert.NoError(t, err)

			err = db.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM user WHERE department_id = ?",
				report.ID).Scan(&report.EmployeeCount)
			if err != nil {
				report.EmployeeCount = 0
			}

			var avgSalary sql.NullFloat64
			err = db.QueryRowContext(ctx, `
				SELECT AVG(s.base_salary)
				FROM user u
				JOIN salary s ON u.id = s.user_id
				WHERE u.department_id = ?
			`, report.ID).Scan(&avgSalary)
			if err == nil && avgSalary.Valid {
				report.AvgSalary = avgSalary.Float64
			}

			var totalOrders sql.NullFloat64
			err = db.QueryRowContext(ctx, `
				SELECT SUM(o.amount)
				FROM user u
				JOIN orders o ON u.id = o.user_id
				WHERE u.department_id = ?
			`, report.ID).Scan(&totalOrders)
			if err == nil && totalOrders.Valid {
				report.TotalOrders = totalOrders.Float64
			}

			err = db.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM project WHERE department_id = ?",
				report.ID).Scan(&report.ProjectCount)
			if err != nil {
				report.ProjectCount = 0
			}

			var trainingHours sql.NullInt64
			err = db.QueryRowContext(ctx, `
				SELECT SUM(duration_days)
				FROM training
				WHERE department_id = ?
			`, report.ID).Scan(&trainingHours)
			if err == nil && trainingHours.Valid {
				report.TrainingHours = int(trainingHours.Int64)
			}

			reports = append(reports, report)
		}
		assert.NoError(t, deptRows.Err())

		assert.NotEmpty(t, reports, "Should have department reports")
		for _, report := range reports {
			assert.NotEmpty(t, report.Name)
			// At least one metric should be non-zero for each department
			hasData := report.EmployeeCount > 0 ||
				report.AvgSalary > 0 ||
				report.TotalOrders > 0 ||
				report.ProjectCount > 0 ||
				report.TrainingHours > 0
			assert.True(t, hasData, "Department %s should have at least some data", report.Name)
		}
	})

	t.Run("Recursive-like query simulation", func(t *testing.T) {
		// Tests graph traversal pattern without CTE support using iterative queries

		var seedUserID int
		var seedUserName string
		err := db.QueryRowContext(ctx, `
			SELECT u.id, u.name
			FROM user u
			JOIN user_project up ON u.id = up.user_id
			GROUP BY u.id, u.name
			HAVING COUNT(up.project_id) > 1
			LIMIT 1
		`).Scan(&seedUserID, &seedUserName)
		require.NoError(t, err)

		projectQuery := "SELECT project_id FROM user_project WHERE user_id = ?"
		projectRows, err := db.QueryContext(ctx, projectQuery, seedUserID)
		require.NoError(t, err)

		var projectIDs []int
		for projectRows.Next() {
			var projectID int
			err := projectRows.Scan(&projectID)
			assert.NoError(t, err)
			projectIDs = append(projectIDs, projectID)
		}
		assert.NoError(t, projectRows.Err())
		_ = projectRows.Close()
		assert.NotEmpty(t, projectIDs, "Seed user should have projects")

		connectedUsers := make(map[int]string)
		for _, projectID := range projectIDs {
			userQuery := `
				SELECT DISTINCT u.id, u.name
				FROM user u
				JOIN user_project up ON u.id = up.user_id
				WHERE up.project_id = ?
				AND u.id != ?
			`
			userRows, err := db.QueryContext(ctx, userQuery, projectID, seedUserID)
			if err != nil {
				continue
			}

			for userRows.Next() {
				var userID int
				var userName string
				err := userRows.Scan(&userID, &userName)
				if err == nil {
					connectedUsers[userID] = userName
				}
			}
			assert.NoError(t, userRows.Err())
			_ = userRows.Close()
		}

		assert.NotEmpty(t, connectedUsers, "Should find users connected through projects")
	})

	t.Run("Batch processing simulation", func(t *testing.T) {
		// Validates pagination logic for large datasets

		const batchSize = 100
		offset := 0
		totalProcessed := 0
		totalAmount := 0.0

		for {
			query := `
				SELECT id, user_id, amount, status
				FROM orders
				WHERE amount > 1000
				ORDER BY id
				LIMIT ? OFFSET ?
			`
			rows, err := db.QueryContext(ctx, query, batchSize, offset)
			require.NoError(t, err)

			batchCount := 0
			batchAmount := 0.0

			for rows.Next() {
				var orderID, userID int
				var amount float64
				var status sql.NullString
				err := rows.Scan(&orderID, &userID, &amount, &status)
				assert.NoError(t, err)
				assert.Greater(t, amount, 1000.0)

				batchCount++
				batchAmount += amount
			}
			assert.NoError(t, rows.Err())
			_ = rows.Close()

			if batchCount == 0 {
				break
			}

			totalProcessed += batchCount
			totalAmount += batchAmount
			offset += batchSize

			// Limit iterations to prevent test timeout while still validating batch logic
			if offset >= batchSize*3 {
				break
			}
		}

		assert.Greater(t, totalProcessed, 0, "Should have processed some orders")
		assert.Greater(t, totalAmount, 0.0, "Should have accumulated order amounts")
	})
}

func TestDataIntegrityValidation(t *testing.T) {
	t.Parallel()

	companyDir := filepath.Join("testdata", "company")
	db, err := Open(companyDir)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	t.Run("Comprehensive foreign key validation", func(t *testing.T) {
		var userDeptOrphans int
		err := db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM user u
			WHERE u.department_id IS NOT NULL
			AND u.department_id != ''
			AND NOT EXISTS (
				SELECT 1 FROM department d WHERE d.id = u.department_id
			)
		`).Scan(&userDeptOrphans)
		require.NoError(t, err)
		assert.Equal(t, 0, userDeptOrphans, "All user.department_id should reference valid departments")

		var orderUserOrphans int
		err = db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM orders o
			WHERE NOT EXISTS (
				SELECT 1 FROM user u WHERE u.id = o.user_id
			)
		`).Scan(&orderUserOrphans)
		require.NoError(t, err)
		assert.Equal(t, 0, orderUserOrphans, "All orders.user_id should reference valid users")

		var salaryUserOrphans int
		err = db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM salary s
			WHERE NOT EXISTS (
				SELECT 1 FROM user u WHERE u.id = s.user_id
			)
		`).Scan(&salaryUserOrphans)
		require.NoError(t, err)
		assert.Equal(t, 0, salaryUserOrphans, "All salary.user_id should reference valid users")

		var projectDeptOrphans int
		err = db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM project p
			WHERE p.department_id IS NOT NULL
			AND p.department_id != ''
			AND NOT EXISTS (
				SELECT 1 FROM department d WHERE d.id = p.department_id
			)
		`).Scan(&projectDeptOrphans)
		require.NoError(t, err)
		assert.Equal(t, 0, projectDeptOrphans, "All project.department_id should reference valid departments")

		var userCount, salaryCount int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM user").Scan(&userCount)
		require.NoError(t, err)
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM salary").Scan(&salaryCount)
		require.NoError(t, err)
		assert.Equal(t, userCount, salaryCount, "Every user should have a salary record")
	})

	t.Run("Data type consistency validation", func(t *testing.T) {
		// Verify that numeric fields can be properly cast
		numericQuery := `
			SELECT 
				COUNT(*) as total,
				SUM(CASE WHEN CAST(amount AS REAL) > 0 THEN 1 ELSE 0 END) as valid_amounts
			FROM orders
			WHERE amount IS NOT NULL AND amount != ''
		`
		var total, validAmounts int
		err := db.QueryRowContext(ctx, numericQuery).Scan(&total, &validAmounts)
		require.NoError(t, err)
		assert.Equal(t, total, validAmounts, "All amounts should be valid positive numbers")
	})

	t.Run("Duplicate detection", func(t *testing.T) {
		// Primary keys must be unique to maintain data integrity
		duplicateQuery := `
			SELECT id, COUNT(*) as count
			FROM user
			GROUP BY id
			HAVING COUNT(*) > 1
		`
		rows, err := db.QueryContext(ctx, duplicateQuery)
		require.NoError(t, err)
		defer rows.Close()

		duplicates := 0
		for rows.Next() {
			var id, count int
			err := rows.Scan(&id, &count)
			assert.NoError(t, err)
			duplicates++
		}
		assert.NoError(t, rows.Err())
		assert.Equal(t, 0, duplicates, "Should have no duplicate user IDs")
	})
}
