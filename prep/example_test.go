package prep_test

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/nao1215/filesql/prep"
)

func readAllExampleOutput(reader io.Reader) ([]byte, error) {
	return io.ReadAll(reader)
}

// User represents a user record with preprocessing and validation
type User struct {
	Name  string `prep:"trim" validate:"required"`
	Email string `prep:"trim,lowercase" validate:"required"`
	Age   string
}

func Example() {
	// Sample CSV data with whitespace that needs trimming
	csvData := `name,email,age
  John Doe  ,JOHN@EXAMPLE.COM,30
Jane Smith,jane@example.com,25
`

	// Create a processor for CSV files
	processor := prep.NewProcessor(prep.FileTypeCSV)

	// Prepare a slice to hold the parsed records
	var users []User

	// Process the data
	reader, result, err := processor.Process(strings.NewReader(csvData), &users)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Print processing results
	fmt.Printf("Processed %d rows, %d valid\n", result.RowCount, result.ValidRowCount)

	// Print parsed records (with preprocessing applied)
	for i, user := range users {
		fmt.Printf("User %d: Name=%q, Email=%q\n", i+1, user.Name, user.Email)
	}

	// The reader can be passed to filesql
	// For demonstration, we just read and show the preprocessed output
	output, err := io.ReadAll(reader)
	if err != nil {
		fmt.Printf("Error reading output: %v\n", err)
		return
	}
	fmt.Printf("Output for filesql:\n%s", output)

	// Output:
	// Processed 2 rows, 2 valid
	// User 1: Name="John Doe", Email="john@example.com"
	// User 2: Name="Jane Smith", Email="jane@example.com"
	// Output for filesql:
	// name,email,age
	// John Doe,john@example.com,30
	// Jane Smith,jane@example.com,25
}

func Example_validation() {
	// CSV data with validation error (empty required name)
	csvData := `name,email,age
,john@example.com,30
Jane,jane@example.com,25
`

	processor := prep.NewProcessor(prep.FileTypeCSV)
	var users []User

	_, result, err := processor.Process(strings.NewReader(csvData), &users)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Check for validation errors
	if result.HasErrors() {
		fmt.Printf("Found %d validation errors:\n", len(result.Errors))
		for _, e := range result.ValidationErrors() {
			fmt.Printf("  Row %d, Column %q: %s\n", e.Row, e.Column, e.Message)
		}
	}

	fmt.Printf("Valid rows: %d/%d\n", result.ValidRowCount, result.RowCount)

	// Output:
	// Found 1 validation errors:
	//   Row 1, Column "name": value is required
	// Valid rows: 1/2
}

// Example_complexPrepAndValidation demonstrates comprehensive preprocessing and validation
// using multiple prep tags and validation rules for realistic data processing scenarios.
func Example_complexPrepAndValidation() {
	// Product represents a product record with comprehensive preprocessing and validation
	type Product struct {
		// Name: trim whitespace, require non-empty
		Name string `prep:"trim" validate:"required"`
		// SKU: trim, uppercase, require non-empty and alphanumeric
		SKU string `prep:"trim,uppercase" validate:"required,alphanumeric"`
		// Price: trim, set default if empty, validate as number (int or decimal)
		Price string `prep:"trim,default=0.00" validate:"number"`
		// Quantity: trim, coerce to int, validate as integer
		Quantity string `prep:"trim,coerce=int" validate:"numeric"`
		// Category: trim, lowercase, collapse multiple spaces
		Category string `prep:"trim,lowercase,collapse_space"`
		// Description: trim, strip HTML, truncate to 200 chars
		Description string `prep:"trim,strip_html,truncate=200"`
		// URL: trim, fix scheme (add https:// if missing)
		URL string `prep:"trim,fix_scheme=https"`
		// Tags: trim, replace semicolons with commas
		Tags string `prep:"trim,replace=;:,"`
	}

	// Sample CSV with various data quality issues
	csvData := `name,sku,price,quantity,category,description,url,tags
  Widget Pro  ,ABC123,19.99,100,  Electronics   ,<p>A <b>great</b> product!</p>,example.com/widget,tag1;tag2;tag3
 Gadget Plus ,DEF456,29.99,50,  home  goods  ,Simple description,https://example.com/gadget,electronics;sale
  ,ABC@#$,not_a_number,5,test,desc,url,tags
`

	processor := prep.NewProcessor(prep.FileTypeCSV)
	var products []Product

	reader, result, err := processor.Process(strings.NewReader(csvData), &products)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Show processing summary
	fmt.Printf("Processing Summary:\n")
	fmt.Printf("  Total rows: %d\n", result.RowCount)
	fmt.Printf("  Valid rows: %d\n", result.ValidRowCount)
	fmt.Printf("  Invalid rows: %d\n", result.InvalidRowCount())

	// Show validation errors
	if result.HasErrors() {
		fmt.Printf("\nValidation Errors:\n")
		for _, ve := range result.ValidationErrors() {
			fmt.Printf("  Row %d, Field %q: %s\n", ve.Row, ve.Field, ve.Message)
		}
	}

	// Show preprocessed records
	fmt.Printf("\nPreprocessed Records:\n")
	for i, p := range products {
		if p.Name != "" { // Skip invalid rows for display
			fmt.Printf("  [%d] Name=%q, SKU=%q, Price=%q, Category=%q\n",
				i+1, p.Name, p.SKU, p.Price, p.Category)
		}
	}

	// Show that URL scheme was fixed
	fmt.Printf("\nURL Examples (scheme fixed):\n")
	for i, p := range products {
		if p.URL != "" && p.Name != "" {
			fmt.Printf("  [%d] %s\n", i+1, p.URL)
		}
	}

	// The reader can be used with filesql
	if _, err := readAllExampleOutput(reader); err != nil {
		fmt.Printf("Error reading output: %v\n", err)
		return
	}

	// Output:
	// Processing Summary:
	//   Total rows: 3
	//   Valid rows: 2
	//   Invalid rows: 1
	//
	// Validation Errors:
	//   Row 3, Field "Name": value is required
	//   Row 3, Field "SKU": value must contain only alphanumeric characters
	//   Row 3, Field "Price": value must be a valid number
	//
	// Preprocessed Records:
	//   [1] Name="Widget Pro", SKU="ABC123", Price="19.99", Category="electronics"
	//   [2] Name="Gadget Plus", SKU="DEF456", Price="29.99", Category="home goods"
	//
	// URL Examples (scheme fixed):
	//   [1] https://example.com/widget
	//   [2] https://example.com/gadget
}

// Example_crossFieldValidation demonstrates validation rules that compare values
// between different fields, such as password confirmation matching.
func Example_crossFieldValidation() {
	// UserForm represents a user registration form with cross-field validation
	type UserForm struct {
		Username        string `prep:"trim,lowercase" validate:"required"`
		Password        string `validate:"required"`
		ConfirmPassword string `validate:"required,eqfield=Password"`
	}

	csvData := `username,password,confirm_password
  Alice  ,secret123,secret123
Bob,password1,wrongpass
`

	processor := prep.NewProcessor(prep.FileTypeCSV)
	var users []UserForm

	_, result, err := processor.Process(strings.NewReader(csvData), &users)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Valid: %d/%d\n", result.ValidRowCount, result.RowCount)

	if result.HasErrors() {
		fmt.Printf("Errors:\n")
		for _, ve := range result.ValidationErrors() {
			fmt.Printf("  Row %d, Field %q: %s\n", ve.Row, ve.Field, ve.Message)
		}
	}

	// Output:
	// Valid: 1/2
	// Errors:
	//   Row 2, Field "ConfirmPassword": value must equal field Password
}

// Example_detectFileType demonstrates automatic file type detection from file paths.
func Example_detectFileType() {
	files := []string{
		"data.csv",
		"data.csv.gz",
		"report.xlsx",
		"logs.tsv.bz2",
		"events.parquet",
		"access.ltsv.zst",
		"config.json",
		"config.json.gz",
		"events.jsonl",
		"events.jsonl.zst",
	}

	for _, f := range files {
		ft := prep.DetectFileType(f)
		fmt.Printf("%s -> %s (compressed: %v)\n", f, ft, prep.IsCompressed(ft))
	}

	// Output:
	// data.csv -> CSV (compressed: false)
	// data.csv.gz -> CSV (gzip) (compressed: true)
	// report.xlsx -> XLSX (compressed: false)
	// logs.tsv.bz2 -> TSV (bzip2) (compressed: true)
	// events.parquet -> Parquet (compressed: false)
	// access.ltsv.zst -> LTSV (zstd) (compressed: true)
	// config.json -> JSON (compressed: false)
	// config.json.gz -> JSON (gzip) (compressed: true)
	// events.jsonl -> JSONL (compressed: false)
	// events.jsonl.zst -> JSONL (zstd) (compressed: true)
}

// Example_jsonProcessing demonstrates processing JSON data.
// JSON arrays are parsed into rows, each containing the raw JSON element
// in a single "data" column.
func Example_jsonProcessing() {
	type Record struct {
		Data string `name:"data" validate:"required"`
	}

	jsonData := `[{"name":"Alice","age":30},{"name":"Bob","age":25}]`

	processor := prep.NewProcessor(prep.FileTypeJSON)
	var records []Record

	reader, result, err := processor.Process(strings.NewReader(jsonData), &records)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Processed %d rows, %d valid\n", result.RowCount, result.ValidRowCount)
	for i, r := range records {
		fmt.Printf("Row %d: %s\n", i+1, r.Data)
	}

	output, err := readAllExampleOutput(reader)
	if err != nil {
		fmt.Printf("Error reading output: %v\n", err)
		return
	}
	fmt.Printf("JSONL output:\n%s", output)

	// Output:
	// Processed 2 rows, 2 valid
	// Row 1: {"name":"Alice","age":30}
	// Row 2: {"name":"Bob","age":25}
	// JSONL output:
	// {"name":"Alice","age":30}
	// {"name":"Bob","age":25}
}

// Example_employeePreprocessing demonstrates the full power of fileprep:
// combining multiple preprocessors and validators to clean and validate real-world messy data.
func Example_employeePreprocessing() {
	// Employee represents employee data with comprehensive preprocessing and validation
	type Employee struct {
		// ID: pad to 6 digits, must be numeric
		EmployeeID string `name:"id" prep:"trim,pad_left=6:0" validate:"required,numeric,len=6"`
		// Name: clean whitespace, required alphabetic with spaces
		FullName string `name:"name" prep:"trim,collapse_space" validate:"required,alphaspace"`
		// Email: normalize to lowercase, validate format
		Email string `prep:"trim,lowercase" validate:"required,email"`
		// Department: normalize case, must be one of allowed values
		Department string `prep:"trim,uppercase" validate:"required,oneof=ENGINEERING SALES MARKETING HR"`
		// Salary: keep only digits, validate range
		Salary string `prep:"trim,keep_digits" validate:"required,numeric,gte=30000,lte=500000"`
		// Phone: extract digits, validate E.164 format after adding country code
		Phone string `prep:"trim,keep_digits,prefix=+1" validate:"e164"`
		// Start date: validate datetime format
		StartDate string `name:"start_date" prep:"trim" validate:"required,datetime=2006-01-02"`
		// Manager ID: required only if department is not HR
		ManagerID string `name:"manager_id" prep:"trim,pad_left=6:0" validate:"required_unless=Department HR"`
		// Website: fix missing scheme, validate URL
		Website string `prep:"trim,lowercase,fix_scheme=https" validate:"url"`
	}

	// Messy real-world CSV data
	csvData := `id,name,email,department,salary,phone,start_date,manager_id,website
42,  John   Doe  ,JOHN.DOE@COMPANY.COM,engineering,75000,5551234567,2023-01-15,1,company.com/john
7,Jane Smith,jane@COMPANY.com,  Sales  ,120000,5559876543,2022-06-01,2,WWW.LINKEDIN.COM/in/jane
123,Bob Wilson,bob.wilson@company.com,HR,45000,5551112222,2024-03-20,,
99,Alice Brown,alice@company.com,Marketing,88500,5554443333,2023-09-10,3,https://alice.dev
`

	processor := prep.NewProcessor(prep.FileTypeCSV)
	var employees []Employee

	_, result, err := processor.Process(strings.NewReader(csvData), &employees)
	if err != nil {
		fmt.Printf("Fatal error: %v\n", err)
		return
	}

	fmt.Printf("=== Processing Result ===\n")
	fmt.Printf("Total rows: %d, Valid rows: %d\n\n", result.RowCount, result.ValidRowCount)

	for i, emp := range employees {
		fmt.Printf("Employee %d:\n", i+1)
		fmt.Printf("  ID:         %s\n", emp.EmployeeID)
		fmt.Printf("  Name:       %s\n", emp.FullName)
		fmt.Printf("  Email:      %s\n", emp.Email)
		fmt.Printf("  Department: %s\n", emp.Department)
		fmt.Printf("  Salary:     %s\n", emp.Salary)
		fmt.Printf("  Phone:      %s\n", emp.Phone)
		fmt.Printf("  Start Date: %s\n", emp.StartDate)
		fmt.Printf("  Manager ID: %s\n", emp.ManagerID)
		if emp.Website != "" {
			fmt.Printf("  Website:    %s\n\n", emp.Website)
		} else {
			fmt.Printf("  Website:    (none)\n\n")
		}
	}

	// Output:
	// === Processing Result ===
	// Total rows: 4, Valid rows: 3
	//
	// Employee 1:
	//   ID:         000042
	//   Name:       John Doe
	//   Email:      john.doe@company.com
	//   Department: ENGINEERING
	//   Salary:     75000
	//   Phone:      +15551234567
	//   Start Date: 2023-01-15
	//   Manager ID: 000001
	//   Website:    https://company.com/john
	//
	// Employee 2:
	//   ID:         000007
	//   Name:       Jane Smith
	//   Email:      jane@company.com
	//   Department: SALES
	//   Salary:     120000
	//   Phone:      +15559876543
	//   Start Date: 2022-06-01
	//   Manager ID: 000002
	//   Website:    https://www.linkedin.com/in/jane
	//
	// Employee 3:
	//   ID:         000123
	//   Name:       Bob Wilson
	//   Email:      bob.wilson@company.com
	//   Department: HR
	//   Salary:     45000
	//   Phone:      +15551112222
	//   Start Date: 2024-03-20
	//   Manager ID: 000000
	//   Website:    (none)
	//
	// Employee 4:
	//   ID:         000099
	//   Name:       Alice Brown
	//   Email:      alice@company.com
	//   Department: MARKETING
	//   Salary:     88500
	//   Phone:      +15554443333
	//   Start Date: 2023-09-10
	//   Manager ID: 000003
	//   Website:    https://alice.dev
}

// Example_detailedErrorReporting demonstrates precise error information including
// row number, column name, and specific validation failure reason.
func Example_detailedErrorReporting() {
	// Order represents an order with strict validation rules
	type Order struct {
		OrderID    string `name:"order_id" validate:"required,uuid4"`
		CustomerID string `name:"customer_id" validate:"required,numeric"`
		Email      string `validate:"required,email"`
		Amount     string `validate:"required,number,gt=0,lte=10000"`
		Currency   string `validate:"required,len=3,uppercase"`
		Country    string `validate:"required,alpha,len=2"`
		OrderDate  string `name:"order_date" validate:"required,datetime=2006-01-02"`
		ShipDate   string `name:"ship_date" validate:"datetime=2006-01-02,gtfield=OrderDate"`
		IPAddress  string `name:"ip_address" validate:"required,ip_addr"`
		PromoCode  string `name:"promo_code" validate:"alphanumeric"`
		Quantity   string `validate:"required,numeric,gte=1,lte=100"`
		UnitPrice  string `name:"unit_price" validate:"required,number,gt=0"`
		TotalCheck string `name:"total_check" validate:"required,eqfield=Amount"`
	}

	// CSV with multiple validation errors
	csvData := `order_id,customer_id,email,amount,currency,country,order_date,ship_date,ip_address,promo_code,quantity,unit_price,total_check
550e8400-e29b-41d4-a716-446655440000,12345,alice@example.com,500.00,USD,US,2024-01-15,2024-01-20,192.168.1.1,SAVE10,2,250.00,500.00
invalid-uuid,abc,not-an-email,-100,US,USA,2024/01/15,2024-01-10,999.999.999.999,PROMO-CODE-TOO-LONG!!,0,0,999
550e8400-e29b-41d4-a716-446655440001,,bob@test,50000,EURO,J1,not-a-date,,2001:db8::1,VALID20,101,-50,50000
123e4567-e89b-42d3-a456-426614174000,99999,charlie@company.com,1500.50,JPY,JP,2024-02-28,2024-02-25,10.0.0.1,VIP,5,300.10,1500.50
`

	processor := prep.NewProcessor(prep.FileTypeCSV)
	var orders []Order

	_, result, err := processor.Process(strings.NewReader(csvData), &orders)
	if err != nil {
		fmt.Printf("Fatal error: %v\n", err)
		return
	}

	fmt.Printf("=== Validation Report ===\n")
	fmt.Printf("Total rows:     %d\n", result.RowCount)
	fmt.Printf("Valid rows:     %d\n", result.ValidRowCount)
	fmt.Printf("Invalid rows:   %d\n", result.RowCount-result.ValidRowCount)
	fmt.Printf("Total errors:   %d\n\n", len(result.ValidationErrors()))

	if result.HasErrors() {
		fmt.Println("=== Error Details ===")
		for _, e := range result.ValidationErrors() {
			fmt.Printf("Row %d, Column '%s': %s\n", e.Row, e.Column, e.Message)
		}
	}

	// Output:
	// === Validation Report ===
	// Total rows:     4
	// Valid rows:     1
	// Invalid rows:   3
	// Total errors:   23
	//
	// === Error Details ===
	// Row 2, Column 'order_id': value must be a valid UUID version 4
	// Row 2, Column 'customer_id': value must be numeric
	// Row 2, Column 'email': value must be a valid email address
	// Row 2, Column 'amount': value must be greater than 0
	// Row 2, Column 'currency': value must have exactly 3 characters
	// Row 2, Column 'country': value must have exactly 2 characters
	// Row 2, Column 'order_date': value must be a valid datetime in format: 2006-01-02
	// Row 2, Column 'ip_address': value must be a valid IP address
	// Row 2, Column 'promo_code': value must contain only alphanumeric characters
	// Row 2, Column 'quantity': value must be greater than or equal to 1
	// Row 2, Column 'unit_price': value must be greater than 0
	// Row 2, Column 'ship_date': value must be greater than field OrderDate
	// Row 2, Column 'total_check': value must equal field Amount
	// Row 3, Column 'customer_id': value is required
	// Row 3, Column 'email': value must be a valid email address
	// Row 3, Column 'amount': value must be less than or equal to 10000
	// Row 3, Column 'currency': value must have exactly 3 characters
	// Row 3, Column 'country': value must contain only alphabetic characters
	// Row 3, Column 'order_date': value must be a valid datetime in format: 2006-01-02
	// Row 3, Column 'quantity': value must be less than or equal to 100
	// Row 3, Column 'unit_price': value must be greater than 0
	// Row 3, Column 'ship_date': value must be greater than field OrderDate
	// Row 4, Column 'ship_date': value must be greater than field OrderDate
}

// Example_processToWriter demonstrates ProcessToWriter, which writes
// preprocessed output directly to an io.Writer instead of buffering
// the entire output in memory.
func Example_processToWriter() {
	type Record struct {
		Name  string `prep:"trim" validate:"required"`
		Email string `prep:"trim,lowercase"`
	}

	csvData := "name,email\n  Alice  ,ALICE@EXAMPLE.COM\n  Bob  ,BOB@EXAMPLE.COM\n"
	processor := prep.NewProcessor(prep.FileTypeCSV)
	var records []Record

	var buf bytes.Buffer
	result, err := processor.ProcessToWriter(strings.NewReader(csvData), &records, &buf)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Processed %d rows, %d valid\n", result.RowCount, result.ValidRowCount)
	fmt.Printf("Output:\n%s", buf.String())
	// Output:
	// Processed 2 rows, 2 valid
	// Output:
	// name,email
	// Alice,alice@example.com
	// Bob,bob@example.com
}
