package prep

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// BenchmarkRecord represents a complex record with multiple prep and validate tags.
// This struct is designed to test real-world preprocessing and validation scenarios.
type BenchmarkRecord struct {
	// Basic text fields with common preprocessing
	FirstName  string `name:"first_name" prep:"trim,lowercase" validate:"required,alpha,min=2,max=50"` // FirstName holds the given name.
	LastName   string `name:"last_name" prep:"trim,lowercase" validate:"required,alpha,min=2,max=50"`  // LastName holds the family name.
	MiddleName string `name:"middle_name" prep:"trim,lowercase,default=N/A" validate:"alpha"`          // MiddleName holds the optional middle name.

	// Email with extensive preprocessing and validation
	Email string `name:"email" prep:"trim,lowercase,collapse_space" validate:"required,email"` // Email holds the contact address.

	// Numeric fields with validation
	Age    string `name:"age" prep:"trim,keep_digits" validate:"required,numeric,min=0,max=150"` // Age holds the normalized age.
	Salary string `name:"salary" prep:"trim,keep_digits" validate:"numeric,min=0"`               // Salary holds the annual salary digits.
	Score  string `name:"score" prep:"trim" validate:"number,min=0,max=100"`                     // Score holds a decimal score.

	// ID fields with formatting
	UserID     string `name:"user_id" prep:"trim,uppercase,pad_left=10:0" validate:"required,alphanumeric"` // UserID holds the external user identifier.
	EmployeeID string `name:"employee_id" prep:"trim,pad_left=8:0" validate:"numeric"`                      // EmployeeID holds the internal employee identifier.

	// URL and network fields
	Website string `name:"website" prep:"trim,lowercase,fix_scheme=https" validate:"url"` // Website holds the user-facing URL.
	IPAddr  string `name:"ip_addr" prep:"trim" validate:"ip_addr"`                        // IPAddr holds the source IP address.

	// Text content with HTML handling
	Bio         string `name:"bio" prep:"trim,strip_html,collapse_space,truncate=500" validate:"printascii"` // Bio holds the rich-text biography.
	Description string `name:"description" prep:"trim,strip_newline,collapse_space" validate:"ascii"`        // Description holds the free-form description.

	// Status and category fields
	Status   string `name:"status" prep:"trim,uppercase" validate:"required,oneof=ACTIVE INACTIVE PENDING"` // Status holds the workflow state.
	Category string `name:"category" prep:"trim,lowercase" validate:"required,alpha"`                       // Category holds the business category.

	// Date-like fields
	CreatedAt string `name:"created_at" prep:"trim" validate:"required"` // CreatedAt holds the creation date.
	UpdatedAt string `name:"updated_at" prep:"trim"`                     // UpdatedAt holds the update date.

	// Phone number with character filtering
	Phone string `name:"phone" prep:"trim,keep_digits,pad_left=10:0" validate:"numeric,len=10"` // Phone holds the normalized local phone number.

	// UUID field
	UUID string `name:"uuid" prep:"trim,lowercase" validate:"uuid"` // UUID holds the record identifier.

	// Fields with cross-field validation (reference only, validated separately)
	Password        string `name:"password" prep:"trim" validate:"required,min=8"`                    // Password holds the normalized password input.
	ConfirmPassword string `name:"confirm_password" prep:"trim" validate:"required,eqfield=Password"` // ConfirmPassword holds the confirmation input.
}

// generateBenchmarkCSV creates a CSV with the specified number of records.
// Data is realistic and designed to exercise various preprocessors and validators.
func generateBenchmarkCSV(numRecords int) string {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	// Header row
	headers := []string{
		"first_name", "last_name", "middle_name", "email", "age", "salary", "score",
		"user_id", "employee_id", "website", "ip_addr", "bio", "description",
		"status", "category", "created_at", "updated_at", "phone", "uuid",
		"password", "confirm_password",
	}
	if err := writer.Write(headers); err != nil {
		panic(err)
	}

	// Sample data templates (will be rotated)
	firstNames := []string{"  John  ", " JANE ", "  bob  ", " Alice ", "  CHARLIE  "}
	lastNames := []string{"  Smith  ", " DOE ", "  johnson  ", " WILLIAMS ", "  Brown  "}
	statuses := []string{"active", "INACTIVE", "  pending  ", "Active", "PENDING"}
	categories := []string{"  technology  ", " FINANCE ", "healthcare", "  EDUCATION  ", "retail"}

	for i := range numRecords {
		idx := i % 5
		fields := []string{
			firstNames[idx],
			lastNames[idx],
			"",
			fmt.Sprintf("  USER%d@EXAMPLE.COM  ", i),
			fmt.Sprintf("  %d years  ", 20+idx*10),
			fmt.Sprintf("  $%d,000  ", 50+i%50),
			fmt.Sprintf("  %.1f  ", float64(i%100)+0.5),
			fmt.Sprintf("  usr%d  ", i),
			fmt.Sprintf("  %d  ", 1000+i),
			fmt.Sprintf("  example%d.com  ", i),
			fmt.Sprintf("192.168.%d.%d", i%256, (i+1)%256),
			fmt.Sprintf("  <p>Bio for user %d</p>  <br/>  ", i),
			fmt.Sprintf("  Description\nwith  multiple   spaces   for %d  ", i),
			statuses[idx],
			categories[idx],
			fmt.Sprintf("2024-01-%02d", (i%28)+1),
			fmt.Sprintf("2024-06-%02d", (i%28)+1),
			fmt.Sprintf("  (%03d) %03d-%04d  ", i%1000, i%1000, i%10000),
			fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", i, i%65536, i%65536, i%65536, i),
			fmt.Sprintf("Password%d!", i),
			fmt.Sprintf("Password%d!", i),
		}
		if err := writer.Write(fields); err != nil {
			panic(err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		panic(err)
	}

	return buf.String()
}

// BenchmarkProcessCSV_Small benchmarks processing 100 records
func BenchmarkProcessCSV_Small(b *testing.B) {
	csvData := generateBenchmarkCSV(100)
	processor := NewProcessor(FileTypeCSV)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		var records []BenchmarkRecord
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkProcessCSV_Medium benchmarks processing 1,000 records
func BenchmarkProcessCSV_Medium(b *testing.B) {
	csvData := generateBenchmarkCSV(1000)
	processor := NewProcessor(FileTypeCSV)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		var records []BenchmarkRecord
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkProcessCSV_Large benchmarks processing 10,000 records
func BenchmarkProcessCSV_Large(b *testing.B) {
	csvData := generateBenchmarkCSV(10000)
	processor := NewProcessor(FileTypeCSV)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		var records []BenchmarkRecord
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkProcessCSV_VeryLarge benchmarks processing 50,000 records
func BenchmarkProcessCSV_VeryLarge(b *testing.B) {
	csvData := generateBenchmarkCSV(50000)
	processor := NewProcessor(FileTypeCSV)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		var records []BenchmarkRecord
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPreprocessorsOnly benchmarks just the preprocessing step
func BenchmarkPreprocessorsOnly(b *testing.B) {
	// Create preprocessors chain that matches BenchmarkRecord
	preps := preprocessors{
		newTrimPreprocessor(),
		newLowercasePreprocessor(),
		newCollapseSpacePreprocessor(),
	}

	testValues := []string{
		"  HELLO WORLD  ",
		"  This Is A    Test   String  ",
		"  UPPERCASE    WITH    SPACES  ",
		"  mixed Case   Text  ",
		"   multiple     spaces    here   ",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, v := range testValues {
			_ = preps.Process(v)
		}
	}
}

// BenchmarkValidatorsOnly benchmarks just the validation step
func BenchmarkValidatorsOnly(b *testing.B) {
	// Create validators chain
	vals := validators{
		newRequiredValidator(),
		newAlphaValidator(),
		newMinValidator(integerNumber(2)),
		newMaxValidator(integerNumber(50)),
	}

	testValues := []string{
		"john",
		"jane",
		"alice",
		"bob",
		"charlie",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, v := range testValues {
			_, _ = vals.Validate(v)
		}
	}
}

// BenchmarkEmailValidation benchmarks email validation specifically
func BenchmarkEmailValidation(b *testing.B) {
	validator := newEmailValidator()
	emails := []string{
		"user@example.com",
		"test.user@domain.org",
		"admin@company.co.jp",
		"info@subdomain.example.com",
		"contact@business.net",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, email := range emails {
			_ = validator.Validate(email)
		}
	}
}

// BenchmarkStripHTML benchmarks HTML stripping
func BenchmarkStripHTML(b *testing.B) {
	prep := newStripHTMLPreprocessor()
	htmlStrings := []string{
		"<p>Hello World</p>",
		"<div class=\"test\"><span>Nested content</span></div>",
		"<script>alert('xss')</script>Normal text",
		"<a href=\"http://example.com\">Link</a>",
		"<br/><hr/><img src=\"test.jpg\"/>",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, html := range htmlStrings {
			_ = prep.Process(html)
		}
	}
}

// BenchmarkCollapseSpace benchmarks space collapsing
func BenchmarkCollapseSpace(b *testing.B) {
	prep := newCollapseSpacePreprocessor()
	strings := []string{
		"hello    world",
		"this   is   a   test   with   many   spaces",
		"  leading and trailing  ",
		"multiple\t\ttabs\t\there",
		"mixed   spaces\tand\ttabs",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, s := range strings {
			_ = prep.Process(s)
		}
	}
}

// BenchmarkKeepDigits benchmarks digit extraction
func BenchmarkKeepDigits(b *testing.B) {
	prep := newKeepDigitsPreprocessor()
	strings := []string{
		"(123) 456-7890",
		"$1,234,567.89",
		"Phone: +1-555-123-4567",
		"ID: ABC-12345-XYZ",
		"2024-01-15T10:30:00Z",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, s := range strings {
			_ = prep.Process(s)
		}
	}
}

// BenchmarkPadLeft benchmarks left padding
func BenchmarkPadLeft(b *testing.B) {
	prep := newPadLeftPreprocessor(10, '0')
	strings := []string{
		"1",
		"12",
		"123",
		"1234",
		"12345",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, s := range strings {
			_ = prep.Process(s)
		}
	}
}

// BenchmarkFixScheme benchmarks URL scheme completion over the value shapes
// that take different paths through it: one that needs a scheme, one that is
// upgraded, one that is left alone, a host with a port, and a scheme written
// without an authority.
func BenchmarkFixScheme(b *testing.B) {
	prep := newFixSchemePreprocessor("https")
	values := []string{
		"example.com/path",
		"http://example.com/path",
		"https://example.com/path",
		"example.com:8080/path",
		"mailto:user@example.com",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, v := range values {
			_ = prep.Process(v)
		}
	}
}

// BenchmarkUUIDValidation benchmarks UUID validation
func BenchmarkUUIDValidation(b *testing.B) {
	validator := newUUIDValidator(uuidTagValue)
	uuids := []string{
		"550e8400-e29b-41d4-a716-446655440000",
		"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"f47ac10b-58cc-4372-a567-0e02b2c3d479",
		"7c9e6679-7425-40de-944b-e07fc1f90ae7",
		"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, uuid := range uuids {
			_ = validator.Validate(uuid)
		}
	}
}

// BenchmarkIPAddressValidation benchmarks IP address validation
func BenchmarkIPAddressValidation(b *testing.B) {
	validator := newIPAddrValidator(ipAddrTagValue)
	ips := []string{
		"192.168.1.1",
		"10.0.0.1",
		"172.16.0.1",
		"::1",
		"2001:0db8:85a3:0000:0000:8a2e:0370:7334",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, ip := range ips {
			_ = validator.Validate(ip)
		}
	}
}

// BenchmarkNumericValidation benchmarks numeric validation
func BenchmarkNumericValidation(b *testing.B) {
	validators := validators{
		newNumericValidator(),
		newMinValidator(integerNumber(0)),
		newMaxValidator(integerNumber(100)),
	}

	values := []string{
		"0",
		"50",
		"100",
		"25",
		"75",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, v := range values {
			_, _ = validators.Validate(v)
		}
	}
}

// BenchmarkComplexPreprocessorChain benchmarks a complex chain of preprocessors
func BenchmarkComplexPreprocessorChain(b *testing.B) {
	preps := preprocessors{
		newTrimPreprocessor(),
		newLowercasePreprocessor(),
		newStripHTMLPreprocessor(),
		newStripNewlinePreprocessor(),
		newCollapseSpacePreprocessor(),
		newTruncatePreprocessor(100),
	}

	testValues := []string{
		"  <p>HELLO</p>   World  \n\n  with   newlines  ",
		"  <div>COMPLEX</div>   <span>HTML</span>   content\r\nhere  ",
		"  <script>ALERT</script>   normal    text   \n  follows  ",
		"  <a href='#'>LINK</a>   and   some    long    text  ",
		"  <br/>  MIXED   \n\n  case   and\t\tspaces  ",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, v := range testValues {
			_ = preps.Process(v)
		}
	}
}

// BenchmarkComplexValidatorChain benchmarks a complex chain of validators
func BenchmarkComplexValidatorChain(b *testing.B) {
	vals := validators{
		newRequiredValidator(),
		newASCIIValidator(),
		newPrintASCIIValidator(),
		newMinValidator(integerNumber(5)),
		newMaxValidator(integerNumber(100)),
		newStartsWithValidator("user"),
		newEndsWithValidator(".com"),
		newContainsValidator("@"),
	}

	testValues := []string{
		"user@example.com",
		"user.test@domain.com",
		"user_admin@company.com",
		"user.support@service.com",
		"user.info@platform.com",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, v := range testValues {
			_, _ = vals.Validate(v)
		}
	}
}

// BenchmarkStructTagParsing benchmarks struct tag parsing
func BenchmarkStructTagParsing(b *testing.B) {
	var records []BenchmarkRecord

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		structType, err := getStructType(&records)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := parseStructType(structType, false); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCSVOutput benchmarks output generation
func BenchmarkCSVOutput(b *testing.B) {
	headers := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	records := make([][]string, 1000)
	for i := range records {
		records[i] = []string{"val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8", "val9", "val10"}
	}

	processor := &Processor{fileType: FileTypeCSV.internal()}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		var buf bytes.Buffer
		buf.Grow(processor.estimateOutputSize(headers, records))
		if err := processor.writeOutput(&buf, headers, records); err != nil {
			b.Fatal(err)
		}
	}
}

// crossFieldRecord carries one tag from each cross-field family, so the
// benchmark measures the per-row cost of resolving target fields.
type crossFieldRecord struct {
	Start   string `validate:"required"`
	End     string `validate:"gtefield=Start"`
	Invoice string `validate:"required_if=Kind paid Tier gold"`
	Address string `validate:"required_with=City Zip"`
	Kind    string
	Tier    string
	City    string
	Zip     string
}

// generateCrossFieldCSV builds a CSV whose every row passes, so the benchmark
// measures the validation path rather than error reporting.
func generateCrossFieldCSV(rows int) string {
	var sb strings.Builder
	sb.WriteString("start,end,invoice,address,kind,tier,city,zip\n")
	for i := range rows {
		sb.WriteString("2024-01-01,2024-12-31,INV-")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(",1 Main St,paid,gold,Kyoto,600-8216\n")
	}
	return sb.String()
}

// BenchmarkProcessCrossFieldCSV benchmarks a struct whose fields carry
// cross-field tags, the path that resolves one target value per named field on
// every row.
func BenchmarkProcessCrossFieldCSV(b *testing.B) {
	csvData := generateCrossFieldCSV(1000)
	processor := NewProcessor(FileTypeCSV)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		var records []crossFieldRecord
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// uniqueRecord measures what a unique column costs: one map lookup and one
// insert per row, and a map that grows with the number of distinct values.
type uniqueRecord struct {
	ID    string `validate:"unique"`
	Name  string
	Email string
}

// generateUniqueCSV builds a CSV whose id column holds distinct values, which
// is the case the seen map grows in.
func generateUniqueCSV(rows int) string {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write([]string{"id", "name", "email"}); err != nil {
		panic(err)
	}
	for i := range rows {
		if err := writer.Write([]string{
			fmt.Sprintf("ID-%08d", i),
			fmt.Sprintf("Name %d", i),
			fmt.Sprintf("user%d@example.com", i),
		}); err != nil {
			panic(err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		panic(err)
	}
	return buf.String()
}

// BenchmarkProcessUniqueCSV benchmarks a unique column over 10,000 rows
func BenchmarkProcessUniqueCSV(b *testing.B) {
	csvData := generateUniqueCSV(10000)
	processor := NewProcessor(FileTypeCSV)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		var records []uniqueRecord
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTrimTags benchmarks the three trimming tags over the value shapes a
// column holds: padded on both sides, padded on one, and not padded at all.
func BenchmarkTrimTags(b *testing.B) {
	values := []string{
		"  hello world  ",
		"\thello world",
		"hello world\t",
		"hello world",
		"　hello world ",
	}

	for _, tc := range []struct {
		name string
		prep preprocessor
	}{
		{"trim", newTrimPreprocessor()},
		{"ltrim", newLtrimPreprocessor()},
		{"rtrim", newRtrimPreprocessor()},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				for _, v := range values {
					_ = tc.prep.Process(v)
				}
			}
		})
	}
}
