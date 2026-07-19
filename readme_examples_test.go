package filesql_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/nao1215/filesql"
	"github.com/nao1215/filesql/frame"
	"github.com/nao1215/filesql/prep"
)

func TestReadmePrepExample(t *testing.T) {
	t.Parallel()

	type user struct {
		Name  string `prep:"trim" validate:"required"`
		Email string `prep:"trim,lowercase" validate:"required,email"`
		Role  string `prep:"trim,uppercase" validate:"required,oneof=ADMIN USER"`
	}

	csvData := `name,email,role
  Alice  ,ALICE@EXAMPLE.COM, admin
Bob,bob@example.com,user
`

	processor := prep.NewProcessor(prep.FileTypeCSV)
	var users []user

	reader, result, err := processor.Process(strings.NewReader(csvData), &users)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if result.HasErrors() {
		t.Fatalf("Process() returned validation errors: %v", result.ValidationErrors())
	}
	if len(users) != 2 {
		t.Fatalf("len(users) = %d, want 2", len(users))
	}
	if users[0].Name != "Alice" || users[0].Email != "alice@example.com" || users[0].Role != "ADMIN" {
		t.Fatalf("users[0] = %#v, want normalized values", users[0])
	}

	cleaned, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	wantCleaned := "name,email,role\nAlice,alice@example.com,ADMIN\nBob,bob@example.com,USER\n"
	if string(cleaned) != wantCleaned {
		t.Fatalf("cleaned output = %q, want %q", string(cleaned), wantCleaned)
	}

	ctx := context.Background()
	validatedBuilder, err := filesql.NewBuilder().
		AddReader(strings.NewReader(string(cleaned)), "users", filesql.FileTypeCSV).
		Build(ctx)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	var role string
	if err := db.QueryRowContext(ctx, "SELECT role FROM users WHERE name = 'Alice'").Scan(&role); err != nil {
		t.Fatalf("QueryRowContext() error = %v", err)
	}
	if role != "ADMIN" {
		t.Fatalf("role = %q, want %q", role, "ADMIN")
	}
}

func TestReadmeFrameExample(t *testing.T) {
	t.Parallel()

	csvData := `region,product,qty,price
north,apple,2,100
south,apple,1,100
north,orange,3,80
north,apple,1,100
`

	df, err := frame.NewDataFrame(strings.NewReader(csvData), frame.CSV)
	if err != nil {
		t.Fatalf("NewDataFrame() error = %v", err)
	}

	sales := df.Mutate("revenue", func(row map[string]any) any {
		qty, ok := row["qty"].(int64)
		if !ok {
			t.Fatalf("row[qty] has type %T, want int64", row["qty"])
		}
		price, ok := row["price"].(int64)
		if !ok {
			t.Fatalf("row[price] has type %T, want int64", row["price"])
		}
		return qty * price
	})

	northOnly := sales.Filter(func(row map[string]any) bool {
		region, ok := row["region"].(string)
		if !ok {
			t.Fatalf("row[region] has type %T, want string", row["region"])
		}
		return region == "north"
	})

	grouped, err := northOnly.GroupBy("product")
	if err != nil {
		t.Fatalf("GroupBy() error = %v", err)
	}

	summary, err := grouped.Sum("revenue")
	if err != nil {
		t.Fatalf("Sum() error = %v", err)
	}

	records := summary.ToRecords()
	if len(records) != 2 {
		t.Fatalf("len(records) = %d, want 2", len(records))
	}

	if got := records[0]["product"]; got != "apple" {
		t.Fatalf("records[0][product] = %v, want apple", got)
	}
	if got := records[0]["sum_revenue"]; got != float64(300) {
		t.Fatalf("records[0][sum_revenue] = %v, want 300", got)
	}
	if got := records[1]["product"]; got != "orange" {
		t.Fatalf("records[1][product] = %v, want orange", got)
	}
	if got := records[1]["sum_revenue"]; got != float64(240) {
		t.Fatalf("records[1][sum_revenue] = %v, want 240", got)
	}
}
