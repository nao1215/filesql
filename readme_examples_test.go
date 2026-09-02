package filesql_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/nao1215/filesql"
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
	db, err := filesql.NewBuilder().
		AddReader(strings.NewReader(string(cleaned)), "users", filesql.FileTypeCSV).
		Open(ctx)
	if err != nil {
		t.Fatalf("Open(context.Background(), ) error = %v", err)
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
