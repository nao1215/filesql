package prep

import (
	"strings"
	"testing"
)

func TestValidationError_Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     *ValidationError
		wantRow int
		wantCol string
	}{
		{
			name: "basic error",
			err: &ValidationError{
				Row:     1,
				Column:  "email",
				Field:   "Email",
				Value:   "invalid",
				Tag:     "email",
				Message: "must be a valid email",
			},
			wantRow: 1,
			wantCol: "email",
		},
		{
			name: "empty value",
			err: &ValidationError{
				Row:     5,
				Column:  "name",
				Field:   "Name",
				Value:   "",
				Tag:     "required",
				Message: "field is required",
			},
			wantRow: 5,
			wantCol: "name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			errStr := tt.err.Error()
			if !strings.Contains(errStr, "row") {
				t.Error("error should contain 'row'")
			}
			if !strings.Contains(errStr, tt.wantCol) {
				t.Errorf("error should contain column %q", tt.wantCol)
			}
		})
	}
}

func Test_newValidationError(t *testing.T) {
	t.Parallel()

	err := newValidationError(1, "email", "Email", "invalid", "email", "must be a valid email")

	if err.Row != 1 {
		t.Errorf("Row = %d, want 1", err.Row)
	}
	if err.Column != "email" {
		t.Errorf("Column = %q, want %q", err.Column, "email")
	}
	if err.Field != "Email" {
		t.Errorf("Field = %q, want %q", err.Field, "Email")
	}
	if err.Value != "invalid" {
		t.Errorf("Value = %q, want %q", err.Value, "invalid")
	}
	if err.Tag != "email" {
		t.Errorf("Tag = %q, want %q", err.Tag, "email")
	}
	if err.Message != "must be a valid email" {
		t.Errorf("Message = %q, want %q", err.Message, "must be a valid email")
	}
}

func TestPrepError_Error(t *testing.T) {
	t.Parallel()

	err := &PrepError{
		Row:     2,
		Column:  "value",
		Field:   "Value",
		Tag:     "truncate",
		Message: "truncation failed",
	}

	errStr := err.Error()
	if !strings.Contains(errStr, "row 2") {
		t.Error("error should contain 'row 2'")
	}
	if !strings.Contains(errStr, "value") {
		t.Error("error should contain 'value'")
	}
	if !strings.Contains(errStr, "prep error") {
		t.Error("error should contain 'prep error'")
	}
}

func Test_newPrepError(t *testing.T) {
	t.Parallel()

	err := newPrepError(3, "data", "Data", "regex_replace", "invalid regex pattern")

	if err.Row != 3 {
		t.Errorf("Row = %d, want 3", err.Row)
	}
	if err.Column != "data" {
		t.Errorf("Column = %q, want %q", err.Column, "data")
	}
	if err.Field != "Data" {
		t.Errorf("Field = %q, want %q", err.Field, "Data")
	}
	if err.Tag != "regex_replace" {
		t.Errorf("Tag = %q, want %q", err.Tag, "regex_replace")
	}
	if err.Message != "invalid regex pattern" {
		t.Errorf("Message = %q, want %q", err.Message, "invalid regex pattern")
	}
}

func TestProcessResult_InvalidRowCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		rowCount      int
		validRowCount int
		want          int
	}{
		{"all valid", 10, 10, 0},
		{"some invalid", 10, 7, 3},
		{"all invalid", 10, 0, 10},
		{"empty", 0, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &ProcessResult{
				RowCount:      tt.rowCount,
				ValidRowCount: tt.validRowCount,
			}
			if got := r.InvalidRowCount(); got != tt.want {
				t.Errorf("InvalidRowCount() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestProcessResult_HasErrors(t *testing.T) {
	t.Parallel()

	t.Run("no errors", func(t *testing.T) {
		t.Parallel()
		r := &ProcessResult{}
		if r.HasErrors() {
			t.Error("HasErrors() should return false for empty errors")
		}
	})

	t.Run("with errors", func(t *testing.T) {
		t.Parallel()
		r := &ProcessResult{
			Errors: []error{newValidationError(1, "col", "Field", "val", "tag", "msg")},
		}
		if !r.HasErrors() {
			t.Error("HasErrors() should return true when errors exist")
		}
	})
}

func TestProcessResult_ValidationErrors(t *testing.T) {
	t.Parallel()

	ve1 := newValidationError(1, "col1", "Field1", "val1", "tag1", "msg1")
	ve2 := newValidationError(2, "col2", "Field2", "val2", "tag2", "msg2")
	pe1 := newPrepError(3, "col3", "Field3", "tag3", "msg3")

	r := &ProcessResult{
		Errors: []error{ve1, pe1, ve2},
	}

	validationErrors := r.ValidationErrors()
	if len(validationErrors) != 2 {
		t.Errorf("ValidationErrors() returned %d errors, want 2", len(validationErrors))
	}
}

func TestProcessResult_PrepErrors(t *testing.T) {
	t.Parallel()

	ve1 := newValidationError(1, "col1", "Field1", "val1", "tag1", "msg1")
	pe1 := newPrepError(2, "col2", "Field2", "tag2", "msg2")
	pe2 := newPrepError(3, "col3", "Field3", "tag3", "msg3")

	r := &ProcessResult{
		Errors: []error{ve1, pe1, pe2},
	}

	prepErrors := r.PrepErrors()
	if len(prepErrors) != 2 {
		t.Errorf("PrepErrors() returned %d errors, want 2", len(prepErrors))
	}
}
