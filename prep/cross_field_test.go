package prep

import (
	"errors"
	"strings"
	"testing"
)

func TestEqFieldValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		srcValue    string
		targetValue string
		targetField string
		wantErr     bool
	}{
		{
			name:        "equal strings pass",
			srcValue:    "hello",
			targetValue: "hello",
			targetField: "Other",
			wantErr:     false,
		},
		{
			name:        "different strings fail",
			srcValue:    "hello",
			targetValue: "world",
			targetField: "Other",
			wantErr:     true,
		},
		{
			name:        "empty strings pass",
			srcValue:    "",
			targetValue: "",
			targetField: "Other",
			wantErr:     false,
		},
		{
			name:        "equal numbers as strings pass",
			srcValue:    "123",
			targetValue: "123",
			targetField: "Other",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := newEqFieldValidator(tt.targetField)
			got := v.Validate(tt.srcValue, tt.targetValue)
			if (got != "") != tt.wantErr {
				t.Errorf("eqFieldValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != eqFieldTagValue {
				t.Errorf("eqFieldValidator.Name() = %q, want %q", v.Name(), eqFieldTagValue)
			}
			if v.TargetField() != tt.targetField {
				t.Errorf("eqFieldValidator.TargetField() = %q, want %q", v.TargetField(), tt.targetField)
			}
		})
	}
}

func TestNeFieldValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		srcValue    string
		targetValue string
		targetField string
		wantErr     bool
	}{
		{
			name:        "different strings pass",
			srcValue:    "hello",
			targetValue: "world",
			targetField: "Other",
			wantErr:     false,
		},
		{
			name:        "equal strings fail",
			srcValue:    "hello",
			targetValue: "hello",
			targetField: "Other",
			wantErr:     true,
		},
		{
			name:        "empty vs non-empty pass",
			srcValue:    "",
			targetValue: "something",
			targetField: "Other",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := newNeFieldValidator(tt.targetField)
			got := v.Validate(tt.srcValue, tt.targetValue)
			if (got != "") != tt.wantErr {
				t.Errorf("neFieldValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != neFieldTagValue {
				t.Errorf("neFieldValidator.Name() = %q, want %q", v.Name(), neFieldTagValue)
			}
		})
	}
}

func TestGtFieldValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		srcValue    string
		targetValue string
		targetField string
		wantErr     bool
	}{
		{
			name:        "numeric greater than passes",
			srcValue:    "100",
			targetValue: "50",
			targetField: "Min",
			wantErr:     false,
		},
		{
			name:        "numeric equal fails",
			srcValue:    "50",
			targetValue: "50",
			targetField: "Min",
			wantErr:     true,
		},
		{
			name:        "numeric less than fails",
			srcValue:    "25",
			targetValue: "50",
			targetField: "Min",
			wantErr:     true,
		},
		{
			name:        "string comparison greater passes",
			srcValue:    "b",
			targetValue: "a",
			targetField: "Min",
			wantErr:     false,
		},
		{
			name:        "string comparison equal fails",
			srcValue:    "a",
			targetValue: "a",
			targetField: "Min",
			wantErr:     true,
		},
		{
			name:        "float greater than passes",
			srcValue:    "10.5",
			targetValue: "10.4",
			targetField: "Min",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := newGtFieldValidator(tt.targetField)
			got := v.Validate(tt.srcValue, tt.targetValue)
			if (got != "") != tt.wantErr {
				t.Errorf("gtFieldValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != gtFieldTagValue {
				t.Errorf("gtFieldValidator.Name() = %q, want %q", v.Name(), gtFieldTagValue)
			}
		})
	}
}

func TestGteFieldValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		srcValue    string
		targetValue string
		targetField string
		wantErr     bool
	}{
		{
			name:        "numeric greater than passes",
			srcValue:    "100",
			targetValue: "50",
			targetField: "Min",
			wantErr:     false,
		},
		{
			name:        "numeric equal passes",
			srcValue:    "50",
			targetValue: "50",
			targetField: "Min",
			wantErr:     false,
		},
		{
			name:        "numeric less than fails",
			srcValue:    "25",
			targetValue: "50",
			targetField: "Min",
			wantErr:     true,
		},
		{
			name:        "float equal passes",
			srcValue:    "10.5",
			targetValue: "10.5",
			targetField: "Min",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := newGteFieldValidator(tt.targetField)
			got := v.Validate(tt.srcValue, tt.targetValue)
			if (got != "") != tt.wantErr {
				t.Errorf("gteFieldValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != gteFieldTagValue {
				t.Errorf("gteFieldValidator.Name() = %q, want %q", v.Name(), gteFieldTagValue)
			}
		})
	}
}

func TestLtFieldValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		srcValue    string
		targetValue string
		targetField string
		wantErr     bool
	}{
		{
			name:        "numeric less than passes",
			srcValue:    "25",
			targetValue: "50",
			targetField: "Max",
			wantErr:     false,
		},
		{
			name:        "numeric equal fails",
			srcValue:    "50",
			targetValue: "50",
			targetField: "Max",
			wantErr:     true,
		},
		{
			name:        "numeric greater than fails",
			srcValue:    "100",
			targetValue: "50",
			targetField: "Max",
			wantErr:     true,
		},
		{
			name:        "string comparison less passes",
			srcValue:    "a",
			targetValue: "b",
			targetField: "Max",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := newLtFieldValidator(tt.targetField)
			got := v.Validate(tt.srcValue, tt.targetValue)
			if (got != "") != tt.wantErr {
				t.Errorf("ltFieldValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != ltFieldTagValue {
				t.Errorf("ltFieldValidator.Name() = %q, want %q", v.Name(), ltFieldTagValue)
			}
		})
	}
}

func TestLteFieldValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		srcValue    string
		targetValue string
		targetField string
		wantErr     bool
	}{
		{
			name:        "numeric less than passes",
			srcValue:    "25",
			targetValue: "50",
			targetField: "Max",
			wantErr:     false,
		},
		{
			name:        "numeric equal passes",
			srcValue:    "50",
			targetValue: "50",
			targetField: "Max",
			wantErr:     false,
		},
		{
			name:        "numeric greater than fails",
			srcValue:    "100",
			targetValue: "50",
			targetField: "Max",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := newLteFieldValidator(tt.targetField)
			got := v.Validate(tt.srcValue, tt.targetValue)
			if (got != "") != tt.wantErr {
				t.Errorf("lteFieldValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != lteFieldTagValue {
				t.Errorf("lteFieldValidator.Name() = %q, want %q", v.Name(), lteFieldTagValue)
			}
		})
	}
}

func TestFieldContainsValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		srcValue    string
		targetValue string
		targetField string
		wantErr     bool
	}{
		{
			name:        "contains substring passes",
			srcValue:    "hello world",
			targetValue: "world",
			targetField: "Substr",
			wantErr:     false,
		},
		{
			name:        "does not contain fails",
			srcValue:    "hello world",
			targetValue: "foo",
			targetField: "Substr",
			wantErr:     true,
		},
		{
			name:        "empty target always passes",
			srcValue:    "hello",
			targetValue: "",
			targetField: "Substr",
			wantErr:     false,
		},
		{
			name:        "exact match passes",
			srcValue:    "hello",
			targetValue: "hello",
			targetField: "Substr",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := newFieldContainsValidator(tt.targetField)
			got := v.Validate(tt.srcValue, tt.targetValue)
			if (got != "") != tt.wantErr {
				t.Errorf("fieldContainsValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != fieldContainsTagValue {
				t.Errorf("fieldContainsValidator.Name() = %q, want %q", v.Name(), fieldContainsTagValue)
			}
		})
	}
}

func TestFieldExcludesValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		srcValue    string
		targetValue string
		targetField string
		wantErr     bool
	}{
		{
			name:        "does not contain passes",
			srcValue:    "hello world",
			targetValue: "foo",
			targetField: "Forbidden",
			wantErr:     false,
		},
		{
			name:        "contains substring fails",
			srcValue:    "hello world",
			targetValue: "world",
			targetField: "Forbidden",
			wantErr:     true,
		},
		{
			name:        "empty target always fails (empty string is contained)",
			srcValue:    "hello",
			targetValue: "",
			targetField: "Forbidden",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := newFieldExcludesValidator(tt.targetField)
			got := v.Validate(tt.srcValue, tt.targetValue)
			if (got != "") != tt.wantErr {
				t.Errorf("fieldExcludesValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != fieldExcludesTagValue {
				t.Errorf("fieldExcludesValidator.Name() = %q, want %q", v.Name(), fieldExcludesTagValue)
			}
		})
	}
}

func TestRequiredIfValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		srcValue      string
		targetValue   string
		targetField   string
		expectedValue string
		wantErr       bool
	}{
		{
			name:          "required when target matches and source empty fails",
			srcValue:      "",
			targetValue:   "active",
			targetField:   "Status",
			expectedValue: "active",
			wantErr:       true,
		},
		{
			name:          "required when target matches and source present passes",
			srcValue:      "some-value",
			targetValue:   "active",
			targetField:   "Status",
			expectedValue: "active",
			wantErr:       false,
		},
		{
			name:          "not required when target does not match",
			srcValue:      "",
			targetValue:   "inactive",
			targetField:   "Status",
			expectedValue: "active",
			wantErr:       false,
		},
		{
			name:          "not required when target is empty",
			srcValue:      "",
			targetValue:   "",
			targetField:   "Status",
			expectedValue: "active",
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := newRequiredIfValidator(tt.targetField, tt.expectedValue)
			got := v.Validate(tt.srcValue, tt.targetValue)
			if (got != "") != tt.wantErr {
				t.Errorf("requiredIfValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != requiredIfTagValue {
				t.Errorf("requiredIfValidator.Name() = %q, want %q", v.Name(), requiredIfTagValue)
			}
			if v.TargetField() != tt.targetField {
				t.Errorf("requiredIfValidator.TargetField() = %q, want %q", v.TargetField(), tt.targetField)
			}
		})
	}
}

func TestRequiredUnlessValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		srcValue    string
		targetValue string
		targetField string
		exceptValue string
		wantErr     bool
	}{
		{
			name:        "required when target does not match except value and source empty fails",
			srcValue:    "",
			targetValue: "admin",
			targetField: "Role",
			exceptValue: "guest",
			wantErr:     true,
		},
		{
			name:        "required when target does not match except value and source present passes",
			srcValue:    "some-value",
			targetValue: "admin",
			targetField: "Role",
			exceptValue: "guest",
			wantErr:     false,
		},
		{
			name:        "not required when target matches except value",
			srcValue:    "",
			targetValue: "guest",
			targetField: "Role",
			exceptValue: "guest",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := newRequiredUnlessValidator(tt.targetField, tt.exceptValue)
			got := v.Validate(tt.srcValue, tt.targetValue)
			if (got != "") != tt.wantErr {
				t.Errorf("requiredUnlessValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != requiredUnlessTagValue {
				t.Errorf("requiredUnlessValidator.Name() = %q, want %q", v.Name(), requiredUnlessTagValue)
			}
			if v.TargetField() != tt.targetField {
				t.Errorf("requiredUnlessValidator.TargetField() = %q, want %q", v.TargetField(), tt.targetField)
			}
		})
	}
}

func TestRequiredWithValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		srcValue    string
		targetValue string
		targetField string
		wantErr     bool
	}{
		{
			name:        "required when target present and source empty fails",
			srcValue:    "",
			targetValue: "john@example.com",
			targetField: "Email",
			wantErr:     true,
		},
		{
			name:        "required when target present and source present passes",
			srcValue:    "John",
			targetValue: "john@example.com",
			targetField: "Email",
			wantErr:     false,
		},
		{
			name:        "not required when target absent",
			srcValue:    "",
			targetValue: "",
			targetField: "Email",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := newRequiredWithValidator(tt.targetField)
			got := v.Validate(tt.srcValue, tt.targetValue)
			if (got != "") != tt.wantErr {
				t.Errorf("requiredWithValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != requiredWithTagValue {
				t.Errorf("requiredWithValidator.Name() = %q, want %q", v.Name(), requiredWithTagValue)
			}
			if v.TargetField() != tt.targetField {
				t.Errorf("requiredWithValidator.TargetField() = %q, want %q", v.TargetField(), tt.targetField)
			}
		})
	}
}

func TestRequiredWithoutValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		srcValue    string
		targetValue string
		targetField string
		wantErr     bool
	}{
		{
			name:        "required when target absent and source empty fails",
			srcValue:    "",
			targetValue: "",
			targetField: "Phone",
			wantErr:     true,
		},
		{
			name:        "required when target absent and source present passes",
			srcValue:    "john@example.com",
			targetValue: "",
			targetField: "Phone",
			wantErr:     false,
		},
		{
			name:        "not required when target present",
			srcValue:    "",
			targetValue: "555-1234",
			targetField: "Phone",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := newRequiredWithoutValidator(tt.targetField)
			got := v.Validate(tt.srcValue, tt.targetValue)
			if (got != "") != tt.wantErr {
				t.Errorf("requiredWithoutValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != requiredWithoutTagValue {
				t.Errorf("requiredWithoutValidator.Name() = %q, want %q", v.Name(), requiredWithoutTagValue)
			}
			if v.TargetField() != tt.targetField {
				t.Errorf("requiredWithoutValidator.TargetField() = %q, want %q", v.TargetField(), tt.targetField)
			}
		})
	}
}

func TestConditionalCrossFieldValidation_Processor(t *testing.T) {
	t.Parallel()

	type RequiredIfRecord struct {
		Status  string
		Details string `validate:"required_if=Status active"`
	}

	t.Run("required_if triggers when condition met", func(t *testing.T) {
		t.Parallel()
		csvData := "status,details\nactive,\n"
		var records []RequiredIfRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(result.Errors) != 1 {
			t.Errorf("expected 1 error, got %d: %v", len(result.Errors), result.Errors)
		}
	})

	t.Run("required_if passes when condition met and value present", func(t *testing.T) {
		t.Parallel()
		csvData := "status,details\nactive,some details\n"
		var records []RequiredIfRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(result.Errors) != 0 {
			t.Errorf("expected 0 errors, got %d: %v", len(result.Errors), result.Errors)
		}
	})

	t.Run("required_if does not trigger when condition not met", func(t *testing.T) {
		t.Parallel()
		csvData := "status,details\ninactive,\n"
		var records []RequiredIfRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(result.Errors) != 0 {
			t.Errorf("expected 0 errors, got %d: %v", len(result.Errors), result.Errors)
		}
	})

	type RequiredUnlessRecord struct {
		Type    string
		Profile string `validate:"required_unless=Type guest"`
	}

	t.Run("required_unless triggers when condition not met", func(t *testing.T) {
		t.Parallel()
		csvData := "type,profile\nadmin,\n"
		var records []RequiredUnlessRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(result.Errors) != 1 {
			t.Errorf("expected 1 error, got %d: %v", len(result.Errors), result.Errors)
		}
	})

	t.Run("required_unless passes when except value matches", func(t *testing.T) {
		t.Parallel()
		csvData := "type,profile\nguest,\n"
		var records []RequiredUnlessRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(result.Errors) != 0 {
			t.Errorf("expected 0 errors, got %d: %v", len(result.Errors), result.Errors)
		}
	})

	type RequiredWithRecord struct {
		Email string
		Name  string `validate:"required_with=Email"`
	}

	t.Run("required_with triggers when target present", func(t *testing.T) {
		t.Parallel()
		csvData := "email,name\njohn@example.com,\n"
		var records []RequiredWithRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(result.Errors) != 1 {
			t.Errorf("expected 1 error, got %d: %v", len(result.Errors), result.Errors)
		}
	})

	t.Run("required_with passes when target absent", func(t *testing.T) {
		t.Parallel()
		csvData := "email,name\n,\n"
		var records []RequiredWithRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(result.Errors) != 0 {
			t.Errorf("expected 0 errors, got %d: %v", len(result.Errors), result.Errors)
		}
	})

	type RequiredWithoutRecord struct {
		Phone string
		Email string `validate:"required_without=Phone"`
	}

	t.Run("required_without triggers when target absent", func(t *testing.T) {
		t.Parallel()
		csvData := "phone,email\n,\n"
		var records []RequiredWithoutRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(result.Errors) != 1 {
			t.Errorf("expected 1 error, got %d: %v", len(result.Errors), result.Errors)
		}
	})

	t.Run("required_without passes when target present", func(t *testing.T) {
		t.Parallel()
		csvData := "phone,email\n555-1234,\n"
		var records []RequiredWithoutRecord

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(result.Errors) != 0 {
			t.Errorf("expected 0 errors, got %d: %v", len(result.Errors), result.Errors)
		}
	})
}

func TestCrossFieldValidation_Integration(t *testing.T) {
	t.Parallel()

	// Test parsing cross-field validators
	t.Run("parse cross-field validators", func(t *testing.T) {
		t.Parallel()
		vals, crossVals, err := parseValidateTag("gtfield=MaxPrice", false)
		if err != nil {
			t.Fatalf("parseValidateTag() error = %v", err)
		}
		if len(vals) != 0 {
			t.Errorf("expected 0 validators, got %d", len(vals))
		}
		if len(crossVals) != 1 {
			t.Errorf("expected 1 cross-field validator, got %d", len(crossVals))
		}
		if len(crossVals) > 0 {
			if crossVals[0].Name() != gtFieldTagValue {
				t.Errorf("expected validator name %q, got %q", gtFieldTagValue, crossVals[0].Name())
			}
			if crossVals[0].TargetField() != "MaxPrice" {
				t.Errorf("expected target field %q, got %q", "MaxPrice", crossVals[0].TargetField())
			}
		}
	})

	// Test multiple cross-field validators
	t.Run("parse multiple cross-field validators", func(t *testing.T) {
		t.Parallel()
		vals, crossVals, err := parseValidateTag("required,eqfield=Other,nefield=Another", false)
		if err != nil {
			t.Fatalf("parseValidateTag() error = %v", err)
		}
		if len(vals) != 1 {
			t.Errorf("expected 1 validator, got %d", len(vals))
		}
		if len(crossVals) != 2 {
			t.Errorf("expected 2 cross-field validators, got %d", len(crossVals))
		}
	})

	// Test all cross-field validator types are parsed
	t.Run("parse all cross-field validator types", func(t *testing.T) {
		t.Parallel()
		testCases := []struct {
			tag      string
			expected string
		}{
			{"eqfield=X", eqFieldTagValue},
			{"nefield=X", neFieldTagValue},
			{"gtfield=X", gtFieldTagValue},
			{"gtefield=X", gteFieldTagValue},
			{"ltfield=X", ltFieldTagValue},
			{"ltefield=X", lteFieldTagValue},
			{"fieldcontains=X", fieldContainsTagValue},
			{"fieldexcludes=X", fieldExcludesTagValue},
		}

		for _, tc := range testCases {
			_, crossVals, err := parseValidateTag(tc.tag, false)
			if err != nil {
				t.Errorf("tag %q: parseValidateTag() error = %v", tc.tag, err)
				continue
			}
			if len(crossVals) != 1 {
				t.Errorf("tag %q: expected 1 cross-field validator, got %d", tc.tag, len(crossVals))
				continue
			}
			if crossVals[0].Name() != tc.expected {
				t.Errorf("tag %q: expected validator name %q, got %q", tc.tag, tc.expected, crossVals[0].Name())
			}
		}
	})
}

func TestCrossFieldValidation_Processor(t *testing.T) {
	t.Parallel()

	type DateRange struct {
		StartDate string `validate:"ltfield=EndDate"`
		EndDate   string
	}

	t.Run("cross-field validation passes when condition met", func(t *testing.T) {
		t.Parallel()
		csvData := "start_date,end_date\n2024-01-01,2024-12-31\n"
		var records []DateRange

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(result.Errors) != 0 {
			t.Errorf("expected 0 errors, got %d: %v", len(result.Errors), result.Errors)
		}
		if result.ValidRowCount != 1 {
			t.Errorf("expected 1 valid row, got %d", result.ValidRowCount)
		}
	})

	t.Run("cross-field validation fails when condition not met", func(t *testing.T) {
		t.Parallel()
		// StartDate should be less than EndDate, but here StartDate > EndDate
		csvData := "start_date,end_date\n2024-12-31,2024-01-01\n"
		var records []DateRange

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(result.Errors) != 1 {
			t.Errorf("expected 1 error, got %d: %v", len(result.Errors), result.Errors)
		}
		if result.ValidRowCount != 0 {
			t.Errorf("expected 0 valid rows, got %d", result.ValidRowCount)
		}
	})

	type Password struct {
		Password        string `validate:"eqfield=ConfirmPassword"`
		ConfirmPassword string
	}

	t.Run("password confirmation validation", func(t *testing.T) {
		t.Parallel()
		csvData := "password,confirm_password\nsecret123,secret123\n"
		var records []Password

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(result.Errors) != 0 {
			t.Errorf("expected 0 errors, got %d: %v", len(result.Errors), result.Errors)
		}
	})

	t.Run("password confirmation mismatch", func(t *testing.T) {
		t.Parallel()
		csvData := "password,confirm_password\nsecret123,different\n"
		var records []Password

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(result.Errors) != 1 {
			t.Errorf("expected 1 error, got %d: %v", len(result.Errors), result.Errors)
		}
	})

	type NumericRange struct {
		Min string `validate:"ltefield=Max"`
		Max string `validate:"gtefield=Min"`
	}

	t.Run("numeric range validation with both directions", func(t *testing.T) {
		t.Parallel()
		csvData := "min,max\n10,100\n"
		var records []NumericRange

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(result.Errors) != 0 {
			t.Errorf("expected 0 errors, got %d: %v", len(result.Errors), result.Errors)
		}
	})

	type InvalidTarget struct {
		Value string `validate:"eqfield=NonExistent"`
	}

	t.Run("cross-field validation with non-existent target field", func(t *testing.T) {
		t.Parallel()
		csvData := "value\ntest\n"
		var records []InvalidTarget

		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(result.Errors) != 1 {
			t.Errorf("expected 1 error for non-existent field, got %d: %v", len(result.Errors), result.Errors)
		}
	})

	type MissingSrcField struct {
		SrcField    string `validate:"eqfield=TargetField"`
		TargetField string
	}

	t.Run("a field whose column is missing is refused rather than treated as empty", func(t *testing.T) {
		t.Parallel()
		// This used to produce srcValue="" and let the comparison run, which
		// cannot be told apart from a row whose cell really is empty. Refusing
		// says which of the two it is; a field that is meant to work without a
		// column carries prep:"default=..." and is still accepted.
		csvData := "target_field\nhello\n"
		var records []MissingSrcField

		processor := NewProcessor(FileTypeCSV)
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err == nil {
			t.Fatal("Process accepted a struct whose SrcField matches no column")
		}
		if !errors.Is(err, ErrUnknownColumn) {
			t.Fatalf("err = %v, want ErrUnknownColumn", err)
		}
	})
}
