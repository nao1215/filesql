package prep

import (
	"errors"
	"strconv"
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
			got := v.Validate(tt.srcValue, []string{tt.targetValue})
			if (got != "") != tt.wantErr {
				t.Errorf("eqFieldValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != eqFieldTagValue {
				t.Errorf("eqFieldValidator.Name() = %q, want %q", v.Name(), eqFieldTagValue)
			}
			if v.TargetFields()[0] != tt.targetField {
				t.Errorf("eqFieldValidator.TargetField() = %q, want %q", v.TargetFields()[0], tt.targetField)
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
			got := v.Validate(tt.srcValue, []string{tt.targetValue})
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
			got := v.Validate(tt.srcValue, []string{tt.targetValue})
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
			got := v.Validate(tt.srcValue, []string{tt.targetValue})
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
			got := v.Validate(tt.srcValue, []string{tt.targetValue})
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
			got := v.Validate(tt.srcValue, []string{tt.targetValue})
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
			got := v.Validate(tt.srcValue, []string{tt.targetValue})
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
			got := v.Validate(tt.srcValue, []string{tt.targetValue})
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
			v := newRequiredIfValidator([]fieldCondition{{field: tt.targetField, expected: tt.expectedValue}})
			got := v.Validate(tt.srcValue, []string{tt.targetValue})
			if (got != "") != tt.wantErr {
				t.Errorf("requiredIfValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != requiredIfTagValue {
				t.Errorf("requiredIfValidator.Name() = %q, want %q", v.Name(), requiredIfTagValue)
			}
			if v.TargetFields()[0] != tt.targetField {
				t.Errorf("requiredIfValidator.TargetField() = %q, want %q", v.TargetFields()[0], tt.targetField)
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
			v := newRequiredUnlessValidator([]fieldCondition{{field: tt.targetField, expected: tt.exceptValue}})
			got := v.Validate(tt.srcValue, []string{tt.targetValue})
			if (got != "") != tt.wantErr {
				t.Errorf("requiredUnlessValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != requiredUnlessTagValue {
				t.Errorf("requiredUnlessValidator.Name() = %q, want %q", v.Name(), requiredUnlessTagValue)
			}
			if v.TargetFields()[0] != tt.targetField {
				t.Errorf("requiredUnlessValidator.TargetField() = %q, want %q", v.TargetFields()[0], tt.targetField)
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
			v := newRequiredWithValidator([]string{tt.targetField}, false)
			got := v.Validate(tt.srcValue, []string{tt.targetValue})
			if (got != "") != tt.wantErr {
				t.Errorf("requiredWithValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != requiredWithTagValue {
				t.Errorf("requiredWithValidator.Name() = %q, want %q", v.Name(), requiredWithTagValue)
			}
			if v.TargetFields()[0] != tt.targetField {
				t.Errorf("requiredWithValidator.TargetField() = %q, want %q", v.TargetFields()[0], tt.targetField)
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
			v := newRequiredWithoutValidator([]string{tt.targetField}, false)
			got := v.Validate(tt.srcValue, []string{tt.targetValue})
			if (got != "") != tt.wantErr {
				t.Errorf("requiredWithoutValidator.Validate() = %q, wantErr %v", got, tt.wantErr)
			}
			if v.Name() != requiredWithoutTagValue {
				t.Errorf("requiredWithoutValidator.Name() = %q, want %q", v.Name(), requiredWithoutTagValue)
			}
			if v.TargetFields()[0] != tt.targetField {
				t.Errorf("requiredWithoutValidator.TargetField() = %q, want %q", v.TargetFields()[0], tt.targetField)
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
			if crossVals[0].TargetFields()[0] != "MaxPrice" {
				t.Errorf("expected target field %q, got %q", "MaxPrice", crossVals[0].TargetFields()[0])
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

// crossFieldHolds reports whether one CSV row of two cells passes the
// cross-field tag the target's first field carries.
func crossFieldHolds(t *testing.T, target any, a, b string) bool {
	t.Helper()
	_, result, err := NewProcessor(FileTypeCSV).Process(
		strings.NewReader("a,b\n"+a+","+b+"\n"), target)
	if err != nil {
		t.Fatalf("Process(%q, %q) error = %v", a, b, err)
	}
	return len(result.Errors) == 0
}

// TestCrossFieldComparisonsFollowTheField pins that a cross-field comparison
// means on each kind of field what the single-field comparison of the same name
// means: the string itself on a string field, the number on any other.
//
// The two families used to disagree with each other. The four ordering tags
// parsed both cells as numbers before falling back to text and the two equality
// tags never did, so a pair could be neither greater than, nor equal to, nor
// less than its target: "007" against "7" answered false to all three.
func TestCrossFieldComparisonsFollowTheField(t *testing.T) {
	t.Parallel()

	type strGt struct {
		A string `validate:"gtfield=B"`
		B string
	}
	type strEq struct {
		A string `validate:"eqfield=B"`
		B string
	}
	type strNe struct {
		A string `validate:"nefield=B"`
		B string
	}
	type strLt struct {
		A string `validate:"ltfield=B"`
		B string
	}
	type numGt struct {
		A float64 `validate:"gtfield=B"`
		B float64
	}
	type numEq struct {
		A float64 `validate:"eqfield=B"`
		B float64
	}
	type numLt struct {
		A float64 `validate:"ltfield=B"`
		B float64
	}
	type intGt struct {
		A int `validate:"gtfield=B"`
		B int
	}
	type intEq struct {
		A int `validate:"eqfield=B"`
		B int
	}
	type intNe struct {
		A int `validate:"nefield=B"`
		B int
	}

	t.Run("exactly one of greater, equal and less holds", func(t *testing.T) {
		t.Parallel()
		pairs := [][2]string{
			{"007", "7"}, {"1.0", "1"}, {"0", "-0"}, {"1e3", "1000"},
			{"abc", "abc"}, {"abc", "abd"}, {"10", "9"}, {"9", "10"},
		}
		for _, pair := range pairs {
			a, b := pair[0], pair[1]
			for _, kind := range []struct {
				name       string
				gt, eq, lt any
			}{
				{"string fields", &[]strGt{}, &[]strEq{}, &[]strLt{}},
				{"numeric fields", &[]numGt{}, &[]numEq{}, &[]numLt{}},
			} {
				if kind.name == "numeric fields" && !allNumeric(a, b) {
					continue
				}
				held := 0
				for _, target := range []any{kind.gt, kind.eq, kind.lt} {
					if crossFieldHolds(t, target, a, b) {
						held++
					}
				}
				if held != 1 {
					t.Errorf("%s (%q, %q): %d of gtfield, eqfield, ltfield hold, want exactly 1", kind.name, a, b, held)
				}
			}
		}
	})

	t.Run("a numeric field compares the number", func(t *testing.T) {
		t.Parallel()
		if !crossFieldHolds(t, &[]intEq{}, "007", "7") {
			t.Error("eqfield: 007 and 7 are the same number")
		}
		if crossFieldHolds(t, &[]intNe{}, "007", "7") {
			t.Error("nefield: 007 and 7 are the same number")
		}
		if !crossFieldHolds(t, &[]intEq{}, "0", "-0") {
			t.Error("eqfield: 0 and -0 are the same number")
		}
		if !crossFieldHolds(t, &[]intGt{}, "10", "9") {
			t.Error("gtfield: 10 is greater than 9")
		}
	})

	t.Run("a string field compares the string", func(t *testing.T) {
		t.Parallel()
		if !crossFieldHolds(t, &[]strLt{}, "007", "7") {
			t.Error("ltfield: 007 sorts before 7")
		}
		if crossFieldHolds(t, &[]strEq{}, "1.0", "1") {
			t.Error("eqfield: 1.0 and 1 are two different strings, which is what a password confirmation needs")
		}
		if !crossFieldHolds(t, &[]strNe{}, "1.0", "1") {
			t.Error("nefield: 1.0 and 1 are two different strings")
		}
		// The date range this package is asked for most often, which the
		// character-count rule of the reference dialect cannot express: both
		// dates are ten characters.
		if !crossFieldHolds(t, &[]strLt{}, "2024-01-01", "2024-12-31") {
			t.Error("ltfield: a forward date range must pass")
		}
		if crossFieldHolds(t, &[]strLt{}, "2024-12-31", "2024-01-01") {
			t.Error("ltfield: a backward date range must fail")
		}
	})
}

// allNumeric reports whether both cells can be read as numbers, so a pair that
// only a string column can hold is not fed to a numeric one.
func allNumeric(values ...string) bool {
	for _, v := range values {
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			return false
		}
	}
	return true
}

// crossFieldRow reports whether one CSV row passes the cross-field tags the
// target's fields carry. The columns are named a, b, c ... in order, so a
// struct field named A carries the tag under test and B and C are its targets.
func crossFieldRow(t *testing.T, target any, cells ...string) bool {
	t.Helper()
	headers := make([]string, len(cells))
	for i := range cells {
		headers[i] = string(rune('a' + i))
	}
	csv := strings.Join(headers, ",") + "\n" + strings.Join(cells, ",") + "\n"
	_, result, err := NewProcessor(FileTypeCSV).Process(strings.NewReader(csv), target)
	if err != nil {
		t.Fatalf("Process(%q) error = %v", csv, err)
	}
	return len(result.Errors) == 0
}

// TestConditionalRequiredReadsEveryPair pins that required_if and
// required_unless read their parameter as a list of field and value pairs, the
// way the dialect this package follows defines them. Both used to keep the
// first field name and fold the rest of the parameter into the expected value,
// so a tag naming two pairs compared one cell against a string no cell holds
// and the field was silently optional in every row.
func TestConditionalRequiredReadsEveryPair(t *testing.T) {
	t.Parallel()

	type ifTwo struct {
		A string `validate:"required_if=B paid C gold"`
		B string
		C string
	}
	type unlessTwo struct {
		A string `validate:"required_unless=B free C trial"`
		B string
		C string
	}
	type ifQuoted struct {
		A string `validate:"required_if=B 'on hold'"`
		B string
	}

	t.Run("required_if demands a value only when every pair matches", func(t *testing.T) {
		t.Parallel()
		if crossFieldRow(t, &[]ifTwo{}, "", "paid", "gold") {
			t.Error("both pairs match and the cell is empty, so the row must be invalid")
		}
		if !crossFieldRow(t, &[]ifTwo{}, "x", "paid", "gold") {
			t.Error("both pairs match and the cell is filled, so the row must be valid")
		}
		if !crossFieldRow(t, &[]ifTwo{}, "", "paid", "silver") {
			t.Error("only the first pair matches, so the empty cell must be allowed")
		}
		if !crossFieldRow(t, &[]ifTwo{}, "", "free", "gold") {
			t.Error("only the second pair matches, so the empty cell must be allowed")
		}
	})

	t.Run("required_unless allows an empty cell as soon as one pair matches", func(t *testing.T) {
		t.Parallel()
		if crossFieldRow(t, &[]unlessTwo{}, "", "paid", "gold") {
			t.Error("no pair matches and the cell is empty, so the row must be invalid")
		}
		if !crossFieldRow(t, &[]unlessTwo{}, "", "free", "gold") {
			t.Error("the first pair matches, so the empty cell must be allowed")
		}
		if !crossFieldRow(t, &[]unlessTwo{}, "", "paid", "trial") {
			t.Error("the second pair matches, so the empty cell must be allowed")
		}
	})

	t.Run("a quoted expected value keeps its spaces and loses its quotes", func(t *testing.T) {
		t.Parallel()
		if crossFieldRow(t, &[]ifQuoted{}, "", "on hold") {
			t.Error("the cell holds the quoted value, so the empty cell must be refused")
		}
		if !crossFieldRow(t, &[]ifQuoted{}, "", "'on hold'") {
			t.Error("a cell holding the quotes themselves is a different value and must not match")
		}
		if !crossFieldRow(t, &[]ifQuoted{}, "", "active") {
			t.Error("a cell holding another value must leave the empty cell alone")
		}
	})
}

// TestConditionalRequiredReadsEveryField pins that the required_with and
// required_without families read their parameter as a list of field names.
// Both used to take the whole parameter as one field name, so a correctly
// spelled list was reported as a missing field on every row.
func TestConditionalRequiredReadsEveryField(t *testing.T) {
	t.Parallel()

	type withAny struct {
		A string `validate:"required_with=B C"`
		B string
		C string
	}
	type withAll struct {
		A string `validate:"required_with_all=B C"`
		B string
		C string
	}
	type withoutAny struct {
		A string `validate:"required_without=B C"`
		B string
		C string
	}
	type withoutAll struct {
		A string `validate:"required_without_all=B C"`
		B string
		C string
	}
	type misspelled struct {
		A string `validate:"required_with=Nowhere"`
		B string
	}

	t.Run("required_with fires when any named field carries a value", func(t *testing.T) {
		t.Parallel()
		if !crossFieldRow(t, &[]withAny{}, "", "", "") {
			t.Error("no target carries a value, so the empty cell must be allowed")
		}
		if crossFieldRow(t, &[]withAny{}, "", "street", "") {
			t.Error("the first target carries a value, so the empty cell must be refused")
		}
		if crossFieldRow(t, &[]withAny{}, "", "", "zip") {
			t.Error("the second target carries a value, so the empty cell must be refused")
		}
		if !crossFieldRow(t, &[]withAny{}, "kyoto", "street", "zip") {
			t.Error("the cell is filled, so the row must be valid")
		}
	})

	t.Run("required_with_all fires only when every named field carries a value", func(t *testing.T) {
		t.Parallel()
		if !crossFieldRow(t, &[]withAll{}, "", "street", "") {
			t.Error("only one target carries a value, so the empty cell must be allowed")
		}
		if crossFieldRow(t, &[]withAll{}, "", "street", "zip") {
			t.Error("both targets carry a value, so the empty cell must be refused")
		}
	})

	t.Run("required_without fires when any named field is empty", func(t *testing.T) {
		t.Parallel()
		if !crossFieldRow(t, &[]withoutAny{}, "", "street", "zip") {
			t.Error("every target carries a value, so the empty cell must be allowed")
		}
		if crossFieldRow(t, &[]withoutAny{}, "", "street", "") {
			t.Error("one target is empty, so the empty cell must be refused")
		}
	})

	t.Run("required_without_all fires only when every named field is empty", func(t *testing.T) {
		t.Parallel()
		if !crossFieldRow(t, &[]withoutAll{}, "", "street", "") {
			t.Error("one target carries a value, so the empty cell must be allowed")
		}
		if crossFieldRow(t, &[]withoutAll{}, "", "", "") {
			t.Error("every target is empty, so the empty cell must be refused")
		}
	})

	t.Run("a field name no struct field answers to is still reported", func(t *testing.T) {
		t.Parallel()
		if crossFieldRow(t, &[]misspelled{}, "kyoto", "street") {
			t.Error("a misspelled field name must be reported, which is the only report it gets")
		}
	})
}

// TestCrossFieldComparisonsPassAnEmptyCell pins that the comparison and
// substring tags follow the package rule that an empty cell passes every
// validator but required, which the single-field validators already follow.
// They used to compare an empty cell against a filled one, so a column that
// was optional by every other measure was reported invalid on every row that
// left it blank.
func TestCrossFieldComparisonsPassAnEmptyCell(t *testing.T) {
	t.Parallel()

	type eq struct {
		A string `validate:"eqfield=B"`
		B string
	}
	type ne struct {
		A string `validate:"nefield=B"`
		B string
	}
	type gt struct {
		A string `validate:"gtfield=B"`
		B string
	}
	type gte struct {
		A string `validate:"gtefield=B"`
		B string
	}
	type lt struct {
		A string `validate:"ltfield=B"`
		B string
	}
	type lte struct {
		A string `validate:"ltefield=B"`
		B string
	}
	type fcontains struct {
		A string `validate:"fieldcontains=B"`
		B string
	}
	type fexcludes struct {
		A string `validate:"fieldexcludes=B"`
		B string
	}

	tags := []struct {
		name string
		make func() any
	}{
		{"eqfield", func() any { return &[]eq{} }},
		{"nefield", func() any { return &[]ne{} }},
		{"gtfield", func() any { return &[]gt{} }},
		{"gtefield", func() any { return &[]gte{} }},
		{"ltfield", func() any { return &[]lt{} }},
		{"ltefield", func() any { return &[]lte{} }},
		{"fieldcontains", func() any { return &[]fcontains{} }},
		{"fieldexcludes", func() any { return &[]fexcludes{} }},
	}

	t.Run("a missing cell on either side is not compared", func(t *testing.T) {
		t.Parallel()
		for _, tag := range tags {
			if !crossFieldRow(t, tag.make(), "", "b") {
				t.Errorf("%s: an empty source cell must pass", tag.name)
			}
			if !crossFieldRow(t, tag.make(), "a", "") {
				t.Errorf("%s: an empty target cell must pass", tag.name)
			}
			if !crossFieldRow(t, tag.make(), "", "") {
				t.Errorf("%s: two empty cells must pass", tag.name)
			}
		}
	})

	t.Run("two filled cells are still compared", func(t *testing.T) {
		t.Parallel()
		if crossFieldRow(t, &[]eq{}, "a", "b") {
			t.Error("eqfield: two different strings must fail")
		}
		if crossFieldRow(t, &[]ne{}, "a", "a") {
			t.Error("nefield: two equal strings must fail")
		}
		if crossFieldRow(t, &[]gt{}, "a", "b") {
			t.Error("gtfield: a sorts before b")
		}
		if crossFieldRow(t, &[]gte{}, "a", "b") {
			t.Error("gtefield: a sorts before b")
		}
		if crossFieldRow(t, &[]lt{}, "b", "a") {
			t.Error("ltfield: b sorts after a")
		}
		if crossFieldRow(t, &[]lte{}, "b", "a") {
			t.Error("ltefield: b sorts after a")
		}
		if crossFieldRow(t, &[]fcontains{}, "abc", "zz") {
			t.Error("fieldcontains: abc does not contain zz")
		}
		if crossFieldRow(t, &[]fexcludes{}, "abc", "bc") {
			t.Error("fieldexcludes: abc contains bc")
		}
	})

	t.Run("the tags that decide presence still run on an empty cell", func(t *testing.T) {
		t.Parallel()
		type reqIf struct {
			A string `validate:"required_if=B paid"`
			B string
		}
		type reqUnless struct {
			A string `validate:"required_unless=B free"`
			B string
		}
		type reqWith struct {
			A string `validate:"required_with=B"`
			B string
		}
		type reqWithout struct {
			A string `validate:"required_without=B"`
			B string
		}
		if crossFieldRow(t, &[]reqIf{}, "", "paid") {
			t.Error("required_if must refuse an empty cell when its condition holds")
		}
		if crossFieldRow(t, &[]reqUnless{}, "", "paid") {
			t.Error("required_unless must refuse an empty cell when no condition holds")
		}
		if crossFieldRow(t, &[]reqWith{}, "", "b") {
			t.Error("required_with must refuse an empty cell when its target is present")
		}
		if crossFieldRow(t, &[]reqWithout{}, "", "") {
			t.Error("required_without must refuse an empty cell when its target is absent")
		}
	})
}

// TestCrossFieldValidatorsWithoutTargets pins that a validator handed no target
// value answers rather than panicking. The processor always hands one value per
// name the validator asks for, so this guards the interface itself.
func TestCrossFieldValidatorsWithoutTargets(t *testing.T) {
	t.Parallel()

	t.Run("a comparison reads a missing target as an empty value", func(t *testing.T) {
		t.Parallel()
		if msg := newEqFieldValidator("Other").Validate("", nil); msg != "" {
			t.Errorf("eqFieldValidator.Validate(\"\", nil) = %q, want no message", msg)
		}
		if msg := newFieldContainsValidator("Other").Validate("abc", nil); msg != "" {
			t.Errorf("fieldContainsValidator.Validate(\"abc\", nil) = %q, want no message", msg)
		}
	})

	t.Run("a tag naming no field asks for nothing", func(t *testing.T) {
		t.Parallel()
		if msg := newRequiredWithValidator(nil, false).Validate("", nil); msg != "" {
			t.Errorf("requiredWithValidator.Validate(\"\", nil) = %q, want no message", msg)
		}
		if msg := newRequiredWithoutValidator(nil, true).Validate("", nil); msg != "" {
			t.Errorf("requiredWithoutValidator.Validate(\"\", nil) = %q, want no message", msg)
		}
		if got := newRequiredWithValidator(nil, false).firstTarget(); got != "" {
			t.Errorf("firstTarget() = %q, want an empty string", got)
		}
	})

	t.Run("a condition without its value is not met", func(t *testing.T) {
		t.Parallel()
		if msg := newRequiredIfValidator(nil).Validate("", nil); msg != "" {
			t.Errorf("requiredIfValidator.Validate(\"\", nil) = %q, want no message", msg)
		}
		conditions := []fieldCondition{{field: "Kind", expected: "paid"}}
		if msg := newRequiredIfValidator(conditions).Validate("", nil); msg != "" {
			t.Errorf("requiredIfValidator with no value = %q, want no message", msg)
		}
		if msg := newRequiredUnlessValidator(conditions).Validate("x", nil); msg != "" {
			t.Errorf("requiredUnlessValidator with a filled cell = %q, want no message", msg)
		}
	})
}

// The excluded_* family is the negation of the required_* family: each rule
// only ever complains about a cell that carries a value where its condition
// says there must be none.
func TestExcludedCrossFieldValidators(t *testing.T) {
	t.Parallel()

	conditions := []fieldCondition{{field: "Kind", expected: "paid"}, {field: "Tier", expected: "gold"}}

	tests := []struct {
		name      string
		validator crossFieldValidator
		srcValue  string
		targets   []string
		wantErr   bool
	}{
		{"excluded_if fires when every pair matches", newExcludedIfValidator(conditions), "x", []string{"paid", "gold"}, true},
		{"excluded_if allows an empty cell", newExcludedIfValidator(conditions), "", []string{"paid", "gold"}, false},
		{"excluded_if says nothing when a pair differs", newExcludedIfValidator(conditions), "x", []string{"paid", "silver"}, false},

		{"excluded_unless fires when no pair matches", newExcludedUnlessValidator(conditions), "x", []string{"free", "silver"}, true},
		{"excluded_unless allows an empty cell", newExcludedUnlessValidator(conditions), "", []string{"free", "silver"}, false},
		{"excluded_unless says nothing when one pair matches", newExcludedUnlessValidator(conditions), "x", []string{"paid", "silver"}, false},
		{"excluded_unless says nothing when every pair matches", newExcludedUnlessValidator(conditions), "x", []string{"paid", "gold"}, false},

		{"excluded_with fires when any field is present", newExcludedWithValidator([]string{"A", "B"}, false), "x", []string{"", "b"}, true},
		{"excluded_with allows an empty cell", newExcludedWithValidator([]string{"A", "B"}, false), "", []string{"", "b"}, false},
		{"excluded_with says nothing when every field is empty", newExcludedWithValidator([]string{"A", "B"}, false), "x", []string{"", ""}, false},

		{"excluded_with_all fires when every field is present", newExcludedWithValidator([]string{"A", "B"}, true), "x", []string{"a", "b"}, true},
		{"excluded_with_all allows an empty cell", newExcludedWithValidator([]string{"A", "B"}, true), "", []string{"a", "b"}, false},
		{"excluded_with_all says nothing when one field is empty", newExcludedWithValidator([]string{"A", "B"}, true), "x", []string{"a", ""}, false},

		{"excluded_without fires when any field is empty", newExcludedWithoutValidator([]string{"A", "B"}, false), "x", []string{"a", ""}, true},
		{"excluded_without allows an empty cell", newExcludedWithoutValidator([]string{"A", "B"}, false), "", []string{"a", ""}, false},
		{"excluded_without says nothing when every field is present", newExcludedWithoutValidator([]string{"A", "B"}, false), "x", []string{"a", "b"}, false},

		{"excluded_without_all fires when every field is empty", newExcludedWithoutValidator([]string{"A", "B"}, true), "x", []string{"", ""}, true},
		{"excluded_without_all allows an empty cell", newExcludedWithoutValidator([]string{"A", "B"}, true), "", []string{"", ""}, false},
		{"excluded_without_all says nothing when one field is present", newExcludedWithoutValidator([]string{"A", "B"}, true), "x", []string{"", "b"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			msg := tt.validator.Validate(tt.srcValue, tt.targets)
			if (msg != "") != tt.wantErr {
				t.Errorf("%s.Validate(%q, %v) = %q, wantErr %v", tt.validator.Name(), tt.srcValue, tt.targets, msg, tt.wantErr)
			}
		})
	}
}

// Each excluded_* tag names itself, so an error reports the spelling the caller
// wrote rather than the family.
func TestExcludedValidatorNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		validator crossFieldValidator
		want      string
	}{
		{newExcludedIfValidator(nil), excludedIfTagValue},
		{newExcludedUnlessValidator(nil), excludedUnlessTagValue},
		{newExcludedWithValidator(nil, false), excludedWithTagValue},
		{newExcludedWithValidator(nil, true), excludedWithAllTagValue},
		{newExcludedWithoutValidator(nil, false), excludedWithoutTagValue},
		{newExcludedWithoutValidator(nil, true), excludedWithoutAllTagValue},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			if got := tt.validator.Name(); got != tt.want {
				t.Errorf("Name() = %q, want %q", got, tt.want)
			}
			// A tag naming no field asks for nothing.
			if msg := tt.validator.Validate("x", nil); msg != "" {
				t.Errorf("Validate(\"x\", nil) = %q, want no message", msg)
			}
		})
	}
}

// The excluded_* family decides presence, so it has to run on the rows a
// comparison would be skipped on. excluded_without is where getting that wrong
// shows: its condition is that the named field is empty, which is exactly the
// row skipCrossField would drop.
func TestExcludedWithoutFiresWhenItsTargetIsEmpty(t *testing.T) {
	t.Parallel()

	type record struct {
		Code string `validate:"excluded_without=Name"`
		Name string
	}

	input := "code,name\nA1,\nA2,given\n,\n"
	processor := NewProcessor(FileTypeCSV)

	var records []record
	_, result, err := processor.Process(strings.NewReader(input), &records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(result.Errors) != 1 {
		t.Fatalf("Errors = %v, want exactly the first row reported", result.Errors)
	}
	if !strings.Contains(result.Errors[0].Error(), "row 1") {
		t.Errorf("Errors[0] = %v, want the report to name row 1", result.Errors[0])
	}
}

// The whole family runs through a Processor, so the wiring from tag to row is
// pinned as well as the validators themselves.
func TestExcludedCrossFieldValidation_Processor(t *testing.T) {
	t.Parallel()

	t.Run("excluded_if refuses a cell the condition forbids", func(t *testing.T) {
		t.Parallel()
		type record struct {
			Kind   string
			Coupon string `validate:"excluded_if=Kind free"`
		}

		input := "kind,coupon\nfree,SAVE10\nfree,\npaid,SAVE10\n"
		var records []record
		_, result, err := NewProcessor(FileTypeCSV).Process(strings.NewReader(input), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if len(result.Errors) != 1 {
			t.Fatalf("Errors = %v, want exactly the first row reported", result.Errors)
		}
		if result.ValidRowCount != 2 {
			t.Errorf("ValidRowCount = %d, want 2", result.ValidRowCount)
		}
	})

	t.Run("excluded_with_all refuses a cell only when every named field is present", func(t *testing.T) {
		t.Parallel()
		type record struct {
			Email string
			Phone string
			Fax   string `validate:"excluded_with_all=Email Phone"`
		}

		input := "email,phone,fax\na@example.com,0312345678,0398765432\na@example.com,,0398765432\n"
		var records []record
		_, result, err := NewProcessor(FileTypeCSV).Process(strings.NewReader(input), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if len(result.Errors) != 1 {
			t.Fatalf("Errors = %v, want exactly the first row reported", result.Errors)
		}
	})
}

// TestCrossFieldComparisonOrdersIntegersPastDoublePrecision holds the
// cross-field comparisons on an integer field to the integers the two cells
// spell. 9007199254740993 and 9007199254740992 round to the same float64, so a
// comparison decided by the doubles calls two different numbers one number.
func TestCrossFieldComparisonOrdersIntegersPastDoublePrecision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rows       any
		row        string
		wantReject bool
	}{
		{
			name: "eqfield refuses two integers that differ",
			rows: &[]struct {
				A int64 `name:"a"`
				B int64 `name:"b" validate:"eqfield=A"`
			}{},
			row:        "9007199254740992,9007199254740993",
			wantReject: true,
		},
		{
			name: "nefield accepts two integers that differ",
			rows: &[]struct {
				A int64 `name:"a"`
				B int64 `name:"b" validate:"nefield=A"`
			}{},
			row:        "9007199254740992,9007199254740993",
			wantReject: false,
		},
		{
			name: "gtfield accepts the integer above",
			rows: &[]struct {
				A int64 `name:"a"`
				B int64 `name:"b" validate:"gtfield=A"`
			}{},
			row:        "9007199254740992,9007199254740993",
			wantReject: false,
		},
		{
			name: "gtefield refuses the integer below",
			rows: &[]struct {
				A int64 `name:"a"`
				B int64 `name:"b" validate:"gtefield=A"`
			}{},
			row:        "9007199254740993,9007199254740992",
			wantReject: true,
		},
		{
			name: "ltfield accepts the integer below",
			rows: &[]struct {
				A int64 `name:"a"`
				B int64 `name:"b" validate:"ltfield=A"`
			}{},
			row:        "9007199254740993,9007199254740992",
			wantReject: false,
		},
		{
			name: "ltefield refuses the integer above",
			rows: &[]struct {
				A int64 `name:"a"`
				B int64 `name:"b" validate:"ltefield=A"`
			}{},
			row:        "9007199254740992,9007199254740993",
			wantReject: true,
		},
		{
			name: "gtfield accepts the integer above at the negative end",
			rows: &[]struct {
				A int64 `name:"a"`
				B int64 `name:"b" validate:"gtfield=A"`
			}{},
			row:        "-9223372036854775808,-9223372036854775807",
			wantReject: false,
		},
		{
			name: "gtfield still orders two fractions",
			rows: &[]struct {
				A float64 `name:"a"`
				B float64 `name:"b" validate:"gtfield=A"`
			}{},
			row:        "1.5,1.4",
			wantReject: true,
		},
		{
			name: "ltfield still orders two strings as strings",
			rows: &[]struct {
				A string `name:"a"`
				B string `name:"b" validate:"ltfield=A"`
			}{},
			row:        "2026-01-01,2025-12-31",
			wantReject: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, result, err := NewProcessor(FileTypeCSV).Process(strings.NewReader("a,b\n"+tt.row+"\n"), tt.rows)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			rejected := result.InvalidRowCount() == 1
			if rejected != tt.wantReject {
				t.Errorf("row %q rejected = %v, want %v (%v)", tt.row, rejected, tt.wantReject, result.Errors)
			}
		})
	}
}

// TestComparisonAgreesAcrossTagAndField runs the same pair of values through a
// comparison against a parameter and against another field, and holds the two
// answers together. The two comparisons live in different files and have
// drifted apart before.
func TestComparisonAgreesAcrossTagAndField(t *testing.T) {
	t.Parallel()

	values := []string{"9007199254740992", "9007199254740993", "-9223372036854775808", "5", "5.0", "1e3", "1000"}

	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			t.Parallel()

			againstParameter := &[]struct {
				V int64 `name:"v" validate:"gt=9007199254740992"`
			}{}
			_, parameterResult, err := NewProcessor(FileTypeCSV).Process(
				strings.NewReader("v\n"+value+"\n"), againstParameter)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			againstField := &[]struct {
				A int64 `name:"a"`
				B int64 `name:"b" validate:"gtfield=A"`
			}{}
			_, fieldResult, err := NewProcessor(FileTypeCSV).Process(
				strings.NewReader("a,b\n9007199254740992,"+value+"\n"), againstField)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			if parameterResult.InvalidRowCount() != fieldResult.InvalidRowCount() {
				t.Errorf("gt= rejected %d rows and gtfield= rejected %d for value %q: %v / %v",
					parameterResult.InvalidRowCount(), fieldResult.InvalidRowCount(), value,
					parameterResult.Errors, fieldResult.Errors)
			}
		})
	}
}

// TestExcludedUnlessNegatesRequiredUnless holds the two tags together: they
// name the same condition and read it the same way, so on any row exactly one
// of them has something to say about the cell they guard.
func TestExcludedUnlessNegatesRequiredUnless(t *testing.T) {
	t.Parallel()

	rows := []string{"paid,gold", "paid,silver", "free,gold", "free,silver"}

	for _, row := range rows {
		t.Run(row, func(t *testing.T) {
			t.Parallel()

			excluded := &[]struct {
				Kind string `name:"kind"`
				Tier string `name:"tier"`
				Note string `name:"note" validate:"excluded_unless=Kind paid Tier gold"`
			}{}
			_, excludedResult, err := NewProcessor(FileTypeCSV).Process(
				strings.NewReader("kind,tier,note\n"+row+",x\n"), excluded)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			required := &[]struct {
				Kind string `name:"kind"`
				Tier string `name:"tier"`
				Note string `name:"note" validate:"required_unless=Kind paid Tier gold"`
			}{}
			_, requiredResult, err := NewProcessor(FileTypeCSV).Process(
				strings.NewReader("kind,tier,note\n"+row+",\n"), required)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			if excludedResult.InvalidRowCount() != requiredResult.InvalidRowCount() {
				t.Errorf("row %q: excluded_unless rejected %d and required_unless rejected %d: %v / %v",
					row, excludedResult.InvalidRowCount(), requiredResult.InvalidRowCount(),
					excludedResult.Errors, requiredResult.Errors)
			}
		})
	}
}

// TestExcludedIfNegatesRequiredIf holds the other conditional pair to the same
// rule, so the sibling of the tag that drifted is covered too.
func TestExcludedIfNegatesRequiredIf(t *testing.T) {
	t.Parallel()

	rows := []string{"free,low", "free,high", "paid,low", "paid,high"}

	for _, row := range rows {
		t.Run(row, func(t *testing.T) {
			t.Parallel()

			excluded := &[]struct {
				Kind string `name:"kind"`
				Tier string `name:"tier"`
				Note string `name:"note" validate:"excluded_if=Kind free Tier low"`
			}{}
			_, excludedResult, err := NewProcessor(FileTypeCSV).Process(
				strings.NewReader("kind,tier,note\n"+row+",x\n"), excluded)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			required := &[]struct {
				Kind string `name:"kind"`
				Tier string `name:"tier"`
				Note string `name:"note" validate:"required_if=Kind free Tier low"`
			}{}
			_, requiredResult, err := NewProcessor(FileTypeCSV).Process(
				strings.NewReader("kind,tier,note\n"+row+",\n"), required)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			if excludedResult.InvalidRowCount() != requiredResult.InvalidRowCount() {
				t.Errorf("row %q: excluded_if rejected %d and required_if rejected %d: %v / %v",
					row, excludedResult.InvalidRowCount(), requiredResult.InvalidRowCount(),
					excludedResult.Errors, requiredResult.Errors)
			}
		})
	}
}
