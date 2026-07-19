package prep

import (
	"strconv"
	"strings"
)

// CrossFieldValidator defines the interface for validators that compare values across fields
type CrossFieldValidator interface {
	// Validate checks if the source value is valid compared to the target value
	// Returns empty string if validation passes, error message otherwise
	Validate(srcValue, targetValue string) string
	// Name returns the name of the validator for error reporting
	Name() string
	// TargetField returns the name of the field to compare against
	TargetField() string
}

// crossFieldValidators is a slice of CrossFieldValidator
type crossFieldValidators []CrossFieldValidator

// baseCrossFieldValidator contains common fields for cross-field validators
type baseCrossFieldValidator struct {
	targetField string
}

// TargetField returns the name of the field to compare against
func (b *baseCrossFieldValidator) TargetField() string {
	return b.targetField
}

// =====================================
// eqFieldValidator - Equal to another field
// =====================================

type eqFieldValidator struct {
	baseCrossFieldValidator
}

// newEqFieldValidator creates a new equal field validator
func newEqFieldValidator(targetField string) *eqFieldValidator {
	return &eqFieldValidator{baseCrossFieldValidator{targetField: targetField}}
}

// Validate checks if the source value equals the target value
func (v *eqFieldValidator) Validate(srcValue, targetValue string) string {
	if srcValue != targetValue {
		return "value must equal field " + v.targetField
	}
	return ""
}

// Name returns the validator name
func (v *eqFieldValidator) Name() string {
	return eqFieldTagValue
}

// =====================================
// neFieldValidator - Not equal to another field
// =====================================

type neFieldValidator struct {
	baseCrossFieldValidator
}

// newNeFieldValidator creates a new not equal field validator
func newNeFieldValidator(targetField string) *neFieldValidator {
	return &neFieldValidator{baseCrossFieldValidator{targetField: targetField}}
}

// Validate checks if the source value does not equal the target value
func (v *neFieldValidator) Validate(srcValue, targetValue string) string {
	if srcValue == targetValue {
		return "value must not equal field " + v.targetField
	}
	return ""
}

// Name returns the validator name
func (v *neFieldValidator) Name() string {
	return neFieldTagValue
}

// =====================================
// gtFieldValidator - Greater than another field
// =====================================

type gtFieldValidator struct {
	baseCrossFieldValidator
}

// newGtFieldValidator creates a new greater than field validator
func newGtFieldValidator(targetField string) *gtFieldValidator {
	return &gtFieldValidator{baseCrossFieldValidator{targetField: targetField}}
}

// Validate checks if the source value is greater than the target value
func (v *gtFieldValidator) Validate(srcValue, targetValue string) string {
	srcFloat, srcErr := strconv.ParseFloat(srcValue, 64)
	targetFloat, targetErr := strconv.ParseFloat(targetValue, 64)

	errMsg := "value must be greater than field " + v.targetField

	if srcErr != nil || targetErr != nil {
		// Fall back to string comparison
		if srcValue <= targetValue {
			return errMsg
		}
		return ""
	}

	if srcFloat <= targetFloat {
		return errMsg
	}
	return ""
}

// Name returns the validator name
func (v *gtFieldValidator) Name() string {
	return gtFieldTagValue
}

// =====================================
// gteFieldValidator - Greater than or equal to another field
// =====================================

type gteFieldValidator struct {
	baseCrossFieldValidator
}

// newGteFieldValidator creates a new greater than or equal field validator
func newGteFieldValidator(targetField string) *gteFieldValidator {
	return &gteFieldValidator{baseCrossFieldValidator{targetField: targetField}}
}

// Validate checks if the source value is greater than or equal to the target value
func (v *gteFieldValidator) Validate(srcValue, targetValue string) string {
	srcFloat, srcErr := strconv.ParseFloat(srcValue, 64)
	targetFloat, targetErr := strconv.ParseFloat(targetValue, 64)

	errMsg := "value must be greater than or equal to field " + v.targetField

	if srcErr != nil || targetErr != nil {
		// Fall back to string comparison
		if srcValue < targetValue {
			return errMsg
		}
		return ""
	}

	if srcFloat < targetFloat {
		return errMsg
	}
	return ""
}

// Name returns the validator name
func (v *gteFieldValidator) Name() string {
	return gteFieldTagValue
}

// =====================================
// ltFieldValidator - Less than another field
// =====================================

type ltFieldValidator struct {
	baseCrossFieldValidator
}

// newLtFieldValidator creates a new less than field validator
func newLtFieldValidator(targetField string) *ltFieldValidator {
	return &ltFieldValidator{baseCrossFieldValidator{targetField: targetField}}
}

// Validate checks if the source value is less than the target value
func (v *ltFieldValidator) Validate(srcValue, targetValue string) string {
	srcFloat, srcErr := strconv.ParseFloat(srcValue, 64)
	targetFloat, targetErr := strconv.ParseFloat(targetValue, 64)

	errMsg := "value must be less than field " + v.targetField

	if srcErr != nil || targetErr != nil {
		// Fall back to string comparison
		if srcValue >= targetValue {
			return errMsg
		}
		return ""
	}

	if srcFloat >= targetFloat {
		return errMsg
	}
	return ""
}

// Name returns the validator name
func (v *ltFieldValidator) Name() string {
	return ltFieldTagValue
}

// =====================================
// lteFieldValidator - Less than or equal to another field
// =====================================

type lteFieldValidator struct {
	baseCrossFieldValidator
}

// newLteFieldValidator creates a new less than or equal field validator
func newLteFieldValidator(targetField string) *lteFieldValidator {
	return &lteFieldValidator{baseCrossFieldValidator{targetField: targetField}}
}

// Validate checks if the source value is less than or equal to the target value
func (v *lteFieldValidator) Validate(srcValue, targetValue string) string {
	srcFloat, srcErr := strconv.ParseFloat(srcValue, 64)
	targetFloat, targetErr := strconv.ParseFloat(targetValue, 64)

	errMsg := "value must be less than or equal to field " + v.targetField

	if srcErr != nil || targetErr != nil {
		// Fall back to string comparison
		if srcValue > targetValue {
			return errMsg
		}
		return ""
	}

	if srcFloat > targetFloat {
		return errMsg
	}
	return ""
}

// Name returns the validator name
func (v *lteFieldValidator) Name() string {
	return lteFieldTagValue
}

// =====================================
// fieldContainsValidator - Field contains another field's value
// =====================================

type fieldContainsValidator struct {
	baseCrossFieldValidator
}

// newFieldContainsValidator creates a new field contains validator
func newFieldContainsValidator(targetField string) *fieldContainsValidator {
	return &fieldContainsValidator{baseCrossFieldValidator{targetField: targetField}}
}

// Validate checks if the source value contains the target value
func (v *fieldContainsValidator) Validate(srcValue, targetValue string) string {
	if !strings.Contains(srcValue, targetValue) {
		return "value must contain field " + v.targetField + " value"
	}
	return ""
}

// Name returns the validator name
func (v *fieldContainsValidator) Name() string {
	return fieldContainsTagValue
}

// =====================================
// fieldExcludesValidator - Field excludes another field's value
// =====================================

type fieldExcludesValidator struct {
	baseCrossFieldValidator
}

// newFieldExcludesValidator creates a new field excludes validator
func newFieldExcludesValidator(targetField string) *fieldExcludesValidator {
	return &fieldExcludesValidator{baseCrossFieldValidator{targetField: targetField}}
}

// Validate checks if the source value does not contain the target value
func (v *fieldExcludesValidator) Validate(srcValue, targetValue string) string {
	if strings.Contains(srcValue, targetValue) {
		return "value must not contain field " + v.targetField + " value"
	}
	return ""
}

// Name returns the validator name
func (v *fieldExcludesValidator) Name() string {
	return fieldExcludesTagValue
}

// =====================================
// requiredIfValidator - Required if another field equals a specific value
// =====================================

// requiredIfValidator validates that a field is required when another field has a specific value
type requiredIfValidator struct {
	baseCrossFieldValidator
	expectedValue string
}

// newRequiredIfValidator creates a new required_if validator
// targetField is the field name, expectedValue is the value that triggers the requirement
func newRequiredIfValidator(targetField, expectedValue string) *requiredIfValidator {
	return &requiredIfValidator{
		baseCrossFieldValidator: baseCrossFieldValidator{targetField: targetField},
		expectedValue:           expectedValue,
	}
}

// Validate checks if the source value is present when target field equals expected value
func (v *requiredIfValidator) Validate(srcValue, targetValue string) string {
	// If target field equals expected value, source field is required
	if targetValue == v.expectedValue && srcValue == "" {
		return "value is required when " + v.targetField + " is " + v.expectedValue
	}
	return ""
}

// Name returns the validator name
func (v *requiredIfValidator) Name() string {
	return requiredIfTagValue
}

// =====================================
// requiredUnlessValidator - Required unless another field equals a specific value
// =====================================

// requiredUnlessValidator validates that a field is required unless another field has a specific value
type requiredUnlessValidator struct {
	baseCrossFieldValidator
	exceptValue string
}

// newRequiredUnlessValidator creates a new required_unless validator
// targetField is the field name, exceptValue is the value that exempts the requirement
func newRequiredUnlessValidator(targetField, exceptValue string) *requiredUnlessValidator {
	return &requiredUnlessValidator{
		baseCrossFieldValidator: baseCrossFieldValidator{targetField: targetField},
		exceptValue:             exceptValue,
	}
}

// Validate checks if the source value is present unless target field equals except value
func (v *requiredUnlessValidator) Validate(srcValue, targetValue string) string {
	// If target field does NOT equal except value, source field is required
	if targetValue != v.exceptValue && srcValue == "" {
		return "value is required unless " + v.targetField + " is " + v.exceptValue
	}
	return ""
}

// Name returns the validator name
func (v *requiredUnlessValidator) Name() string {
	return requiredUnlessTagValue
}

// =====================================
// requiredWithValidator - Required if another field is present (non-empty)
// =====================================

// requiredWithValidator validates that a field is required when another field is present
type requiredWithValidator struct {
	baseCrossFieldValidator
}

// newRequiredWithValidator creates a new required_with validator
func newRequiredWithValidator(targetField string) *requiredWithValidator {
	return &requiredWithValidator{baseCrossFieldValidator{targetField: targetField}}
}

// Validate checks if the source value is present when target field is non-empty
func (v *requiredWithValidator) Validate(srcValue, targetValue string) string {
	// If target field is present (non-empty), source field is required
	if targetValue != "" && srcValue == "" {
		return "value is required when " + v.targetField + " is present"
	}
	return ""
}

// Name returns the validator name
func (v *requiredWithValidator) Name() string {
	return requiredWithTagValue
}

// =====================================
// requiredWithoutValidator - Required if another field is absent (empty)
// =====================================

// requiredWithoutValidator validates that a field is required when another field is absent
type requiredWithoutValidator struct {
	baseCrossFieldValidator
}

// newRequiredWithoutValidator creates a new required_without validator
func newRequiredWithoutValidator(targetField string) *requiredWithoutValidator {
	return &requiredWithoutValidator{baseCrossFieldValidator{targetField: targetField}}
}

// Validate checks if the source value is present when target field is empty
func (v *requiredWithoutValidator) Validate(srcValue, targetValue string) string {
	// If target field is absent (empty), source field is required
	if targetValue == "" && srcValue == "" {
		return "value is required when " + v.targetField + " is absent"
	}
	return ""
}

// Name returns the validator name
func (v *requiredWithoutValidator) Name() string {
	return requiredWithoutTagValue
}
