package prep

import (
	"cmp"
	"strconv"
	"strings"
)

// crossFieldValidator defines the interface for validators that compare values across fields
type crossFieldValidator interface {
	// Validate checks the source value against the values of the fields named
	// by TargetFields, in that order. It returns an empty string when the row
	// passes and an error message otherwise.
	Validate(srcValue string, targetValues []string) string
	// Name returns the name of the validator for error reporting
	Name() string
	// TargetFields returns the names of the fields to compare against
	TargetFields() []string
	// decidesPresence reports whether the tag exists to say when an empty cell
	// is allowed. Those tags run on every row; the rest follow the rule that an
	// empty cell passes every validator but required, so they are skipped as
	// soon as either side of the comparison is missing.
	decidesPresence() bool
}

// crossFieldValidators is a slice of crossFieldValidator
type crossFieldValidators []crossFieldValidator

// baseCrossFieldValidator contains common fields for cross-field validators
type baseCrossFieldValidator struct {
	targetFields []string
	// comparesText makes a comparison read the two cells as the text they are.
	// It follows the kind of field the tag lands on; see specializeCrossField.
	// Only the six comparison tags set it — the rest read text either way.
	comparesText bool
}

// TargetFields returns the names of the fields to compare against
func (b *baseCrossFieldValidator) TargetFields() []string {
	return b.targetFields
}

// decidesPresence answers for every tag that compares values rather than
// deciding whether an empty cell is allowed.
func (b *baseCrossFieldValidator) decidesPresence() bool {
	return false
}

// firstTarget names the single field a comparison tag takes, for its message.
func (b *baseCrossFieldValidator) firstTarget() string {
	if len(b.targetFields) == 0 {
		return ""
	}
	return b.targetFields[0]
}

// presenceCrossFieldValidator is the base of the tags that decide whether an
// empty cell is allowed. They run on every row, including the rows where the
// cell they guard is empty, which is the only row where they have anything to
// say.
type presenceCrossFieldValidator struct {
	baseCrossFieldValidator
}

// decidesPresence reports that this family runs on an empty cell.
func (p *presenceCrossFieldValidator) decidesPresence() bool {
	return true
}

// singleTarget returns the one value a comparison tag compares against.
func singleTarget(targetValues []string) string {
	if len(targetValues) == 0 {
		return ""
	}
	return targetValues[0]
}

// compare orders the two cells: negative when the source sorts first, zero when
// the two are the same, positive when the source sorts last.
//
// One function answers for the whole comparison family, because the six tags
// have to agree about what the values are. They did not: the four ordering tags
// read numbers and the two equality tags read text, so "007" against "7" was
// neither greater, nor equal, nor less. A comparison on a string field now
// reads the strings, one on any other field reads the numbers the cells spell,
// and a cell that spells no number falls back to its text so the fallback is
// the same for all six.
func (b *baseCrossFieldValidator) compare(srcValue, targetValue string) int {
	if !b.comparesText {
		srcFloat, srcErr := strconv.ParseFloat(srcValue, 64)
		targetFloat, targetErr := strconv.ParseFloat(targetValue, 64)
		if srcErr == nil && targetErr == nil {
			return cmp.Compare(srcFloat, targetFloat)
		}
	}
	return strings.Compare(srcValue, targetValue)
}

// specializeCrossField gives each comparison the meaning the field's kind
// decides, the way specializeValidator does for the single-field tags. The
// conditional-required family and the substring pair read text whatever field
// they land on, and are left alone.
func specializeCrossField(vals crossFieldValidators, isString bool) {
	for _, v := range vals {
		switch typed := v.(type) {
		case *eqFieldValidator:
			typed.comparesText = isString
		case *neFieldValidator:
			typed.comparesText = isString
		case *gtFieldValidator:
			typed.comparesText = isString
		case *gteFieldValidator:
			typed.comparesText = isString
		case *ltFieldValidator:
			typed.comparesText = isString
		case *lteFieldValidator:
			typed.comparesText = isString
		}
	}
}

// newBaseCrossField builds the base a single-target comparison tag needs.
func newBaseCrossField(targetField string) baseCrossFieldValidator {
	return baseCrossFieldValidator{targetFields: []string{targetField}}
}

// =====================================
// eqFieldValidator - Equal to another field
// =====================================

type eqFieldValidator struct {
	baseCrossFieldValidator
}

// newEqFieldValidator creates a new equal field validator
func newEqFieldValidator(targetField string) *eqFieldValidator {
	return &eqFieldValidator{newBaseCrossField(targetField)}
}

// Validate checks if the source value equals the target value
func (v *eqFieldValidator) Validate(srcValue string, targetValues []string) string {
	if v.compare(srcValue, singleTarget(targetValues)) != 0 {
		return "value must equal field " + v.firstTarget()
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
	return &neFieldValidator{newBaseCrossField(targetField)}
}

// Validate checks if the source value does not equal the target value
func (v *neFieldValidator) Validate(srcValue string, targetValues []string) string {
	if v.compare(srcValue, singleTarget(targetValues)) == 0 {
		return "value must not equal field " + v.firstTarget()
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
	return &gtFieldValidator{newBaseCrossField(targetField)}
}

// Validate checks if the source value is greater than the target value
func (v *gtFieldValidator) Validate(srcValue string, targetValues []string) string {
	if v.compare(srcValue, singleTarget(targetValues)) <= 0 {
		return "value must be greater than field " + v.firstTarget()
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
	return &gteFieldValidator{newBaseCrossField(targetField)}
}

// Validate checks if the source value is greater than or equal to the target value
func (v *gteFieldValidator) Validate(srcValue string, targetValues []string) string {
	if v.compare(srcValue, singleTarget(targetValues)) < 0 {
		return "value must be greater than or equal to field " + v.firstTarget()
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
	return &ltFieldValidator{newBaseCrossField(targetField)}
}

// Validate checks if the source value is less than the target value
func (v *ltFieldValidator) Validate(srcValue string, targetValues []string) string {
	if v.compare(srcValue, singleTarget(targetValues)) >= 0 {
		return "value must be less than field " + v.firstTarget()
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
	return &lteFieldValidator{newBaseCrossField(targetField)}
}

// Validate checks if the source value is less than or equal to the target value
func (v *lteFieldValidator) Validate(srcValue string, targetValues []string) string {
	if v.compare(srcValue, singleTarget(targetValues)) > 0 {
		return "value must be less than or equal to field " + v.firstTarget()
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
	return &fieldContainsValidator{newBaseCrossField(targetField)}
}

// Validate checks if the source value contains the target value
func (v *fieldContainsValidator) Validate(srcValue string, targetValues []string) string {
	if !strings.Contains(srcValue, singleTarget(targetValues)) {
		return "value must contain field " + v.firstTarget() + " value"
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
	return &fieldExcludesValidator{newBaseCrossField(targetField)}
}

// Validate checks if the source value does not contain the target value
func (v *fieldExcludesValidator) Validate(srcValue string, targetValues []string) string {
	if strings.Contains(srcValue, singleTarget(targetValues)) {
		return "value must not contain field " + v.firstTarget() + " value"
	}
	return ""
}

// Name returns the validator name
func (v *fieldExcludesValidator) Name() string {
	return fieldExcludesTagValue
}

// =====================================
// Conditional required validators
// =====================================

// The two words an error message joins a list of fields with: "and" when every
// one of them takes part, "or" when any one of them is enough.
const (
	joinerAnd = "and"
	joinerOr  = "or"
)

// fieldCondition pairs a field name with the value that field must hold for the
// condition to count as met.
type fieldCondition struct {
	field    string
	expected string
}

// fieldNames returns the field names the conditions name, in order.
func fieldNames(conditions []fieldCondition) []string {
	names := make([]string, len(conditions))
	for i, c := range conditions {
		names[i] = c.field
	}
	return names
}

// describeConditions spells the conditions the way the error message reads
// them, joined by the word the tag's rule uses.
func describeConditions(conditions []fieldCondition, joiner string) string {
	parts := make([]string, len(conditions))
	for i, c := range conditions {
		parts[i] = c.field + " is " + c.expected
	}
	return strings.Join(parts, " "+joiner+" ")
}

// describeFields spells a list of field names with the verb that agrees with
// it: "A or B is present" reads of either one, "A and B are present" reads of
// both together, and one field alone keeps the message this package has always
// produced.
func describeFields(names []string, joiner string) (string, string) {
	verb := "is"
	if len(names) > 1 && joiner == joinerAnd {
		verb = "are"
	}
	return strings.Join(names, " "+joiner+" "), verb
}

// joinerFor names the word a rule over a list of fields reads with.
func joinerFor(all bool) string {
	if all {
		return joinerAnd
	}
	return joinerOr
}

// conditionsMet reports whether every condition, or any one of them, is met by
// the row's values, which arrive in the order the conditions name their fields.
func conditionsMet(conditions []fieldCondition, values []string, all bool) bool {
	if len(conditions) == 0 || len(values) != len(conditions) {
		return false
	}
	for i, c := range conditions {
		if (values[i] == c.expected) != all {
			return !all
		}
	}
	return all
}

// requiredIfValidator validates that a field is required when every named field
// holds the value paired with it.
type requiredIfValidator struct {
	presenceCrossFieldValidator
	conditions []fieldCondition
}

// newRequiredIfValidator creates a new required_if validator from the field and
// value pairs the tag names.
func newRequiredIfValidator(conditions []fieldCondition) *requiredIfValidator {
	return &requiredIfValidator{
		presenceCrossFieldValidator: presenceCrossFieldValidator{
			baseCrossFieldValidator{targetFields: fieldNames(conditions)},
		},
		conditions: conditions,
	}
}

// Validate checks if the source value is present when every condition holds
func (v *requiredIfValidator) Validate(srcValue string, targetValues []string) string {
	if !conditionsMet(v.conditions, targetValues, true) {
		return ""
	}
	if srcValue == "" {
		return "value is required when " + describeConditions(v.conditions, joinerAnd)
	}
	return ""
}

// Name returns the validator name
func (v *requiredIfValidator) Name() string {
	return requiredIfTagValue
}

// requiredUnlessValidator validates that a field is required unless at least
// one named field holds the value paired with it.
type requiredUnlessValidator struct {
	presenceCrossFieldValidator
	conditions []fieldCondition
}

// newRequiredUnlessValidator creates a new required_unless validator from the
// field and value pairs the tag names.
func newRequiredUnlessValidator(conditions []fieldCondition) *requiredUnlessValidator {
	return &requiredUnlessValidator{
		presenceCrossFieldValidator: presenceCrossFieldValidator{
			baseCrossFieldValidator{targetFields: fieldNames(conditions)},
		},
		conditions: conditions,
	}
}

// Validate checks if the source value is present unless one condition holds
func (v *requiredUnlessValidator) Validate(srcValue string, targetValues []string) string {
	if conditionsMet(v.conditions, targetValues, false) {
		return ""
	}
	if srcValue == "" {
		return "value is required unless " + describeConditions(v.conditions, joinerOr)
	}
	return ""
}

// Name returns the validator name
func (v *requiredUnlessValidator) Name() string {
	return requiredUnlessTagValue
}

// requiredWithValidator validates that a field is required when the named
// fields carry values. all decides whether every named field must carry one or
// whether any of them is enough.
type requiredWithValidator struct {
	presenceCrossFieldValidator
	all bool
}

// newRequiredWithValidator creates a new required_with validator
func newRequiredWithValidator(targetFields []string, all bool) *requiredWithValidator {
	return &requiredWithValidator{
		presenceCrossFieldValidator: presenceCrossFieldValidator{
			baseCrossFieldValidator{targetFields: targetFields},
		},
		all: all,
	}
}

// Validate checks if the source value is present when the named fields are
func (v *requiredWithValidator) Validate(srcValue string, targetValues []string) string {
	if !fires(targetValues, v.all, func(value string) bool { return value != "" }) {
		return ""
	}
	if srcValue == "" {
		names, verb := describeFields(v.targetFields, joinerFor(v.all))
		return "value is required when " + names + " " + verb + " present"
	}
	return ""
}

// Name returns the validator name
func (v *requiredWithValidator) Name() string {
	if v.all {
		return requiredWithAllTagValue
	}
	return requiredWithTagValue
}

// requiredWithoutValidator validates that a field is required when the named
// fields are empty. all decides whether every named field must be empty or
// whether any of them is enough.
type requiredWithoutValidator struct {
	presenceCrossFieldValidator
	all bool
}

// newRequiredWithoutValidator creates a new required_without validator
func newRequiredWithoutValidator(targetFields []string, all bool) *requiredWithoutValidator {
	return &requiredWithoutValidator{
		presenceCrossFieldValidator: presenceCrossFieldValidator{
			baseCrossFieldValidator{targetFields: targetFields},
		},
		all: all,
	}
}

// Validate checks if the source value is present when the named fields are not
func (v *requiredWithoutValidator) Validate(srcValue string, targetValues []string) string {
	if !fires(targetValues, v.all, func(value string) bool { return value == "" }) {
		return ""
	}
	if srcValue == "" {
		names, verb := describeFields(v.targetFields, joinerFor(v.all))
		return "value is required when " + names + " " + verb + " absent"
	}
	return ""
}

// Name returns the validator name
func (v *requiredWithoutValidator) Name() string {
	if v.all {
		return requiredWithoutAllTagValue
	}
	return requiredWithoutTagValue
}

// fires reports whether the values satisfy holds, either all of them or any of
// them. An empty list never fires, since a tag that names no field asks for
// nothing.
func fires(values []string, all bool, holds func(string) bool) bool {
	if len(values) == 0 {
		return false
	}
	for _, value := range values {
		if holds(value) != all {
			return !all
		}
	}
	return all
}
