package prep

import "testing"

// The published tables have to be whole. An exact count would go stale the
// next time ISO assigns or withdraws a code, so the check is a lower bound
// that a truncated paste would still fail.
func TestPublishedCodeTablesAreWhole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		size int
		min  int
	}{
		{"iso3166 alpha-2", len(iso3166Alpha2Set), 240},
		{"iso3166 alpha-3", len(iso3166Alpha3Set), 240},
		{"iso3166 numeric", len(iso3166NumericSet), 240},
		// ISO 4217 assigns fewer codes than ISO 3166-1 does, so its bound is
		// its own rather than the one the country tables share.
		{"iso4217", len(iso4217Set), 150},
		{"iso4217 numeric", len(iso4217NumericSet), 150},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.size < tt.min {
				t.Errorf("%s holds %d codes, want at least %d", tt.name, tt.size, tt.min)
			}
		})
	}

	// Each ISO 3166-1 form names as many codes as the table has rows, which is
	// what a duplicated or blank cell in one column would break.
	for _, got := range []int{len(iso3166Alpha2Set), len(iso3166Alpha3Set), len(iso3166NumericSet)} {
		if got != len(iso3166Countries) {
			t.Errorf("a country code form holds %d codes, want %d, one per assignment", got, len(iso3166Countries))
		}
	}
	if got := len(iso4217Set); got != len(iso4217Currencies) {
		t.Errorf("iso4217Set holds %d codes, want %d", got, len(iso4217Currencies))
	}
	// Every active currency the source lists carries a numeric code, so the
	// two currency forms name the same number of codes.
	if got := len(iso4217NumericSet); got != len(iso4217Currencies) {
		t.Errorf("iso4217NumericSet holds %d codes, want %d", got, len(iso4217Currencies))
	}
}

// The lookups are exact, so a lowercase spelling and an unassigned code both
// fail, and a numeric code keeps the leading zeros the standard prints.
func TestCountryAndCurrencyCodeValidators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tag       string
		validator validator
		pass      []string
		fail      []string
	}{
		{
			tag:       iso3166Alpha2TagValue,
			validator: newISO3166Alpha2Validator(),
			pass:      []string{"JP", "US", "AR"},
			// XK is widely used for Kosovo and is not an assigned code.
			fail: []string{"jp", "XX", "JPN", "J", "XK"},
		},
		{
			tag:       iso3166Alpha3TagValue,
			validator: newISO3166Alpha3Validator(),
			pass:      []string{"JPN", "USA", "ARG"},
			fail:      []string{"jpn", "JPX", "JP", "JPNN"},
		},
		{
			tag:       iso3166NumericTagValue,
			validator: newISO3166NumericValidator(),
			pass:      []string{"392", "032", "840"},
			fail:      []string{"32", "3921", "999", "JPN"},
		},
		{
			tag:       countryCodeTagValue,
			validator: newCountryCodeValidator(),
			pass:      []string{"JP", "JPN", "392"},
			fail:      []string{"JAPAN", "jp", "32"},
		},
		{
			tag:       iso4217TagValue,
			validator: newISO4217Validator(),
			pass:      []string{"JPY", "USD", "EUR", "XAU"},
			fail:      []string{"jpy", "YEN", "JP", "ZWL"},
		},
		{
			tag:       iso4217NumericTagValue,
			validator: newISO4217NumericValidator(),
			// XXX, the no-currency code the alphabetic set includes, is 999.
			pass: []string{"392", "008", "840", "999"},
			fail: []string{"8", "3920", "000", "JPY"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			t.Parallel()
			if got := tt.validator.Name(); got != tt.tag {
				t.Errorf("Name() = %q, want %q", got, tt.tag)
			}
			vs := validators{tt.validator}
			for _, value := range tt.pass {
				if _, msg := vs.Validate(value); msg != "" {
					t.Errorf("%s.Validate(%q) = %q, want a pass", tt.tag, value, msg)
				}
			}
			for _, value := range tt.fail {
				if _, msg := vs.Validate(value); msg == "" {
					t.Errorf("%s.Validate(%q) passed, want a failure", tt.tag, value)
				}
			}
			// An empty cell passes every validator but required, so a country
			// column is optional unless required says otherwise.
			if _, msg := vs.Validate(""); msg != "" {
				t.Errorf("%s.Validate(\"\") = %q, want a pass", tt.tag, msg)
			}
		})
	}
}
