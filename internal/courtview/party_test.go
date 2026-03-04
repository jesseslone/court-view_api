package courtview

import "testing"

func TestIsLikelyPersonName(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{name: "Smith, John", want: true},
		{name: "John Michael Smith", want: true},
		{name: "State of Alaska", want: false},
		{name: "Municipality of Anchorage", want: false},
		{name: "Anchorage Police Department", want: false},
		{name: "ACME 123 LLC", want: false},
	}

	for _, tc := range tests {
		got := IsLikelyPersonName(tc.name)
		if got != tc.want {
			t.Fatalf("IsLikelyPersonName(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestExtractDefendantPartiesFiltersGovernmentEntities(t *testing.T) {
	rows := []SearchResultRow{
		{
			Values: map[string]string{
				"Party/Company": "State of Alaska",
				"Party Type":    "Defendant",
			},
		},
		{
			Values: map[string]string{
				"Party/Company": "Doe, Jane",
				"Party Type":    "Defendant",
			},
		},
		{
			Values: map[string]string{
				"Party/Company": "Municipality of Anchorage",
				"Party Type":    "Prosecution",
			},
		},
	}

	parties := ExtractDefendantParties(rows)
	if len(parties) != 1 {
		t.Fatalf("ExtractDefendantParties length = %d, want 1", len(parties))
	}
	if parties[0].FullName != "Doe, Jane" {
		t.Fatalf("ExtractDefendantParties[0] = %q, want %q", parties[0].FullName, "Doe, Jane")
	}
}
