package sqlserver

import (
	"testing"

	"courtview_lookup/internal/courtview"
)

func TestExtractChargesFromTextFallback(t *testing.T) {
	caseDetails := courtview.CaseDetails{
		CaseNumber: "3AN-26-90002CR",
		Current: courtview.PageSnapshot{
			MainTextExcerpt: "Case Type: Criminal Case Status: Open " +
				"Charge # 1: TEST1001 - Class A Misdemeanor (City) TEST11.11.111(A)(1): Sample Assault " +
				"Original Charge TEST1001 TEST11.11.111(A)(1): Sample Assault (Class A Misdemeanor (City)) " +
				"Indicted Charge Amended Charge DV Related? Yes Modifiers None Stage Date 01/01/2026 " +
				"ATN # 000000001 Tracking # 001 Offense Location Example City Date of Offense 01/01/2026",
		},
		Tabs: map[string]courtview.PageSnapshot{
			"Charge": {MainTextExcerpt: "tab fetch failed: http 500"},
		},
	}

	charges := extractCharges(caseDetails)
	if len(charges) != 1 {
		t.Fatalf("expected 1 charge, got %d", len(charges))
	}
	got := charges[0]
	if got.chargeCode != "TEST1001" {
		t.Fatalf("expected charge code TEST1001, got %q", got.chargeCode)
	}
	if got.atn != "000000001" {
		t.Fatalf("expected ATN 000000001, got %q", got.atn)
	}
	if got.trackingNumber != "001" {
		t.Fatalf("expected tracking number 001, got %q", got.trackingNumber)
	}
	if got.offenseLevel != "Class A Misdemeanor" {
		t.Fatalf("expected offense level Class A Misdemeanor, got %q", got.offenseLevel)
	}
	if got.stageDate == nil || got.offenseDate == nil {
		t.Fatalf("expected stage/offense dates to be parsed, got stage=%v offense=%v", got.stageDate, got.offenseDate)
	}
	if got.dvRelated == nil || !*got.dvRelated {
		t.Fatalf("expected dv_related=true, got %v", got.dvRelated)
	}
}

func TestExtractChargesMergesTableAndFallbackFields(t *testing.T) {
	caseDetails := courtview.CaseDetails{
		CaseNumber: "3AN-26-90003CR",
		Current: courtview.PageSnapshot{
			MainTextExcerpt: "Charge # 1: TEST1001 - Class A Misdemeanor (City) " +
				"Original Charge TEST1001 Sample Assault " +
				"DV Related? Yes Stage Date 01/01/2026 ATN # 000000001 Tracking # 001 Date of Offense 01/01/2026",
			Tables: []courtview.TableSnapshot{
				{
					Index:   0,
					Headers: []string{"Charge #", "Charge", "Disposition"},
					Rows: [][]string{
						{"1", "Sample Assault", "Convicted"},
					},
				},
			},
		},
	}

	charges := extractCharges(caseDetails)
	if len(charges) != 1 {
		t.Fatalf("expected 1 merged charge, got %d", len(charges))
	}
	got := charges[0]
	if got.dispositionStatus != "Convicted" {
		t.Fatalf("expected disposition Convicted, got %q", got.dispositionStatus)
	}
	if got.atn != "000000001" {
		t.Fatalf("expected ATN to merge from fallback text, got %q", got.atn)
	}
	if got.offenseDate == nil || got.stageDate == nil {
		t.Fatalf("expected dates to be present after merge")
	}
}
