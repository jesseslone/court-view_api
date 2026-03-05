package store

import (
	"context"

	"courtview_lookup/internal/courtview"
)

// Store is the persistence contract used by the API server.
type Store interface {
	Enabled() bool
	Close() error
	UpsertCase(ctx context.Context, caseDetails courtview.CaseDetails, rows []courtview.SearchResultRow) (bool, error)
	GetCase(ctx context.Context, caseNumber string) (*courtview.CaseDetails, bool, error)
	CandidateCaseNumbersForParty(ctx context.Context, party courtview.PartyRecord, maxCases int) ([]string, error)
}
