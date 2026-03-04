package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"courtview_lookup/internal/courtview"
	"courtview_lookup/internal/store/sqlserver"
)

type Server struct {
	client *courtview.Client
	store  *sqlserver.Store
}

func NewServer(client *courtview.Client, store *sqlserver.Store) *Server {
	return &Server{client: client, store: store}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/v1/search/name", s.handleNameSearch)
	mux.HandleFunc("/v1/search/case", s.handleCaseSearch)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":         true,
		"db_enabled": s.store != nil && s.store.Enabled(),
	})
}

func (s *Server) handleNameSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	first := strings.TrimSpace(r.URL.Query().Get("first"))
	last := strings.TrimSpace(r.URL.Query().Get("last"))
	if first == "" || last == "" {
		writeError(w, http.StatusBadRequest, "first and last are required query parameters")
		return
	}

	dob := strings.TrimSpace(r.URL.Query().Get("dob"))
	includeCases, maxCases, ok := parseCaseDetailOptions(w, r)
	if !ok {
		return
	}
	allPages, maxPages, ok := parsePaginationOptions(w, r)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()

	resp, err := s.client.SearchByName(ctx, courtview.NameSearchRequest{
		FirstName: first,
		LastName:  last,
		DOBFrom:   dob,
		DOBTo:     dob,
	}, includeCases, maxCases, allPages, maxPages)
	if err != nil {
		status := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		writeError(w, status, err.Error())
		return
	}

	if err := s.persistNameSearchResponse(ctx, resp); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("persist name search response: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleCaseSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	rawCaseNumber := strings.TrimSpace(r.URL.Query().Get("case_number"))
	if rawCaseNumber == "" {
		rawCaseNumber = strings.TrimSpace(r.URL.Query().Get("case"))
	}
	if rawCaseNumber == "" {
		writeError(w, http.StatusBadRequest, "case_number is a required query parameter")
		return
	}

	normalizedCaseNumber, normalizeErr := courtview.NormalizeCaseNumber(rawCaseNumber)
	if normalizeErr != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("normalize case number: %v", normalizeErr))
		return
	}

	includeCases, maxCases, ok := parseCaseDetailOptions(w, r)
	if !ok {
		return
	}
	allPages, maxPages, ok := parsePaginationOptions(w, r)
	if !ok {
		return
	}
	includeNetwork, maxRelatedParties, maxRelatedCases, ok := parseDefendantNetworkOptions(w, r)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()

	resp, err := s.client.SearchByCaseNumber(ctx, rawCaseNumber, includeCases, maxCases, allPages, maxPages)
	if err != nil {
		if s.store != nil && s.store.Enabled() {
			cachedCase, found, cacheErr := s.store.GetCase(ctx, normalizedCaseNumber)
			if cacheErr == nil && found {
				cachedResp := courtview.CaseSearchResponse{
					Request: courtview.CaseSearchRequest{
						CaseNumber:           rawCaseNumber,
						NormalizedCaseNumber: normalizedCaseNumber,
					},
					Results: courtview.SearchResults{
						SourceURL: "cache",
					},
					Cases: []courtview.CaseDetails{*cachedCase},
				}
				writeJSON(w, http.StatusOK, cachedResp)
				return
			}
		}

		status := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		writeError(w, status, err.Error())
		return
	}

	if includeNetwork {
		relatedCases, relatedParties, expandErr := s.expandDefendantNetwork(
			ctx,
			resp.Results.Rows,
			maxRelatedParties,
			maxRelatedCases,
			allPages,
			maxPages,
		)
		if expandErr != nil {
			writeError(w, http.StatusBadGateway, fmt.Sprintf("expand defendant network: %v", expandErr))
			return
		}
		resp.RelatedParties = relatedParties
		resp.Cases = dedupeCases(append(resp.Cases, relatedCases...))
	}

	if err := s.persistCaseSearchResponse(ctx, resp); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("persist case search response: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) expandDefendantNetwork(
	ctx context.Context,
	rows []courtview.SearchResultRow,
	maxRelatedParties int,
	maxRelatedCases int,
	allPages bool,
	maxPages int,
) ([]courtview.CaseDetails, []courtview.PartyRecord, error) {
	parties := courtview.ExtractDefendantParties(rows)
	if maxRelatedParties > 0 && len(parties) > maxRelatedParties {
		parties = parties[:maxRelatedParties]
	}

	relatedCases := make([]courtview.CaseDetails, 0)
	seenNames := make(map[string]struct{})

	for _, party := range parties {
		if strings.TrimSpace(party.FirstName) == "" || strings.TrimSpace(party.LastName) == "" {
			continue
		}
		nameKey := strings.ToLower(strings.TrimSpace(party.LastName + "," + party.FirstName))
		if _, ok := seenNames[nameKey]; ok {
			continue
		}
		seenNames[nameKey] = struct{}{}

		nameResp, err := s.client.SearchByName(ctx, courtview.NameSearchRequest{
			FirstName: party.FirstName,
			LastName:  party.LastName,
			DOBFrom:   party.DOB,
			DOBTo:     party.DOB,
		}, true, maxRelatedCases, allPages, maxPages)
		if err != nil {
			continue
		}

		if persistErr := s.persistNameSearchResponse(ctx, nameResp); persistErr != nil {
			return nil, parties, persistErr
		}

		relatedCases = append(relatedCases, nameResp.Cases...)
	}

	return dedupeCases(relatedCases), parties, nil
}

func (s *Server) persistNameSearchResponse(ctx context.Context, resp courtview.NameSearchResponse) error {
	if s.store == nil || !s.store.Enabled() {
		return nil
	}
	for _, caseDetails := range resp.Cases {
		if strings.TrimSpace(caseDetails.CaseNumber) == "" {
			continue
		}
		rows := rowsForCase(resp.Results.Rows, caseDetails.CaseNumber)
		if _, err := s.store.UpsertCase(ctx, caseDetails, rows); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) persistCaseSearchResponse(ctx context.Context, resp courtview.CaseSearchResponse) error {
	if s.store == nil || !s.store.Enabled() {
		return nil
	}
	for _, caseDetails := range resp.Cases {
		if strings.TrimSpace(caseDetails.CaseNumber) == "" {
			continue
		}
		rows := rowsForCase(resp.Results.Rows, caseDetails.CaseNumber)
		if _, err := s.store.UpsertCase(ctx, caseDetails, rows); err != nil {
			return err
		}
	}
	return nil
}

func rowsForCase(rows []courtview.SearchResultRow, caseNumber string) []courtview.SearchResultRow {
	caseNumber = strings.TrimSpace(strings.ToUpper(caseNumber))
	if caseNumber == "" {
		return nil
	}
	out := make([]courtview.SearchResultRow, 0)
	for _, row := range rows {
		if strings.TrimSpace(strings.ToUpper(row.CaseNumber)) == caseNumber {
			out = append(out, row)
		}
	}
	return out
}

func dedupeCases(cases []courtview.CaseDetails) []courtview.CaseDetails {
	seen := make(map[string]struct{})
	out := make([]courtview.CaseDetails, 0, len(cases))
	for _, c := range cases {
		key := strings.TrimSpace(strings.ToUpper(c.CaseNumber))
		if key == "" {
			key = strings.TrimSpace(strings.ToLower(c.CaseURL))
		}
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, c)
	}
	return out
}

func parseCaseDetailOptions(w http.ResponseWriter, r *http.Request) (bool, int, bool) {
	includeCases := true
	if raw := strings.TrimSpace(r.URL.Query().Get("include_cases")); raw != "" {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "include_cases must be true or false")
			return false, 0, false
		}
		includeCases = b
	}

	maxCases := 25
	if raw := strings.TrimSpace(r.URL.Query().Get("max_cases")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 500 {
			writeError(w, http.StatusBadRequest, "max_cases must be an integer between 1 and 500")
			return false, 0, false
		}
		maxCases = n
	}

	return includeCases, maxCases, true
}

func parsePaginationOptions(w http.ResponseWriter, r *http.Request) (bool, int, bool) {
	allPages := true
	if raw := strings.TrimSpace(r.URL.Query().Get("all_pages")); raw != "" {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "all_pages must be true or false")
			return false, 0, false
		}
		allPages = b
	}

	maxPages := 20
	if raw := strings.TrimSpace(r.URL.Query().Get("max_pages")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 200 {
			writeError(w, http.StatusBadRequest, "max_pages must be an integer between 1 and 200")
			return false, 0, false
		}
		maxPages = n
	}
	return allPages, maxPages, true
}

func parseDefendantNetworkOptions(w http.ResponseWriter, r *http.Request) (bool, int, int, bool) {
	includeNetwork := true
	if raw := strings.TrimSpace(r.URL.Query().Get("include_defendant_network")); raw != "" {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "include_defendant_network must be true or false")
			return false, 0, 0, false
		}
		includeNetwork = b
	}

	maxRelatedParties := 10
	if raw := strings.TrimSpace(r.URL.Query().Get("max_related_parties")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			writeError(w, http.StatusBadRequest, "max_related_parties must be an integer between 1 and 100")
			return false, 0, 0, false
		}
		maxRelatedParties = n
	}

	maxRelatedCases := 100
	if raw := strings.TrimSpace(r.URL.Query().Get("max_related_cases")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 1000 {
			writeError(w, http.StatusBadRequest, "max_related_cases must be an integer between 1 and 1000")
			return false, 0, 0, false
		}
		maxRelatedCases = n
	}

	return includeNetwork, maxRelatedParties, maxRelatedCases, true
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{
		"error": message,
	})
}
