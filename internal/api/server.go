package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
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

type backfillRequest struct {
	Count                   int  `json:"count"`
	YearTwoDigits           int  `json:"year_two_digits"`
	StartSequence           int  `json:"start_sequence"`
	MaxAttempts             int  `json:"max_attempts"`
	TimeoutSeconds          int  `json:"timeout_seconds"`
	IncludeDefendantNetwork bool `json:"include_defendant_network"`
	MaxRelatedParties       int  `json:"max_related_parties"`
	MaxRelatedCases         int  `json:"max_related_cases"`
}

type backfillSummary struct {
	Complete                  bool     `json:"complete"`
	TargetCases               int      `json:"target_cases"`
	FoundCases                int      `json:"found_cases"`
	AttemptedCaseNumbers      int      `json:"attempted_case_numbers"`
	LastSequenceTried         int      `json:"last_sequence_tried"`
	SearchErrors              int      `json:"search_errors"`
	ExpandErrors              int      `json:"expand_errors"`
	PersistErrors             int      `json:"persist_errors"`
	PersistedCases            int      `json:"persisted_cases"`
	PersistedChangedCases     int      `json:"persisted_changed_cases"`
	PersistedUnchangedCases   int      `json:"persisted_unchanged_cases"`
	UniqueCaseNumbersCaptured []string `json:"unique_case_numbers_captured"`
}

type durationStats struct {
	Count int     `json:"count"`
	MinMS float64 `json:"min_ms"`
	AvgMS float64 `json:"avg_ms"`
	P50MS float64 `json:"p50_ms"`
	P90MS float64 `json:"p90_ms"`
	P95MS float64 `json:"p95_ms"`
	MaxMS float64 `json:"max_ms"`
}

type backfillMetrics struct {
	StartedAt        string        `json:"started_at"`
	FinishedAt       string        `json:"finished_at"`
	TotalDurationMS  float64       `json:"total_duration_ms"`
	AttemptsPerSec   float64       `json:"attempts_per_second"`
	CasesPerSec      float64       `json:"cases_per_second"`
	AttemptDuration  durationStats `json:"attempt_duration"`
	SearchDuration   durationStats `json:"search_duration"`
	ExpandDuration   durationStats `json:"expand_duration"`
	PersistDuration  durationStats `json:"persist_duration"`
	ErrorSample      []string      `json:"error_sample,omitempty"`
	PartialOnTimeout bool          `json:"partial_on_timeout"`
}

type backfillResponse struct {
	Request backfillRequest `json:"request"`
	Summary backfillSummary `json:"summary"`
	Metrics backfillMetrics `json:"metrics"`
}

func NewServer(client *courtview.Client, store *sqlserver.Store) *Server {
	return &Server{client: client, store: store}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/v1/search/name", s.handleNameSearch)
	mux.HandleFunc("/v1/search/case", s.handleCaseSearch)
	mux.HandleFunc("/v1/admin/backfill/anchorage-criminal", s.handleBackfillAnchorageCriminal)
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

func (s *Server) handleBackfillAnchorageCriminal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.store == nil || !s.store.Enabled() {
		writeError(w, http.StatusServiceUnavailable, "database is not enabled")
		return
	}

	req, ok := parseBackfillOptions(w, r)
	if !ok {
		return
	}

	startedAt := time.Now().UTC()
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(req.TimeoutSeconds)*time.Second)
	defer cancel()

	caseNumberSet := make(map[string]struct{})
	caseNumbers := make([]string, 0, req.Count)
	searchErrors := 0
	expandErrors := 0
	persistErrors := 0
	persistedCases := 0
	changedCases := 0
	unchangedCases := 0
	errorSample := make([]string, 0, 10)
	lastSequence := req.StartSequence - 1

	attemptSamples := make([]float64, 0, req.MaxAttempts)
	searchSamples := make([]float64, 0, req.MaxAttempts)
	expandSamples := make([]float64, 0, req.MaxAttempts)
	persistSamples := make([]float64, 0, req.MaxAttempts)
	partialOnTimeout := false

	for i := 0; i < req.MaxAttempts && len(caseNumbers) < req.Count; i++ {
		if ctx.Err() != nil {
			partialOnTimeout = true
			break
		}

		seq := req.StartSequence + i
		lastSequence = seq
		caseNumber := formatAnchorageCriminalCaseNumber(req.YearTwoDigits, seq)
		attemptStart := time.Now()

		searchStart := time.Now()
		resp, err := s.client.SearchByCaseNumber(ctx, caseNumber, true, 50, true, 20)
		searchSamples = append(searchSamples, time.Since(searchStart).Seconds()*1000)
		if err != nil {
			searchErrors++
			if len(errorSample) < cap(errorSample) {
				errorSample = append(errorSample, fmt.Sprintf("%s search: %v", caseNumber, err))
			}
			attemptSamples = append(attemptSamples, time.Since(attemptStart).Seconds()*1000)
			continue
		}

		if req.IncludeDefendantNetwork {
			expandStart := time.Now()
			relatedCases, _, expandErr := s.expandDefendantNetwork(
				ctx,
				resp.Results.Rows,
				req.MaxRelatedParties,
				req.MaxRelatedCases,
				true,
				20,
			)
			expandSamples = append(expandSamples, time.Since(expandStart).Seconds()*1000)
			if expandErr != nil {
				expandErrors++
				if len(errorSample) < cap(errorSample) {
					errorSample = append(errorSample, fmt.Sprintf("%s expand: %v", caseNumber, expandErr))
				}
			} else {
				resp.Cases = dedupeCases(append(resp.Cases, relatedCases...))
			}
		}

		for _, row := range resp.Results.Rows {
			cn := strings.ToUpper(strings.TrimSpace(row.CaseNumber))
			if cn == "" {
				continue
			}
			if !strings.HasPrefix(cn, fmt.Sprintf("3AN-%02d-", req.YearTwoDigits)) {
				continue
			}
			if !strings.HasSuffix(cn, "CR") {
				continue
			}
			if _, exists := caseNumberSet[cn]; exists {
				continue
			}
			caseNumberSet[cn] = struct{}{}
			caseNumbers = append(caseNumbers, cn)
		}
		for _, c := range resp.Cases {
			cn := strings.ToUpper(strings.TrimSpace(c.CaseNumber))
			if cn == "" {
				continue
			}
			if !strings.HasPrefix(cn, fmt.Sprintf("3AN-%02d-", req.YearTwoDigits)) {
				continue
			}
			if !strings.HasSuffix(cn, "CR") {
				continue
			}
			if _, exists := caseNumberSet[cn]; exists {
				continue
			}
			caseNumberSet[cn] = struct{}{}
			caseNumbers = append(caseNumbers, cn)
		}

		persistStart := time.Now()
		persisted, changed, persistErr := s.persistCaseSearchResponseWithStats(ctx, resp)
		persistSamples = append(persistSamples, time.Since(persistStart).Seconds()*1000)
		if persistErr != nil {
			persistErrors++
			if len(errorSample) < cap(errorSample) {
				errorSample = append(errorSample, fmt.Sprintf("%s persist: %v", caseNumber, persistErr))
			}
		} else {
			persistedCases += persisted
			changedCases += changed
			unchangedCases += (persisted - changed)
		}

		attemptSamples = append(attemptSamples, time.Since(attemptStart).Seconds()*1000)
	}

	if len(caseNumbers) > req.Count {
		caseNumbers = caseNumbers[:req.Count]
	}

	finishedAt := time.Now().UTC()
	totalDurationMS := finishedAt.Sub(startedAt).Seconds() * 1000
	if totalDurationMS < 1 {
		totalDurationMS = 1
	}
	attempts := len(attemptSamples)
	foundCases := len(caseNumbers)

	resp := backfillResponse{
		Request: req,
		Summary: backfillSummary{
			Complete:                  foundCases >= req.Count,
			TargetCases:               req.Count,
			FoundCases:                foundCases,
			AttemptedCaseNumbers:      attempts,
			LastSequenceTried:         lastSequence,
			SearchErrors:              searchErrors,
			ExpandErrors:              expandErrors,
			PersistErrors:             persistErrors,
			PersistedCases:            persistedCases,
			PersistedChangedCases:     changedCases,
			PersistedUnchangedCases:   unchangedCases,
			UniqueCaseNumbersCaptured: caseNumbers,
		},
		Metrics: backfillMetrics{
			StartedAt:        startedAt.Format(time.RFC3339),
			FinishedAt:       finishedAt.Format(time.RFC3339),
			TotalDurationMS:  totalDurationMS,
			AttemptsPerSec:   float64(attempts) / (totalDurationMS / 1000),
			CasesPerSec:      float64(foundCases) / (totalDurationMS / 1000),
			AttemptDuration:  calculateDurationStats(attemptSamples),
			SearchDuration:   calculateDurationStats(searchSamples),
			ExpandDuration:   calculateDurationStats(expandSamples),
			PersistDuration:  calculateDurationStats(persistSamples),
			ErrorSample:      errorSample,
			PartialOnTimeout: partialOnTimeout,
		},
	}

	status := http.StatusOK
	if !resp.Summary.Complete {
		status = http.StatusPartialContent
	}
	writeJSON(w, status, resp)
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
	_, _, err := s.persistCaseSearchResponseWithStats(ctx, resp)
	return err
}

func (s *Server) persistCaseSearchResponseWithStats(ctx context.Context, resp courtview.CaseSearchResponse) (int, int, error) {
	if s.store == nil || !s.store.Enabled() {
		return 0, 0, nil
	}
	persisted := 0
	changed := 0
	for _, caseDetails := range resp.Cases {
		if strings.TrimSpace(caseDetails.CaseNumber) == "" {
			continue
		}
		rows := rowsForCase(resp.Results.Rows, caseDetails.CaseNumber)
		wasChanged, err := s.store.UpsertCase(ctx, caseDetails, rows)
		if err != nil {
			return persisted, changed, err
		}
		persisted++
		if wasChanged {
			changed++
		}
	}
	return persisted, changed, nil
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

func parseBackfillOptions(w http.ResponseWriter, r *http.Request) (backfillRequest, bool) {
	nowYear := time.Now().UTC().Year() % 100
	req := backfillRequest{
		Count:                   100,
		YearTwoDigits:           nowYear,
		StartSequence:           1,
		MaxAttempts:             5000,
		TimeoutSeconds:          900,
		IncludeDefendantNetwork: false,
		MaxRelatedParties:       10,
		MaxRelatedCases:         100,
	}

	if raw := strings.TrimSpace(r.URL.Query().Get("count")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 500 {
			writeError(w, http.StatusBadRequest, "count must be an integer between 1 and 500")
			return backfillRequest{}, false
		}
		req.Count = n
	}

	if raw := strings.TrimSpace(r.URL.Query().Get("year")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "year must be an integer")
			return backfillRequest{}, false
		}
		switch {
		case n >= 2000 && n <= 2099:
			req.YearTwoDigits = n % 100
		case n >= 0 && n <= 99:
			req.YearTwoDigits = n
		default:
			writeError(w, http.StatusBadRequest, "year must be between 2000-2099 or 0-99")
			return backfillRequest{}, false
		}
	}

	if raw := strings.TrimSpace(r.URL.Query().Get("start_seq")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 99999 {
			writeError(w, http.StatusBadRequest, "start_seq must be an integer between 1 and 99999")
			return backfillRequest{}, false
		}
		req.StartSequence = n
	}

	if raw := strings.TrimSpace(r.URL.Query().Get("max_attempts")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < req.Count || n > 50000 {
			writeError(w, http.StatusBadRequest, "max_attempts must be an integer between count and 50000")
			return backfillRequest{}, false
		}
		req.MaxAttempts = n
	}

	if raw := strings.TrimSpace(r.URL.Query().Get("timeout_seconds")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 10 || n > 7200 {
			writeError(w, http.StatusBadRequest, "timeout_seconds must be an integer between 10 and 7200")
			return backfillRequest{}, false
		}
		req.TimeoutSeconds = n
	}

	if raw := strings.TrimSpace(r.URL.Query().Get("include_defendant_network")); raw != "" {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "include_defendant_network must be true or false")
			return backfillRequest{}, false
		}
		req.IncludeDefendantNetwork = b
	}

	if raw := strings.TrimSpace(r.URL.Query().Get("max_related_parties")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			writeError(w, http.StatusBadRequest, "max_related_parties must be an integer between 1 and 100")
			return backfillRequest{}, false
		}
		req.MaxRelatedParties = n
	}

	if raw := strings.TrimSpace(r.URL.Query().Get("max_related_cases")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 1000 {
			writeError(w, http.StatusBadRequest, "max_related_cases must be an integer between 1 and 1000")
			return backfillRequest{}, false
		}
		req.MaxRelatedCases = n
	}

	return req, true
}

func formatAnchorageCriminalCaseNumber(yearTwoDigits, seq int) string {
	return fmt.Sprintf("3AN-%02d-%05dCR", yearTwoDigits%100, seq)
}

func calculateDurationStats(samples []float64) durationStats {
	if len(samples) == 0 {
		return durationStats{}
	}
	copied := make([]float64, len(samples))
	copy(copied, samples)
	sort.Float64s(copied)

	sum := 0.0
	for _, v := range copied {
		sum += v
	}

	return durationStats{
		Count: len(copied),
		MinMS: copied[0],
		AvgMS: sum / float64(len(copied)),
		P50MS: percentile(copied, 50),
		P90MS: percentile(copied, 90),
		P95MS: percentile(copied, 95),
		MaxMS: copied[len(copied)-1],
	}
}

func percentile(sorted []float64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	index := (float64(p) / 100.0) * float64(len(sorted)-1)
	lower := int(index)
	upper := lower + 1
	if upper >= len(sorted) {
		return sorted[lower]
	}
	frac := index - float64(lower)
	return sorted[lower] + frac*(sorted[upper]-sorted[lower])
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
