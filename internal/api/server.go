package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"courtview_lookup/internal/courtview"
)

type Server struct {
	client *courtview.Client
}

func NewServer(client *courtview.Client) *Server {
	return &Server{client: client}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/v1/search/name", s.handleNameSearch)
	mux.HandleFunc("/v1/search/case", s.handleCaseSearch)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
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

	resp, err := s.client.SearchByCaseNumber(ctx, rawCaseNumber, includeCases, maxCases, allPages, maxPages)
	if err != nil {
		status := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		if strings.Contains(strings.ToLower(err.Error()), "normalize case number") {
			status = http.StatusBadRequest
		}
		writeError(w, status, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, resp)
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
		if err != nil || n < 1 || n > 200 {
			writeError(w, http.StatusBadRequest, "max_cases must be an integer between 1 and 200")
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
		if err != nil || n < 1 || n > 100 {
			writeError(w, http.StatusBadRequest, "max_pages must be an integer between 1 and 100")
			return false, 0, false
		}
		maxPages = n
	}
	return allPages, maxPages, true
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
