package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"courtview_lookup/internal/api"
	"courtview_lookup/internal/courtview"
	storeiface "courtview_lookup/internal/store"
	"courtview_lookup/internal/store/sqlite"
	"courtview_lookup/internal/store/sqlserver"
)

func main() {
	baseURL := strings.TrimSpace(os.Getenv("COURTVIEW_BASE_URL"))
	if baseURL == "" {
		baseURL = "https://records.courts.alaska.gov/eaccess/home.page.2"
	}
	addr := strings.TrimSpace(os.Getenv("SERVICE_ADDR"))
	if addr == "" {
		addr = ":8088"
	}

	client, err := courtview.NewClient(baseURL)
	if err != nil {
		log.Fatalf("failed to create courtview client: %v", err)
	}
	fetchCaseTabs, err := boolFromEnv("COURTVIEW_FETCH_CASE_TABS", false)
	if err != nil {
		log.Fatalf("invalid COURTVIEW_FETCH_CASE_TABS: %v", err)
	}
	client.SetFetchCaseTabs(fetchCaseTabs)

	initCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	provider := strings.ToLower(strings.TrimSpace(os.Getenv("DB_PROVIDER")))
	if provider == "" {
		provider = "sqlite"
	}

	var store storeiface.Store
	switch provider {
	case "sqlite":
		sqliteStore, err := sqlite.NewFromEnv(initCtx)
		if err != nil {
			log.Fatalf("failed to initialize sqlite store: %v", err)
		}
		store = sqliteStore
	case "sqlserver", "mssql":
		sqlStore, err := sqlserver.NewFromEnv(initCtx)
		if err != nil {
			log.Fatalf("failed to initialize sqlserver store: %v", err)
		}
		store = sqlStore
	case "none", "disabled":
		store = nil
	default:
		log.Fatalf("unsupported DB_PROVIDER %q (supported: sqlite, sqlserver, none)", provider)
	}
	if store != nil {
		defer store.Close()
	}

	server := api.NewServer(client, store)
	log.Printf("courtview api listening on %s", addr)
	if err := http.ListenAndServe(addr, server.Handler()); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}

func boolFromEnv(key string, defaultValue bool) (bool, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return defaultValue, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, err
	}
	return b, nil
}
