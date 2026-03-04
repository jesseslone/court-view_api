package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"courtview_lookup/internal/api"
	"courtview_lookup/internal/courtview"
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

	initCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	store, err := sqlserver.NewFromEnv(initCtx)
	if err != nil {
		log.Fatalf("failed to initialize sqlserver store: %v", err)
	}
	defer store.Close()

	server := api.NewServer(client, store)
	log.Printf("courtview api listening on %s", addr)
	if err := http.ListenAndServe(addr, server.Handler()); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}
