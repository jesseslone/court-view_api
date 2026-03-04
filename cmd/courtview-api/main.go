package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	"courtview_lookup/internal/api"
	"courtview_lookup/internal/courtview"
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

	server := api.NewServer(client)
	log.Printf("courtview api listening on %s", addr)
	if err := http.ListenAndServe(addr, server.Handler()); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}
