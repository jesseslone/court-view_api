# CourtView Scraper API (Go)

Production-oriented API service for querying Alaska CourtView and returning structured JSON.

## Current capabilities

- `GET /healthz`
- `GET /v1/search/name`
  - required: `first`, `last`
  - optional: `dob`, `include_cases`, `max_cases`, `all_pages`, `max_pages`
- `GET /v1/search/case`
  - required: `case_number` (or `case`)
  - optional: `include_cases`, `max_cases`, `all_pages`, `max_pages`

## Case-number normalization

Common malformed case-number inputs are normalized before search.

Examples:

- `3ANS11123CR` -> `3AN-11-00123CR`
- `3AN1100123` -> `3AN-11-00123CR`
- `3KE-25-184cr` -> `3KE-25-00184CR`

## Pagination behavior

Pagination is enabled by default.

- `all_pages` default: `true`
- `max_pages` default: `20`

The API response includes:

- `results`: aggregated rows across collected pages
- `results_pages`: per-page raw result payloads

## Local run

```bash
go mod tidy
go test ./...
go run ./cmd/courtview-api
```

Default bind: `:8088`

Environment variables:

- `SERVICE_ADDR` (default `:8088`)
- `COURTVIEW_BASE_URL` (default `https://records.courts.alaska.gov/eaccess/home.page.2`)

## Docker

Build and run:

```bash
docker build -t courtview-api:latest .
docker run --rm -p 8088:8088 --name courtview-api courtview-api:latest
```

With compose:

```bash
docker compose up --build -d
```

## Database schema

Suggested relational schema for persistence and sync metadata:

- `sql/schema.sql`

Includes:

- person/case/charge/disposition/event/docket tables
- `source_hash` fields for change detection
- sync run/error tables

## Clean-repo guidance

If you need guaranteed clean history, create a brand-new repository from this sanitized working tree and do not push old history.

Reference steps are in `docs/clean-history.md`.
