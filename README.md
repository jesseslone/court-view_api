# CourtView Scraper API (Go)

Production-oriented API for Alaska CourtView with:

- direct CourtView scraping (no runtime browser)
- case-number normalization
- pagination traversal
- SQL Server (T-SQL) persistence and retention

## Endpoints

- `GET /healthz`
- `GET /v1/search/name`
  - required: `first`, `last`
  - optional:
    - `dob`
    - `include_cases` (default `true`)
    - `max_cases` (default `25`)
    - `all_pages` (default `true`)
    - `max_pages` (default `20`)
- `GET /v1/search/case`
  - required: `case_number` (or `case`)
  - optional:
    - `include_cases` (default `true`)
    - `max_cases` (default `25`)
    - `all_pages` (default `true`)
    - `max_pages` (default `20`)
    - `include_defendant_network` (default `true`)
    - `max_related_parties` (default `10`)
    - `max_related_cases` (default `100`)

When using `search/case`, the API identifies defendant/co-defendant parties on the case and expands to their additional case records.

## Case-number normalization

Examples:

- `3ANS11123CR` -> `3AN-11-00123CR`
- `3AN1100123` -> `3AN-11-00123CR`
- `3KE-25-184cr` -> `3KE-25-00184CR`

## SQL Server behavior

On startup (when DB is enabled), the service:

1. ensures database exists
2. applies schema
3. sets DB file max size defaults (`100MB` total budget split across data/log files, default `90MB` data + `10MB` log)

On writes, the service:

- compares case payload hash (`payload_hash`) to existing record
- updates only when changed (or touches query timestamp if unchanged)
- purges oldest case records as capacity is approached (oldest `last_query_at` first)

## Local run (without DB)

```bash
go mod tidy
go test ./...
go run ./cmd/courtview-api
```

## Docker Compose (API + SQL Server)

```bash
# optional: provide a strong SA password
export MSSQL_SA_PASSWORD='YourStrongPassword!123'

docker compose up --build -d
```

Services:

- API: `http://localhost:8088`
- SQL Server: `localhost:14333`

## Environment variables

Core:

- `SERVICE_ADDR` (default `:8088`)
- `COURTVIEW_BASE_URL` (default `https://records.courts.alaska.gov/eaccess/home.page.2`)

DB:

- `DB_ENABLED` (`true`/`false`)
- `DB_HOST` (default `sqlserver`)
- `DB_PORT` (default `1433`)
- `DB_USER` (default `sa`)
- `DB_PASSWORD` (required when DB enabled)
- `DB_NAME` (default `courtview`)
- `DB_ENCRYPT` (default `disable`)
- `DB_MAX_SIZE_MB` (default `100`, total SQL data+log file budget in MB)
- `DB_LOG_MAX_SIZE_MB` (default `10`, log file budget in MB)
- `DB_PURGE_TARGET_MB` (default `80`, data-file usage target after purge)

## T-SQL schema reference

- `sql/schema.sql`

## Clean history

- `docs/clean-history.md`
