# CourtView Scraper API (Go)

Production-oriented API for Alaska CourtView with:

- direct CourtView scraping (no runtime browser)
- case-number normalization
- pagination traversal
- default SQLite quick-start persistence
- SQL Server (T-SQL) persistence and retention option
- built-in proof-of-concept web UI (`/ui`)

## Endpoints

- `GET /healthz`
- `GET /ui` (redirects to `/ui/`)
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
- `POST /v1/admin/backfill/anchorage-criminal` (also accepts `GET`)
  - purpose: pull and persist the first N Anchorage criminal (`CR`) cases for a given year
  - defaults: `count=100`, `year=current year`, `start_seq=1`, `max_attempts=5000`
  - optional:
    - `timeout_seconds` (default `900`)
    - `concurrency` (default `1`, max `24`)
    - `include_defendant_network` (default `false`)
    - `max_related_parties` (default `10`)
    - `max_related_cases` (default `100`)

When using `search/case`, the API identifies defendant/co-defendant parties on the case and expands to their additional case records.

Example backfill call:

```bash
curl -X POST "http://localhost:8088/v1/admin/backfill/anchorage-criminal?count=100"
```

Concurrent backfill example:

```bash
curl -X POST "http://localhost:8088/v1/admin/backfill/anchorage-criminal?count=100&concurrency=6"
```

Backfill response includes pull metrics:

- total duration
- attempts/sec and cases/sec
- stage timing stats (`attempt`, `search`, optional `expand`, `persist`) with min/avg/p50/p90/p95/max
- error counts and sample errors

## Party data

- `case_parties` stores extracted parties for each case (for example `Defendant`, `Prosecution`).
- Prosecution rows can be entities like `State of Alaska` or `Municipality of Anchorage`.
- Defendant expansion/backfill targeting now filters to likely person defendants and excludes government/entity names.

## Case-number normalization

Examples:

- `3ANS11123CR` -> `3AN-11-00123CR`
- `3AN1100123` -> `3AN-11-00123CR`
- `3KE-25-184cr` -> `3KE-25-00184CR`

## Storage behavior

The API supports:

- `DB_PROVIDER=sqlite` (default, lightweight quick start)
- `DB_PROVIDER=sqlserver` (existing or dedicated MSSQL server)

On startup (when DB is enabled), the service:

1. ensures database exists
2. applies schema
3. applies retention defaults (`DB_MAX_SIZE_MB=100`, `DB_PURGE_TARGET_MB=80`)

On writes, the service:

- compares case payload hash (`payload_hash`) to existing record
- updates detailed case data only when payload hash changes (otherwise only touches query/snapshot timestamps)
- uses `Current/All Information` as the default authoritative case page (`COURTVIEW_FETCH_CASE_TABS=false`)
- optionally fetches case sub-tabs when `COURTVIEW_FETCH_CASE_TABS=true` (best-effort, slower, and more prone to CourtView 5xx responses)
- populates `cv_charges`/`cv_charge_dispositions` from structured tables, with text fallback from `Current/All Information` when charge tabs fail
- tracks `last_observed_change_at` when a case/person profile materially changes
- tracks `last_successful_payload_hash` and skips replacing good payloads with transient tab-fetch failure payloads
- purges oldest case records as capacity is approached (oldest `last_query_at` first)

## Local run (quick start)

```bash
go mod tidy
go test ./...
go run ./cmd/courtview-api
```

This defaults to SQLite at `./data/courtview.sqlite`.

UI:

- `http://localhost:8088/ui/`

## Python examples

Scripts are in `examples/python/` and use only Python stdlib (no extra deps).

- `courtview_api_client.py`:
  - minimal client for `health`, `name`, `case`, and `backfill` endpoints
- `criminal_charge_report.py`:
  - lookup by `--case-number` OR `--first/--last` (+ optional `--dob`, `--atn`)
  - returns criminal defendant records
  - includes only non-dismissed charges
  - labels each charge as `original`, `amended`, `downgraded`, or `amended_downgraded`
  - emits case-level conviction flag
- `runtime_api_tests.py`:
  - runs runtime integration checks with dynamically discovered defendants
  - discovers subjects from backfilled Anchorage criminal cases
  - prints only redacted subject IDs (no names in test output)

Examples:

```bash
# 1) Case-number report (JSON to stdout, CSV to file)
python3 examples/python/criminal_charge_report.py \
  --base-url http://localhost:8088 \
  --case-number 3AN-26-00001CR \
  --include-defendant-network \
  --csv-out examples/python/output/case-report.csv

# 2) Name+DOB (+ optional ATN) report
python3 examples/python/criminal_charge_report.py \
  --base-url http://localhost:8088 \
  --first Jane --last Doe --dob 01/02/1990 --atn 12345678

# 3) Runtime integration checks against live API+DB
python3 examples/python/runtime_api_tests.py \
  --base-url http://localhost:8088 \
  --year 2026 \
  --backfill-count 25 \
  --subject-count 3 \
  --json-out examples/python/output/runtime-tests.json

# 4) Fast local unit tests for extraction logic (no live data needed)
python3 -m unittest discover -s examples/python -p 'test_*.py' -v
```

Outside-container benchmark example:

```bash
# terminal 1: run API on host using local SQL Server container
DB_PROVIDER=sqlserver DB_ENABLED=true DB_HOST=localhost DB_PORT=14333 DB_USER=sa DB_PASSWORD='YourStrongPassword!123' \
go run ./cmd/courtview-api

# terminal 2: compare sequential vs concurrent
curl -s -X POST "http://localhost:8088/v1/admin/backfill/anchorage-criminal?count=50&concurrency=1" | jq '.metrics'
curl -s -X POST "http://localhost:8088/v1/admin/backfill/anchorage-criminal?count=50&concurrency=6" | jq '.metrics'
```

## Docker quick start (SQLite default)

```bash
docker compose -f docker-compose.quickstart.yml up --build -d
```

Service:

- API + UI: `http://localhost:8088` (`/ui/` for the web interface)

## Docker Compose (API + SQL Server)

```bash
# optional: provide a strong SA password
export MSSQL_SA_PASSWORD='YourStrongPassword!123'

# preflight: verify Docker VM free space for your SQL size budget
./scripts/preflight_docker_storage.sh

docker compose up --build -d
```

Services:

- API: `http://localhost:8088`
- SQL Server: `localhost:14333`

## Architecture (amd64 + arm64)

- The API Docker image now builds for the target platform (`amd64` or `arm64`) automatically.
- To publish a multi-arch API image:

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t yourrepo/courtview-api:latest \
  --push .
```

- If you are on Apple Silicon and need SQL Server container compatibility, you can run compose under amd64 emulation:

```bash
export DOCKER_DEFAULT_PLATFORM=linux/amd64
docker compose up --build -d
```

## Environment variables

Core:

- `SERVICE_ADDR` (default `:8088`)
- `COURTVIEW_BASE_URL` (default `https://records.courts.alaska.gov/eaccess/home.page.2`)
- `COURTVIEW_FETCH_CASE_TABS` (default `false`; set `true` to crawl case sub-tabs in addition to `All Information`)

Provider selection:

- `DB_PROVIDER` (`sqlite` default, `sqlserver`, `none`)
- `DB_ENABLED` (`true`/`false`)

SQLite:

- `SQLITE_PATH` (default `data/courtview.sqlite`)
- `DB_MAX_SIZE_MB` (default `100`)
- `DB_PURGE_TARGET_MB` (default `80`)

SQL Server:

- `DB_HOST` (default `sqlserver`)
- `DB_PORT` (default `1433`)
- `DB_USER` (default `sa`)
- `DB_PASSWORD` (required when DB enabled)
- `DB_NAME` (default `courtview`)
- `DB_ENCRYPT` (default `disable`)
- `DB_TRUST_SERVER_CERTIFICATE` (default `true`; set to `false` for strict TLS validation)
- `DB_MAX_SIZE_MB` (default `100`, total SQL data+log file budget in MB)
- `DB_LOG_MAX_SIZE_MB` (default `10`, log file budget in MB)
- `DB_PURGE_TARGET_MB` (default `80`, data-file usage target after purge)

Container image defaults:

- `DB_MAX_SIZE_MB=100`
- `DB_LOG_MAX_SIZE_MB=10`
- `DB_PURGE_TARGET_MB=80`

Override at runtime with `docker run -e ...` or in compose env.

Docker storage preflight (for local SQL container):

- `DOCKER_HEADROOM_MB` (default `2048`): extra MB required above data/log budgets
- `DOCKER_SPACE_PROBE_IMAGE` (default `alpine:latest`): image used to run `df` inside Docker VM
- check command:

```bash
DB_MAX_SIZE_MB=100 DB_LOG_MAX_SIZE_MB=10 DOCKER_HEADROOM_MB=2048 \
./scripts/preflight_docker_storage.sh
```

## SQL Server hardening options

- See [`docs/mssql-setup.md`](docs/mssql-setup.md) for:
  - Option A (private-only network + TLS validation + least-privilege SQL login)
  - Option B (Microsoft Entra/passwordless, when your SQL target supports it)
  - Microsoft documentation links for each SQL Server-side setup step

## T-SQL schema reference

- `sql/schema.sql`
- `sql/schema_detailed.sql` (expanded analytics/reporting model for parties, charges, dispositions, events, dockets, and raw tab snapshots)
  - core detailed tables (`cv_cases`, `cv_case_snapshots`, `cv_people`, `cv_person_aliases`, `cv_person_dobs`, `cv_case_parties`, `cv_case_events`, `cv_docket_entries`, `cv_tab_*`) are auto-migrated and populated by the API store layer

## Clean history

- `docs/clean-history.md`
