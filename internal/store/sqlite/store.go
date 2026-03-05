package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"courtview_lookup/internal/courtview"

	_ "modernc.org/sqlite"
)

type Store struct {
	db            *sql.DB
	enabled       bool
	dbPath        string
	maxSizeMB     int
	purgeTargetMB int
}

func NewFromEnv(ctx context.Context) (*Store, error) {
	enabled := true
	if raw := strings.TrimSpace(os.Getenv("DB_ENABLED")); raw != "" {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("parse DB_ENABLED: %w", err)
		}
		enabled = b
	}
	if !enabled {
		return &Store{enabled: false}, nil
	}

	dbPath := strings.TrimSpace(os.Getenv("SQLITE_PATH"))
	if dbPath == "" {
		dbPath = "data/courtview.sqlite"
	}
	if dbPath != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			return nil, fmt.Errorf("create sqlite directory: %w", err)
		}
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite db: %w", err)
	}

	pragmas := []string{
		`PRAGMA journal_mode=WAL;`,
		`PRAGMA synchronous=NORMAL;`,
		`PRAGMA busy_timeout=5000;`,
		`PRAGMA foreign_keys=ON;`,
	}
	for _, stmt := range pragmas {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("apply sqlite pragma: %w", err)
		}
	}

	if err := runMigrations(ctx, db); err != nil {
		db.Close()
		return nil, err
	}

	maxSizeMB := intFromEnv("DB_MAX_SIZE_MB", 100)
	if maxSizeMB < 10 {
		maxSizeMB = 10
	}
	purgeTargetMB := intFromEnv("DB_PURGE_TARGET_MB", 80)
	if purgeTargetMB < 1 {
		purgeTargetMB = maxSizeMB - 10
	}
	if purgeTargetMB >= maxSizeMB {
		purgeTargetMB = maxSizeMB - 5
	}
	if purgeTargetMB < 1 {
		purgeTargetMB = 1
	}

	return &Store{
		db:            db,
		enabled:       true,
		dbPath:        dbPath,
		maxSizeMB:     maxSizeMB,
		purgeTargetMB: purgeTargetMB,
	}, nil
}

func (s *Store) Enabled() bool {
	return s != nil && s.enabled && s.db != nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) UpsertCase(ctx context.Context, caseDetails courtview.CaseDetails, rows []courtview.SearchResultRow) (bool, error) {
	if !s.Enabled() {
		return false, nil
	}
	caseNumber := strings.TrimSpace(caseDetails.CaseNumber)
	if caseNumber == "" {
		return false, errors.New("cannot persist case without case_number")
	}

	payload, err := json.Marshal(caseDetails)
	if err != nil {
		return false, fmt.Errorf("marshal case payload: %w", err)
	}
	hash := sha256.Sum256(payload)
	hashHex := hex.EncodeToString(hash[:])

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var existingHash string
	err = tx.QueryRowContext(ctx, `
SELECT payload_hash
FROM case_records
WHERE case_number = ?
`, caseNumber).Scan(&existingHash)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("read existing case hash: %w", err)
	}

	changed := false
	now := time.Now().UTC().Format(time.RFC3339Nano)
	sourceURL := strings.TrimSpace(caseDetails.CaseURL)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		changed = true
		if _, err := tx.ExecContext(ctx, `
INSERT INTO case_records (
    case_number, source_url, payload, payload_hash, created_at, updated_at, last_query_at,
    last_observed_change_at, last_successful_payload_hash, last_successful_at, last_scrape_had_errors, last_scrape_error_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
`, caseNumber, sourceURL, string(payload), hashHex, now, now, now, now, hashHex, now); err != nil {
			return false, fmt.Errorf("insert case_record: %w", err)
		}
	case existingHash == hashHex:
		if _, err := tx.ExecContext(ctx, `
UPDATE case_records
SET last_query_at = ?
WHERE case_number = ?
`, now, caseNumber); err != nil {
			return false, fmt.Errorf("touch case_record: %w", err)
		}
	default:
		changed = true
		if _, err := tx.ExecContext(ctx, `
UPDATE case_records
SET source_url = ?,
    payload = ?,
    payload_hash = ?,
    updated_at = ?,
    last_query_at = ?,
    last_observed_change_at = ?,
    last_successful_payload_hash = ?,
    last_successful_at = ?,
    last_scrape_had_errors = 0,
    last_scrape_error_at = NULL
WHERE case_number = ?
`, sourceURL, string(payload), hashHex, now, now, now, hashHex, now, caseNumber); err != nil {
			return false, fmt.Errorf("update case_record: %w", err)
		}
	}

	if changed {
		if _, err := tx.ExecContext(ctx, `DELETE FROM case_parties WHERE case_number = ?`, caseNumber); err != nil {
			return false, fmt.Errorf("clear case parties: %w", err)
		}

		parties := courtview.ExtractParties(rows)
		for _, party := range parties {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO case_parties (
    case_number, role, full_name, first_name, last_name, dob, normalized_name, last_seen_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`, caseNumber, party.Role, party.FullName, party.FirstName, party.LastName, party.DOB, party.NormalizedName, now); err != nil {
				return false, fmt.Errorf("insert case party: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit case upsert: %w", err)
	}
	if err := s.PurgeIfNeeded(ctx); err != nil {
		return changed, err
	}
	return changed, nil
}

func (s *Store) GetCase(ctx context.Context, caseNumber string) (*courtview.CaseDetails, bool, error) {
	if !s.Enabled() {
		return nil, false, nil
	}
	caseNumber = strings.TrimSpace(caseNumber)
	if caseNumber == "" {
		return nil, false, nil
	}

	var payload string
	err := s.db.QueryRowContext(ctx, `
SELECT payload
FROM case_records
WHERE case_number = ?
`, caseNumber).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("fetch cached case: %w", err)
	}

	var details courtview.CaseDetails
	if err := json.Unmarshal([]byte(payload), &details); err != nil {
		return nil, false, fmt.Errorf("decode cached case payload: %w", err)
	}
	return &details, true, nil
}

func (s *Store) CandidateCaseNumbersForParty(ctx context.Context, party courtview.PartyRecord, maxCases int) ([]string, error) {
	if !s.Enabled() || maxCases <= 0 {
		return nil, nil
	}
	normalizedName := strings.TrimSpace(strings.ToLower(party.NormalizedName))
	if normalizedName == "" {
		return nil, nil
	}
	dob := strings.TrimSpace(party.DOB)

	rows, err := s.db.QueryContext(ctx, `
SELECT case_number
FROM case_parties
WHERE normalized_name = ?
  AND (? = '' OR dob = ? OR dob = '')
GROUP BY case_number
ORDER BY MAX(last_seen_at) DESC
LIMIT ?
`, normalizedName, dob, dob, maxCases)
	if err != nil {
		return nil, fmt.Errorf("query candidate cases for party: %w", err)
	}
	defer rows.Close()

	out := make([]string, 0, maxCases)
	for rows.Next() {
		var caseNumber string
		if err := rows.Scan(&caseNumber); err != nil {
			return nil, fmt.Errorf("scan candidate case: %w", err)
		}
		caseNumber = strings.TrimSpace(caseNumber)
		if caseNumber != "" {
			out = append(out, caseNumber)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate candidate cases: %w", err)
	}
	return out, nil
}

func (s *Store) PurgeIfNeeded(ctx context.Context) error {
	if !s.Enabled() || s.dbPath == ":memory:" {
		return nil
	}
	info, err := os.Stat(s.dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat sqlite db file: %w", err)
	}

	maxBytes := int64(s.maxSizeMB) * 1024 * 1024
	targetBytes := int64(s.purgeTargetMB) * 1024 * 1024
	if info.Size() <= maxBytes {
		return nil
	}

	for i := 0; i < 20; i++ {
		if _, err := s.db.ExecContext(ctx, `
DELETE FROM case_records
WHERE case_number IN (
    SELECT case_number
    FROM case_records
    ORDER BY last_query_at ASC
    LIMIT 25
)
`); err != nil {
			return fmt.Errorf("purge oldest case records: %w", err)
		}
		if _, err := s.db.ExecContext(ctx, `VACUUM`); err != nil {
			return fmt.Errorf("vacuum sqlite db: %w", err)
		}
		info, err = os.Stat(s.dbPath)
		if err != nil {
			return fmt.Errorf("stat sqlite db file: %w", err)
		}
		if info.Size() <= targetBytes {
			return nil
		}
	}
	if info.Size() > targetBytes {
		return fmt.Errorf("sqlite db remains above purge target: %.2fMB", float64(info.Size())/1024.0/1024.0)
	}
	return nil
}

func runMigrations(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS case_records (
    case_number TEXT PRIMARY KEY,
    source_url TEXT,
    payload TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_query_at TEXT NOT NULL,
    last_observed_change_at TEXT NOT NULL,
    last_successful_payload_hash TEXT,
    last_successful_at TEXT,
    last_scrape_had_errors INTEGER NOT NULL DEFAULT 0,
    last_scrape_error_at TEXT
);`,
		`CREATE TABLE IF NOT EXISTS case_parties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_number TEXT NOT NULL,
    role TEXT,
    full_name TEXT,
    first_name TEXT,
    last_name TEXT,
    dob TEXT,
    normalized_name TEXT,
    last_seen_at TEXT NOT NULL,
    FOREIGN KEY (case_number) REFERENCES case_records(case_number) ON DELETE CASCADE
);`,
		`CREATE INDEX IF NOT EXISTS idx_case_records_last_query_at
ON case_records (last_query_at ASC);`,
		`CREATE INDEX IF NOT EXISTS idx_case_records_last_observed_change_at
ON case_records (last_observed_change_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_case_parties_normalized_name
ON case_parties (normalized_name, last_seen_at DESC);`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("run sqlite migration: %w", err)
		}
	}
	return nil
}

func intFromEnv(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
