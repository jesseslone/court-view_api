package sqlserver

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"courtview_lookup/internal/courtview"

	_ "github.com/microsoft/go-mssqldb"
)

type Store struct {
	db            *sql.DB
	enabled       bool
	maxSizeMB     int
	dataMaxSizeMB int
	logMaxSizeMB  int
	purgeTargetMB int
}

func NewFromEnv(ctx context.Context) (*Store, error) {
	host := strings.TrimSpace(os.Getenv("DB_HOST"))
	enabled := true
	if raw := strings.TrimSpace(os.Getenv("DB_ENABLED")); raw != "" {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("parse DB_ENABLED: %w", err)
		}
		enabled = b
	} else if host == "" {
		enabled = false
	}
	if !enabled {
		return &Store{enabled: false}, nil
	}

	if host == "" {
		host = "sqlserver"
	}
	port := intFromEnv("DB_PORT", 1433)
	user := stringOrDefault("DB_USER", "sa")
	password := strings.TrimSpace(os.Getenv("DB_PASSWORD"))
	if password == "" {
		return nil, errors.New("DB_PASSWORD is required when DB is enabled")
	}
	dbName := stringOrDefault("DB_NAME", "courtview")
	encrypt := stringOrDefault("DB_ENCRYPT", "disable")
	trustServerCertificate := boolFromEnv("DB_TRUST_SERVER_CERTIFICATE", true)
	maxSizeMB := intFromEnv("DB_MAX_SIZE_MB", 100)
	if maxSizeMB < 30 {
		maxSizeMB = 30
	}
	logMaxSizeMB := intFromEnv("DB_LOG_MAX_SIZE_MB", 10)
	if logMaxSizeMB < 5 {
		logMaxSizeMB = 5
	}
	if logMaxSizeMB >= maxSizeMB-10 {
		logMaxSizeMB = max(5, maxSizeMB/5)
		if logMaxSizeMB >= maxSizeMB {
			logMaxSizeMB = 5
		}
	}
	dataMaxSizeMB := maxSizeMB - logMaxSizeMB
	if dataMaxSizeMB < 20 {
		dataMaxSizeMB = 20
		logMaxSizeMB = max(5, maxSizeMB-dataMaxSizeMB)
	}

	purgeTarget := intFromEnv("DB_PURGE_TARGET_MB", dataMaxSizeMB-10)
	if purgeTarget < 10 {
		purgeTarget = 10
	}
	if purgeTarget >= dataMaxSizeMB {
		purgeTarget = dataMaxSizeMB - 5
	}
	if purgeTarget < 1 {
		purgeTarget = 1
	}

	adminDSN := buildDSN(user, password, host, port, "master", encrypt, trustServerCertificate)
	adminDB, err := sql.Open("sqlserver", adminDSN)
	if err != nil {
		return nil, fmt.Errorf("open sqlserver admin connection: %w", err)
	}
	defer adminDB.Close()

	adminCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := adminDB.PingContext(adminCtx); err != nil {
		return nil, fmt.Errorf("ping sqlserver admin: %w", err)
	}
	if err := ensureDatabase(ctx, adminDB, dbName); err != nil {
		return nil, err
	}

	appDSN := buildDSN(user, password, host, port, dbName, encrypt, trustServerCertificate)
	appDB, err := sql.Open("sqlserver", appDSN)
	if err != nil {
		return nil, fmt.Errorf("open sqlserver app connection: %w", err)
	}
	appDB.SetMaxOpenConns(10)
	appDB.SetMaxIdleConns(5)
	appDB.SetConnMaxLifetime(30 * time.Minute)

	appCtx, appCancel := context.WithTimeout(ctx, 30*time.Second)
	defer appCancel()
	if err := appDB.PingContext(appCtx); err != nil {
		appDB.Close()
		return nil, fmt.Errorf("ping sqlserver app db: %w", err)
	}

	if err := setDatabaseMaxSize(ctx, appDB, dbName, dataMaxSizeMB, logMaxSizeMB); err != nil {
		appDB.Close()
		return nil, err
	}
	if err := runMigrations(ctx, appDB); err != nil {
		appDB.Close()
		return nil, err
	}

	return &Store{
		db:            appDB,
		enabled:       true,
		maxSizeMB:     maxSizeMB,
		dataMaxSizeMB: dataMaxSizeMB,
		logMaxSizeMB:  logMaxSizeMB,
		purgeTargetMB: purgeTarget,
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
	hashPayload, err := json.Marshal(canonicalCaseDetailsForHash(caseDetails))
	if err != nil {
		return false, fmt.Errorf("marshal canonical case payload for hash: %w", err)
	}
	hash := sha256.Sum256(hashPayload)
	hashHex := hex.EncodeToString(hash[:])

	parties := courtview.ExtractParties(rows)
	hasTransientErrors := caseHasTransientTabFailures(caseDetails) || strings.TrimSpace(caseDetails.Error) != ""

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var existingHash string
	var existingSuccessfulHash sql.NullString
	err = tx.QueryRowContext(ctx, `
SELECT payload_hash, last_successful_payload_hash
FROM case_records
WHERE case_number = @p1
`, caseNumber).Scan(&existingHash, &existingSuccessfulHash)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("read existing case hash: %w", err)
	}

	changed := false
	now := time.Now().UTC()
	sourceURL := strings.TrimSpace(caseDetails.CaseURL)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		changed = true
		if hasTransientErrors {
			if _, execErr := tx.ExecContext(ctx, `
INSERT INTO case_records (
    case_number, source_url, payload, payload_hash, created_at, updated_at, last_query_at,
    last_observed_change_at, last_successful_payload_hash, last_successful_at, last_scrape_had_errors, last_scrape_error_at
)
VALUES (
    @p1, @p2, @p3, @p4, @p5, @p5, @p5, @p5, NULL, NULL, 1, @p5
)
`, caseNumber, sourceURL, string(payload), hashHex, now); execErr != nil {
				return false, fmt.Errorf("insert case_record: %w", execErr)
			}
		} else {
			if _, execErr := tx.ExecContext(ctx, `
INSERT INTO case_records (
    case_number, source_url, payload, payload_hash, created_at, updated_at, last_query_at,
    last_observed_change_at, last_successful_payload_hash, last_successful_at, last_scrape_had_errors, last_scrape_error_at
)
VALUES (
    @p1, @p2, @p3, @p4, @p5, @p5, @p5, @p5, @p4, @p5, 0, NULL
)
`, caseNumber, sourceURL, string(payload), hashHex, now); execErr != nil {
				return false, fmt.Errorf("insert case_record: %w", execErr)
			}
		}
	case existingHash == hashHex:
		if hasTransientErrors {
			if _, execErr := tx.ExecContext(ctx, `
UPDATE case_records
SET last_query_at = @p2,
    last_scrape_had_errors = 1,
    last_scrape_error_at = @p2
WHERE case_number = @p1
`, caseNumber, now); execErr != nil {
				return false, fmt.Errorf("touch case_record: %w", execErr)
			}
		} else {
			if _, execErr := tx.ExecContext(ctx, `
UPDATE case_records
SET last_query_at = @p2,
    last_successful_payload_hash = @p3,
    last_successful_at = @p2,
    last_scrape_had_errors = 0,
    last_scrape_error_at = NULL
WHERE case_number = @p1
`, caseNumber, now, hashHex); execErr != nil {
				return false, fmt.Errorf("touch case_record: %w", execErr)
			}
		}
	default:
		if hasTransientErrors && existingSuccessfulHash.Valid && strings.TrimSpace(existingSuccessfulHash.String) != "" {
			if _, execErr := tx.ExecContext(ctx, `
UPDATE case_records
SET last_query_at = @p2,
    last_scrape_had_errors = 1,
    last_scrape_error_at = @p2
WHERE case_number = @p1
`, caseNumber, now); execErr != nil {
				return false, fmt.Errorf("touch degraded case_record: %w", execErr)
			}
			break
		}

		changed = true
		if hasTransientErrors {
			if _, execErr := tx.ExecContext(ctx, `
UPDATE case_records
SET source_url = @p2,
    payload = @p3,
    payload_hash = @p4,
    updated_at = @p5,
    last_query_at = @p5,
    last_observed_change_at = @p5,
    last_scrape_had_errors = 1,
    last_scrape_error_at = @p5
WHERE case_number = @p1
`, caseNumber, sourceURL, string(payload), hashHex, now); execErr != nil {
				return false, fmt.Errorf("update case_record: %w", execErr)
			}
		} else {
			if _, execErr := tx.ExecContext(ctx, `
UPDATE case_records
SET source_url = @p2,
    payload = @p3,
    payload_hash = @p4,
    updated_at = @p5,
    last_query_at = @p5,
    last_observed_change_at = @p5,
    last_successful_payload_hash = @p4,
    last_successful_at = @p5,
    last_scrape_had_errors = 0,
    last_scrape_error_at = NULL
WHERE case_number = @p1
`, caseNumber, sourceURL, string(payload), hashHex, now); execErr != nil {
				return false, fmt.Errorf("update case_record: %w", execErr)
			}
		}
	}

	if changed {
		if _, execErr := tx.ExecContext(ctx, `DELETE FROM case_parties WHERE case_number = @p1`, caseNumber); execErr != nil {
			return false, fmt.Errorf("clear case parties: %w", execErr)
		}

		for _, party := range parties {
			if _, execErr := tx.ExecContext(ctx, `
INSERT INTO case_parties (
    case_number, role, full_name, first_name, last_name, dob, normalized_name, last_seen_at
) VALUES (
    @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8
)
`, caseNumber, party.Role, party.FullName, party.FirstName, party.LastName, party.DOB, party.NormalizedName, now); execErr != nil {
				return false, fmt.Errorf("insert case party: %w", execErr)
			}
		}
	}

	if err := s.upsertDetailedCaseData(ctx, tx, caseDetails, rows, string(payload), hashHex, now, changed); err != nil {
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit case upsert: %w", err)
	}

	if purgeErr := s.PurgeIfNeeded(ctx); purgeErr != nil {
		return changed, purgeErr
	}

	return changed, nil
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
SELECT TOP (@p3) case_number
FROM case_parties
WHERE normalized_name = @p1
  AND (@p2 = N'' OR dob = @p2 OR dob IS NULL OR dob = N'')
GROUP BY case_number
ORDER BY MAX(last_seen_at) DESC
`, normalizedName, dob, maxCases)
	if err != nil {
		return nil, fmt.Errorf("query candidate cases for party: %w", err)
	}
	defer rows.Close()

	caseNumbers := make([]string, 0, maxCases)
	for rows.Next() {
		var caseNumber string
		if scanErr := rows.Scan(&caseNumber); scanErr != nil {
			return nil, fmt.Errorf("scan candidate case number: %w", scanErr)
		}
		caseNumber = strings.TrimSpace(caseNumber)
		if caseNumber != "" {
			caseNumbers = append(caseNumbers, caseNumber)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate candidate case numbers: %w", err)
	}
	return caseNumbers, nil
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
WHERE case_number = @p1
`, caseNumber).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("fetch cached case: %w", err)
	}

	var caseDetails courtview.CaseDetails
	if err := json.Unmarshal([]byte(payload), &caseDetails); err != nil {
		return nil, false, fmt.Errorf("unmarshal cached case payload: %w", err)
	}
	return &caseDetails, true, nil
}

func (s *Store) ListCaseNumbersByNormalizedNames(ctx context.Context, names []string, limit int) ([]string, error) {
	if !s.Enabled() {
		return nil, nil
	}
	if len(names) == 0 {
		return []string{}, nil
	}
	if limit <= 0 {
		limit = 200
	}

	cleaned := make([]string, 0, len(names))
	seen := make(map[string]struct{})
	for _, n := range names {
		n = strings.TrimSpace(strings.ToLower(n))
		if n == "" {
			continue
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		cleaned = append(cleaned, n)
	}
	if len(cleaned) == 0 {
		return []string{}, nil
	}

	sort.Strings(cleaned)
	args := make([]any, 0, len(cleaned)+1)
	args = append(args, limit)
	placeholders := make([]string, 0, len(cleaned))
	for i, n := range cleaned {
		placeholders = append(placeholders, fmt.Sprintf("@p%d", i+2))
		args = append(args, n)
	}

	query := fmt.Sprintf(`
SELECT TOP (@p1) case_number
FROM case_parties
WHERE normalized_name IN (%s)
GROUP BY case_number
ORDER BY MAX(last_seen_at) DESC
`, strings.Join(placeholders, ", "))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query case numbers by normalized name: %w", err)
	}
	defer rows.Close()

	caseNumbers := make([]string, 0)
	for rows.Next() {
		var caseNumber string
		if scanErr := rows.Scan(&caseNumber); scanErr != nil {
			return nil, fmt.Errorf("scan case number: %w", scanErr)
		}
		caseNumbers = append(caseNumbers, caseNumber)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate case number rows: %w", err)
	}
	return caseNumbers, nil
}

type caseMetadata struct {
	caseType   string
	caseStatus string
	caseJudge  string
	fileDate   *time.Time
}

type extractedEvent struct {
	eventDateTime *time.Time
	eventDate     *time.Time
	eventTime     string
	location      string
	eventType     string
	eventResult   string
	eventJudge    string
	sourceTab     string
	sourceRowIdx  int
	rawRowJSON    string
}

type extractedDocket struct {
	docketDate   *time.Time
	docketText   string
	sequence     int
	sourceTab    string
	sourceRowIdx int
	rawRowJSON   string
}

type extractedCharge struct {
	sequence          int
	chargeCode        string
	chargeDescription string
	statute           string
	offenseLevel      string
	chargeDate        *time.Time
	offenseDate       *time.Time
	stageDate         *time.Time
	filingDate        *time.Time
	isAttempt         bool
	isAmended         bool
	offenseLocation   string
	atn               string
	trackingNumber    string
	dvRelated         *bool
	originalCharge    string
	indictedCharge    string
	amendedCharge     string
	modifiers         string
	dispositionStatus string
	dispositionDate   *time.Time
	plea              string
	sentenceSummary   string
	sourceTab         string
	sourceRowIdx      int
	rawRowJSON        string
}

var (
	reCaseType    = regexp.MustCompile(`(?i)Case Type:\s*(.+?)\s+Case Status:`)
	reCaseStatus  = regexp.MustCompile(`(?i)Case Status:\s*(.+?)\s+File Date:`)
	reFileDate    = regexp.MustCompile(`(?i)File Date:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})`)
	reCaseJudge   = regexp.MustCompile(`(?i)Case Judge:\s*(.+?)(?:\s+Next Event:|\s+All Information|\s+Party Information|\s+Charge Information|\s+Event Information|\s+Docket Information|$)`)
	reURLsInText  = regexp.MustCompile(`https?://[^\s]+`)
	reChargeStart = regexp.MustCompile(`(?i)Charge\s*#\s*([0-9]+)\s*:`)
)

func (s *Store) upsertDetailedCaseData(
	ctx context.Context,
	tx *sql.Tx,
	caseDetails courtview.CaseDetails,
	rows []courtview.SearchResultRow,
	payloadJSON string,
	payloadHash string,
	now time.Time,
	changed bool,
) error {
	caseNumber := strings.TrimSpace(caseDetails.CaseNumber)
	if caseNumber == "" {
		return nil
	}
	normalizedCaseNumber := strings.ToUpper(caseNumber)
	if normalized, err := courtview.NormalizeCaseNumber(caseNumber); err == nil {
		normalizedCaseNumber = normalized
	}

	meta := extractCaseMetadata(caseDetails)
	sourceURL := strings.TrimSpace(caseDetails.CaseURL)

	var caseID int64
	err := tx.QueryRowContext(ctx, `
SELECT case_id
FROM cv_cases
WHERE case_number = @p1
`, caseNumber).Scan(&caseID)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx, `
INSERT INTO cv_cases (
    case_number, case_number_normalized, case_type, case_status, file_date, case_judge, source_url, first_seen_at, last_seen_at, last_observed_change_at, is_active
)
OUTPUT INSERTED.case_id
VALUES (
    @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p8, @p8, 1
)
`, caseNumber, normalizedCaseNumber, meta.caseType, meta.caseStatus, meta.fileDate, meta.caseJudge, sourceURL, now).Scan(&caseID)
		if err != nil {
			return fmt.Errorf("insert cv_cases: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("read cv_cases: %w", err)
	} else {
		if _, err := tx.ExecContext(ctx, `
UPDATE cv_cases
SET case_number_normalized = @p2,
    case_type = COALESCE(NULLIF(@p3, N''), case_type),
    case_status = COALESCE(NULLIF(@p4, N''), case_status),
    file_date = COALESCE(@p5, file_date),
    case_judge = COALESCE(NULLIF(@p6, N''), case_judge),
    source_url = COALESCE(NULLIF(@p7, N''), source_url),
    last_seen_at = @p8,
    last_observed_change_at = CASE WHEN @p9 = 1 THEN @p8 ELSE last_observed_change_at END,
    is_active = 1
WHERE case_id = @p1
`, caseID, normalizedCaseNumber, meta.caseType, meta.caseStatus, meta.fileDate, meta.caseJudge, sourceURL, now, changed); err != nil {
			return fmt.Errorf("update cv_cases: %w", err)
		}
	}

	needsDetailedRefresh := changed
	var existingSnapshotID int64
	snapshotErr := tx.QueryRowContext(ctx, `
SELECT TOP (1) snapshot_id
FROM cv_case_snapshots
WHERE case_id = @p1
  AND is_latest = 1
ORDER BY captured_at DESC
`, caseID).Scan(&existingSnapshotID)
	if errors.Is(snapshotErr, sql.ErrNoRows) {
		needsDetailedRefresh = true
	} else if snapshotErr != nil {
		return fmt.Errorf("read latest cv_case_snapshots: %w", snapshotErr)
	}

	if !needsDetailedRefresh {
		refreshForMissingCharges, err := shouldRefreshForMissingCharges(ctx, tx, caseID, caseDetails)
		if err != nil {
			return err
		}
		if refreshForMissingCharges {
			needsDetailedRefresh = true
		}
	}

	if !needsDetailedRefresh {
		if _, err := tx.ExecContext(ctx, `
UPDATE cv_case_snapshots
SET captured_at = @p2
WHERE case_id = @p1
  AND is_latest = 1
`, caseID, now); err != nil {
			return fmt.Errorf("touch cv_case_snapshots: %w", err)
		}
		return nil
	}

	if _, err := tx.ExecContext(ctx, `
UPDATE cv_case_snapshots
SET is_latest = 0
WHERE case_id = @p1
  AND is_latest = 1
`, caseID); err != nil {
		return fmt.Errorf("unset latest cv_case_snapshots: %w", err)
	}

	var snapshotID int64
	if err := tx.QueryRowContext(ctx, `
INSERT INTO cv_case_snapshots (
    case_id, payload_json, payload_hash, parser_version, source_url, captured_at, is_latest
)
OUTPUT INSERTED.snapshot_id
VALUES (
    @p1, @p2, @p3, N'v1', @p4, @p5, 1
)
`, caseID, payloadJSON, payloadHash, sourceURL, now).Scan(&snapshotID); err != nil {
		return fmt.Errorf("insert cv_case_snapshots: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM cv_case_parties WHERE case_id = @p1`, caseID); err != nil {
		return fmt.Errorf("clear cv_case_parties: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM cv_case_events WHERE case_id = @p1`, caseID); err != nil {
		return fmt.Errorf("clear cv_case_events: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM cv_docket_entries WHERE case_id = @p1`, caseID); err != nil {
		return fmt.Errorf("clear cv_docket_entries: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM cv_charge_dispositions
WHERE charge_id IN (
    SELECT charge_id
    FROM cv_charges
    WHERE case_id = @p1
)`, caseID); err != nil {
		return fmt.Errorf("clear cv_charge_dispositions: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM cv_charges WHERE case_id = @p1`, caseID); err != nil {
		return fmt.Errorf("clear cv_charges: %w", err)
	}

	parties := courtview.ExtractParties(rows)
	primaryDefendantAssigned := false
	for _, party := range parties {
		personID, err := upsertPerson(ctx, tx, caseID, party, now)
		if err != nil {
			return err
		}
		roleGroup := partyRoleGroup(party.Role)
		isPrimaryDefendant := false
		if strings.Contains(strings.ToLower(strings.TrimSpace(party.Role)), "defendant") && !primaryDefendantAssigned {
			primaryDefendantAssigned = true
			isPrimaryDefendant = true
		}

		if _, err := tx.ExecContext(ctx, `
INSERT INTO cv_case_parties (
    case_id, person_id, party_name_raw, party_role, party_role_group, dob_raw, is_primary_defendant, is_active, first_seen_at, last_seen_at
) VALUES (
    @p1, @p2, @p3, @p4, @p5, @p6, @p7, 1, @p8, @p8
)
`, caseID, personID, party.FullName, party.Role, roleGroup, party.DOB, isPrimaryDefendant, now); err != nil {
			return fmt.Errorf("insert cv_case_parties: %w", err)
		}
	}

	events, dockets := extractEventsAndDockets(caseDetails)
	for _, event := range events {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO cv_case_events (
    case_id, event_datetime, event_date, event_time, location, event_type, event_result, event_judge, source_tab, source_row_index, raw_row_json, first_seen_at, last_seen_at
) VALUES (
    @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p12
)
`, caseID, event.eventDateTime, event.eventDate, event.eventTime, event.location, event.eventType, event.eventResult, event.eventJudge, event.sourceTab, event.sourceRowIdx, event.rawRowJSON, now); err != nil {
			return fmt.Errorf("insert cv_case_events: %w", err)
		}
	}

	for _, docket := range dockets {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO cv_docket_entries (
    case_id, docket_date, docket_text, docket_sequence, source_tab, source_row_index, raw_row_json, first_seen_at, last_seen_at
) VALUES (
    @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p8
)
`, caseID, docket.docketDate, docket.docketText, docket.sequence, docket.sourceTab, docket.sourceRowIdx, docket.rawRowJSON, now); err != nil {
			return fmt.Errorf("insert cv_docket_entries: %w", err)
		}
	}

	primaryCasePartyID := sql.NullInt64{}
	if err := tx.QueryRowContext(ctx, `
SELECT TOP (1) case_party_id
FROM cv_case_parties
WHERE case_id = @p1
  AND is_primary_defendant = 1
ORDER BY case_party_id ASC
`, caseID).Scan(&primaryCasePartyID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read primary defendant case_party_id: %w", err)
	}

	charges := extractCharges(caseDetails)
	for _, charge := range charges {
		if strings.TrimSpace(charge.chargeDescription) == "" && strings.TrimSpace(charge.statute) == "" && strings.TrimSpace(charge.chargeCode) == "" {
			continue
		}
		chargeDescription := strings.TrimSpace(charge.chargeDescription)
		if chargeDescription == "" {
			chargeDescription = strings.TrimSpace(charge.originalCharge)
		}
		if chargeDescription == "" {
			chargeDescription = strings.TrimSpace(charge.statute)
		}
		if chargeDescription == "" {
			chargeDescription = strings.TrimSpace(charge.chargeCode)
		}
		if chargeDescription == "" {
			continue
		}

		var chargeID int64
		if err := tx.QueryRowContext(ctx, `
INSERT INTO cv_charges (
    case_id, case_party_id, charge_sequence, charge_code, charge_description, statute, offense_level,
    charge_date, offense_date, stage_date, filing_date, is_attempt, is_amended,
    offense_location, atn, tracking_number, dv_related,
    original_charge_text, indicted_charge_text, amended_charge_text, modifiers,
    source_tab, source_row_index, raw_row_json, first_seen_at, last_seen_at
)
OUTPUT INSERTED.charge_id
VALUES (
    @p1, @p2, @p3, @p4, @p5, @p6, @p7,
    @p8, @p9, @p10, @p11, @p12, @p13,
    @p14, @p15, @p16, @p17,
    @p18, @p19, @p20, @p21,
    @p22, @p23, @p24, @p25, @p25
)
`, caseID, primaryCasePartyID, nullablePositiveInt(charge.sequence), nullableString(charge.chargeCode), chargeDescription, nullableString(charge.statute), nullableString(charge.offenseLevel),
			charge.chargeDate, charge.offenseDate, charge.stageDate, charge.filingDate, charge.isAttempt, charge.isAmended,
			nullableString(charge.offenseLocation), nullableString(charge.atn), nullableString(charge.trackingNumber), nullableBool(charge.dvRelated),
			nullableString(charge.originalCharge), nullableString(charge.indictedCharge), nullableString(charge.amendedCharge), nullableString(charge.modifiers),
			nullableString(charge.sourceTab), nullablePositiveInt(charge.sourceRowIdx), nullableString(charge.rawRowJSON), now).Scan(&chargeID); err != nil {
			return fmt.Errorf("insert cv_charges: %w", err)
		}

		if strings.TrimSpace(charge.dispositionStatus) == "" && charge.dispositionDate == nil && strings.TrimSpace(charge.plea) == "" && strings.TrimSpace(charge.sentenceSummary) == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO cv_charge_dispositions (
    charge_id, disposition_status, disposition_date, plea, sentence_summary, is_final, raw_row_json, first_seen_at, last_seen_at
)
VALUES (
    @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p8
)
`, chargeID, nullableString(charge.dispositionStatus), charge.dispositionDate, nullableString(charge.plea), nullableString(charge.sentenceSummary), isDispositionFinal(charge.dispositionStatus), nullableString(charge.rawRowJSON), now); err != nil {
			return fmt.Errorf("insert cv_charge_dispositions: %w", err)
		}
	}

	if err := insertTabSnapshotData(ctx, tx, snapshotID, caseDetails, now); err != nil {
		return err
	}

	return nil
}

func upsertPerson(ctx context.Context, tx *sql.Tx, caseID int64, party courtview.PartyRecord, now time.Time) (sql.NullInt64, error) {
	fullName := strings.TrimSpace(party.FullName)
	if fullName == "" {
		return sql.NullInt64{}, nil
	}
	normalizedName := strings.TrimSpace(strings.ToLower(party.NormalizedName))
	if normalizedName == "" {
		normalizedName = strings.ToLower(fullName)
	}
	dobRaw := strings.TrimSpace(party.DOB)
	dob := parseFlexibleDate(dobRaw)

	entityType := "Person"
	isGovernment := courtview.IsLikelyGovernmentEntity(fullName)
	if isGovernment {
		entityType = "Government"
	}

	var personID int64
	var existingFullName string
	var existingFirstName string
	var existingLastName string
	var existingDOBRaw string
	var existingIsGovernment bool
	var existingEntityType string
	profileChanged := false
	personInserted := false
	err := tx.QueryRowContext(ctx, `
SELECT TOP (1)
    person_id,
    ISNULL(full_name_raw, N''),
    ISNULL(first_name, N''),
    ISNULL(last_name, N''),
    ISNULL(dob_raw, N''),
    is_government_entity,
    ISNULL(entity_type, N'')
FROM cv_people
WHERE normalized_name = @p1
ORDER BY CASE WHEN ISNULL(dob_raw, N'') = @p2 THEN 0 ELSE 1 END, person_id
`, normalizedName, dobRaw).Scan(
		&personID,
		&existingFullName,
		&existingFirstName,
		&existingLastName,
		&existingDOBRaw,
		&existingIsGovernment,
		&existingEntityType,
	)
	if errors.Is(err, sql.ErrNoRows) {
		personInserted = true
		if err := tx.QueryRowContext(ctx, `
INSERT INTO cv_people (
    full_name_raw, normalized_name, first_name, last_name, dob, dob_raw, is_government_entity, entity_type, first_seen_at, last_seen_at, last_observed_change_at
)
OUTPUT INSERTED.person_id
VALUES (
    @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p9, @p9
)
`, fullName, normalizedName, party.FirstName, party.LastName, dob, dobRaw, isGovernment, entityType, now).Scan(&personID); err != nil {
			return sql.NullInt64{}, fmt.Errorf("insert cv_people: %w", err)
		}
	} else if err != nil {
		return sql.NullInt64{}, fmt.Errorf("read cv_people: %w", err)
	} else {
		newFullName := existingFullName
		if fullName != "" && !strings.EqualFold(existingFullName, fullName) {
			newFullName = fullName
			profileChanged = true
		}

		newFirstName := existingFirstName
		if v := strings.TrimSpace(party.FirstName); v != "" && !strings.EqualFold(existingFirstName, v) {
			newFirstName = v
			profileChanged = true
		}

		newLastName := existingLastName
		if v := strings.TrimSpace(party.LastName); v != "" && !strings.EqualFold(existingLastName, v) {
			newLastName = v
			profileChanged = true
		}

		dobForUpdate := any(nil)
		if existingDOBRaw == "" && dobRaw != "" {
			profileChanged = true
			dobForUpdate = dob
		} else if existingDOBRaw != "" && dobRaw != "" && !strings.EqualFold(existingDOBRaw, dobRaw) {
			// Keep the canonical DOB on cv_people stable, but record the new observed DOB below.
			profileChanged = true
		}

		newDOBRaw := existingDOBRaw
		if existingDOBRaw == "" && dobRaw != "" {
			newDOBRaw = dobRaw
		}

		if existingIsGovernment != isGovernment || !strings.EqualFold(existingEntityType, entityType) {
			profileChanged = true
		}

		if _, err := tx.ExecContext(ctx, `
UPDATE cv_people
SET full_name_raw = @p2,
    first_name = @p3,
    last_name = @p4,
    dob = COALESCE(@p5, dob),
    dob_raw = @p6,
    is_government_entity = @p7,
    entity_type = @p8,
    last_seen_at = @p9,
    last_observed_change_at = CASE WHEN @p10 = 1 THEN @p9 ELSE last_observed_change_at END
WHERE person_id = @p1
`, personID, newFullName, newFirstName, newLastName, dobForUpdate, newDOBRaw, isGovernment, entityType, now, profileChanged); err != nil {
			return sql.NullInt64{}, fmt.Errorf("update cv_people: %w", err)
		}
	}

	aliasObserved, err := upsertPersonAlias(ctx, tx, personID, caseID, fullName, normalizedName, now)
	if err != nil {
		return sql.NullInt64{}, err
	}
	dobObserved := false
	if dobRaw != "" {
		dobObserved, err = upsertPersonDOB(ctx, tx, personID, caseID, dobRaw, dob, now)
		if err != nil {
			return sql.NullInt64{}, err
		}
	}
	if !personInserted && !profileChanged && (aliasObserved || dobObserved) {
		if _, err := tx.ExecContext(ctx, `
UPDATE cv_people
SET last_observed_change_at = @p2
WHERE person_id = @p1
`, personID, now); err != nil {
			return sql.NullInt64{}, fmt.Errorf("mark cv_people observed change: %w", err)
		}
	}

	return sql.NullInt64{Int64: personID, Valid: true}, nil
}

func upsertPersonAlias(ctx context.Context, tx *sql.Tx, personID, caseID int64, aliasRaw, aliasNormalized string, now time.Time) (bool, error) {
	aliasRaw = strings.TrimSpace(aliasRaw)
	if aliasRaw == "" {
		return false, nil
	}
	aliasNormalized = strings.TrimSpace(strings.ToLower(aliasNormalized))
	if aliasNormalized == "" {
		aliasNormalized = strings.ToLower(aliasRaw)
	}

	var aliasID int64
	err := tx.QueryRowContext(ctx, `
SELECT TOP (1) person_alias_id
FROM cv_person_aliases
WHERE person_id = @p1
  AND alias_name_normalized = @p2
ORDER BY person_alias_id
`, personID, aliasNormalized).Scan(&aliasID)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO cv_person_aliases (
    person_id, source_case_id, alias_name_raw, alias_name_normalized, first_seen_at, last_seen_at
) VALUES (
    @p1, @p2, @p3, @p4, @p5, @p5
)
`, personID, caseID, aliasRaw, aliasNormalized, now); err != nil {
			return false, fmt.Errorf("insert cv_person_aliases: %w", err)
		}
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read cv_person_aliases: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE cv_person_aliases
SET alias_name_raw = @p2,
    source_case_id = @p3,
    last_seen_at = @p4
WHERE person_alias_id = @p1
`, aliasID, aliasRaw, caseID, now); err != nil {
		return false, fmt.Errorf("update cv_person_aliases: %w", err)
	}
	return false, nil
}

func upsertPersonDOB(ctx context.Context, tx *sql.Tx, personID, caseID int64, dobRaw string, dobDate *time.Time, now time.Time) (bool, error) {
	dobRaw = strings.TrimSpace(dobRaw)
	if dobRaw == "" {
		return false, nil
	}

	var personDOBID int64
	err := tx.QueryRowContext(ctx, `
SELECT TOP (1) person_dob_id
FROM cv_person_dobs
WHERE person_id = @p1
  AND dob_raw = @p2
ORDER BY person_dob_id
`, personID, dobRaw).Scan(&personDOBID)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO cv_person_dobs (
    person_id, source_case_id, dob_raw, dob_date, first_seen_at, last_seen_at
) VALUES (
    @p1, @p2, @p3, @p4, @p5, @p5
)
`, personID, caseID, dobRaw, dobDate, now); err != nil {
			return false, fmt.Errorf("insert cv_person_dobs: %w", err)
		}
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read cv_person_dobs: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE cv_person_dobs
SET source_case_id = @p2,
    dob_date = COALESCE(@p3, dob_date),
    last_seen_at = @p4
WHERE person_dob_id = @p1
`, personDOBID, caseID, dobDate, now); err != nil {
		return false, fmt.Errorf("update cv_person_dobs: %w", err)
	}
	return false, nil
}

func insertTabSnapshotData(ctx context.Context, tx *sql.Tx, snapshotID int64, caseDetails courtview.CaseDetails, now time.Time) error {
	type pageItem struct {
		name string
		page courtview.PageSnapshot
	}

	tabNames := make([]string, 0, len(caseDetails.Tabs))
	for name := range caseDetails.Tabs {
		tabNames = append(tabNames, name)
	}
	sort.Strings(tabNames)

	pages := make([]pageItem, 0, len(tabNames)+1)
	pages = append(pages, pageItem{name: "Current", page: caseDetails.Current})
	for _, name := range tabNames {
		pages = append(pages, pageItem{name: name, page: caseDetails.Tabs[name]})
	}

	for _, item := range pages {
		var tabPageID int64
		if err := tx.QueryRowContext(ctx, `
INSERT INTO cv_tab_pages (
    snapshot_id, tab_name, page_url, page_title, page_h1, main_text_excerpt, captured_at
)
OUTPUT INSERTED.tab_page_id
VALUES (
    @p1, @p2, @p3, @p4, @p5, @p6, @p7
)
`, snapshotID, item.name, item.page.URL, item.page.Title, item.page.H1, item.page.MainTextExcerpt, now).Scan(&tabPageID); err != nil {
			return fmt.Errorf("insert cv_tab_pages: %w", err)
		}

		labelKeys := make([]string, 0, len(item.page.LabelValues))
		for label := range item.page.LabelValues {
			labelKeys = append(labelKeys, label)
		}
		sort.Strings(labelKeys)
		for _, label := range labelKeys {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO cv_tab_label_values (tab_page_id, label_name, label_value, captured_at)
VALUES (@p1, @p2, @p3, @p4)
`, tabPageID, label, item.page.LabelValues[label], now); err != nil {
				return fmt.Errorf("insert cv_tab_label_values: %w", err)
			}
		}

		for _, table := range item.page.Tables {
			headersJSON, err := json.Marshal(table.Headers)
			if err != nil {
				return fmt.Errorf("marshal tab table headers: %w", err)
			}
			var tabTableID int64
			if err := tx.QueryRowContext(ctx, `
INSERT INTO cv_tab_tables (tab_page_id, table_index, headers_json, captured_at)
OUTPUT INSERTED.tab_table_id
VALUES (@p1, @p2, @p3, @p4)
`, tabPageID, table.Index, string(headersJSON), now).Scan(&tabTableID); err != nil {
				return fmt.Errorf("insert cv_tab_tables: %w", err)
			}

			for rowIndex, cells := range table.Rows {
				cellsJSON, err := json.Marshal(cells)
				if err != nil {
					return fmt.Errorf("marshal tab table row cells: %w", err)
				}
				if _, err := tx.ExecContext(ctx, `
INSERT INTO cv_tab_table_rows (tab_table_id, row_index, cells_json, captured_at)
VALUES (@p1, @p2, @p3, @p4)
`, tabTableID, rowIndex+1, string(cellsJSON), now); err != nil {
					return fmt.Errorf("insert cv_tab_table_rows: %w", err)
				}
			}
		}
	}

	return nil
}

func extractCaseMetadata(caseDetails courtview.CaseDetails) caseMetadata {
	meta := caseMetadata{}

	if v := firstCaseLabelValue(caseDetails, "Case Type"); v != "" {
		meta.caseType = v
	}
	if v := firstCaseLabelValue(caseDetails, "Case Status"); v != "" {
		meta.caseStatus = v
	}
	if v := firstCaseLabelValue(caseDetails, "Case Judge"); v != "" {
		meta.caseJudge = v
	}
	if v := firstCaseLabelValue(caseDetails, "File Date"); v != "" {
		meta.fileDate = parseFlexibleDate(v)
	}

	text := combinedCaseText(caseDetails)
	if meta.caseType == "" {
		meta.caseType = firstRegexGroup(reCaseType, text)
	}
	if meta.caseStatus == "" {
		meta.caseStatus = firstRegexGroup(reCaseStatus, text)
	}
	if meta.fileDate == nil {
		if fileDateRaw := firstRegexGroup(reFileDate, text); fileDateRaw != "" {
			meta.fileDate = parseFlexibleDate(fileDateRaw)
		}
	}
	if meta.caseJudge == "" {
		meta.caseJudge = firstRegexGroup(reCaseJudge, text)
	}

	meta.caseType = strings.TrimSpace(meta.caseType)
	meta.caseStatus = strings.TrimSpace(meta.caseStatus)
	meta.caseJudge = strings.TrimSpace(meta.caseJudge)
	if len(meta.caseType) > 128 {
		meta.caseType = meta.caseType[:128]
	}
	if len(meta.caseStatus) > 64 {
		meta.caseStatus = meta.caseStatus[:64]
	}
	if len(meta.caseJudge) > 1024 {
		meta.caseJudge = meta.caseJudge[:1024]
	}
	return meta
}

func firstCaseLabelValue(caseDetails courtview.CaseDetails, key string) string {
	if v, ok := caseDetails.Current.LabelValues[key]; ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	for _, page := range caseDetails.Tabs {
		if v, ok := page.LabelValues[key]; ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func combinedCaseText(caseDetails courtview.CaseDetails) string {
	parts := make([]string, 0, len(caseDetails.Tabs)+1)
	if t := strings.TrimSpace(caseDetails.Current.MainTextExcerpt); t != "" {
		parts = append(parts, t)
	}
	for _, page := range caseDetails.Tabs {
		if t := strings.TrimSpace(page.MainTextExcerpt); t != "" {
			parts = append(parts, t)
		}
	}
	return strings.Join(parts, " ")
}

func shouldRefreshForMissingCharges(ctx context.Context, tx *sql.Tx, caseID int64, caseDetails courtview.CaseDetails) (bool, error) {
	caseText := strings.ToLower(combinedCaseText(caseDetails))
	hasChargeSignal := strings.Contains(caseText, "charge #") || strings.Contains(caseText, "charge information")
	if !hasChargeSignal {
		return false, nil
	}
	var chargeCount int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(1)
FROM cv_charges
WHERE case_id = @p1
`, caseID).Scan(&chargeCount); err != nil {
		return false, fmt.Errorf("read cv_charges count: %w", err)
	}
	return chargeCount == 0, nil
}

func firstRegexGroup(re *regexp.Regexp, value string) string {
	m := re.FindStringSubmatch(value)
	if len(m) < 2 {
		return ""
	}
	return strings.TrimSpace(m[1])
}

func extractEventsAndDockets(caseDetails courtview.CaseDetails) ([]extractedEvent, []extractedDocket) {
	type pageItem struct {
		name string
		page courtview.PageSnapshot
	}

	tabNames := make([]string, 0, len(caseDetails.Tabs))
	for name := range caseDetails.Tabs {
		tabNames = append(tabNames, name)
	}
	sort.Strings(tabNames)

	pages := make([]pageItem, 0, len(tabNames)+1)
	pages = append(pages, pageItem{name: "Current", page: caseDetails.Current})
	for _, name := range tabNames {
		pages = append(pages, pageItem{name: name, page: caseDetails.Tabs[name]})
	}

	events := make([]extractedEvent, 0)
	dockets := make([]extractedDocket, 0)

	for _, item := range pages {
		for _, table := range item.page.Tables {
			headerMap := normalizedHeaderIndex(table.Headers)
			if isEventTable(headerMap) {
				for rowIndex, row := range table.Rows {
					rowMap := rowToHeaderMap(table.Headers, row)
					rawJSON := marshalToJSONString(rowMap)

					dateTimeRaw := firstValue(rowMap, "date/time", "datetime", "date")
					eventDateTime, eventDate, eventTime := parseFlexibleDateTime(dateTimeRaw)
					event := extractedEvent{
						eventDateTime: eventDateTime,
						eventDate:     eventDate,
						eventTime:     eventTime,
						location:      firstValue(rowMap, "location"),
						eventType:     firstValue(rowMap, "type", "event type"),
						eventResult:   firstValue(rowMap, "result"),
						eventJudge:    firstValue(rowMap, "event judge", "judge"),
						sourceTab:     item.name,
						sourceRowIdx:  rowIndex + 1,
						rawRowJSON:    rawJSON,
					}
					if strings.TrimSpace(event.location+event.eventType+event.eventResult+event.eventJudge+dateTimeRaw) == "" {
						continue
					}
					events = append(events, event)
				}
			}

			if isDocketTable(headerMap) {
				for rowIndex, row := range table.Rows {
					rowMap := rowToHeaderMap(table.Headers, row)
					docketText := firstValue(rowMap, "docket text", "text")
					if docketText == "" {
						continue
					}
					dockets = append(dockets, extractedDocket{
						docketDate:   parseFlexibleDate(firstValue(rowMap, "date")),
						docketText:   docketText,
						sequence:     rowIndex + 1,
						sourceTab:    item.name,
						sourceRowIdx: rowIndex + 1,
						rawRowJSON:   marshalToJSONString(rowMap),
					})
				}
			}
		}
	}

	return events, dockets
}

func extractCharges(caseDetails courtview.CaseDetails) []extractedCharge {
	fromTables := extractChargesFromTables(caseDetails)
	fromText := extractChargesFromTextFallback(caseDetails)
	if len(fromText) == 0 {
		return dedupeCharges(fromTables)
	}
	if len(fromTables) == 0 {
		return dedupeCharges(fromText)
	}
	return dedupeCharges(mergeCharges(fromTables, fromText))
}

func extractChargesFromTables(caseDetails courtview.CaseDetails) []extractedCharge {
	type pageItem struct {
		name string
		page courtview.PageSnapshot
	}
	tabNames := make([]string, 0, len(caseDetails.Tabs))
	for name := range caseDetails.Tabs {
		tabNames = append(tabNames, name)
	}
	sort.Strings(tabNames)

	pages := make([]pageItem, 0, len(tabNames)+1)
	pages = append(pages, pageItem{name: "Current", page: caseDetails.Current})
	for _, name := range tabNames {
		pages = append(pages, pageItem{name: name, page: caseDetails.Tabs[name]})
	}

	charges := make([]extractedCharge, 0)
	for _, item := range pages {
		for _, table := range item.page.Tables {
			headerMap := normalizedHeaderIndex(table.Headers)
			if !isChargeTable(headerMap) {
				continue
			}
			for rowIndex, row := range table.Rows {
				rowMap := rowToHeaderMap(table.Headers, row)
				charge := extractedCharge{
					sequence:          parseIntLoose(firstValue(rowMap, "charge #", "charge no", "count", "count #")),
					chargeCode:        firstValue(rowMap, "charge code", "code"),
					chargeDescription: firstValue(rowMap, "charge description", "charge", "count description", "offense", "offense description"),
					statute:           firstValue(rowMap, "statute", "citation", "as", "law"),
					offenseLevel:      firstValue(rowMap, "offense level", "charge level", "degree", "class", "severity"),
					chargeDate:        parseFlexibleDate(firstValue(rowMap, "charge date")),
					offenseDate:       parseFlexibleDate(firstValue(rowMap, "offense date", "date of offense")),
					stageDate:         parseFlexibleDate(firstValue(rowMap, "stage date")),
					dispositionStatus: firstValue(rowMap, "disposition", "result", "judgment", "status"),
					dispositionDate:   parseFlexibleDate(firstValue(rowMap, "disposition date")),
					plea:              firstValue(rowMap, "plea"),
					sentenceSummary:   firstValue(rowMap, "sentence", "sentence summary"),
					offenseLocation:   firstValue(rowMap, "offense location", "location"),
					atn:               firstValue(rowMap, "atn", "atn #"),
					trackingNumber:    firstValue(rowMap, "tracking #", "tracking number"),
					originalCharge:    firstValue(rowMap, "original charge"),
					indictedCharge:    firstValue(rowMap, "indicted charge"),
					amendedCharge:     firstValue(rowMap, "amended charge"),
					modifiers:         firstValue(rowMap, "modifiers"),
					sourceTab:         item.name,
					sourceRowIdx:      rowIndex + 1,
					rawRowJSON:        marshalToJSONString(rowMap),
				}
				if charge.chargeDescription == "" {
					charge.chargeDescription = firstValueByHeaderContains(rowMap, "charge")
				}
				if charge.statute == "" {
					charge.statute = firstValueByHeaderContains(rowMap, "statute")
				}
				if charge.offenseLevel == "" {
					charge.offenseLevel = classifyOffenseLevel(charge.chargeDescription + " " + charge.originalCharge + " " + charge.amendedCharge)
				}
				dv := parseBoolLoose(firstValue(rowMap, "dv related?", "dv related"))
				charge.dvRelated = dv
				charge.isAttempt = strings.Contains(strings.ToLower(charge.chargeDescription), "attempt")
				charge.isAmended = isAmendedCharge(charge.amendedCharge, charge.chargeDescription, charge.dispositionStatus)
				if charge.chargeCode == "" {
					charge.chargeCode = firstStatuteLikeToken(charge.chargeDescription)
				}
				if charge.chargeDescription == "" && charge.statute == "" && charge.chargeCode == "" && charge.dispositionStatus == "" {
					continue
				}
				charges = append(charges, charge)
			}
		}
	}
	return charges
}

func extractChargesFromTextFallback(caseDetails courtview.CaseDetails) []extractedCharge {
	text := combinedCaseText(caseDetails)
	text = strings.Join(strings.Fields(text), " ")
	if text == "" {
		return nil
	}
	startMatches := reChargeStart.FindAllStringSubmatchIndex(text, -1)
	if len(startMatches) == 0 {
		return nil
	}

	out := make([]extractedCharge, 0, len(startMatches))
	for i, match := range startMatches {
		if len(match) < 4 {
			continue
		}
		blockStart := match[1]
		blockEnd := len(text)
		if i+1 < len(startMatches) {
			blockEnd = startMatches[i+1][0]
		}
		sequence := parseIntLoose(text[match[2]:match[3]])
		block := strings.TrimSpace(text[blockStart:blockEnd])
		if block == "" {
			continue
		}

		header := firstRegexGroup(regexp.MustCompile(`(?i)^\s*(.*?)(?:\s+Original Charge|\s+Indicted Charge|\s+Amended Charge|\s+DV Related\?|\s+Modifiers|\s+Stage Date|\s+ATN\s*#|\s+Tracking\s*#|\s+Offense Location|\s+Date of Offense|$)`), block)
		chargeCode := firstRegexGroup(regexp.MustCompile(`(?i)^\s*([A-Z]{2,}[A-Z0-9.-]+)\b`), header)
		chargeDescription := header
		if parts := strings.SplitN(header, " - ", 2); len(parts) == 2 {
			if chargeCode == "" {
				chargeCode = strings.TrimSpace(parts[0])
			}
			chargeDescription = strings.TrimSpace(parts[1])
		}
		if chargeDescription == "" {
			chargeDescription = header
		}

		originalCharge := firstRegexGroup(regexp.MustCompile(`(?i)Original Charge\s*(.*?)\s*(?:Indicted Charge|Amended Charge|DV Related\?|Modifiers|Stage Date|ATN\s*#|Tracking\s*#|Offense Location|Date of Offense|$)`), block)
		indictedCharge := firstRegexGroup(regexp.MustCompile(`(?i)Indicted Charge\s*(.*?)\s*(?:Amended Charge|DV Related\?|Modifiers|Stage Date|ATN\s*#|Tracking\s*#|Offense Location|Date of Offense|$)`), block)
		amendedCharge := firstRegexGroup(regexp.MustCompile(`(?i)Amended Charge\s*(.*?)\s*(?:DV Related\?|Modifiers|Stage Date|ATN\s*#|Tracking\s*#|Offense Location|Date of Offense|$)`), block)
		modifiers := firstRegexGroup(regexp.MustCompile(`(?i)Modifiers\s*(.*?)\s*(?:Stage Date|ATN\s*#|Tracking\s*#|Offense Location|Date of Offense|$)`), block)

		statute := firstStatuteLikeToken(originalCharge)
		if statute == "" {
			statute = firstStatuteLikeToken(header)
		}
		offenseLevel := classifyOffenseLevel(strings.Join([]string{header, originalCharge, amendedCharge}, " "))
		stageDate := parseFlexibleDate(firstRegexGroup(regexp.MustCompile(`(?i)Stage Date\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})`), block))
		offenseDate := parseFlexibleDate(firstRegexGroup(regexp.MustCompile(`(?i)Date of Offense\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})`), block))
		atn := firstRegexGroup(regexp.MustCompile(`(?i)\bATN\s*#\s*([A-Za-z0-9-]+)\b`), block)
		trackingNumber := firstRegexGroup(regexp.MustCompile(`(?i)\bTracking\s*#\s*([A-Za-z0-9-]+)\b`), block)
		offenseLocation := firstRegexGroup(regexp.MustCompile(`(?i)\bOffense Location\s*(.*?)\s*(?:Date of Offense|$)`), block)
		dvRelated := parseBoolLoose(firstRegexGroup(regexp.MustCompile(`(?i)\bDV Related\?\s*(Yes|No)\b`), block))
		dispositionStatus := firstRegexGroup(regexp.MustCompile(`(?i)\bDisposition\s*:?\s*(.*?)\s*(?:Original Charge|Indicted Charge|Amended Charge|DV Related\?|Modifiers|Stage Date|ATN\s*#|Tracking\s*#|Offense Location|Date of Offense|$)`), block)

		rawJSON := marshalToJSONString(map[string]any{
			"source": "CurrentTextFallback",
			"block":  block,
		})
		out = append(out, extractedCharge{
			sequence:          sequence,
			chargeCode:        strings.TrimSpace(chargeCode),
			chargeDescription: strings.TrimSpace(chargeDescription),
			statute:           strings.TrimSpace(statute),
			offenseLevel:      strings.TrimSpace(offenseLevel),
			stageDate:         stageDate,
			offenseDate:       offenseDate,
			offenseLocation:   strings.TrimSpace(offenseLocation),
			atn:               strings.TrimSpace(atn),
			trackingNumber:    strings.TrimSpace(trackingNumber),
			dvRelated:         dvRelated,
			originalCharge:    strings.TrimSpace(originalCharge),
			indictedCharge:    strings.TrimSpace(indictedCharge),
			amendedCharge:     strings.TrimSpace(amendedCharge),
			modifiers:         strings.TrimSpace(modifiers),
			dispositionStatus: strings.TrimSpace(dispositionStatus),
			isAttempt:         strings.Contains(strings.ToLower(chargeDescription), "attempt"),
			isAmended:         isAmendedCharge(amendedCharge, chargeDescription, dispositionStatus),
			sourceTab:         "CurrentTextFallback",
			sourceRowIdx:      sequence,
			rawRowJSON:        rawJSON,
		})
	}

	return out
}

func mergeCharges(primary, fallback []extractedCharge) []extractedCharge {
	out := append([]extractedCharge(nil), primary...)
	bySequence := make(map[int]int, len(out))
	for i, charge := range out {
		if charge.sequence > 0 {
			bySequence[charge.sequence] = i
		}
	}
	for _, extra := range fallback {
		if extra.sequence > 0 {
			if idx, ok := bySequence[extra.sequence]; ok {
				out[idx] = mergeChargeRecord(out[idx], extra)
				continue
			}
		}
		out = append(out, extra)
		if extra.sequence > 0 {
			bySequence[extra.sequence] = len(out) - 1
		}
	}
	return out
}

func mergeChargeRecord(base, extra extractedCharge) extractedCharge {
	if base.chargeCode == "" {
		base.chargeCode = extra.chargeCode
	}
	if base.chargeDescription == "" {
		base.chargeDescription = extra.chargeDescription
	}
	if base.statute == "" {
		base.statute = extra.statute
	}
	if base.offenseLevel == "" {
		base.offenseLevel = extra.offenseLevel
	}
	if base.chargeDate == nil {
		base.chargeDate = extra.chargeDate
	}
	if base.offenseDate == nil {
		base.offenseDate = extra.offenseDate
	}
	if base.stageDate == nil {
		base.stageDate = extra.stageDate
	}
	if base.filingDate == nil {
		base.filingDate = extra.filingDate
	}
	if base.offenseLocation == "" {
		base.offenseLocation = extra.offenseLocation
	}
	if base.atn == "" {
		base.atn = extra.atn
	}
	if base.trackingNumber == "" {
		base.trackingNumber = extra.trackingNumber
	}
	if base.dvRelated == nil {
		base.dvRelated = extra.dvRelated
	}
	if base.originalCharge == "" {
		base.originalCharge = extra.originalCharge
	}
	if base.indictedCharge == "" {
		base.indictedCharge = extra.indictedCharge
	}
	if base.amendedCharge == "" {
		base.amendedCharge = extra.amendedCharge
	}
	if base.modifiers == "" {
		base.modifiers = extra.modifiers
	}
	if base.dispositionStatus == "" {
		base.dispositionStatus = extra.dispositionStatus
	}
	if base.dispositionDate == nil {
		base.dispositionDate = extra.dispositionDate
	}
	if base.plea == "" {
		base.plea = extra.plea
	}
	if base.sentenceSummary == "" {
		base.sentenceSummary = extra.sentenceSummary
	}
	base.isAttempt = base.isAttempt || extra.isAttempt
	base.isAmended = base.isAmended || extra.isAmended
	if base.sourceTab == "" {
		base.sourceTab = extra.sourceTab
	}
	if base.sourceRowIdx == 0 {
		base.sourceRowIdx = extra.sourceRowIdx
	}
	if base.rawRowJSON == "" {
		base.rawRowJSON = extra.rawRowJSON
	}
	return base
}

func dedupeCharges(charges []extractedCharge) []extractedCharge {
	seen := make(map[string]struct{}, len(charges))
	out := make([]extractedCharge, 0, len(charges))
	for _, charge := range charges {
		if charge.offenseLevel == "" {
			charge.offenseLevel = classifyOffenseLevel(strings.Join([]string{
				charge.chargeDescription,
				charge.originalCharge,
				charge.amendedCharge,
			}, " "))
		}
		if charge.chargeCode == "" {
			charge.chargeCode = firstStatuteLikeToken(charge.chargeDescription)
		}
		key := strings.ToLower(strings.Join([]string{
			strconv.Itoa(charge.sequence),
			strings.TrimSpace(charge.chargeCode),
			strings.TrimSpace(charge.chargeDescription),
			strings.TrimSpace(charge.statute),
			strings.TrimSpace(charge.dispositionStatus),
			formatDateForKey(charge.offenseDate),
		}, "|"))
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, charge)
	}
	return out
}

func isChargeTable(headerMap map[string]int) bool {
	hasChargeHint := containsHeader(headerMap, "charge") || containsHeader(headerMap, "offense") || containsHeader(headerMap, "count")
	if !hasChargeHint {
		return false
	}
	hasDetail := containsHeader(headerMap, "statute") ||
		containsHeader(headerMap, "disposition") ||
		containsHeader(headerMap, "level") ||
		containsHeader(headerMap, "degree") ||
		containsHeader(headerMap, "class") ||
		containsHeader(headerMap, "date of offense") ||
		containsHeader(headerMap, "offense date") ||
		containsHeader(headerMap, "amended charge")
	return hasDetail
}

func containsHeader(headerMap map[string]int, snippet string) bool {
	snippet = normalizeHeader(snippet)
	for key := range headerMap {
		if strings.Contains(key, snippet) {
			return true
		}
	}
	return false
}

func firstValueByHeaderContains(values map[string]string, snippet string) string {
	snippet = normalizeHeader(snippet)
	for key, value := range values {
		if strings.Contains(key, snippet) && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func parseIntLoose(value string) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	n, err := strconv.Atoi(value)
	if err == nil {
		return n
	}
	m := regexp.MustCompile(`\d+`).FindString(value)
	if m == "" {
		return 0
	}
	n, err = strconv.Atoi(m)
	if err != nil {
		return 0
	}
	return n
}

func parseBoolLoose(value string) *bool {
	v := strings.ToLower(strings.TrimSpace(value))
	switch v {
	case "yes", "y", "true", "1":
		b := true
		return &b
	case "no", "n", "false", "0":
		b := false
		return &b
	default:
		return nil
	}
}

func isAmendedCharge(amendedCharge, chargeDescription, disposition string) bool {
	if v := strings.TrimSpace(amendedCharge); v != "" {
		lv := strings.ToLower(v)
		if lv != "none" && lv != "no" && lv != "n/a" {
			return true
		}
	}
	bucket := strings.ToLower(strings.Join([]string{chargeDescription, disposition}, " "))
	return strings.Contains(bucket, "amend")
}

func firstStatuteLikeToken(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	re := regexp.MustCompile(`(?i)\b([A-Z]{2,}[A-Z0-9()./-]*[0-9][A-Z0-9()./-]*)\b`)
	m := re.FindStringSubmatch(value)
	if len(m) < 2 {
		return ""
	}
	return strings.TrimSpace(m[1])
}

func classifyOffenseLevel(value string) string {
	v := strings.ToLower(strings.TrimSpace(value))
	switch {
	case strings.Contains(v, "unclassified felony"):
		return "Unclassified Felony"
	case strings.Contains(v, "class a felony"), strings.Contains(v, "felony a"):
		return "Class A Felony"
	case strings.Contains(v, "class b felony"), strings.Contains(v, "felony b"):
		return "Class B Felony"
	case strings.Contains(v, "class c felony"), strings.Contains(v, "felony c"):
		return "Class C Felony"
	case strings.Contains(v, "class a misdemeanor"), strings.Contains(v, "misdemeanor a"):
		return "Class A Misdemeanor"
	case strings.Contains(v, "class b misdemeanor"), strings.Contains(v, "misdemeanor b"):
		return "Class B Misdemeanor"
	case strings.Contains(v, "violation"):
		return "Violation"
	default:
		return ""
	}
}

func formatDateForKey(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format("2006-01-02")
}

func partyRoleGroup(role string) string {
	r := strings.ToLower(strings.TrimSpace(role))
	switch {
	case strings.Contains(r, "defendant"):
		return "Defendant-side"
	case strings.Contains(r, "prosecution"), strings.Contains(r, "plaintiff"):
		return "Prosecution-side"
	case strings.Contains(r, "judge"), strings.Contains(r, "court"):
		return "Court"
	default:
		return "Other"
	}
}

func parseFlexibleDate(value string) *time.Time {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return nil
	}
	layouts := []string{
		"01/02/2006",
		"1/2/2006",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, raw); err == nil {
			tt := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
			return &tt
		}
	}
	return nil
}

func parseFlexibleDateTime(value string) (*time.Time, *time.Time, string) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return nil, nil, ""
	}
	layoutsWithTime := []string{
		"01/02/2006 3:04 PM",
		"1/2/2006 3:04 PM",
		"01/02/2006 15:04",
		"1/2/2006 15:04",
	}
	for _, layout := range layoutsWithTime {
		if t, err := time.Parse(layout, raw); err == nil {
			tu := t.UTC()
			dateOnly := time.Date(tu.Year(), tu.Month(), tu.Day(), 0, 0, 0, 0, time.UTC)
			return &tu, &dateOnly, strings.TrimSpace(tu.Format("15:04"))
		}
	}
	if d := parseFlexibleDate(raw); d != nil {
		return nil, d, ""
	}
	return nil, nil, ""
}

func normalizedHeaderIndex(headers []string) map[string]int {
	idx := make(map[string]int, len(headers))
	for i, h := range headers {
		k := normalizeHeader(h)
		if k != "" {
			idx[k] = i
		}
	}
	return idx
}

func normalizeHeader(value string) string {
	v := strings.ToLower(strings.TrimSpace(value))
	v = strings.ReplaceAll(v, "_", " ")
	v = strings.ReplaceAll(v, "-", " ")
	v = strings.Join(strings.Fields(v), " ")
	return v
}

func isEventTable(headerMap map[string]int) bool {
	_, hasDateTime := headerMap["date/time"]
	_, hasType := headerMap["type"]
	return hasDateTime && hasType
}

func isDocketTable(headerMap map[string]int) bool {
	_, hasDate := headerMap["date"]
	_, hasText := headerMap["docket text"]
	return hasDate && hasText
}

func rowToHeaderMap(headers, row []string) map[string]string {
	out := make(map[string]string, len(headers))
	for i, header := range headers {
		if i >= len(row) {
			continue
		}
		key := normalizeHeader(header)
		if key == "" {
			continue
		}
		out[key] = strings.TrimSpace(row[i])
	}
	return out
}

func firstValue(values map[string]string, keys ...string) string {
	for _, key := range keys {
		if v, ok := values[normalizeHeader(key)]; ok {
			if strings.TrimSpace(v) != "" {
				return strings.TrimSpace(v)
			}
		}
	}
	return ""
}

func marshalToJSONString(value any) string {
	b, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func nullableString(value string) sql.NullString {
	v := strings.TrimSpace(value)
	if v == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: v, Valid: true}
}

func nullablePositiveInt(value int) sql.NullInt64 {
	if value <= 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(value), Valid: true}
}

func nullableBool(value *bool) sql.NullBool {
	if value == nil {
		return sql.NullBool{}
	}
	return sql.NullBool{Bool: *value, Valid: true}
}

func isDispositionFinal(status string) bool {
	s := strings.ToLower(strings.TrimSpace(status))
	if s == "" {
		return false
	}
	finalMarkers := []string{
		"dismiss",
		"guilty",
		"not guilty",
		"acquitt",
		"convict",
		"sentenc",
		"closed",
	}
	for _, marker := range finalMarkers {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

func caseHasTransientTabFailures(caseDetails courtview.CaseDetails) bool {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(caseDetails.Current.MainTextExcerpt)), "tab fetch failed:") {
		return true
	}
	for _, page := range caseDetails.Tabs {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(page.MainTextExcerpt)), "tab fetch failed:") {
			return true
		}
	}
	return false
}

func canonicalCaseDetailsForHash(caseDetails courtview.CaseDetails) courtview.CaseDetails {
	canonical := caseDetails
	canonical.CaseURL = normalizeURLForHash(canonical.CaseURL)
	canonical.Current.URL = normalizeURLForHash(canonical.Current.URL)
	canonical.Current.MainTextExcerpt = normalizeURLsInText(canonical.Current.MainTextExcerpt)
	canonical.Current.Tabs = append([]string(nil), canonical.Current.Tabs...)
	sort.Strings(canonical.Current.Tabs)

	if canonical.Tabs != nil {
		normalizedTabs := make(map[string]courtview.PageSnapshot, len(canonical.Tabs))
		for name, page := range canonical.Tabs {
			p := page
			p.URL = normalizeURLForHash(p.URL)
			p.MainTextExcerpt = normalizeURLsInText(p.MainTextExcerpt)
			p.Tabs = append([]string(nil), p.Tabs...)
			sort.Strings(p.Tabs)
			normalizedTabs[name] = p
		}
		canonical.Tabs = normalizedTabs
	}

	return canonical
}

func normalizeURLForHash(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

func normalizeURLsInText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return reURLsInText.ReplaceAllStringFunc(value, normalizeURLForHash)
}

func (s *Store) PurgeIfNeeded(ctx context.Context) error {
	if !s.Enabled() {
		return nil
	}

	usedMB, err := s.usedDataMB(ctx)
	if err != nil {
		return err
	}
	if usedMB <= float64(s.dataMaxSizeMB-2) {
		return nil
	}

	for i := 0; i < 20; i++ {
		if usedMB <= float64(s.purgeTargetMB) {
			return nil
		}
		if _, err := s.db.ExecContext(ctx, `
;WITH oldest AS (
    SELECT TOP (25) case_number
    FROM case_records
    ORDER BY last_query_at ASC
)
DELETE FROM case_records
WHERE case_number IN (SELECT case_number FROM oldest)
`); err != nil {
			return fmt.Errorf("purge oldest case records: %w", err)
		}
		usedMB, err = s.usedDataMB(ctx)
		if err != nil {
			return err
		}
	}

	if usedMB > float64(s.purgeTargetMB) {
		return fmt.Errorf("database remains above purge target: %.2fMB used", usedMB)
	}
	return nil
}

func (s *Store) usedDataMB(ctx context.Context) (float64, error) {
	var usedMB float64
	err := s.db.QueryRowContext(ctx, `
SELECT COALESCE(SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS BIGINT) * 8.0 / 1024.0), 0)
FROM sys.database_files
WHERE type_desc = 'ROWS'
`).Scan(&usedMB)
	if err != nil {
		return 0, fmt.Errorf("query used data MB: %w", err)
	}
	return usedMB, nil
}

func ensureDatabase(ctx context.Context, adminDB *sql.DB, dbName string) error {
	dbName = strings.TrimSpace(dbName)
	if dbName == "" {
		return errors.New("database name cannot be empty")
	}

	_, err := adminDB.ExecContext(ctx, `
IF DB_ID(@p1) IS NULL
BEGIN
    DECLARE @sql NVARCHAR(MAX) = N'CREATE DATABASE [' + REPLACE(@p1, ']', ']]') + N']';
    EXEC(@sql);
END
`, dbName)
	if err != nil {
		return fmt.Errorf("ensure database exists: %w", err)
	}
	return nil
}

func setDatabaseMaxSize(ctx context.Context, appDB *sql.DB, dbName string, dataMaxSizeMB, logMaxSizeMB int) error {
	dbNameEscaped := strings.ReplaceAll(dbName, "]", "]]")

	rows, err := appDB.QueryContext(ctx, `
SELECT name, type_desc
FROM sys.database_files
`)
	if err != nil {
		return fmt.Errorf("list database files: %w", err)
	}
	defer rows.Close()

	type dbFile struct {
		name     string
		typeDesc string
	}
	files := make([]dbFile, 0)
	dataFileCount := 0
	logFileCount := 0
	for rows.Next() {
		var f dbFile
		if scanErr := rows.Scan(&f.name, &f.typeDesc); scanErr != nil {
			return fmt.Errorf("scan database file metadata: %w", scanErr)
		}
		files = append(files, f)
		if strings.EqualFold(f.typeDesc, "LOG") {
			logFileCount++
		} else {
			dataFileCount++
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate database files: %w", err)
	}

	for _, f := range files {
		fileNameEscaped := strings.ReplaceAll(f.name, "]", "]]")
		maxSize := max(1, dataMaxSizeMB/max(1, dataFileCount))
		if strings.EqualFold(f.typeDesc, "LOG") {
			maxSize = max(1, logMaxSizeMB/max(1, logFileCount))
		}
		growthSize := max(1, maxSize/5)
		stmtGrowth := fmt.Sprintf(`ALTER DATABASE [%s] MODIFY FILE (NAME = N'%s', FILEGROWTH = %dMB);`, dbNameEscaped, fileNameEscaped, growthSize)
		if _, err := appDB.ExecContext(ctx, stmtGrowth); err != nil {
			return fmt.Errorf("set growth for db file %q: %w", f.name, err)
		}
		stmtMax := fmt.Sprintf(`ALTER DATABASE [%s] MODIFY FILE (NAME = N'%s', MAXSIZE = %dMB);`, dbNameEscaped, fileNameEscaped, maxSize)
		if _, err := appDB.ExecContext(ctx, stmtMax); err != nil {
			return fmt.Errorf("set max size for db file %q: %w", f.name, err)
		}
	}

	return nil
}

func runMigrations(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`IF OBJECT_ID(N'dbo.case_records', N'U') IS NULL
BEGIN
	    CREATE TABLE dbo.case_records (
	        case_number NVARCHAR(64) NOT NULL PRIMARY KEY,
	        source_url NVARCHAR(2048) NULL,
	        payload NVARCHAR(MAX) NOT NULL,
	        payload_hash CHAR(64) NOT NULL,
	        created_at DATETIME2 NOT NULL,
	        updated_at DATETIME2 NOT NULL,
	        last_query_at DATETIME2 NOT NULL,
	        last_observed_change_at DATETIME2 NOT NULL,
	        last_successful_payload_hash CHAR(64) NULL,
	        last_successful_at DATETIME2 NULL,
	        last_scrape_had_errors BIT NOT NULL DEFAULT 0,
	        last_scrape_error_at DATETIME2 NULL
	    );
	END`,
		`IF COL_LENGTH(N'dbo.case_records', N'last_observed_change_at') IS NULL
BEGIN
    ALTER TABLE dbo.case_records
    ADD last_observed_change_at DATETIME2 NULL;
END`,
		`IF COL_LENGTH(N'dbo.case_records', N'last_observed_change_at') IS NOT NULL
BEGIN
    UPDATE dbo.case_records
    SET last_observed_change_at = ISNULL(updated_at, created_at)
    WHERE last_observed_change_at IS NULL;
END`,
		`IF EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.case_records')
      AND name = N'last_observed_change_at'
      AND is_nullable = 1
)
BEGIN
    ALTER TABLE dbo.case_records
    ALTER COLUMN last_observed_change_at DATETIME2 NOT NULL;
END`,
		`IF COL_LENGTH(N'dbo.case_records', N'last_successful_payload_hash') IS NULL
BEGIN
    ALTER TABLE dbo.case_records
    ADD last_successful_payload_hash CHAR(64) NULL;
END`,
		`IF COL_LENGTH(N'dbo.case_records', N'last_successful_at') IS NULL
BEGIN
    ALTER TABLE dbo.case_records
    ADD last_successful_at DATETIME2 NULL;
END`,
		`IF COL_LENGTH(N'dbo.case_records', N'last_scrape_had_errors') IS NULL
BEGIN
    ALTER TABLE dbo.case_records
    ADD last_scrape_had_errors BIT NOT NULL CONSTRAINT DF_case_records_last_scrape_had_errors DEFAULT 0;
END`,
		`IF COL_LENGTH(N'dbo.case_records', N'last_scrape_error_at') IS NULL
BEGIN
    ALTER TABLE dbo.case_records
    ADD last_scrape_error_at DATETIME2 NULL;
END`,
		`UPDATE dbo.case_records
SET last_successful_payload_hash = payload_hash
WHERE last_successful_payload_hash IS NULL`,
		`UPDATE dbo.case_records
SET last_successful_at = updated_at
WHERE last_successful_at IS NULL
  AND last_successful_payload_hash IS NOT NULL`,
		`IF OBJECT_ID(N'dbo.case_parties', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.case_parties (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        case_number NVARCHAR(64) NOT NULL,
        role NVARCHAR(128) NULL,
        full_name NVARCHAR(256) NULL,
        first_name NVARCHAR(128) NULL,
        last_name NVARCHAR(128) NULL,
        dob NVARCHAR(32) NULL,
        normalized_name NVARCHAR(256) NULL,
        last_seen_at DATETIME2 NOT NULL,
        CONSTRAINT FK_case_parties_case_records
            FOREIGN KEY (case_number)
            REFERENCES dbo.case_records(case_number)
            ON DELETE CASCADE
    );
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'idx_case_parties_normalized_name'
      AND object_id = OBJECT_ID(N'dbo.case_parties')
)
BEGIN
    CREATE INDEX idx_case_parties_normalized_name
    ON dbo.case_parties (normalized_name, last_seen_at DESC);
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'idx_case_records_last_query_at'
      AND object_id = OBJECT_ID(N'dbo.case_records')
)
BEGIN
    CREATE INDEX idx_case_records_last_query_at
    ON dbo.case_records (last_query_at ASC);
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'idx_case_records_last_observed_change_at'
      AND object_id = OBJECT_ID(N'dbo.case_records')
)
BEGIN
    CREATE INDEX idx_case_records_last_observed_change_at
    ON dbo.case_records (last_observed_change_at DESC);
END`,
		`IF OBJECT_ID(N'dbo.cv_cases', N'U') IS NULL
BEGIN
	    CREATE TABLE dbo.cv_cases (
	        case_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	        case_number NVARCHAR(64) NOT NULL,
	        case_number_normalized NVARCHAR(64) NOT NULL,
	        case_type NVARCHAR(128) NULL,
	        case_status NVARCHAR(64) NULL,
	        file_date DATE NULL,
	        case_judge NVARCHAR(1024) NULL,
	        source_url NVARCHAR(2048) NULL,
	        first_seen_at DATETIME2 NOT NULL,
	        last_seen_at DATETIME2 NOT NULL,
	        last_observed_change_at DATETIME2 NOT NULL,
	        is_active BIT NOT NULL DEFAULT 1,
	        CONSTRAINT UQ_cv_cases_case_number UNIQUE (case_number),
	        CONSTRAINT FK_cv_cases_case_records
	            FOREIGN KEY (case_number)
            REFERENCES dbo.case_records(case_number)
            ON DELETE CASCADE
    );
END`,
		`IF COL_LENGTH(N'dbo.cv_cases', N'case_judge') IS NOT NULL
   AND COL_LENGTH(N'dbo.cv_cases', N'case_judge') < 1024
	BEGIN
	    ALTER TABLE dbo.cv_cases
	    ALTER COLUMN case_judge NVARCHAR(1024) NULL;
	END`,
		`IF COL_LENGTH(N'dbo.cv_cases', N'last_observed_change_at') IS NULL
BEGIN
    ALTER TABLE dbo.cv_cases
    ADD last_observed_change_at DATETIME2 NULL;
END`,
		`IF COL_LENGTH(N'dbo.cv_cases', N'last_observed_change_at') IS NOT NULL
BEGIN
    UPDATE dbo.cv_cases
    SET last_observed_change_at = ISNULL(last_seen_at, first_seen_at)
    WHERE last_observed_change_at IS NULL;
END`,
		`IF EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.cv_cases')
      AND name = N'last_observed_change_at'
      AND is_nullable = 1
)
BEGIN
    ALTER TABLE dbo.cv_cases
    ALTER COLUMN last_observed_change_at DATETIME2 NOT NULL;
END`,
		`IF OBJECT_ID(N'dbo.cv_case_snapshots', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_case_snapshots (
        snapshot_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        case_id BIGINT NOT NULL,
        payload_json NVARCHAR(MAX) NOT NULL,
        payload_hash CHAR(64) NOT NULL,
        parser_version NVARCHAR(32) NOT NULL,
        source_url NVARCHAR(2048) NULL,
        captured_at DATETIME2 NOT NULL,
        is_latest BIT NOT NULL DEFAULT 1,
        CONSTRAINT FK_cv_case_snapshots_case
            FOREIGN KEY (case_id)
            REFERENCES dbo.cv_cases(case_id)
            ON DELETE CASCADE
    );
END`,
		`IF OBJECT_ID(N'dbo.cv_people', N'U') IS NULL
BEGIN
	    CREATE TABLE dbo.cv_people (
	        person_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	        full_name_raw NVARCHAR(256) NOT NULL,
	        normalized_name NVARCHAR(256) NOT NULL,
        first_name NVARCHAR(128) NULL,
        middle_name NVARCHAR(128) NULL,
        last_name NVARCHAR(128) NULL,
        suffix NVARCHAR(32) NULL,
        dob DATE NULL,
	        dob_raw NVARCHAR(32) NULL,
	        is_government_entity BIT NOT NULL DEFAULT 0,
	        entity_type NVARCHAR(64) NULL,
	        first_seen_at DATETIME2 NOT NULL,
	        last_seen_at DATETIME2 NOT NULL,
	        last_observed_change_at DATETIME2 NOT NULL
	    );
	END`,
		`IF COL_LENGTH(N'dbo.cv_people', N'last_observed_change_at') IS NULL
BEGIN
    ALTER TABLE dbo.cv_people
    ADD last_observed_change_at DATETIME2 NULL;
END`,
		`IF COL_LENGTH(N'dbo.cv_people', N'last_observed_change_at') IS NOT NULL
BEGIN
    UPDATE dbo.cv_people
    SET last_observed_change_at = ISNULL(last_seen_at, first_seen_at)
    WHERE last_observed_change_at IS NULL;
END`,
		`IF EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dbo.cv_people')
      AND name = N'last_observed_change_at'
      AND is_nullable = 1
)
BEGIN
    ALTER TABLE dbo.cv_people
    ALTER COLUMN last_observed_change_at DATETIME2 NOT NULL;
END`,
		`IF OBJECT_ID(N'dbo.cv_person_aliases', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_person_aliases (
        person_alias_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        person_id BIGINT NOT NULL,
        source_case_id BIGINT NULL,
        alias_name_raw NVARCHAR(256) NOT NULL,
        alias_name_normalized NVARCHAR(256) NOT NULL,
        first_seen_at DATETIME2 NOT NULL,
        last_seen_at DATETIME2 NOT NULL,
        CONSTRAINT FK_cv_person_aliases_person
            FOREIGN KEY (person_id)
            REFERENCES dbo.cv_people(person_id)
            ON DELETE CASCADE,
        CONSTRAINT FK_cv_person_aliases_case
            FOREIGN KEY (source_case_id)
            REFERENCES dbo.cv_cases(case_id)
            ON DELETE SET NULL
    );
END`,
		`IF OBJECT_ID(N'dbo.cv_person_dobs', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_person_dobs (
        person_dob_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        person_id BIGINT NOT NULL,
        source_case_id BIGINT NULL,
        dob_raw NVARCHAR(32) NOT NULL,
        dob_date DATE NULL,
        first_seen_at DATETIME2 NOT NULL,
        last_seen_at DATETIME2 NOT NULL,
        CONSTRAINT FK_cv_person_dobs_person
            FOREIGN KEY (person_id)
            REFERENCES dbo.cv_people(person_id)
            ON DELETE CASCADE,
        CONSTRAINT FK_cv_person_dobs_case
            FOREIGN KEY (source_case_id)
            REFERENCES dbo.cv_cases(case_id)
            ON DELETE SET NULL
    );
END`,
		`IF OBJECT_ID(N'dbo.cv_case_parties', N'U') IS NOT NULL
BEGIN
INSERT INTO dbo.cv_person_aliases (
    person_id, source_case_id, alias_name_raw, alias_name_normalized, first_seen_at, last_seen_at
)
SELECT
    cp.person_id,
    MIN(cp.case_id) AS source_case_id,
    MAX(LTRIM(RTRIM(cp.party_name_raw))) AS alias_name_raw,
    LOWER(LTRIM(RTRIM(cp.party_name_raw))) AS alias_name_normalized,
    MIN(cp.first_seen_at) AS first_seen_at,
    MAX(cp.last_seen_at) AS last_seen_at
FROM dbo.cv_case_parties cp
WHERE cp.person_id IS NOT NULL
  AND NULLIF(LTRIM(RTRIM(cp.party_name_raw)), N'') IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dbo.cv_person_aliases a
      WHERE a.person_id = cp.person_id
        AND a.alias_name_normalized = LOWER(LTRIM(RTRIM(cp.party_name_raw)))
  )
GROUP BY cp.person_id, LOWER(LTRIM(RTRIM(cp.party_name_raw)))
END`,
		`IF OBJECT_ID(N'dbo.cv_case_parties', N'U') IS NOT NULL
BEGIN
INSERT INTO dbo.cv_person_dobs (
    person_id, source_case_id, dob_raw, dob_date, first_seen_at, last_seen_at
)
SELECT
    cp.person_id,
    MIN(cp.case_id) AS source_case_id,
    LTRIM(RTRIM(cp.dob_raw)) AS dob_raw,
    TRY_CONVERT(date, LTRIM(RTRIM(cp.dob_raw)), 101) AS dob_date,
    MIN(cp.first_seen_at) AS first_seen_at,
    MAX(cp.last_seen_at) AS last_seen_at
FROM dbo.cv_case_parties cp
WHERE cp.person_id IS NOT NULL
  AND NULLIF(LTRIM(RTRIM(cp.dob_raw)), N'') IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dbo.cv_person_dobs d
      WHERE d.person_id = cp.person_id
        AND d.dob_raw = LTRIM(RTRIM(cp.dob_raw))
  )
GROUP BY cp.person_id, LTRIM(RTRIM(cp.dob_raw))
END`,
		`UPDATE dbo.cv_person_dobs
SET dob_date = TRY_CONVERT(date, dob_raw, 101)
WHERE dob_date IS NULL
  AND TRY_CONVERT(date, dob_raw, 101) IS NOT NULL`,
		`IF OBJECT_ID(N'dbo.cv_case_parties', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_case_parties (
        case_party_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        case_id BIGINT NOT NULL,
        person_id BIGINT NULL,
        party_name_raw NVARCHAR(256) NOT NULL,
        party_role NVARCHAR(128) NOT NULL,
        party_role_group NVARCHAR(64) NULL,
        dob_raw NVARCHAR(32) NULL,
        is_primary_defendant BIT NOT NULL DEFAULT 0,
        is_active BIT NOT NULL DEFAULT 1,
        first_seen_at DATETIME2 NOT NULL,
        last_seen_at DATETIME2 NOT NULL,
        CONSTRAINT FK_cv_case_parties_case
            FOREIGN KEY (case_id)
            REFERENCES dbo.cv_cases(case_id)
            ON DELETE CASCADE,
        CONSTRAINT FK_cv_case_parties_person
            FOREIGN KEY (person_id)
            REFERENCES dbo.cv_people(person_id)
    );
END`,
		`IF OBJECT_ID(N'dbo.cv_charges', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_charges (
        charge_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        case_id BIGINT NOT NULL,
        case_party_id BIGINT NULL,
        charge_sequence INT NULL,
        charge_code NVARCHAR(64) NULL,
        charge_description NVARCHAR(1024) NOT NULL,
        statute NVARCHAR(256) NULL,
        offense_level NVARCHAR(64) NULL,
        charge_date DATE NULL,
        offense_date DATE NULL,
        stage_date DATE NULL,
        filing_date DATE NULL,
        is_attempt BIT NOT NULL DEFAULT 0,
        is_amended BIT NOT NULL DEFAULT 0,
        offense_location NVARCHAR(256) NULL,
        atn NVARCHAR(64) NULL,
        tracking_number NVARCHAR(64) NULL,
        dv_related BIT NULL,
        original_charge_text NVARCHAR(MAX) NULL,
        indicted_charge_text NVARCHAR(MAX) NULL,
        amended_charge_text NVARCHAR(MAX) NULL,
        modifiers NVARCHAR(512) NULL,
        source_tab NVARCHAR(64) NULL,
        source_row_index INT NULL,
        raw_row_json NVARCHAR(MAX) NULL,
        first_seen_at DATETIME2 NOT NULL,
        last_seen_at DATETIME2 NOT NULL,
        CONSTRAINT FK_cv_charges_case
            FOREIGN KEY (case_id)
            REFERENCES dbo.cv_cases(case_id)
            ON DELETE CASCADE,
        CONSTRAINT FK_cv_charges_case_party
            FOREIGN KEY (case_party_id)
            REFERENCES dbo.cv_case_parties(case_party_id)
    );
END`,
		`IF COL_LENGTH(N'dbo.cv_charges', N'stage_date') IS NULL
BEGIN
    ALTER TABLE dbo.cv_charges ADD stage_date DATE NULL;
END`,
		`IF COL_LENGTH(N'dbo.cv_charges', N'offense_location') IS NULL
BEGIN
    ALTER TABLE dbo.cv_charges ADD offense_location NVARCHAR(256) NULL;
END`,
		`IF COL_LENGTH(N'dbo.cv_charges', N'atn') IS NULL
BEGIN
    ALTER TABLE dbo.cv_charges ADD atn NVARCHAR(64) NULL;
END`,
		`IF COL_LENGTH(N'dbo.cv_charges', N'tracking_number') IS NULL
BEGIN
    ALTER TABLE dbo.cv_charges ADD tracking_number NVARCHAR(64) NULL;
END`,
		`IF COL_LENGTH(N'dbo.cv_charges', N'dv_related') IS NULL
BEGIN
    ALTER TABLE dbo.cv_charges ADD dv_related BIT NULL;
END`,
		`IF COL_LENGTH(N'dbo.cv_charges', N'original_charge_text') IS NULL
BEGIN
    ALTER TABLE dbo.cv_charges ADD original_charge_text NVARCHAR(MAX) NULL;
END`,
		`IF COL_LENGTH(N'dbo.cv_charges', N'indicted_charge_text') IS NULL
BEGIN
    ALTER TABLE dbo.cv_charges ADD indicted_charge_text NVARCHAR(MAX) NULL;
END`,
		`IF COL_LENGTH(N'dbo.cv_charges', N'amended_charge_text') IS NULL
BEGIN
    ALTER TABLE dbo.cv_charges ADD amended_charge_text NVARCHAR(MAX) NULL;
END`,
		`IF COL_LENGTH(N'dbo.cv_charges', N'modifiers') IS NULL
BEGIN
    ALTER TABLE dbo.cv_charges ADD modifiers NVARCHAR(512) NULL;
END`,
		`IF COL_LENGTH(N'dbo.cv_charges', N'source_tab') IS NULL
BEGIN
    ALTER TABLE dbo.cv_charges ADD source_tab NVARCHAR(64) NULL;
END`,
		`IF COL_LENGTH(N'dbo.cv_charges', N'source_row_index') IS NULL
BEGIN
    ALTER TABLE dbo.cv_charges ADD source_row_index INT NULL;
END`,
		`IF OBJECT_ID(N'dbo.cv_charge_dispositions', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_charge_dispositions (
        charge_disposition_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        charge_id BIGINT NOT NULL,
        disposition_status NVARCHAR(256) NULL,
        disposition_date DATE NULL,
        plea NVARCHAR(128) NULL,
        sentence_summary NVARCHAR(1024) NULL,
        fine_amount DECIMAL(12,2) NULL,
        jail_days INT NULL,
        probation_days INT NULL,
        is_final BIT NOT NULL DEFAULT 0,
        raw_row_json NVARCHAR(MAX) NULL,
        first_seen_at DATETIME2 NOT NULL,
        last_seen_at DATETIME2 NOT NULL,
        CONSTRAINT FK_cv_charge_dispositions_charge
            FOREIGN KEY (charge_id)
            REFERENCES dbo.cv_charges(charge_id)
            ON DELETE CASCADE
    );
END`,
		`IF OBJECT_ID(N'dbo.cv_case_events', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_case_events (
        event_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        case_id BIGINT NOT NULL,
        event_datetime DATETIME2 NULL,
        event_date DATE NULL,
        event_time NVARCHAR(32) NULL,
        location NVARCHAR(256) NULL,
        event_type NVARCHAR(256) NULL,
        event_result NVARCHAR(512) NULL,
        event_judge NVARCHAR(256) NULL,
        source_tab NVARCHAR(64) NULL,
        source_row_index INT NULL,
        raw_row_json NVARCHAR(MAX) NULL,
        first_seen_at DATETIME2 NOT NULL,
        last_seen_at DATETIME2 NOT NULL,
        CONSTRAINT FK_cv_case_events_case
            FOREIGN KEY (case_id)
            REFERENCES dbo.cv_cases(case_id)
            ON DELETE CASCADE
    );
END`,
		`IF OBJECT_ID(N'dbo.cv_docket_entries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_docket_entries (
        docket_entry_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        case_id BIGINT NOT NULL,
        docket_date DATE NULL,
        docket_text NVARCHAR(MAX) NOT NULL,
        docket_sequence INT NULL,
        source_tab NVARCHAR(64) NULL,
        source_row_index INT NULL,
        raw_row_json NVARCHAR(MAX) NULL,
        first_seen_at DATETIME2 NOT NULL,
        last_seen_at DATETIME2 NOT NULL,
        CONSTRAINT FK_cv_docket_entries_case
            FOREIGN KEY (case_id)
            REFERENCES dbo.cv_cases(case_id)
            ON DELETE CASCADE
    );
END`,
		`IF OBJECT_ID(N'dbo.cv_tab_pages', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_tab_pages (
        tab_page_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        snapshot_id BIGINT NOT NULL,
        tab_name NVARCHAR(128) NOT NULL,
        page_url NVARCHAR(2048) NULL,
        page_title NVARCHAR(512) NULL,
        page_h1 NVARCHAR(512) NULL,
        main_text_excerpt NVARCHAR(MAX) NULL,
        captured_at DATETIME2 NOT NULL,
        CONSTRAINT FK_cv_tab_pages_snapshot
            FOREIGN KEY (snapshot_id)
            REFERENCES dbo.cv_case_snapshots(snapshot_id)
            ON DELETE CASCADE
    );
END`,
		`IF OBJECT_ID(N'dbo.cv_tab_label_values', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_tab_label_values (
        tab_label_value_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        tab_page_id BIGINT NOT NULL,
        label_name NVARCHAR(256) NOT NULL,
        label_value NVARCHAR(MAX) NULL,
        captured_at DATETIME2 NOT NULL,
        CONSTRAINT FK_cv_tab_label_values_tab_page
            FOREIGN KEY (tab_page_id)
            REFERENCES dbo.cv_tab_pages(tab_page_id)
            ON DELETE CASCADE
    );
END`,
		`IF OBJECT_ID(N'dbo.cv_tab_tables', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_tab_tables (
        tab_table_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        tab_page_id BIGINT NOT NULL,
        table_index INT NOT NULL,
        headers_json NVARCHAR(MAX) NOT NULL,
        captured_at DATETIME2 NOT NULL,
        CONSTRAINT FK_cv_tab_tables_tab_page
            FOREIGN KEY (tab_page_id)
            REFERENCES dbo.cv_tab_pages(tab_page_id)
            ON DELETE CASCADE
    );
END`,
		`IF OBJECT_ID(N'dbo.cv_tab_table_rows', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_tab_table_rows (
        tab_table_row_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        tab_table_id BIGINT NOT NULL,
        row_index INT NOT NULL,
        cells_json NVARCHAR(MAX) NOT NULL,
        captured_at DATETIME2 NOT NULL,
        CONSTRAINT FK_cv_tab_table_rows_tab_table
            FOREIGN KEY (tab_table_id)
            REFERENCES dbo.cv_tab_tables(tab_table_id)
            ON DELETE CASCADE
    );
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'idx_cv_cases_last_seen_at'
      AND object_id = OBJECT_ID(N'dbo.cv_cases')
)
BEGIN
    CREATE INDEX idx_cv_cases_last_seen_at
    ON dbo.cv_cases (last_seen_at DESC);
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'idx_cv_cases_last_observed_change_at'
      AND object_id = OBJECT_ID(N'dbo.cv_cases')
)
BEGIN
    CREATE INDEX idx_cv_cases_last_observed_change_at
    ON dbo.cv_cases (last_observed_change_at DESC);
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'idx_cv_case_snapshots_case_latest'
      AND object_id = OBJECT_ID(N'dbo.cv_case_snapshots')
)
BEGIN
    CREATE INDEX idx_cv_case_snapshots_case_latest
    ON dbo.cv_case_snapshots (case_id, is_latest, captured_at DESC);
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'idx_cv_people_normalized_name'
      AND object_id = OBJECT_ID(N'dbo.cv_people')
)
BEGIN
    CREATE INDEX idx_cv_people_normalized_name
    ON dbo.cv_people (normalized_name, dob);
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'idx_cv_people_last_observed_change_at'
      AND object_id = OBJECT_ID(N'dbo.cv_people')
)
BEGIN
    CREATE INDEX idx_cv_people_last_observed_change_at
    ON dbo.cv_people (last_observed_change_at DESC);
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'uq_cv_person_aliases_person_alias'
      AND object_id = OBJECT_ID(N'dbo.cv_person_aliases')
)
BEGIN
    CREATE UNIQUE INDEX uq_cv_person_aliases_person_alias
    ON dbo.cv_person_aliases (person_id, alias_name_normalized);
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'idx_cv_person_aliases_alias_name'
      AND object_id = OBJECT_ID(N'dbo.cv_person_aliases')
)
BEGIN
    CREATE INDEX idx_cv_person_aliases_alias_name
    ON dbo.cv_person_aliases (alias_name_normalized, last_seen_at DESC);
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'uq_cv_person_dobs_person_dob_raw'
      AND object_id = OBJECT_ID(N'dbo.cv_person_dobs')
)
BEGIN
    CREATE UNIQUE INDEX uq_cv_person_dobs_person_dob_raw
    ON dbo.cv_person_dobs (person_id, dob_raw);
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'idx_cv_person_dobs_dob_date'
      AND object_id = OBJECT_ID(N'dbo.cv_person_dobs')
)
BEGIN
    CREATE INDEX idx_cv_person_dobs_dob_date
    ON dbo.cv_person_dobs (dob_date, last_seen_at DESC);
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'idx_cv_case_parties_case_role'
      AND object_id = OBJECT_ID(N'dbo.cv_case_parties')
)
BEGIN
    CREATE INDEX idx_cv_case_parties_case_role
    ON dbo.cv_case_parties (case_id, party_role, last_seen_at DESC);
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'idx_cv_charges_case_sequence'
      AND object_id = OBJECT_ID(N'dbo.cv_charges')
)
BEGIN
    CREATE INDEX idx_cv_charges_case_sequence
    ON dbo.cv_charges (case_id, charge_sequence, last_seen_at DESC);
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'idx_cv_charge_dispositions_charge'
      AND object_id = OBJECT_ID(N'dbo.cv_charge_dispositions')
)
BEGIN
    CREATE INDEX idx_cv_charge_dispositions_charge
    ON dbo.cv_charge_dispositions (charge_id, disposition_date, last_seen_at DESC);
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'idx_cv_case_events_case_datetime'
      AND object_id = OBJECT_ID(N'dbo.cv_case_events')
)
BEGIN
    CREATE INDEX idx_cv_case_events_case_datetime
    ON dbo.cv_case_events (case_id, event_datetime, event_date);
END`,
		`IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'idx_cv_docket_entries_case_date'
      AND object_id = OBJECT_ID(N'dbo.cv_docket_entries')
)
BEGIN
    CREATE INDEX idx_cv_docket_entries_case_date
    ON dbo.cv_docket_entries (case_id, docket_date, docket_sequence);
END`,
	}

	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("run migration: %w", err)
		}
	}
	return nil
}

func buildDSN(user, password, host string, port int, database, encrypt string, trustServerCertificate bool) string {
	q := url.Values{}
	q.Set("database", database)
	q.Set("encrypt", encrypt)
	q.Set("TrustServerCertificate", strconv.FormatBool(trustServerCertificate))
	return fmt.Sprintf("sqlserver://%s:%s@%s:%d?%s",
		url.QueryEscape(user),
		url.QueryEscape(password),
		host,
		port,
		q.Encode(),
	)
}

func stringOrDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
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

func boolFromEnv(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}
