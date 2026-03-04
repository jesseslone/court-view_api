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

	adminDSN := buildDSN(user, password, host, port, "master", encrypt)
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

	appDSN := buildDSN(user, password, host, port, dbName, encrypt)
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
	hash := sha256.Sum256(payload)
	hashHex := hex.EncodeToString(hash[:])

	parties := courtview.ExtractParties(rows)

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var existingHash string
	err = tx.QueryRowContext(ctx, `
SELECT payload_hash
FROM case_records
WHERE case_number = @p1
`, caseNumber).Scan(&existingHash)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("read existing case hash: %w", err)
	}

	changed := false
	now := time.Now().UTC()
	sourceURL := strings.TrimSpace(caseDetails.CaseURL)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		changed = true
		if _, execErr := tx.ExecContext(ctx, `
INSERT INTO case_records (case_number, source_url, payload, payload_hash, created_at, updated_at, last_query_at)
VALUES (@p1, @p2, @p3, @p4, @p5, @p5, @p5)
`, caseNumber, sourceURL, string(payload), hashHex, now); execErr != nil {
			return false, fmt.Errorf("insert case_record: %w", execErr)
		}
	case existingHash == hashHex:
		if _, execErr := tx.ExecContext(ctx, `
UPDATE case_records
SET last_query_at = @p2
WHERE case_number = @p1
`, caseNumber, now); execErr != nil {
			return false, fmt.Errorf("touch case_record: %w", execErr)
		}
	default:
		changed = true
		if _, execErr := tx.ExecContext(ctx, `
UPDATE case_records
SET source_url = @p2,
    payload = @p3,
    payload_hash = @p4,
    updated_at = @p5,
    last_query_at = @p5
WHERE case_number = @p1
`, caseNumber, sourceURL, string(payload), hashHex, now); execErr != nil {
			return false, fmt.Errorf("update case_record: %w", execErr)
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

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit case upsert: %w", err)
	}

	if purgeErr := s.PurgeIfNeeded(ctx); purgeErr != nil {
		return changed, purgeErr
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
		stmt := fmt.Sprintf(`ALTER DATABASE [%s] MODIFY FILE (NAME = N'%s', MAXSIZE = %dMB);`, dbNameEscaped, fileNameEscaped, maxSize)
		if _, err := appDB.ExecContext(ctx, stmt); err != nil {
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
        last_query_at DATETIME2 NOT NULL
    );
END`,
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
	}

	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("run migration: %w", err)
		}
	}
	return nil
}

func buildDSN(user, password, host string, port int, database, encrypt string) string {
	q := url.Values{}
	q.Set("database", database)
	q.Set("encrypt", encrypt)
	q.Set("TrustServerCertificate", "true")
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
