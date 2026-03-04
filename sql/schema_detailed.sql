-- Detailed SQL Server (T-SQL) schema for CourtView data.
--
-- Purpose:
-- 1) Keep a normalized model for analytics/reporting.
-- 2) Keep raw snapshots for traceability and parser evolution.
--
-- Notes:
-- - This schema is additive and does not replace sql/schema.sql runtime tables.
-- - Use this as the target model for richer ingestion pipelines.

IF OBJECT_ID(N'dbo.cv_cases', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_cases (
        case_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        case_number NVARCHAR(64) NOT NULL,
        case_number_normalized NVARCHAR(64) NOT NULL,
        case_type NVARCHAR(128) NULL,
        case_status NVARCHAR(64) NULL,
        file_date DATE NULL,
        case_judge NVARCHAR(256) NULL,
        source_url NVARCHAR(2048) NULL,
        first_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        last_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        is_active BIT NOT NULL DEFAULT 1,
        CONSTRAINT UQ_cv_cases_case_number UNIQUE (case_number),
        CONSTRAINT UQ_cv_cases_case_number_normalized UNIQUE (case_number_normalized)
    );
END
GO

IF OBJECT_ID(N'dbo.cv_case_snapshots', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_case_snapshots (
        snapshot_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        case_id BIGINT NOT NULL,
        payload_json NVARCHAR(MAX) NOT NULL,
        payload_hash CHAR(64) NOT NULL,
        parser_version NVARCHAR(32) NOT NULL DEFAULT N'v1',
        source_url NVARCHAR(2048) NULL,
        captured_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        is_latest BIT NOT NULL DEFAULT 1,
        CONSTRAINT FK_cv_case_snapshots_case
            FOREIGN KEY (case_id) REFERENCES dbo.cv_cases(case_id) ON DELETE CASCADE
    );
END
GO

IF OBJECT_ID(N'dbo.cv_people', N'U') IS NULL
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
        entity_type NVARCHAR(64) NULL, -- Person, Government, Business, Unknown
        first_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        last_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

IF OBJECT_ID(N'dbo.cv_case_parties', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_case_parties (
        case_party_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        case_id BIGINT NOT NULL,
        person_id BIGINT NULL,
        party_name_raw NVARCHAR(256) NOT NULL,
        party_role NVARCHAR(128) NOT NULL, -- Defendant, Prosecution, Victim, Attorney, etc.
        party_role_group NVARCHAR(64) NULL, -- Defendant-side, Prosecution-side, Court, Other
        dob_raw NVARCHAR(32) NULL,
        is_primary_defendant BIT NOT NULL DEFAULT 0,
        is_active BIT NOT NULL DEFAULT 1,
        first_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        last_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_cv_case_parties_case
            FOREIGN KEY (case_id) REFERENCES dbo.cv_cases(case_id) ON DELETE CASCADE,
        CONSTRAINT FK_cv_case_parties_person
            FOREIGN KEY (person_id) REFERENCES dbo.cv_people(person_id)
    );
END
GO

IF OBJECT_ID(N'dbo.cv_charges', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_charges (
        charge_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        case_id BIGINT NOT NULL,
        case_party_id BIGINT NULL, -- usually the defendant this charge belongs to
        charge_sequence INT NULL, -- count/order from CourtView
        charge_code NVARCHAR(64) NULL,
        charge_description NVARCHAR(1024) NOT NULL,
        statute NVARCHAR(256) NULL,
        offense_level NVARCHAR(64) NULL, -- Felony, Misdemeanor, Violation, etc.
        charge_date DATE NULL,
        offense_date DATE NULL,
        filing_date DATE NULL,
        is_attempt BIT NOT NULL DEFAULT 0,
        is_amended BIT NOT NULL DEFAULT 0,
        raw_row_json NVARCHAR(MAX) NULL,
        first_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        last_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_cv_charges_case
            FOREIGN KEY (case_id) REFERENCES dbo.cv_cases(case_id) ON DELETE CASCADE,
        CONSTRAINT FK_cv_charges_case_party
            FOREIGN KEY (case_party_id) REFERENCES dbo.cv_case_parties(case_party_id)
    );
END
GO

IF OBJECT_ID(N'dbo.cv_charge_dispositions', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_charge_dispositions (
        charge_disposition_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        charge_id BIGINT NOT NULL,
        disposition_status NVARCHAR(256) NULL, -- Dismissed, Guilty, Not Guilty, etc.
        disposition_date DATE NULL,
        plea NVARCHAR(128) NULL,
        sentence_summary NVARCHAR(1024) NULL,
        fine_amount DECIMAL(12,2) NULL,
        jail_days INT NULL,
        probation_days INT NULL,
        is_final BIT NOT NULL DEFAULT 0,
        raw_row_json NVARCHAR(MAX) NULL,
        first_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        last_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_cv_charge_dispositions_charge
            FOREIGN KEY (charge_id) REFERENCES dbo.cv_charges(charge_id) ON DELETE CASCADE
    );
END
GO

IF OBJECT_ID(N'dbo.cv_case_events', N'U') IS NULL
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
        source_tab NVARCHAR(64) NULL, -- All Information / Event tab
        source_row_index INT NULL,
        raw_row_json NVARCHAR(MAX) NULL,
        first_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        last_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_cv_case_events_case
            FOREIGN KEY (case_id) REFERENCES dbo.cv_cases(case_id) ON DELETE CASCADE
    );
END
GO

IF OBJECT_ID(N'dbo.cv_docket_entries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_docket_entries (
        docket_entry_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        case_id BIGINT NOT NULL,
        docket_date DATE NULL,
        docket_text NVARCHAR(MAX) NOT NULL,
        docket_sequence INT NULL,
        source_tab NVARCHAR(64) NULL, -- All Information / Docket tab
        source_row_index INT NULL,
        raw_row_json NVARCHAR(MAX) NULL,
        first_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        last_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_cv_docket_entries_case
            FOREIGN KEY (case_id) REFERENCES dbo.cv_cases(case_id) ON DELETE CASCADE
    );
END
GO

IF OBJECT_ID(N'dbo.cv_tab_pages', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_tab_pages (
        tab_page_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        snapshot_id BIGINT NOT NULL,
        tab_name NVARCHAR(128) NOT NULL, -- Current, All Information, Party, Charge, Event, Docket
        page_url NVARCHAR(2048) NULL,
        page_title NVARCHAR(512) NULL,
        page_h1 NVARCHAR(512) NULL,
        main_text_excerpt NVARCHAR(MAX) NULL,
        captured_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_cv_tab_pages_snapshot
            FOREIGN KEY (snapshot_id) REFERENCES dbo.cv_case_snapshots(snapshot_id) ON DELETE CASCADE
    );
END
GO

IF OBJECT_ID(N'dbo.cv_tab_label_values', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_tab_label_values (
        tab_label_value_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        tab_page_id BIGINT NOT NULL,
        label_name NVARCHAR(256) NOT NULL,
        label_value NVARCHAR(MAX) NULL,
        captured_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_cv_tab_label_values_tab_page
            FOREIGN KEY (tab_page_id) REFERENCES dbo.cv_tab_pages(tab_page_id) ON DELETE CASCADE
    );
END
GO

IF OBJECT_ID(N'dbo.cv_tab_tables', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_tab_tables (
        tab_table_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        tab_page_id BIGINT NOT NULL,
        table_index INT NOT NULL,
        headers_json NVARCHAR(MAX) NOT NULL,
        captured_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_cv_tab_tables_tab_page
            FOREIGN KEY (tab_page_id) REFERENCES dbo.cv_tab_pages(tab_page_id) ON DELETE CASCADE
    );
END
GO

IF OBJECT_ID(N'dbo.cv_tab_table_rows', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_tab_table_rows (
        tab_table_row_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        tab_table_id BIGINT NOT NULL,
        row_index INT NOT NULL,
        cells_json NVARCHAR(MAX) NOT NULL,
        captured_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_cv_tab_table_rows_tab_table
            FOREIGN KEY (tab_table_id) REFERENCES dbo.cv_tab_tables(tab_table_id) ON DELETE CASCADE
    );
END
GO

IF OBJECT_ID(N'dbo.cv_extract_runs', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cv_extract_runs (
        extract_run_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        run_type NVARCHAR(64) NOT NULL, -- backfill, case_lookup, name_lookup
        request_json NVARCHAR(MAX) NOT NULL,
        summary_json NVARCHAR(MAX) NOT NULL,
        metrics_json NVARCHAR(MAX) NOT NULL,
        started_at DATETIME2 NOT NULL,
        finished_at DATETIME2 NOT NULL,
        success BIT NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

-- Indexes
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'idx_cv_cases_last_seen_at'
      AND object_id = OBJECT_ID(N'dbo.cv_cases')
)
BEGIN
    CREATE INDEX idx_cv_cases_last_seen_at ON dbo.cv_cases (last_seen_at DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'idx_cv_case_snapshots_case_latest'
      AND object_id = OBJECT_ID(N'dbo.cv_case_snapshots')
)
BEGIN
    CREATE INDEX idx_cv_case_snapshots_case_latest
    ON dbo.cv_case_snapshots (case_id, is_latest, captured_at DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'idx_cv_people_normalized_name'
      AND object_id = OBJECT_ID(N'dbo.cv_people')
)
BEGIN
    CREATE INDEX idx_cv_people_normalized_name ON dbo.cv_people (normalized_name, dob);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'idx_cv_case_parties_case_role'
      AND object_id = OBJECT_ID(N'dbo.cv_case_parties')
)
BEGIN
    CREATE INDEX idx_cv_case_parties_case_role ON dbo.cv_case_parties (case_id, party_role, last_seen_at DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'idx_cv_charges_case_sequence'
      AND object_id = OBJECT_ID(N'dbo.cv_charges')
)
BEGIN
    CREATE INDEX idx_cv_charges_case_sequence ON dbo.cv_charges (case_id, charge_sequence, last_seen_at DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'idx_cv_case_events_case_datetime'
      AND object_id = OBJECT_ID(N'dbo.cv_case_events')
)
BEGIN
    CREATE INDEX idx_cv_case_events_case_datetime ON dbo.cv_case_events (case_id, event_datetime, event_date);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'idx_cv_docket_entries_case_date'
      AND object_id = OBJECT_ID(N'dbo.cv_docket_entries')
)
BEGIN
    CREATE INDEX idx_cv_docket_entries_case_date ON dbo.cv_docket_entries (case_id, docket_date, docket_sequence);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'idx_cv_extract_runs_type_started_at'
      AND object_id = OBJECT_ID(N'dbo.cv_extract_runs')
)
BEGIN
    CREATE INDEX idx_cv_extract_runs_type_started_at ON dbo.cv_extract_runs (run_type, started_at DESC);
END
GO
