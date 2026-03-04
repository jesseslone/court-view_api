-- SQL Server (T-SQL) schema used by the API service.
--
-- The service applies this shape automatically on startup when DB is enabled.
-- This file is provided for review/manual execution.

IF OBJECT_ID(N'dbo.case_records', N'U') IS NULL
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
END
GO

IF OBJECT_ID(N'dbo.case_parties', N'U') IS NULL
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
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'idx_case_parties_normalized_name'
      AND object_id = OBJECT_ID(N'dbo.case_parties')
)
BEGIN
    CREATE INDEX idx_case_parties_normalized_name
    ON dbo.case_parties (normalized_name, last_seen_at DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'idx_case_records_last_query_at'
      AND object_id = OBJECT_ID(N'dbo.case_records')
)
BEGIN
    CREATE INDEX idx_case_records_last_query_at
    ON dbo.case_records (last_query_at ASC);
END
GO
