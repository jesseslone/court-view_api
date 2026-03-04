-- CourtView extraction schema (PostgreSQL)

create table if not exists persons (
    id bigserial primary key,
    source_person_key text not null,
    first_name text,
    middle_name text,
    last_name text,
    suffix text,
    date_of_birth date,
    date_of_death date,
    normalized_name text,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    source_hash text not null,
    unique (source_person_key)
);

create table if not exists cases (
    id bigserial primary key,
    case_number text not null,
    case_url text,
    case_type text,
    case_status text,
    file_date date,
    court_location text,
    atn text,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    last_activity_date date,
    source_hash text not null,
    unique (case_number)
);

create table if not exists person_case_roles (
    id bigserial primary key,
    person_id bigint not null references persons(id) on delete cascade,
    case_id bigint not null references cases(id) on delete cascade,
    party_type text,
    affiliation text,
    source_hash text not null,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    unique (person_id, case_id, coalesce(party_type, ''), coalesce(affiliation, ''))
);

create table if not exists charges (
    id bigserial primary key,
    case_id bigint not null references cases(id) on delete cascade,
    tracking_number text,
    count_number text,
    statute text,
    charge_text text,
    offense_date date,
    charge_date date,
    stage_date date,
    source_hash text not null,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now()
);

create table if not exists charge_dispositions (
    id bigserial primary key,
    charge_id bigint not null references charges(id) on delete cascade,
    disposition text,
    disposition_date date,
    sentence_text text,
    source_hash text not null,
    observed_at timestamptz not null default now()
);

create table if not exists case_events (
    id bigserial primary key,
    case_id bigint not null references cases(id) on delete cascade,
    event_date date,
    event_time text,
    event_type text,
    event_description text,
    judicial_officer text,
    location text,
    source_hash text not null,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now()
);

create table if not exists docket_entries (
    id bigserial primary key,
    case_id bigint not null references cases(id) on delete cascade,
    docket_date date,
    docket_text text,
    filed_by text,
    source_hash text not null,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now()
);

create table if not exists case_snapshots (
    id bigserial primary key,
    case_id bigint not null references cases(id) on delete cascade,
    source_hash text not null,
    main_text_excerpt text,
    raw_payload jsonb not null,
    observed_at timestamptz not null default now(),
    unique (case_id, source_hash)
);

create table if not exists sync_runs (
    id bigserial primary key,
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    trigger_type text not null,
    query_payload jsonb,
    status text not null,
    rows_found integer not null default 0,
    cases_processed integer not null default 0,
    error_count integer not null default 0,
    message text
);

create table if not exists sync_errors (
    id bigserial primary key,
    sync_run_id bigint not null references sync_runs(id) on delete cascade,
    case_number text,
    case_url text,
    error_text text not null,
    created_at timestamptz not null default now()
);

create index if not exists idx_cases_status on cases(case_status);
create index if not exists idx_cases_file_date on cases(file_date);
create index if not exists idx_cases_last_activity_date on cases(last_activity_date);
create index if not exists idx_person_name on persons(last_name, first_name, date_of_birth);
create index if not exists idx_charges_case_id on charges(case_id);
create index if not exists idx_events_case_id on case_events(case_id);
create index if not exists idx_events_date on case_events(event_date);
create index if not exists idx_dockets_case_id on docket_entries(case_id);
create index if not exists idx_snapshots_case_id on case_snapshots(case_id, observed_at desc);
