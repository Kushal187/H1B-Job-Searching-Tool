-- H1B Job Search Tool — Database Schema

CREATE TABLE IF NOT EXISTS sec_formd_companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL,
    cik_number TEXT,
    state TEXT,
    industry_group TEXT,
    total_amount_sold REAL,
    filing_date TEXT,
    normalized_name TEXT
);

CREATE TABLE IF NOT EXISTS h1b_sponsors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employer_name TEXT NOT NULL,
    city TEXT,
    state TEXT,
    naics_code TEXT,
    visa_class TEXT,
    initial_approvals INTEGER DEFAULT 0,
    continuing_approvals INTEGER DEFAULT 0,
    initial_denials INTEGER DEFAULT 0,
    fiscal_year TEXT,
    normalized_name TEXT
);

CREATE TABLE IF NOT EXISTS matched_companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL,
    normalized_name TEXT UNIQUE,
    source TEXT,  -- 'sec_only', 'h1b_only', 'both'
    h1b_approval_count INTEGER DEFAULT 0,
    sec_amount_raised REAL,
    priority_score REAL  -- higher = better target
);

-- Tracks which ATS each company uses (discovery cache)
CREATE TABLE IF NOT EXISTS company_ats_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER REFERENCES matched_companies(id),
    normalized_name TEXT NOT NULL,
    ats_system TEXT,         -- 'greenhouse', 'lever', 'ashby', 'workday', or NULL
    last_checked TEXT,       -- ISO timestamp of last check
    has_jobs INTEGER DEFAULT 0,  -- 1 if jobs were found on last check
    UNIQUE(normalized_name)
);

-- Workday board configuration (tenant/subdomain/board mappings)
CREATE TABLE IF NOT EXISTS workday_boards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER REFERENCES matched_companies(id),
    normalized_name TEXT NOT NULL,
    tenant TEXT NOT NULL,
    subdomain TEXT NOT NULL,
    board TEXT NOT NULL,
    url TEXT NOT NULL,
    job_count INTEGER DEFAULT 0,
    last_scraped TEXT,
    UNIQUE(tenant, board)
);

CREATE TABLE IF NOT EXISTS job_listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER REFERENCES matched_companies(id),
    company_name TEXT,
    ats_system TEXT,  -- 'greenhouse', 'lever', 'ashby', or 'workday'
    job_title TEXT,
    job_location TEXT,
    job_url TEXT UNIQUE,  -- unique constraint prevents duplicate jobs
    department TEXT,
    scraped_at TEXT,
    first_seen_at TEXT,   -- when we first discovered this job
    last_seen_at TEXT,    -- when we last saw this job on the ATS
    posted_at TEXT,       -- original publish date from the ATS
    is_active INTEGER DEFAULT 1,  -- 0 = removed from ATS, 1 = still listed
    raw_json TEXT
);

-- Indexes for fast matching lookups
CREATE INDEX IF NOT EXISTS idx_sec_normalized ON sec_formd_companies(normalized_name);
CREATE INDEX IF NOT EXISTS idx_h1b_normalized ON h1b_sponsors(normalized_name);
-- idx_matched_normalized is redundant — covered by UNIQUE constraint on normalized_name
CREATE INDEX IF NOT EXISTS idx_matched_priority ON matched_companies(priority_score DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_company ON job_listings(company_id);
CREATE INDEX IF NOT EXISTS idx_ats_status ON company_ats_status(ats_system);
CREATE INDEX IF NOT EXISTS idx_ats_normalized ON company_ats_status(normalized_name);
CREATE INDEX IF NOT EXISTS idx_workday_tenant ON workday_boards(tenant);
CREATE INDEX IF NOT EXISTS idx_workday_normalized ON workday_boards(normalized_name);