"""Central configuration for H1B Job Search Tool."""

import os

# ─── Project Paths ────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
SEC_DATA_DIR = os.path.join(DATA_DIR, "sec")
H1B_DATA_DIR = os.path.join(DATA_DIR, "h1b")
JOBS_DATA_DIR = os.path.join(DATA_DIR, "jobs")
GREENHOUSE_DIR = os.path.join(JOBS_DATA_DIR, "greenhouse")
LEVER_DIR = os.path.join(JOBS_DATA_DIR, "lever")
ASHBY_DIR = os.path.join(JOBS_DATA_DIR, "ashby")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
DB_PATH = os.path.join(DATA_DIR, "h1b_jobs.db")

# ─── SEC EDGAR Configuration ─────────────────────────────────────────────────

SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "H1BJobTool contact@example.com")

# Quarterly bulk data sets (tab-delimited ZIPs, ~3MB each)
SEC_BULK_BASE_URL = "https://www.sec.gov/files/structureddata/data/form-d-data-sets"
SEC_QUARTERS = []
for year in [2024, 2025]:
    for quarter in [1, 2, 3, 4]:
        SEC_QUARTERS.append(
            {
                "year": year,
                "quarter": quarter,
                "url": f"{SEC_BULK_BASE_URL}/{year}q{quarter}_d.zip",
                "filename": f"{year}q{quarter}_d.zip",
            }
        )

# EFTS search API for 2026 Q1 gap (recent filings not yet in bulk data)
SEC_EFTS_BASE_URL = "https://efts.sec.gov/LATEST/search-index"
SEC_EFTS_START_DATE = "2026-01-01"
SEC_EFTS_END_DATE = "2026-02-13"
SEC_EFTS_PAGE_SIZE = 100

SEC_RATE_LIMIT_DELAY = 0.15  # seconds between requests (stay under 10/sec)

# ─── DOL LCA Disclosure Data ─────────────────────────────────────────────────

DOL_LCA_BASE_URL = "https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs"
DOL_LCA_FILES = {
    "FY2024": {
        "url": f"{DOL_LCA_BASE_URL}/LCA_Disclosure_Data_FY2024_Q4.xlsx",
        "filename": "LCA_Disclosure_Data_FY2024_Q4.xlsx",
    },
    "FY2025": {
        "url": f"{DOL_LCA_BASE_URL}/LCA_Disclosure_Data_FY2025_Q4.xlsx",
        "filename": "LCA_Disclosure_Data_FY2025_Q4.xlsx",
    },
}

# ─── USCIS H1B Employer Data Hub ─────────────────────────────────────────────

USCIS_BASE_URL = "https://www.uscis.gov/sites/default/files/document/data"
USCIS_FILES = {
    "FY2023": {
        "url": f"{USCIS_BASE_URL}/h1b_datahubexport-2023.csv",
        "filename": "h1b_datahubexport-2023.csv",
    },
    "FY2024": {
        "url": f"{USCIS_BASE_URL}/h1b_datahubexport-2024.csv",
        "filename": "h1b_datahubexport-2024.csv",
    },
    "FY2025": {
        "url": f"{USCIS_BASE_URL}/h1b_datahubexport-2025.csv",
        "filename": "h1b_datahubexport-2025.csv",
    },
}

# Manually-downloaded Tableau exports (placed in H1B_DATA_DIR by user)
USCIS_LOCAL_FILES = [
    "Employer Information FY2024.csv",
    "Employer Information FY2025.csv",
    "Employer Information FY2026.csv",
]

# ─── Scraping Configuration ──────────────────────────────────────────────────

GREENHOUSE_API_URL = "https://boards-api.greenhouse.io/v1/boards/{company}/jobs?content=true"
LEVER_API_URL = "https://api.lever.co/v0/postings/{company}"
ASHBY_API_URL = "https://api.ashbyhq.com/posting-api/job-board/{company}"
WORKDAY_DIR = os.path.join(JOBS_DATA_DIR, "workday")
WORKDAY_PAGE_SIZE = 20  # jobs per API page (Workday default max)
WORKDAY_MAX_PAGES = 100  # safety cap: 100 pages * 20 = 2000 jobs max per board
WORKDAY_URLS_CSV = os.path.join(OUTPUT_DIR, "workday_urls.csv")

SCRAPE_DELAY = 0.15  # seconds between API requests
SCRAPE_TIMEOUT = 10  # request timeout in seconds
SCRAPE_MAX_RETRIES = 3  # retries for transient HTTP errors (429, 5xx, timeout)
SCRAPE_RETRY_BACKOFF = 1.0  # base seconds for exponential backoff (1s, 2s, 4s)
SCRAPE_CIRCUIT_BREAKER_THRESHOLD = 5  # consecutive failures per ATS to trip breaker
SCRAPE_CIRCUIT_BREAKER_COOLDOWN = 60  # seconds to skip requests when breaker is open

# ─── Matching Configuration ──────────────────────────────────────────────────

FUZZY_MATCH_THRESHOLD = 85  # minimum score for fuzzy name match
