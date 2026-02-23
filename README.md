# H1B Job Search Tool

A Python application that helps international students find H1B-sponsoring companies with open jobs. It cross-references SEC Form D filings with H1B employer data, then scrapes ATS job boards (Greenhouse, Lever, Ashby, Workday).

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Database Backends

This project now supports:

- SQLite (default, local file at `data/h1b_jobs.db`)
- Postgres/Supabase (set `DATABASE_URL` or `SUPABASE_DB_URL`)

### Supabase configuration

```bash
export DATABASE_URL="postgresql://<user>:<password>@<host>:5432/postgres?sslmode=require"
```

When `DATABASE_URL` is set, all pipeline and web commands use Postgres automatically.

`SEC_USER_AGENT` is only needed when running `python pipeline.py collect` (SEC API calls).

## Usage

### First run (full bootstrap)

```bash
python pipeline.py collect
python pipeline.py match
python pipeline.py scrape --mode discovery -w 10
python pipeline.py export
```

### Ongoing monitor run

```bash
python pipeline.py scrape --mode monitor -w 10
python pipeline.py export
```

### Migrate existing local SQLite DB to Supabase

```bash
python scripts/migrate_sqlite_to_supabase.py \
  --sqlite-path data/h1b_jobs.db \
  --postgres-url "$DATABASE_URL"
```

Or with Supabase project ref + DB password:

```bash
python scripts/migrate_sqlite_to_supabase.py \
  --sqlite-path data/h1b_jobs.db \
  --project-ref daugalaljnzvhtxfcsmj \
  --db-password "<your-db-password>"
```

### Run web UI

```bash
uvicorn web.app:app --host 0.0.0.0 --port 8000
```

## Deploy Web App

GitHub Pages is not suitable for this app because it requires a Python backend and database access.

Use Vercel for the web app + GitHub Actions for 6-hour monitor scraping.

### Vercel deploy steps

1. Push this repo to GitHub.
2. In Vercel, create a new project from this repo.
3. In project environment variables, set:
   - `DATABASE_URL` (Supabase Pooler URL)
4. Deploy.

This repo already includes:

- `api/index.py` (Vercel Python function entrypoint)
- `vercel.json` (routes all paths to FastAPI app)

Notes:

- `SEC_USER_AGENT` is not needed for monitor-only runs.
- Continue running scraping on GitHub Actions (`.github/workflows/monitor-supabase.yml`), not on Vercel serverless functions.
- On Vercel, avoid direct DB host (`db.<project-ref>.supabase.co:5432`) because it may resolve to IPv6 only.
  Use the Supabase "Connection pooling" URL from Project Settings -> Database -> Connect (typically `*.pooler.supabase.com`).

## GitHub Actions (every 6 hours)

Workflow file: `.github/workflows/monitor-supabase.yml`

Set repository secrets:

- `SUPABASE_DB_URL` (preferred), or:
- `SUPABASE_PROJECT_REF` + `SUPABASE_DB_PASSWORD`

The workflow:

1. Runs every 6 hours (UTC) and on manual dispatch.
2. Resolves `DATABASE_URL` from `SUPABASE_DB_URL`, or from `SUPABASE_PROJECT_REF` + `SUPABASE_DB_PASSWORD`.
3. Initializes schema in Supabase.
4. Runs monitor scrape + export on each run.
5. Uploads `output/` as a workflow artifact.

## Output

`output/` includes:

- `matched_companies.csv`
- `companies_with_jobs.csv`
- `new_jobs.csv`
- `summary_report.json`
