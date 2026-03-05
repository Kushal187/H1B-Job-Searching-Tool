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

### Admin route lock/disable (production)

Admin routes are `/admin` and `/api/admin/*`.

- To lock with Basic Auth, set both:
  - `ADMIN_USERNAME`
  - `ADMIN_PASSWORD`
- To disable admin routes entirely, set:
  - `ADMIN_DISABLED=true`

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

## Resume Autotailor (JD -> Tailored PDF Resume)

This repo now includes a resume-tailoring backend + Chrome extension scaffold.

### New API endpoints

- `POST /api/profile/upsert`
- `GET /api/profile?profile_id=<optional>`
- `POST /api/resume/validate`
- `POST /api/resume/generate`

### New UI page

- `GET /profile` — profile editor UI for personal info + fact library.

### Quick start

1. Install new dependencies:

```bash
pip install -r requirements.txt
```

2. Configure `.env` (see `.env.example`) with Bedrock model IDs/region and optional LaTeX template path.

3. Run web app:

```bash
uvicorn web.app:app --host 0.0.0.0 --port 8000
```

4. Upsert profile:

```bash
curl -X POST http://localhost:8000/api/profile/upsert \\
  -H \"Content-Type: application/json\" \\
  -d '{\n+    \"name\": \"Your Name\",\n+    \"location\": \"Boston, MA\",\n+    \"headline\": \"Software Engineer\",\n+    \"constraints\": {\"inference_mode\": \"light\"},\n+    \"facts\": [\n+      {\n+        \"fact_type\": \"experience\",\n+        \"source_section\": \"Experience\",\n+        \"raw_text\": \"Built retrieval-augmented pipelines using AWS Lambda and vector databases\",\n+        \"normalized_keywords\": [\"aws\", \"rag\", \"lambda\", \"vector database\"],\n+        \"priority\": 95,\n+        \"active\": true\n+      }\n+    ]\n+  }'\n+```

5. Generate tailored resume:

```bash
curl -X POST http://localhost:8000/api/resume/generate \\
  -H \"Content-Type: application/json\" \\
  -d '{\n+    \"jd_text\": \"<paste job description>\",\n+    \"jd_url\": \"https://example.com/jobs/123\",\n+    \"page_title\": \"Software Engineer\",\n+    \"profile_id\": 1,\n+    \"target_role\": \"Software Engineer\",\n+    \"strictness\": \"balanced\",\n+    \"return_pdf_base64\": true\n+  }'\n+```

### Extension

See `extension/README.md` for Chrome setup and side-panel flow.

### PDF generation notes

- Primary path: compile LaTeX with `tectonic` (`TECTONIC_BIN` env var).
- Fallback path: if Tectonic is unavailable, backend returns a fallback PDF with warnings so flow remains usable.

### Bedrock adapter notes

- Provider-specific call adapters are implemented for:
  - Anthropic (messages schema)
  - OpenAI on Bedrock (responses-style schema fallback)
  - Amazon/Nova/Titan (Converse and legacy text schema fallback)
- All model outputs are validated through strict step schemas before use.
- If a model rejects on-demand throughput, set `RESUME_BEDROCK_INFERENCE_PROFILE_ID`
  to a valid Bedrock inference profile ID/ARN; the app auto-retries with that profile.
