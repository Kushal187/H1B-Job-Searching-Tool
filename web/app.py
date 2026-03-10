"""FastAPI web UI for browsing H1B job listings."""

import base64
import hmac
import os
import sys
import threading
from datetime import datetime, timezone
from urllib.parse import unquote

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Allow imports from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import config
from db import database

app = FastAPI(title="H1B Job Search")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "templates")
)
app.mount(
    "/static",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
    name="static",
)


def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _admin_scope(path: str) -> bool:
    return path == "/admin" or path.startswith("/api/admin")


def _admin_auth_ok(request: Request) -> bool:
    username = os.environ.get("ADMIN_USERNAME", "")
    password = os.environ.get("ADMIN_PASSWORD", "")
    if not username or not password:
        return True  # No credentials configured -> open admin (dev default).

    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Basic "):
        return False

    token = auth_header[6:].strip()
    try:
        decoded = base64.b64decode(token).decode("utf-8")
    except Exception:
        return False

    if ":" not in decoded:
        return False
    supplied_user, supplied_pass = decoded.split(":", 1)

    return hmac.compare_digest(supplied_user, username) and hmac.compare_digest(
        supplied_pass, password
    )


@app.middleware("http")
async def admin_access_guard(request: Request, call_next):
    path = request.url.path
    if _admin_scope(path):
        if _is_truthy(os.environ.get("ADMIN_DISABLED")):
            if path.startswith("/api/admin"):
                return JSONResponse({"error": "Not found"}, status_code=404)
            return PlainTextResponse("Not found", status_code=404)

        if not _admin_auth_ok(request):
            headers = {"WWW-Authenticate": 'Basic realm="Admin"'}
            if path.startswith("/api/admin"):
                return JSONResponse(
                    {"error": "Unauthorized"}, status_code=401, headers=headers
                )
            return PlainTextResponse(
                "Unauthorized", status_code=401, headers=headers
            )

    return await call_next(request)


@app.on_event("startup")
def startup_init_db():
    """Initialize local SQLite schema on startup.

    For Postgres/Supabase deployments (e.g. Vercel), skip automatic init/migrate
    during request startup to avoid cold-start latency/timeouts.
    """
    if database.using_postgres():
        return
    database.init_db()


# ── Background task state (for admin scraping) ──────────────────────────────

_scrape_status = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "mode": None,
    "ats": None,
    "workers": None,
    "progress": 0,
    "total": 0,
    "stats": {},
    "error": None,
    "log": [],
}


# ── Helpers ──────────────────────────────────────────────────────────────────

# Use the posted_at column directly (populated by scrapers at insert time),
# falling back to first_seen_at for older rows that may not have posted_at.
# This avoids expensive json_extract() on raw_json at query time.
POSTED_DATE_EXPR = "COALESCE(NULLIF(j.posted_at, ''), j.first_seen_at)"

TEXT_SEARCH_FIELDS = (
    "LOWER(COALESCE(j.job_title, ''))",
    "LOWER(COALESCE(j.company_name, ''))",
)

JOB_PROFILE_TERMS: dict[str, dict[str, str | tuple[str, ...]]] = {
    "new_grad_swe_plus": {
        "label": "New Grad SWE+",
        "description": (
            "Software engineer, backend, full stack, AI/ML, and forward "
            "deployed roles."
        ),
        "terms": (
            "software engineer",
            "software developer",
            "software development engineer",
            "software development",
            "software engineering",
            "swe",
            "sde",
            "backend engineer",
            "backend developer",
            "full stack",
            "full-stack",
            "fullstack",
            "ai engineer",
            "ai/ml engineer",
            "applied ai",
            "machine learning engineer",
            "ml engineer",
            "forward deployed",
            "fde",
            "generative ai",
            "llm",
        ),
    },
    "backend_fullstack": {
        "label": "Backend / Full Stack",
        "description": "Backend, platform, and full-stack engineering roles.",
        "terms": (
            "backend engineer",
            "backend developer",
            "platform engineer",
            "full stack",
            "full-stack",
            "fullstack",
        ),
    },
    "ai_ml": {
        "label": "AI / ML",
        "description": "AI, ML, and applied AI engineering roles.",
        "terms": (
            "ai engineer",
            "applied ai",
            "machine learning engineer",
            "ml engineer",
            "applied ml",
            "research engineer",
        ),
    },
    "forward_deployed": {
        "label": "Forward Deployed",
        "description": "Forward deployed engineer and adjacent FDE roles.",
        "terms": (
            "forward deployed",
            "forward deploy",
            "fde",
        ),
    },
}


def _like_value(value: str) -> str:
    return f"%{value.strip().lower()}%"


def _build_text_search_clause(fields: tuple[str, ...], terms: list[str]) -> tuple[str, list[str]]:
    """Return a parameterized OR-clause matching any term in any field."""
    clauses: list[str] = []
    params: list[str] = []

    for term in terms:
        field_clauses = [f"{field} LIKE ?" for field in fields]
        clauses.append("(" + " OR ".join(field_clauses) + ")")
        params.extend([_like_value(term)] * len(fields))

    return "(" + " OR ".join(clauses) + ")", params


def _build_job_filter_clause(
    *,
    search: str = "",
    profile: str = "",
    company: str = "",
    freshness: str = "",
    active: str = "true",
) -> tuple[str, list[str]]:
    """Build the shared WHERE clause used by jobs and stats endpoints."""
    conditions: list[str] = []
    params: list[str] = []

    if active == "true":
        conditions.append("j.is_active = 1")
    elif active == "false":
        conditions.append("j.is_active = 0")

    if search:
        search_clause, search_params = _build_text_search_clause(
            TEXT_SEARCH_FIELDS, [search]
        )
        conditions.append(search_clause)
        params.extend(search_params)

    if profile:
        profile_config = JOB_PROFILE_TERMS.get(profile)
        if profile_config:
            profile_clause, profile_params = _build_text_search_clause(
                ("LOWER(COALESCE(j.job_title, ''))",),
                list(profile_config["terms"]),
            )
            conditions.append(profile_clause)
            params.extend(profile_params)

    if company:
        conditions.append("j.company_name = ?")
        params.append(company)

    if freshness == "24h":
        conditions.append(f"{POSTED_DATE_EXPR} >= datetime('now', '-24 hours')")
    elif freshness == "48h":
        conditions.append(f"{POSTED_DATE_EXPR} >= datetime('now', '-48 hours')")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    return where, params


# ── Page Routes ──────────────────────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
async def page_index(request: Request):
    """Jobs homepage."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/companies", response_class=HTMLResponse)
async def page_companies(request: Request):
    """Companies browser page."""
    return templates.TemplateResponse("companies.html", {"request": request})


@app.get("/companies/{company_name:path}", response_class=HTMLResponse)
async def page_company_detail(request: Request, company_name: str):
    """Single company detail page."""
    return templates.TemplateResponse(
        "company_detail.html",
        {
            "request": request,
            "company_name": unquote(company_name),
        },
    )


@app.get("/admin", response_class=HTMLResponse)
async def page_admin(request: Request):
    """Admin dashboard page."""
    return templates.TemplateResponse("admin.html", {"request": request})


# ── API: Stats ───────────────────────────────────────────────────────────────


@app.get("/api/stats")
async def get_stats(
    search: str = Query("", description="Search job titles or companies"),
    profile: str = Query("", description="Preset combined role filter"),
    company: str = Query("", description="Filter by company name"),
    freshness: str = Query("", description="Filter: '24h' or '48h' for recent jobs"),
    active: str = Query(
        "true", description="Filter active jobs only ('true'/'false'/'all')"
    ),
):
    """Return summary statistics."""
    try:
        where, params = _build_job_filter_clause(
            search=search,
            profile=profile,
            company=company,
            freshness=freshness,
            active=active,
        )

        rows = database.query(
            f"""
            SELECT
                COUNT(*) as total_jobs,
                COUNT(DISTINCT j.company_name) as companies_with_jobs,
                COUNT(CASE WHEN j.ats_system = 'greenhouse' THEN 1 END) as greenhouse_jobs,
                COUNT(CASE WHEN j.ats_system = 'lever' THEN 1 END) as lever_jobs,
                COUNT(CASE WHEN j.ats_system = 'ashby' THEN 1 END) as ashby_jobs
            FROM job_listings j
            {where}
        """,
            tuple(params),
        )
        stats = rows[0] if rows else {}

        # Count new jobs (using posted_at column, falling back to first_seen_at)
        new_rows = database.query(
            f"""
            SELECT
                COUNT(CASE WHEN {POSTED_DATE_EXPR} >= datetime('now', '-24 hours') THEN 1 END) as new_24h,
                COUNT(CASE WHEN {POSTED_DATE_EXPR} >= datetime('now', '-48 hours') THEN 1 END) as new_48h
            FROM job_listings j
            {where}
        """,
            tuple(params),
        )
        stats["new_24h"] = new_rows[0]["new_24h"] if new_rows else 0
        stats["new_48h"] = new_rows[0]["new_48h"] if new_rows else 0

        sponsor_rows = database.query(
            f"""
            SELECT COUNT(DISTINCT m.id) as total_sponsors
            FROM job_listings j
            JOIN matched_companies m ON j.company_id = m.id
            JOIN h1b_sponsors h ON m.normalized_name = h.normalized_name
            {where}
            AND
            WHERE h.fiscal_year IN ('FY2025', 'FY2026')
            AND (h.naics_code LIKE '54%' OR h.naics_code LIKE '51%')
        """.replace(f"{where}\n            AND\n            WHERE", f"{where}\n            AND" if where else "WHERE"),
            tuple(params),
        )
        stats["total_sponsors"] = (
            sponsor_rows[0]["total_sponsors"] if sponsor_rows else 0
        )
        return stats
    except Exception:
        return {
            "total_jobs": 0,
            "companies_with_jobs": 0,
            "greenhouse_jobs": 0,
            "lever_jobs": 0,
            "ashby_jobs": 0,
            "total_sponsors": 0,
            "new_24h": 0,
            "new_48h": 0,
        }


# ── API: Jobs ────────────────────────────────────────────────────────────────


@app.get("/api/jobs")
async def get_jobs(
    search: str = Query("", description="Search job titles or companies"),
    profile: str = Query("", description="Preset combined role filter"),
    company: str = Query("", description="Filter by company name"),
    freshness: str = Query("", description="Filter: '24h' or '48h' for recent jobs"),
    active: str = Query(
        "true", description="Filter active jobs only ('true'/'false'/'all')"
    ),
    sort: str = Query("posted_desc", description="Sort order"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    """Return paginated job listings."""
    where, params = _build_job_filter_clause(
        search=search,
        profile=profile,
        company=company,
        freshness=freshness,
        active=active,
    )

    sort_map = {
        "posted_desc": f"{POSTED_DATE_EXPR} DESC",
        "posted_asc": f"{POSTED_DATE_EXPR} ASC",
        "company_asc": "j.company_name ASC",
        "company_desc": "j.company_name DESC",
        "title_asc": "j.job_title ASC",
        "title_desc": "j.job_title DESC",
        "priority_desc": "COALESCE(m.priority_score, 0) DESC",
    }
    order_by = sort_map.get(sort, f"{POSTED_DATE_EXPR} DESC")

    count_sql = f"SELECT COUNT(*) as total FROM job_listings j {where}"
    total = database.query(count_sql, tuple(params))[0]["total"]

    offset = (page - 1) * per_page
    sql = f"""
        SELECT
            j.id, j.company_name, j.ats_system, j.job_title,
            j.job_location, j.job_url, j.department, j.first_seen_at,
            {POSTED_DATE_EXPR} as posted_date,
            COALESCE(m.priority_score, 0) as priority_score,
            COALESCE(m.h1b_approval_count, 0) as h1b_approvals,
            m.source
        FROM job_listings j
        LEFT JOIN matched_companies m ON j.company_id = m.id
        {where}
        ORDER BY {order_by}
        LIMIT ? OFFSET ?
    """
    params.extend([per_page, offset])
    rows = database.query(sql, tuple(params))

    jobs = []
    for row in rows:
        jobs.append(
            {
                "id": row["id"],
                "company_name": row["company_name"],
                "ats_system": row["ats_system"],
                "job_title": row["job_title"],
                "job_location": row["job_location"],
                "job_url": row["job_url"],
                "department": row["department"],
                "first_seen_at": row["first_seen_at"],
                "posted_date": row["posted_date"],
                "priority_score": row["priority_score"],
                "h1b_approvals": row["h1b_approvals"],
                "source": row["source"],
            }
        )

    return {
        "jobs": jobs,
        "available_profiles": [
            {
                "id": profile_id,
                "label": profile_data["label"],
                "description": profile_data["description"],
            }
            for profile_id, profile_data in JOB_PROFILE_TERMS.items()
        ],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
    }


# ── API: Companies ───────────────────────────────────────────────────────────


@app.get("/api/companies")
async def get_companies(
    search: str = Query("", description="Search company name"),
    page: int = Query(1, ge=1),
    per_page: int = Query(30, ge=1, le=100),
):
    """Return paginated list of H1B tech sponsors (FY2025-2026).

    Includes companies with NAICS 51/54 that sponsored in the last 2 fiscal
    years, plus any company that already has scraped job listings.
    """
    # Build a CTE of qualifying companies
    base_cte = """
        WITH tech_sponsors AS (
            SELECT DISTINCT m.id, m.company_name, m.normalized_name,
                   m.source, m.h1b_approval_count, m.sec_amount_raised,
                   m.priority_score
            FROM matched_companies m
            JOIN h1b_sponsors h ON m.normalized_name = h.normalized_name
            WHERE h.fiscal_year IN ('FY2025', 'FY2026')
              AND (h.naics_code LIKE '54%' OR h.naics_code LIKE '51%')

            UNION

            SELECT DISTINCT m.id, m.company_name, m.normalized_name,
                   m.source, m.h1b_approval_count, m.sec_amount_raised,
                   m.priority_score
            FROM matched_companies m
            JOIN job_listings j ON j.company_id = m.id
        )
    """

    conditions = []
    params = []
    if search:
        conditions.append("ts.company_name LIKE ?")
        params.append(f"%{search}%")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    # Count
    count_sql = f"{base_cte} SELECT COUNT(*) as total FROM tech_sponsors ts {where}"
    total = database.query(count_sql, tuple(params))[0]["total"]

    # Fetch page
    offset = (page - 1) * per_page
    sql = f"""
        {base_cte}
        SELECT ts.*,
               COALESCE(jc.job_count, 0) as job_count
        FROM tech_sponsors ts
        LEFT JOIN (
            SELECT company_id, COUNT(*) as job_count
            FROM job_listings
            WHERE is_active = 1
            GROUP BY company_id
        ) jc ON jc.company_id = ts.id
        {where}
        ORDER BY COALESCE(jc.job_count, 0) DESC, ts.priority_score DESC
        LIMIT ? OFFSET ?
    """
    params.extend([per_page, offset])
    rows = database.query(sql, tuple(params))

    return {
        "companies": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
    }


@app.get("/api/companies/{company_name:path}")
async def get_company_detail(company_name: str):
    """Return details for a single company, including H1B stats and jobs."""
    name = unquote(company_name)

    # Company info from matched_companies
    company_rows = database.query(
        "SELECT * FROM matched_companies WHERE company_name = ? LIMIT 1",
        (name,),
    )
    if not company_rows:
        return {"error": "Company not found", "company": None, "h1b": [], "jobs": []}

    company = dict(company_rows[0])

    # H1B sponsor records
    h1b_rows = database.query(
        """SELECT employer_name, city, state, visa_class,
                  initial_approvals, continuing_approvals, initial_denials,
                  fiscal_year, naics_code
           FROM h1b_sponsors
           WHERE normalized_name = ?
           ORDER BY fiscal_year DESC""",
        (company["normalized_name"],),
    )

    # Job listings (active only by default)
    job_rows = database.query(
        """SELECT j.id, j.company_name, j.ats_system, j.job_title,
                  j.job_location, j.job_url, j.department,
                  j.first_seen_at, j.is_active,
                  COALESCE(NULLIF(j.posted_at, ''), j.first_seen_at) as posted_date
           FROM job_listings j
           WHERE j.company_id = ?
           ORDER BY j.is_active DESC, j.first_seen_at DESC""",
        (company["id"],),
    )
    jobs = []
    for row in job_rows:
        jobs.append(
            {
                "id": row["id"],
                "company_name": row["company_name"],
                "ats_system": row["ats_system"],
                "job_title": row["job_title"],
                "job_location": row["job_location"],
                "job_url": row["job_url"],
                "department": row["department"],
                "first_seen_at": row["first_seen_at"],
                "posted_date": row["posted_date"],
                "is_active": bool(row["is_active"]),
            }
        )

    return {
        "company": company,
        "h1b": [dict(r) for r in h1b_rows],
        "jobs": jobs,
    }


# ── Admin API ────────────────────────────────────────────────────────────────


@app.get("/api/admin/stats")
async def admin_stats():
    """Return detailed database statistics for the admin dashboard."""
    try:
        rows = database.query(
            """
            SELECT
                (SELECT COUNT(*) FROM job_listings) as total_jobs_all,
                (SELECT COUNT(*) FROM job_listings WHERE is_active = 1) as active_jobs,
                (SELECT COUNT(*) FROM job_listings WHERE is_active = 0) as inactive_jobs,
                (SELECT COUNT(DISTINCT company_name) FROM job_listings) as total_companies_with_jobs,
                (SELECT COUNT(DISTINCT company_name) FROM job_listings WHERE is_active = 1) as active_companies,
                (SELECT COUNT(*) FROM matched_companies) as matched_companies,
                (SELECT COUNT(*) FROM sec_formd_companies) as sec_companies,
                (SELECT COUNT(*) FROM h1b_sponsors) as h1b_sponsors,
                (SELECT COUNT(*) FROM company_ats_status) as ats_checked,
                (SELECT COUNT(*) FROM company_ats_status WHERE ats_system IS NOT NULL) as ats_found
        """
        )
        stats = dict(rows[0]) if rows else {}

        # ATS breakdown
        ats_rows = database.query(
            """
            SELECT ats_system, COUNT(*) as job_count, COUNT(DISTINCT company_name) as company_count
            FROM job_listings WHERE is_active = 1
            GROUP BY ats_system
        """
        )
        stats["ats_breakdown"] = [dict(r) for r in ats_rows]

        # Age distribution (exclusive buckets — each job counted in exactly one)
        age_rows = database.query(
            f"""
            SELECT
                COUNT(CASE WHEN {POSTED_DATE_EXPR} >= datetime('now', '-1 day') THEN 1 END) as last_1d,
                COUNT(CASE WHEN {POSTED_DATE_EXPR} >= datetime('now', '-7 days')
                           AND {POSTED_DATE_EXPR} < datetime('now', '-1 day') THEN 1 END) as days_1_7,
                COUNT(CASE WHEN {POSTED_DATE_EXPR} >= datetime('now', '-30 days')
                           AND {POSTED_DATE_EXPR} < datetime('now', '-7 days') THEN 1 END) as days_7_30,
                COUNT(CASE WHEN {POSTED_DATE_EXPR} >= datetime('now', '-60 days')
                           AND {POSTED_DATE_EXPR} < datetime('now', '-30 days') THEN 1 END) as days_30_60,
                COUNT(CASE WHEN {POSTED_DATE_EXPR} >= datetime('now', '-90 days')
                           AND {POSTED_DATE_EXPR} < datetime('now', '-60 days') THEN 1 END) as days_60_90,
                COUNT(CASE WHEN {POSTED_DATE_EXPR} < datetime('now', '-90 days') THEN 1 END) as older_90d
            FROM job_listings j WHERE j.is_active = 1
        """
        )
        stats["age_distribution"] = dict(age_rows[0]) if age_rows else {}

        # Database size (SQLite file or Postgres database size)
        stats["db_size_mb"] = database.get_db_size_mb()

        # Last scrape time (most recent last_seen_at)
        last_scrape = database.query(
            "SELECT MAX(last_seen_at) as last_scrape FROM job_listings"
        )
        stats["last_scrape"] = last_scrape[0]["last_scrape"] if last_scrape else None

        return stats
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/admin/scrape")
async def admin_scrape(request: Request):
    """Trigger a background scrape operation."""
    global _scrape_status

    # Vercel serverless functions are not suitable for long-running scraper jobs.
    # Use GitHub Actions workflow_dispatch for manual runs in hosted production.
    if os.environ.get("VERCEL") and database.using_postgres():
        return {
            "error": (
                "Scrape from admin is disabled on Vercel. "
                "Run the 'Monitor Jobs (Supabase)' GitHub Actions workflow manually."
            )
        }

    if _scrape_status["running"]:
        return {"error": "A scrape is already running", "status": _scrape_status}

    body = await request.json()
    mode = body.get("mode", "monitor")
    workers = body.get("workers", 10)
    ats_raw = body.get("ats", None)
    limit = body.get("limit", None)
    cleanup_days = body.get("cleanup_days", None)  # auto-remove old jobs after scrape

    # Validate
    if mode not in ("discovery", "monitor"):
        return {"error": f"Invalid mode: {mode}"}

    from pipeline import parse_ats_filter

    try:
        ats_filter = parse_ats_filter(ats_raw)
    except ValueError as e:
        return {"error": str(e)}

    # Reset status
    _scrape_status = {
        "running": True,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "mode": mode,
        "ats": ats_raw,
        "workers": workers,
        "progress": 0,
        "total": 0,
        "stats": {
            "greenhouse": 0,
            "lever": 0,
            "ashby": 0,
            "workday": 0,
            "not_found": 0,
            "total_jobs": 0,
            "new_jobs": 0,
        },
        "error": None,
        "log": [],
    }

    def _run_scrape():
        try:
            _do_background_scrape(mode, workers, ats_filter, limit, cleanup_days)
        except Exception as e:
            _scrape_status["error"] = str(e)
        finally:
            _scrape_status["running"] = False
            _scrape_status["finished_at"] = datetime.now(timezone.utc).isoformat()

    thread = threading.Thread(target=_run_scrape, daemon=True)
    thread.start()

    return {"ok": True, "message": f"Scrape started ({mode}, workers={workers})"}


def _do_background_scrape(
    mode: str,
    workers: int,
    ats_filter: set | None,
    limit: int | None,
    cleanup_days: int | None = None,
):
    """Run the scrape in a background thread, updating _scrape_status."""
    from pipeline import run_scrape

    def on_start(total):
        _scrape_status["total"] = total
        if not total:
            _scrape_status["log"].append("No companies found to scrape.")

    def on_result(result, completed, total):
        _scrape_status["progress"] = completed
        if result["status"] == "found":
            ats = result["ats"]
            _scrape_status["stats"][ats] = (
                _scrape_status["stats"].get(ats, 0) + 1
            )
            _scrape_status["stats"]["total_jobs"] += result["job_count"]
            _scrape_status["stats"]["new_jobs"] += result.get(
                "new_job_count", 0
            )
            _scrape_status["log"].append(
                f"{result['name']}: {result['ats'].upper()} - "
                f"{result['job_count']} jobs "
                f"({result.get('new_job_count', 0)} new)"
            )
        else:
            _scrape_status["stats"]["not_found"] += 1

    def on_error(company, error, completed, total):
        _scrape_status["progress"] = completed
        _scrape_status["stats"]["not_found"] += 1
        _scrape_status["log"].append(f"{company['company_name']}: ERROR - {error}")

    run_scrape(
        mode=mode,
        workers=workers,
        ats_filter=ats_filter,
        limit=limit,
        on_start=on_start,
        on_result=on_result,
        on_error=on_error,
    )

    # Post-scrape cleanup: remove jobs older than N days
    if cleanup_days and cleanup_days > 0:
        _scrape_status["log"].append(
            f"Running post-scrape cleanup: removing jobs older than {cleanup_days} days..."
        )
        try:
            cleanup_count = database.query(
                f"""SELECT COUNT(*) as cnt FROM job_listings j
                    WHERE {POSTED_DATE_EXPR} < datetime('now', ? || ' days')""",
                (f"-{cleanup_days}",),
            )
            cnt = cleanup_count[0]["cnt"] if cleanup_count else 0
            if cnt > 0:
                database.execute(
                    f"""DELETE FROM job_listings
                        WHERE id IN (
                            SELECT j.id FROM job_listings j
                            WHERE {POSTED_DATE_EXPR} < datetime('now', ? || ' days')
                        )""",
                    (f"-{cleanup_days}",),
                )
                _scrape_status["log"].append(
                    f"Post-scrape cleanup: deleted {cnt} jobs older than {cleanup_days} days."
                )
                _scrape_status["stats"]["cleaned_up"] = cnt
            else:
                _scrape_status["log"].append(
                    "Post-scrape cleanup: no old jobs to remove."
                )
        except Exception as e:
            _scrape_status["log"].append(f"Post-scrape cleanup error: {e}")

    # Keep log manageable
    if len(_scrape_status["log"]) > 500:
        _scrape_status["log"] = _scrape_status["log"][-500:]


@app.get("/api/admin/scrape/status")
async def admin_scrape_status():
    """Return current scrape status."""
    return _scrape_status


@app.post("/api/admin/cleanup/inactive")
async def admin_cleanup_inactive():
    """Permanently delete all inactive (stale) jobs."""
    try:
        count_rows = database.query(
            "SELECT COUNT(*) as cnt FROM job_listings WHERE is_active = 0"
        )
        count = count_rows[0]["cnt"] if count_rows else 0

        if count == 0:
            return {"ok": True, "deleted": 0, "message": "No inactive jobs to purge."}

        database.execute("DELETE FROM job_listings WHERE is_active = 0")
        return {
            "ok": True,
            "deleted": count,
            "message": f"Purged {count} inactive jobs.",
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/admin/deactivate-stale")
async def admin_deactivate_stale(request: Request):
    """Mark jobs not seen in the last N days as inactive."""
    body = await request.json()
    days = body.get("days", 7)

    try:
        count_rows = database.query(
            """SELECT COUNT(*) as cnt FROM job_listings
               WHERE is_active = 1
               AND last_seen_at < datetime('now', ? || ' days')""",
            (f"-{days}",),
        )
        count = count_rows[0]["cnt"] if count_rows else 0

        if count == 0:
            return {
                "ok": True,
                "updated": 0,
                "message": f"No stale jobs found (all seen within {days} days).",
            }

        database.execute(
            """UPDATE job_listings SET is_active = 0
               WHERE is_active = 1
               AND last_seen_at < datetime('now', ? || ' days')""",
            (f"-{days}",),
        )
        return {
            "ok": True,
            "updated": count,
            "message": f"Deactivated {count} jobs not seen in {days} days.",
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/admin/export")
async def admin_export():
    """Trigger data export (CSV + JSON)."""
    try:
        import argparse

        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from pipeline import cmd_export

        # Create a minimal args namespace
        args = argparse.Namespace()
        cmd_export(args)

        return {
            "ok": True,
            "message": f"Export complete. Files saved to {config.OUTPUT_DIR}/",
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/admin/vacuum")
async def admin_vacuum():
    """Vacuum the SQLite database to reclaim space."""
    if database.using_postgres():
        return {
            "error": (
                "VACUUM from app is disabled for Postgres/Supabase in this project. "
                "Use Supabase maintenance tools instead."
            )
        }

    try:
        before_size = (
            os.path.getsize(config.DB_PATH) if os.path.exists(config.DB_PATH) else 0
        )
        database.vacuum()
        after_size = (
            os.path.getsize(config.DB_PATH) if os.path.exists(config.DB_PATH) else 0
        )

        saved_mb = round((before_size - after_size) / (1024 * 1024), 2)
        after_mb = round(after_size / (1024 * 1024), 2)
        return {
            "ok": True,
            "message": f"Database vacuumed. Size: {after_mb} MB (saved {saved_mb} MB).",
            "before_mb": round(before_size / (1024 * 1024), 2),
            "after_mb": after_mb,
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/admin/reactivate-all")
async def admin_reactivate_all():
    """Reactivate all inactive jobs (undo mass deactivation)."""
    try:
        count_rows = database.query(
            "SELECT COUNT(*) as cnt FROM job_listings WHERE is_active = 0"
        )
        count = count_rows[0]["cnt"] if count_rows else 0

        if count == 0:
            return {
                "ok": True,
                "updated": 0,
                "message": "No inactive jobs to reactivate.",
            }

        database.execute("UPDATE job_listings SET is_active = 1 WHERE is_active = 0")
        return {"ok": True, "updated": count, "message": f"Reactivated {count} jobs."}
    except Exception as e:
        return {"error": str(e)}
