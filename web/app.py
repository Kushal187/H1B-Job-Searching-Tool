"""FastAPI web UI for browsing H1B job listings."""

import json
import os
import sys
from urllib.parse import quote, unquote

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# Allow imports from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db import database

app = FastAPI(title="H1B Job Search")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))


# ── Helpers ──────────────────────────────────────────────────────────────────

def _extract_posted_date(raw_json: str | None, ats_system: str) -> str | None:
    """Extract the original posting date from the raw API JSON."""
    if not raw_json:
        return None
    try:
        data = json.loads(raw_json)
        if ats_system == "greenhouse":
            return data.get("updated_at")
        elif ats_system == "lever":
            created = data.get("createdAt")
            if created:
                from datetime import datetime, timezone
                dt = datetime.fromtimestamp(created / 1000, tz=timezone.utc)
                return dt.isoformat()
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    return None


POSTED_DATE_EXPR = """
    CASE
        WHEN j.ats_system = 'greenhouse' THEN json_extract(j.raw_json, '$.updated_at')
        WHEN j.ats_system = 'lever' THEN datetime(json_extract(j.raw_json, '$.createdAt') / 1000, 'unixepoch')
        ELSE j.first_seen_at
    END
"""


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
    return templates.TemplateResponse("company_detail.html", {
        "request": request,
        "company_name": unquote(company_name),
    })


# ── API: Stats ───────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats():
    """Return summary statistics."""
    try:
        rows = database.query("""
            SELECT
                (SELECT COUNT(*) FROM job_listings) as total_jobs,
                (SELECT COUNT(DISTINCT company_name) FROM job_listings) as companies_with_jobs,
                (SELECT COUNT(*) FROM job_listings WHERE ats_system = 'greenhouse') as greenhouse_jobs,
                (SELECT COUNT(*) FROM job_listings WHERE ats_system = 'lever') as lever_jobs
        """)
        stats = rows[0] if rows else {}

        # Count new jobs (using posted_at column, falling back to POSTED_DATE_EXPR)
        new_rows = database.query(f"""
            SELECT
                COUNT(CASE WHEN {POSTED_DATE_EXPR} >= datetime('now', '-24 hours') THEN 1 END) as new_24h,
                COUNT(CASE WHEN {POSTED_DATE_EXPR} >= datetime('now', '-48 hours') THEN 1 END) as new_48h
            FROM job_listings j
        """)
        stats["new_24h"] = new_rows[0]["new_24h"] if new_rows else 0
        stats["new_48h"] = new_rows[0]["new_48h"] if new_rows else 0

        sponsor_rows = database.query("""
            SELECT COUNT(DISTINCT m.id) as total_sponsors
            FROM matched_companies m
            JOIN h1b_sponsors h ON m.normalized_name = h.normalized_name
            WHERE h.fiscal_year IN ('FY2025', 'FY2026')
            AND (h.naics_code LIKE '54%' OR h.naics_code LIKE '51%')
        """)
        stats["total_sponsors"] = sponsor_rows[0]["total_sponsors"] if sponsor_rows else 0
        return stats
    except Exception:
        return {"total_jobs": 0, "companies_with_jobs": 0, "greenhouse_jobs": 0, "lever_jobs": 0, "total_sponsors": 0, "new_24h": 0, "new_48h": 0}


# ── API: Jobs ────────────────────────────────────────────────────────────────

@app.get("/api/jobs")
async def get_jobs(
    search: str = Query("", description="Search job titles or companies"),
    company: str = Query("", description="Filter by company name"),
    freshness: str = Query("", description="Filter: '24h' or '48h' for recent jobs"),
    sort: str = Query("posted_desc", description="Sort order"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    """Return paginated job listings."""
    conditions = []
    params = []

    if search:
        conditions.append("(j.job_title LIKE ? OR j.company_name LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])

    if company:
        conditions.append("j.company_name = ?")
        params.append(company)

    if freshness == "24h":
        conditions.append(f"{POSTED_DATE_EXPR} >= datetime('now', '-24 hours')")
    elif freshness == "48h":
        conditions.append(f"{POSTED_DATE_EXPR} >= datetime('now', '-48 hours')")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

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
            j.job_location, j.job_url, j.department, j.first_seen_at, j.raw_json,
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
        posted_date = _extract_posted_date(row.get("raw_json"), row["ats_system"])
        jobs.append({
            "id": row["id"],
            "company_name": row["company_name"],
            "ats_system": row["ats_system"],
            "job_title": row["job_title"],
            "job_location": row["job_location"],
            "job_url": row["job_url"],
            "department": row["department"],
            "first_seen_at": row["first_seen_at"],
            "posted_date": posted_date,
            "priority_score": row["priority_score"],
            "h1b_approvals": row["h1b_approvals"],
            "source": row["source"],
        })

    return {
        "jobs": jobs,
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

    # Job listings
    job_rows = database.query(
        """SELECT j.id, j.company_name, j.ats_system, j.job_title,
                  j.job_location, j.job_url, j.department,
                  j.first_seen_at, j.raw_json
           FROM job_listings j
           WHERE j.company_id = ?
           ORDER BY j.first_seen_at DESC""",
        (company["id"],),
    )
    jobs = []
    for row in job_rows:
        posted_date = _extract_posted_date(row.get("raw_json"), row["ats_system"])
        jobs.append({
            "id": row["id"],
            "company_name": row["company_name"],
            "ats_system": row["ats_system"],
            "job_title": row["job_title"],
            "job_location": row["job_location"],
            "job_url": row["job_url"],
            "department": row["department"],
            "first_seen_at": row["first_seen_at"],
            "posted_date": posted_date,
        })

    return {
        "company": company,
        "h1b": [dict(r) for r in h1b_rows],
        "jobs": jobs,
    }
