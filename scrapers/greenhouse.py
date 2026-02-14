"""Greenhouse job board API scraper.

Queries the public Greenhouse Boards API for open positions at a company,
saves the raw JSON response, and inserts job listings into the database.
"""

import json
import os
import time
from datetime import datetime, timezone

import requests

import config
from scrapers.location_filter import is_usa_location
from scrapers.title_filter import is_target_role


def scrape_greenhouse(
    company_name: str,
    normalized: str,
    output_dir: str,
    delay: float = config.SCRAPE_DELAY,
    save_to_db: bool = True,
    company_id: int | None = None,
    db_conn=None,
) -> dict | None:
    """Query Greenhouse API for a company's open jobs.

    Args:
        company_name: Original company name for display.
        normalized: Normalized slug used as Greenhouse board name.
        output_dir: Base directory for saving JSON output.
        delay: Seconds to wait after the request (rate limiting).
        save_to_db: Whether to insert job records into the database.
        company_id: matched_companies.id for DB foreign key.
        db_conn: Optional database connection (for thread-safe writes).

    Returns:
        Metadata dict if jobs found, None otherwise.
    """
    url = config.GREENHOUSE_API_URL.format(company=normalized)

    try:
        resp = requests.get(url, timeout=config.SCRAPE_TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            all_jobs = data.get("jobs", [])

            # Filter to USA-only, entry-level target roles
            jobs = [
                j for j in all_jobs
                if is_usa_location(
                    (j.get("location") or {}).get("name", "")
                    if isinstance(j.get("location"), dict)
                    else str(j.get("location", ""))
                )
                and is_target_role(j.get("title", ""))
            ]

            scraped_at = datetime.now(timezone.utc).isoformat()

            if jobs:
                # Replace with filtered list for saving
                data["jobs"] = jobs
                # Save to filesystem
                company_dir = os.path.join(output_dir, normalized)
                os.makedirs(company_dir, exist_ok=True)

                with open(os.path.join(company_dir, "jobs.json"), "w") as f:
                    json.dump(data, f, indent=2)

                metadata = {
                    "original_name": company_name,
                    "normalized_name": normalized,
                    "job_count": len(jobs),
                    "ats": "greenhouse",
                    "url": f"https://boards.greenhouse.io/{normalized}",
                    "scraped_at": scraped_at,
                }
                with open(os.path.join(company_dir, "metadata.json"), "w") as f:
                    json.dump(metadata, f, indent=2)

                # Insert into database (upsert — skip existing job URLs)
                if save_to_db:
                    new_count = _upsert_jobs(
                        company_name, normalized, jobs, scraped_at, company_id, db_conn,
                    )
                    metadata["new_job_count"] = new_count
                    # Mark jobs not seen in this scrape as inactive
                    _deactivate_stale_jobs(company_id, scraped_at, db_conn)

                return metadata

            # Company uses Greenhouse but no matching jobs after filtering
            if save_to_db and company_id:
                _deactivate_stale_jobs(company_id, scraped_at, db_conn)
            return {
                "original_name": company_name,
                "normalized_name": normalized,
                "job_count": 0,
                "total_before_filter": len(all_jobs),
                "ats": "greenhouse",
                "url": f"https://boards.greenhouse.io/{normalized}",
                "scraped_at": scraped_at,
            }
        return None
    except Exception as e:
        print(f"  Error scraping Greenhouse for {normalized}: {e}")
        return None
    finally:
        time.sleep(delay)


def _upsert_jobs(
    company_name: str,
    normalized: str,
    jobs: list[dict],
    scraped_at: str,
    company_id: int | None,
    db_conn=None,
) -> int:
    """Insert new Greenhouse job listings, skip duplicates by job_url.

    Returns:
        Number of newly inserted jobs.
    """
    from db import database

    upsert_sql = """
        INSERT INTO job_listings
            (company_id, company_name, ats_system, job_title, job_location,
             job_url, department, scraped_at, first_seen_at, last_seen_at,
             posted_at, is_active, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
        ON CONFLICT(job_url) DO UPDATE SET
            scraped_at = excluded.scraped_at,
            last_seen_at = excluded.last_seen_at,
            is_active = 1,
            raw_json = excluded.raw_json
    """
    check_sql = "SELECT 1 FROM job_listings WHERE job_url = ? LIMIT 1"

    new_count = 0
    for job in jobs:
        location = job.get("location", {})
        location_name = location.get("name", "") if isinstance(location, dict) else str(location)

        departments = job.get("departments", [])
        dept_name = departments[0].get("name", "") if departments else ""
        job_url = job.get("absolute_url", "")

        # Greenhouse provides the original publish date
        posted_at = job.get("first_published") or job.get("updated_at", "")

        params = (
            company_id,
            company_name,
            "greenhouse",
            job.get("title", ""),
            location_name,
            job_url,
            dept_name,
            scraped_at,
            scraped_at,  # first_seen_at (only set on initial insert)
            scraped_at,  # last_seen_at (updated on every upsert)
            posted_at,
            json.dumps(job),
        )

        try:
            if db_conn is not None:
                exists = db_conn.execute(check_sql, (job_url,)).fetchone()
                db_conn.execute(upsert_sql, params)
                if not exists:
                    new_count += 1
            else:
                with database.get_db() as conn:
                    exists = conn.execute(check_sql, (job_url,)).fetchone()
                    conn.execute(upsert_sql, params)
                    if not exists:
                        new_count += 1
        except Exception as e:
            print(f"    DB error inserting job {job_url}: {e}")

    return new_count


def _deactivate_stale_jobs(
    company_id: int | None,
    scraped_at: str,
    db_conn=None,
):
    """Mark jobs as inactive if they weren't seen in the latest scrape.

    Any Greenhouse job for this company whose ``last_seen_at`` is older than
    *scraped_at* is no longer on the board and should be deactivated.
    """
    if not company_id:
        return
    from db import database

    sql = """
        UPDATE job_listings SET is_active = 0
        WHERE company_id = ? AND ats_system = 'greenhouse' AND last_seen_at < ?
    """
    if db_conn is not None:
        db_conn.execute(sql, (company_id, scraped_at))
    else:
        database.execute(sql, (company_id, scraped_at))
