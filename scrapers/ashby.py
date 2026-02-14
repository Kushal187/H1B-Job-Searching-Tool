"""Ashby HQ job board API scraper.

Queries the public Ashby Posting API for open positions at a company,
saves the raw JSON response, and inserts job listings into the database.

API docs: https://developers.ashbyhq.com/docs/public-job-posting-api
"""

import json
import os
import time
from datetime import datetime, timezone

import requests

import config
from scrapers.location_filter import is_usa_location
from scrapers.title_filter import is_target_role

# Countries considered as USA (lowered for comparison)
_USA_COUNTRIES = {"usa", "us", "united states", "united states of america"}


def _is_usa_ashby(job: dict) -> bool:
    """Check if an Ashby job posting is USA-based.

    Uses the structured ``address.postalAddress.addressCountry`` field first
    (most reliable), then falls back to the location string heuristic.
    Also checks ``secondaryLocations`` for multi-location postings.
    """
    # Check primary location structured address
    address = job.get("address") or {}
    postal = address.get("postalAddress") or {}
    country = (postal.get("addressCountry") or "").strip().lower()
    if country in _USA_COUNTRIES:
        return True

    # Check secondary locations structured address
    for sec_loc in (job.get("secondaryLocations") or []):
        sec_addr = sec_loc.get("address") or {}
        sec_country = (sec_addr.get("addressCountry") or "").strip().lower()
        if sec_country in _USA_COUNTRIES:
            return True

    # Fall back to location string heuristic
    location = job.get("location") or ""
    if is_usa_location(location):
        return True

    # Check secondary location strings
    for sec_loc in (job.get("secondaryLocations") or []):
        if is_usa_location(sec_loc.get("location") or ""):
            return True

    return False


def scrape_ashby(
    company_name: str,
    normalized: str,
    output_dir: str,
    delay: float = config.SCRAPE_DELAY,
    save_to_db: bool = True,
    company_id: int | None = None,
    db_conn=None,
) -> dict | None:
    """Query Ashby API for a company's open jobs.

    Args:
        company_name: Original company name for display.
        normalized: Normalized slug used as Ashby board name.
        output_dir: Base directory for saving JSON output.
        delay: Seconds to wait after the request (rate limiting).
        save_to_db: Whether to insert job records into the database.
        company_id: matched_companies.id for DB foreign key.
        db_conn: Optional database connection (for thread-safe writes).

    Returns:
        Metadata dict if company uses Ashby, None otherwise.
    """
    url = config.ASHBY_API_URL.format(company=normalized)

    try:
        resp = requests.get(url, timeout=config.SCRAPE_TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            all_jobs = data.get("jobs", [])

            if not all_jobs:
                # Valid board but no jobs listed
                return None

            # Filter: listed jobs, USA-based, entry-level target roles
            jobs = [
                j for j in all_jobs
                if j.get("isListed", True)
                and _is_usa_ashby(j)
                and is_target_role(j.get("title", ""))
            ]

            scraped_at = datetime.now(timezone.utc).isoformat()

            if jobs:
                # Save to filesystem
                company_dir = os.path.join(output_dir, normalized)
                os.makedirs(company_dir, exist_ok=True)

                with open(os.path.join(company_dir, "jobs.json"), "w") as f:
                    json.dump({"jobs": jobs}, f, indent=2)

                metadata = {
                    "original_name": company_name,
                    "normalized_name": normalized,
                    "job_count": len(jobs),
                    "ats": "ashby",
                    "url": f"https://jobs.ashbyhq.com/{normalized}",
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

            # Company uses Ashby but no matching jobs after filtering
            if save_to_db and company_id:
                _deactivate_stale_jobs(company_id, scraped_at, db_conn)
            return {
                "original_name": company_name,
                "normalized_name": normalized,
                "job_count": 0,
                "total_before_filter": len(all_jobs),
                "ats": "ashby",
                "url": f"https://jobs.ashbyhq.com/{normalized}",
                "scraped_at": scraped_at,
            }
        return None
    except Exception as e:
        print(f"  Error scraping Ashby for {normalized}: {e}")
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
    """Insert new Ashby job listings, skip duplicates by job_url.

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
        location = job.get("location", "")
        department = job.get("department", "")
        job_url = job.get("jobUrl", "")

        # Ashby provides publishedAt as an ISO 8601 timestamp
        posted_at = job.get("publishedAt", "")

        params = (
            company_id,
            company_name,
            "ashby",
            job.get("title", ""),
            location,
            job_url,
            department,
            scraped_at,
            scraped_at,  # first_seen_at (only meaningful on initial insert)
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
    """Mark jobs as inactive if they weren't seen in the latest scrape."""
    if not company_id:
        return
    from db import database

    sql = """
        UPDATE job_listings SET is_active = 0
        WHERE company_id = ? AND ats_system = 'ashby' AND last_seen_at < ?
    """
    if db_conn is not None:
        db_conn.execute(sql, (company_id, scraped_at))
    else:
        database.execute(sql, (company_id, scraped_at))
