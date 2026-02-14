"""Lever job board API scraper.

Queries the public Lever Postings API for open positions at a company,
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


def scrape_lever(
    company_name: str,
    normalized: str,
    output_dir: str,
    delay: float = config.SCRAPE_DELAY,
    save_to_db: bool = True,
    company_id: int | None = None,
    db_conn=None,
) -> dict | None:
    """Query Lever API for a company's open jobs.

    Args:
        company_name: Original company name for display.
        normalized: Normalized slug used as Lever board name.
        output_dir: Base directory for saving JSON output.
        delay: Seconds to wait after the request (rate limiting).
        save_to_db: Whether to insert job records into the database.
        company_id: matched_companies.id for DB foreign key.
        db_conn: Optional database connection (for thread-safe writes).

    Returns:
        Metadata dict if jobs found, None otherwise.
    """
    url = config.LEVER_API_URL.format(company=normalized)

    try:
        resp = requests.get(url, timeout=config.SCRAPE_TIMEOUT)
        if resp.status_code == 200:
            all_data = resp.json()

            # Filter to USA-only, entry-level target roles
            data = [
                j for j in all_data
                if is_usa_location(
                    (j.get("categories") or {}).get("location", "")
                    if isinstance(j.get("categories"), dict)
                    else ""
                )
                and is_target_role(j.get("text", ""))
            ]

            scraped_at = datetime.now(timezone.utc).isoformat()

            if data:
                # Save to filesystem
                company_dir = os.path.join(output_dir, normalized)
                os.makedirs(company_dir, exist_ok=True)

                with open(os.path.join(company_dir, "jobs.json"), "w") as f:
                    json.dump(data, f, indent=2)

                metadata = {
                    "original_name": company_name,
                    "normalized_name": normalized,
                    "job_count": len(data),
                    "ats": "lever",
                    "url": f"https://jobs.lever.co/{normalized}",
                    "scraped_at": scraped_at,
                }
                with open(os.path.join(company_dir, "metadata.json"), "w") as f:
                    json.dump(metadata, f, indent=2)

                # Insert into database (upsert — skip existing job URLs)
                if save_to_db:
                    new_count = _upsert_jobs(
                        company_name, normalized, data, scraped_at, company_id, db_conn,
                    )
                    metadata["new_job_count"] = new_count

                return metadata

            # Company uses Lever but no matching jobs after filtering
            return {
                "original_name": company_name,
                "normalized_name": normalized,
                "job_count": 0,
                "total_before_filter": len(all_data),
                "ats": "lever",
                "url": f"https://jobs.lever.co/{normalized}",
                "scraped_at": scraped_at,
            }
        return None
    except Exception as e:
        print(f"  Error scraping Lever for {normalized}: {e}")
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
    """Insert new Lever job listings, skip duplicates by job_url.

    Returns:
        Number of newly inserted jobs.
    """
    from db import database

    sql = """
        INSERT INTO job_listings
            (company_id, company_name, ats_system, job_title, job_location,
             job_url, department, scraped_at, first_seen_at, posted_at, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_url) DO UPDATE SET
            scraped_at = excluded.scraped_at,
            raw_json = excluded.raw_json
    """

    new_count = 0
    for job in jobs:
        categories = job.get("categories", {})
        department = categories.get("department", "") if isinstance(categories, dict) else ""
        location = categories.get("location", "") if isinstance(categories, dict) else ""
        job_url = job.get("hostedUrl", "")

        # Lever provides createdAt as epoch milliseconds
        created_ms = job.get("createdAt")
        if created_ms and isinstance(created_ms, (int, float)):
            from datetime import datetime, timezone
            posted_at = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc).isoformat()
        else:
            posted_at = ""

        params = (
            company_id,
            company_name,
            "lever",
            job.get("text", ""),
            location,
            job_url,
            department,
            scraped_at,
            scraped_at,  # first_seen_at (only set on initial insert)
            posted_at,
            json.dumps(job),
        )

        try:
            if db_conn is not None:
                cursor = db_conn.execute(sql, params)
                if cursor.rowcount > 0:
                    new_count += 1
            else:
                with database.get_db() as conn:
                    cursor = conn.execute(sql, params)
                    if cursor.rowcount > 0:
                        new_count += 1
        except Exception:
            pass  # skip problematic records

    return new_count
