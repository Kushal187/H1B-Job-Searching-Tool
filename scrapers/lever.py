"""Lever job board API scraper.

Queries the public Lever Postings API for open positions at a company,
saves the raw JSON response, and inserts job listings into the database.
"""

from datetime import datetime, timezone

import config
from scrapers.base import BaseScraper
from scrapers.location_filter import is_usa_location
from scrapers.title_filter import is_target_role


class LeverScraper(BaseScraper):
    """Lever-specific field mappings and filters."""

    ats_name = "lever"
    board_url_template = "https://jobs.lever.co/{slug}"

    def get_api_url(self, normalized: str) -> str:
        return config.LEVER_API_URL.format(company=normalized)

    def extract_all_jobs(self, response_json) -> list[dict]:
        # Lever returns the job list directly (a JSON array, not an object).
        return response_json if isinstance(response_json, list) else []

    def is_job_relevant(self, job: dict) -> bool:
        categories = job.get("categories") or {}
        location_str = (
            categories.get("location", "")
            if isinstance(categories, dict)
            else ""
        )
        return is_usa_location(location_str) and is_target_role(
            job.get("text", "")
        )

    def wrap_for_save(self, jobs: list[dict]) -> list:
        # Lever data is saved as a plain JSON array.
        return jobs

    def extract_job_fields(self, job: dict) -> dict:
        categories = job.get("categories", {})
        department = (
            categories.get("department", "")
            if isinstance(categories, dict)
            else ""
        )
        location = (
            categories.get("location", "")
            if isinstance(categories, dict)
            else ""
        )

        # Lever provides createdAt as epoch milliseconds
        created_ms = job.get("createdAt")
        if created_ms and isinstance(created_ms, (int, float)):
            posted_at = datetime.fromtimestamp(
                created_ms / 1000, tz=timezone.utc
            ).isoformat()
        else:
            posted_at = ""

        return {
            "title": job.get("text", ""),
            "location": location,
            "department": department,
            "job_url": job.get("hostedUrl", ""),
            "posted_at": posted_at,
        }


# ── Module-level convenience function (backward-compatible API) ──────────

_scraper = LeverScraper()


def scrape_lever(
    company_name: str,
    normalized: str,
    output_dir: str,
    delay: float = config.SCRAPE_DELAY,
    save_to_db: bool = True,
    company_id: int | None = None,
    db_conn=None,
) -> dict | None:
    """Query Lever API for a company's open jobs."""
    return _scraper.scrape(
        company_name,
        normalized,
        output_dir,
        delay=delay,
        save_to_db=save_to_db,
        company_id=company_id,
        db_conn=db_conn,
    )
