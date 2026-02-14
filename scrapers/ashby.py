"""Ashby HQ job board API scraper.

Queries the public Ashby Posting API for open positions at a company,
saves the raw JSON response, and inserts job listings into the database.

API docs: https://developers.ashbyhq.com/docs/public-job-posting-api
"""

import config
from scrapers.base import BaseScraper
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
    for sec_loc in job.get("secondaryLocations") or []:
        sec_addr = sec_loc.get("address") or {}
        sec_country = (sec_addr.get("addressCountry") or "").strip().lower()
        if sec_country in _USA_COUNTRIES:
            return True

    # Fall back to location string heuristic
    location = job.get("location") or ""
    if is_usa_location(location):
        return True

    # Check secondary location strings
    for sec_loc in job.get("secondaryLocations") or []:
        if is_usa_location(sec_loc.get("location") or ""):
            return True

    return False


class AshbyScraper(BaseScraper):
    """Ashby-specific field mappings and filters."""

    ats_name = "ashby"
    board_url_template = "https://jobs.ashbyhq.com/{slug}"
    skip_if_board_empty = True  # Empty Ashby board → treat as "not found"

    def get_api_url(self, normalized: str) -> str:
        return config.ASHBY_API_URL.format(company=normalized)

    def extract_all_jobs(self, response_json) -> list[dict]:
        return response_json.get("jobs", [])

    def is_job_relevant(self, job: dict) -> bool:
        return (
            job.get("isListed", True)
            and _is_usa_ashby(job)
            and is_target_role(job.get("title", ""))
        )

    def wrap_for_save(self, jobs: list[dict]) -> dict:
        return {"jobs": jobs}

    def extract_job_fields(self, job: dict) -> dict:
        return {
            "title": job.get("title", ""),
            "location": job.get("location", ""),
            "department": job.get("department", ""),
            "job_url": job.get("jobUrl", ""),
            "posted_at": job.get("publishedAt", ""),
        }


# ── Module-level convenience function (backward-compatible API) ──────────

_scraper = AshbyScraper()


def scrape_ashby(
    company_name: str,
    normalized: str,
    output_dir: str,
    delay: float = config.SCRAPE_DELAY,
    save_to_db: bool = True,
    company_id: int | None = None,
    db_conn=None,
) -> dict | None:
    """Query Ashby API for a company's open jobs."""
    return _scraper.scrape(
        company_name,
        normalized,
        output_dir,
        delay=delay,
        save_to_db=save_to_db,
        company_id=company_id,
        db_conn=db_conn,
    )
