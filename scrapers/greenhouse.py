"""Greenhouse job board API scraper.

Queries the public Greenhouse Boards API for open positions at a company,
saves the raw JSON response, and inserts job listings into the database.
"""

import config
from scrapers.base import BaseScraper
from scrapers.location_filter import is_usa_location
from scrapers.title_filter import is_target_role


class GreenhouseScraper(BaseScraper):
    """Greenhouse-specific field mappings and filters."""

    ats_name = "greenhouse"
    board_url_template = "https://boards.greenhouse.io/{slug}"

    def get_api_url(self, normalized: str) -> str:
        return config.GREENHOUSE_API_URL.format(company=normalized)

    def extract_all_jobs(self, response_json) -> list[dict]:
        return response_json.get("jobs", [])

    def is_job_relevant(self, job: dict) -> bool:
        location = job.get("location", {})
        location_str = (
            location.get("name", "")
            if isinstance(location, dict)
            else str(location)
        )
        return is_usa_location(location_str) and is_target_role(
            job.get("title", "")
        )

    def wrap_for_save(self, jobs: list[dict]) -> dict:
        return {"jobs": jobs}

    def extract_job_fields(self, job: dict) -> dict:
        location = job.get("location", {})
        location_name = (
            location.get("name", "")
            if isinstance(location, dict)
            else str(location)
        )
        departments = job.get("departments", [])
        dept_name = departments[0].get("name", "") if departments else ""

        return {
            "title": job.get("title", ""),
            "location": location_name,
            "department": dept_name,
            "job_url": job.get("absolute_url", ""),
            "posted_at": job.get("first_published") or job.get("updated_at", ""),
        }


# ── Module-level convenience function (backward-compatible API) ──────────

_scraper = GreenhouseScraper()


def scrape_greenhouse(
    company_name: str,
    normalized: str,
    output_dir: str,
    delay: float = config.SCRAPE_DELAY,
    save_to_db: bool = True,
    company_id: int | None = None,
    db_conn=None,
) -> dict | None:
    """Query Greenhouse API for a company's open jobs."""
    return _scraper.scrape(
        company_name,
        normalized,
        output_dir,
        delay=delay,
        save_to_db=save_to_db,
        company_id=company_id,
        db_conn=db_conn,
    )
