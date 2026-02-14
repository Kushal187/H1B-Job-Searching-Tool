# H1B Job Search Tool

A Python application that helps international students find H1B-sponsoring companies with open jobs. It cross-references SEC Form D filings with H1B employer data, then scrapes Greenhouse and Lever for open positions at matched companies.

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

### Run everything end-to-end

```bash
python pipeline.py run-all
```

### Run individual phases

```bash
# Phase 1: Download and parse SEC Form D + H1B/LCA data
python pipeline.py collect

# Phase 2: Normalize names, fuzzy-match, and score companies
python pipeline.py match

# Phase 3: Scrape Greenhouse and Lever for open jobs
python pipeline.py scrape

# Export results to CSV/JSON
python pipeline.py export
```

### Output

After running, check the `output/` directory for:

- `matched_companies.csv` — all matched companies ranked by priority score
- `companies_with_jobs.csv` — companies that have open jobs on Greenhouse or Lever
- `summary_report.json` — aggregate stats from the pipeline run

## Data Sources

- **SEC EDGAR Form D Data Sets** — quarterly bulk downloads of private offering filings
- **DOL LCA Disclosure Data** — Labor Condition Applications for H-1B visas
- **USCIS H1B Employer Data Hub** — petition approval/denial counts by employer
- **Greenhouse API** — public job board API
- **Lever API** — public job board API
