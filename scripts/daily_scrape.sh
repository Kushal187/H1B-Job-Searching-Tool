#!/bin/bash
# Daily job scrape — run via cron or launchd
# Scrapes known Greenhouse/Lever companies, exports results, logs output

set -e

PROJECT_DIR="/Users/kushalpendekanti/Documents/Coding-Fun/job-search-tool"
LOG_DIR="$PROJECT_DIR/logs"
PYTHON="/Users/kushalpendekanti/.pyenv/versions/3.10.13/bin/python3"

mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +%Y-%m-%d_%H%M)
LOG_FILE="$LOG_DIR/scrape_${TIMESTAMP}.log"

echo "=== Scrape started at $(date) ===" >> "$LOG_FILE"

cd "$PROJECT_DIR"

# Monitor scrape (only checks known ATS companies — fast, ~2-3 min)
$PYTHON pipeline.py scrape --mode monitor -w 10 >> "$LOG_FILE" 2>&1

# Export CSVs and reports
$PYTHON pipeline.py export >> "$LOG_FILE" 2>&1

echo "=== Scrape finished at $(date) ===" >> "$LOG_FILE"

# Clean up logs older than 30 days
find "$LOG_DIR" -name "scrape_*.log" -mtime +30 -delete 2>/dev/null

# Optional: send a macOS notification with results
JOB_COUNT=$(sqlite3 "$PROJECT_DIR/data/h1b_jobs.db" "SELECT COUNT(*) FROM job_listings WHERE posted_at >= datetime('now', '-24 hours')")
if [ "$JOB_COUNT" -gt 0 ]; then
    osascript -e "display notification \"$JOB_COUNT new jobs posted in last 24h\" with title \"H1B Job Search\" sound name \"Glass\""
fi
