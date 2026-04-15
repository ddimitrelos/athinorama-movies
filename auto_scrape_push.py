"""
auto_scrape_push.py
-------------------
Runs the Athinorama scraper, then commits movies.db and pushes to origin.
Scheduled to run every Thursday at noon via Windows Task Scheduler.
Also used for on-demand scrapes.

Usage:
    python auto_scrape_push.py
"""

import os
import sys
import logging
import subprocess
from datetime import datetime

# ------------------------------------------------------------------
# Logging – write to a rolling log file next to this script
# ------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE   = os.path.join(SCRIPT_DIR, 'auto_scrape.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def run_scrape():
    """Import and run the scraper synchronously (it is blocking by design)."""
    # Playwright needs the browsers path set before import
    os.environ.setdefault(
        'PLAYWRIGHT_BROWSERS_PATH',
        r'C:\Users\dimitrios.dimitrelos\AppData\Local\ms-playwright',
    )

    sys.path.insert(0, SCRIPT_DIR)
    import scraper  # noqa: E402 – local import after path setup

    logger.info("=== Scrape started ===")
    scraper.run_scrape(full_rescrape=False)

    if scraper.progress.get('error'):
        raise RuntimeError(f"Scraper error: {scraper.progress['error']}")

    new_count     = scraper.progress.get('new_count', 0)
    updated_count = scraper.progress.get('updated_count', 0)
    logger.info(f"Scrape complete — new: {new_count}, updated: {updated_count}")
    return new_count, updated_count


def git(*args, check=True):
    """Run a git command in SCRIPT_DIR and return stdout."""
    result = subprocess.run(
        ['git', *args],
        cwd=SCRIPT_DIR,
        capture_output=True,
        text=True,
        check=check,
    )
    if result.stdout.strip():
        logger.info(f"git {' '.join(args)}: {result.stdout.strip()}")
    if result.stderr.strip():
        logger.warning(f"git {' '.join(args)} stderr: {result.stderr.strip()}")
    return result


def commit_and_push(new_count, updated_count):
    """Stage movies.db (and schedule file), commit, and push."""
    files_to_stage = ['movies.db', '.scrape_schedule.json']
    staged = []

    for f in files_to_stage:
        path = os.path.join(SCRIPT_DIR, f)
        if os.path.exists(path):
            git('add', path)
            staged.append(f)

    # Check if there is anything to commit
    status = git('status', '--porcelain', check=False)
    if not status.stdout.strip():
        logger.info("Nothing to commit — database unchanged.")
        return

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
    message = (
        f"Auto-scrape {timestamp}: +{new_count} new, ~{updated_count} updated\n\n"
        "Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
    )
    git('commit', '-m', message)
    git('push', 'origin', 'master')
    logger.info("=== Pushed to production ===")


def main():
    logger.info("========================================")
    logger.info("Auto scrape + push starting")
    logger.info("========================================")

    try:
        new_count, updated_count = run_scrape()
        commit_and_push(new_count, updated_count)
    except Exception as e:
        logger.exception(f"Auto scrape/push failed: {e}")
        sys.exit(1)

    logger.info("Done.")


if __name__ == '__main__':
    main()
