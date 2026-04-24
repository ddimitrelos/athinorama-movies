"""
scrape_local.py
---------------
Scrape Athinorama and update the LOCAL movies.db only. Does NOT commit
or push — production is managed by GitHub Actions
(.github/workflows/weekly-scrape.yml). This script keeps the local
Flask dev copy of the database in sync on a separate weekly cadence.

Usage:
    python scrape_local.py
"""

import os
import sys
import logging

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE   = os.path.join(SCRIPT_DIR, 'scrape_local.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def main():
    # Playwright browsers path for the local Windows install; scraper.py
    # also sets this via setdefault, but set it here too in case this
    # module is invoked in a context where that import hasn't yet run.
    os.environ.setdefault(
        'PLAYWRIGHT_BROWSERS_PATH',
        r'C:\Users\dimitrios.dimitrelos\AppData\Local\ms-playwright',
    )

    sys.path.insert(0, SCRIPT_DIR)
    import scraper  # noqa: E402 — local import after path setup

    logger.info("=== Local scrape started (no commit/push) ===")
    scraper.run_scrape(full_rescrape=False)

    if scraper.progress.get('error'):
        raise RuntimeError(f"Scraper error: {scraper.progress['error']}")

    new     = scraper.progress.get('new_count', 0)
    updated = scraper.progress.get('updated_count', 0)
    logger.info(f"Local scrape complete — new: {new}, updated: {updated}")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.exception(f"Local scrape failed: {e}")
        sys.exit(1)
