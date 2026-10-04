"""
scrape_local.py
---------------
Scrape Athinorama, update movies.db, commit to master, and deploy to
PythonAnywhere. Runs weekly via Windows Task Scheduler (AthinoramaScrapeLocal).

The GitHub Actions workflow (weekly-scrape.yml) also runs weekly but cannot
reliably discover *new* movie slugs from the archive listing pages (bot/geo
filtering). This local script runs Playwright on a real browser, picks up
new releases, and pushes them to production.

Usage:
    python scrape_local.py

Required env vars (or .env file):
    PA_API_TOKEN   — PythonAnywhere API token
"""

import os
import sys
import logging
import subprocess
import urllib.request
from datetime import datetime

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


def _load_dotenv():
    env_path = os.path.join(SCRIPT_DIR, '.env')
    if not os.path.exists(env_path):
        return
    with open(env_path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, _, val = line.partition('=')
            os.environ.setdefault(key.strip(), val.strip())


_load_dotenv()

PA_USER = 'doctorfiftyfive'
PA_HOST = 'www.pythonanywhere.com'


def git(*args, check=True):
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
    files_to_stage = ['movies.db', '.scrape_schedule.json']
    for f in files_to_stage:
        path = os.path.join(SCRIPT_DIR, f)
        if os.path.exists(path):
            git('add', path)

    status = git('status', '--porcelain', check=False)
    staged = [l for l in status.stdout.splitlines() if not l.startswith('??')]
    if not staged:
        logger.info("Nothing to commit — database unchanged.")
        return False

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
    message = (
        f"Auto-scrape {timestamp}: +{new_count} new, ~{updated_count} updated\n\n"
        "Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
    )
    git('commit', '-m', message)
    git('push', 'origin', 'master')
    logger.info("=== Pushed to origin/master ===")
    return True


def deploy_to_pythonanywhere():
    pa_token = os.environ.get('PA_API_TOKEN')
    if not pa_token:
        logger.warning("PA_API_TOKEN not set — skipping PythonAnywhere deploy.")
        return

    db_path = os.path.join(SCRIPT_DIR, 'movies.db')
    remote_path = f'/home/{PA_USER}/athinorama-movies/movies.db'
    upload_url = f'https://{PA_HOST}/api/v0/user/{PA_USER}/files/path{remote_path}'
    reload_url = f'https://{PA_HOST}/api/v0/user/{PA_USER}/webapps/{PA_USER}.pythonanywhere.com/reload/'
    auth_header = f'Token {pa_token}'

    result = subprocess.run(
        ['curl', '-s', '-X', 'POST',
         '-H', f'Authorization: {auth_header}',
         '-F', f'content=@{db_path}',
         upload_url],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        logger.error(f"PA upload failed: {result.stderr}")
        return
    logger.info(f"PA upload response: {result.stdout[:200]}")

    req = urllib.request.Request(
        reload_url,
        method='POST',
        headers={'Authorization': auth_header},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            logger.info(f"PA reload: {resp.read().decode()}")
    except Exception as exc:
        logger.error(f"PA reload failed: {exc}")


def main():
    os.environ.setdefault(
        'PLAYWRIGHT_BROWSERS_PATH',
        r'C:\Users\dimitrios.dimitrelos\AppData\Local\ms-playwright',
    )

    sys.path.insert(0, SCRIPT_DIR)
    import scraper  # noqa: E402

    logger.info("=== Local scrape started ===")
    scraper.run_scrape(full_rescrape=False)

    if scraper.progress.get('error'):
        raise RuntimeError(f"Scraper error: {scraper.progress['error']}")

    new_count     = scraper.progress.get('new_count', 0)
    updated_count = scraper.progress.get('updated_count', 0)
    logger.info(f"Scrape complete — new: {new_count}, updated: {updated_count}")

    logger.info("=== Missing poster fix started ===")
    scraper.run_missing_posters()
    if scraper.progress.get('error'):
        logger.warning(f"Missing poster fix error: {scraper.progress['error']}")
    else:
        poster_count = scraper.progress.get('updated_count', 0)
        logger.info(f"Missing poster fix complete — filled: {poster_count}")

    pushed = commit_and_push(new_count, updated_count)
    if pushed:
        logger.info("=== Deploying to PythonAnywhere ===")
        deploy_to_pythonanywhere()
    else:
        logger.info("No changes to deploy.")

    logger.info("Done.")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.exception(f"Local scrape failed: {e}")
        sys.exit(1)
