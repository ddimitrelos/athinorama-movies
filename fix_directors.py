"""
One-shot script: re-scrape detail pages for all movies missing director/cast/synopsis.
Runs 16 parallel workers. No Playwright needed — pure requests + BeautifulSoup.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
import database
import scraper   # reuse _scrape_detail_fast

WORKERS = 16

# Use a dedicated session with a large enough connection pool for 16 workers
_session = requests.Session()
_adapter = HTTPAdapter(pool_connections=WORKERS, pool_maxsize=WORKERS)
_session.mount('https://', _adapter)
_session.mount('http://', _adapter)
_session.headers.update(scraper.HEADERS)
# Monkey-patch scraper's SESSION so _scrape_detail_fast uses our pool
scraper.SESSION = _session

def main():
    # Fetch slugs of movies that are missing director data
    with database.get_db() as conn:
        rows = conn.execute(
            """SELECT slug FROM movies
               WHERE detail_scraped = 1
                 AND (director IS NULL OR director = '')
               ORDER BY slug"""
        ).fetchall()
    slugs = [r['slug'] for r in rows]
    total = len(slugs)
    print(f"Movies to fix: {total}")

    done = 0
    errors = 0

    def _fix_one(slug):
        movie_data = scraper._scrape_detail_fast(slug)
        if movie_data and movie_data.get('director'):
            database.upsert_movie(movie_data)
            return 'ok'
        return 'skip'

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(_fix_one, s): s for s in slugs}
        for i, future in enumerate(as_completed(futures), 1):
            slug = futures[future]
            try:
                result = future.result()
                if result == 'ok':
                    done += 1
            except Exception as exc:
                errors += 1
                print(f"  ERROR {slug}: {exc}")
            if i % 500 == 0 or i == total:
                print(f"  {i}/{total} processed — {done} updated, {errors} errors")

    print(f"\nDone. {done} movies updated with director info.")

if __name__ == '__main__':
    main()
