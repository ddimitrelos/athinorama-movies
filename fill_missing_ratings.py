"""
Standalone script to fill in missing ratings for already-scraped movies.
Run locally, then use --export to push results to production.

Usage:
  python fill_missing_ratings.py            # fill local DB + export SQL
  python fill_missing_ratings.py --export   # only export SQL from local DB
"""

import sqlite3
import os
import re
import sys
import time
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'movies.db')
SQL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'rating_updates.sql')
BASE_URL = 'https://www.athinorama.gr'
WORKERS = 8

PROD_URL = 'https://doctorfiftyfive.pythonanywhere.com/api/internal/update-ratings'
RATINGS_SYNC_TOKEN = 'athinorama-ratings-sync-2026'

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'el-GR,el;q=0.9',
})


def get_slugs():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT slug FROM movies WHERE detail_scraped = 1 AND rating IS NULL"
    ).fetchall()
    conn.close()
    return [r['slug'] for r in rows]


def update_rating_local(slug, rating):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE movies SET rating = ?, last_updated = ? WHERE slug = ?",
        (rating, datetime.now().isoformat(), slug)
    )
    conn.commit()
    conn.close()


def fetch_rating(slug):
    url = f"{BASE_URL}/cinema/movie/{slug}/"
    try:
        resp = SESSION.get(url, timeout=15)
        if resp.status_code != 200:
            return slug, None, f'http_{resp.status_code}'
        soup = BeautifulSoup(resp.content, 'html.parser')
        el = soup.select_one('span.rating-value')
        if el:
            r_val = float(el.get_text(strip=True).replace(',', '.'))
            rating = r_val if 0 < r_val <= 5 else None
        else:
            rating = None
        return slug, rating, 'ok'
    except Exception as exc:
        return slug, None, str(exc)[:80]


def push_ratings_to_prod(filled_ratings):
    """Push a list of {slug, rating} dicts to the production endpoint."""
    if not filled_ratings:
        print("No new ratings to push to production.")
        return
    print(f"Pushing {len(filled_ratings)} new ratings to production...", flush=True)
    try:
        resp = requests.post(
            PROD_URL,
            json=filled_ratings,
            headers={'X-Ratings-Token': RATINGS_SYNC_TOKEN},
            timeout=30,
        )
        if resp.status_code == 200:
            result = resp.json()
            print(f"Production updated: {result.get('updated', '?')} ratings synced.")
        else:
            print(f"Production push failed: HTTP {resp.status_code} — {resp.text[:200]}")
    except Exception as exc:
        print(f"Production push error: {exc}")


def export_sql():
    """Export all non-null ratings from local DB as UPDATE statements."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT slug, rating FROM movies WHERE rating IS NOT NULL AND detail_scraped = 1"
    ).fetchall()
    conn.close()

    with open(SQL_PATH, 'w', encoding='utf-8') as f:
        f.write("BEGIN TRANSACTION;\n")
        for r in rows:
            slug = r['slug'].replace("'", "''")
            f.write(f"UPDATE movies SET rating = {r['rating']} WHERE slug = '{slug}';\n")
        f.write("COMMIT;\n")
    print(f"Exported {len(rows)} rating updates to {SQL_PATH}")


def main():
    if '--export' in sys.argv:
        export_sql()
        return

    slugs = get_slugs()
    total = len(slugs)
    print(f"Found {total} movies with missing ratings. Starting...", flush=True)

    filled = 0
    errors = 0
    checked = 0
    start = time.time()
    newly_filled = []  # collect {slug, rating} for production push

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(fetch_rating, slug): slug for slug in slugs}
        for future in as_completed(futures):
            slug, rating, status = future.result()
            checked += 1

            if status != 'ok':
                errors += 1
            elif rating is not None:
                update_rating_local(slug, rating)
                newly_filled.append({'slug': slug, 'rating': rating})
                filled += 1

            if checked % 200 == 0 or checked == total:
                elapsed = time.time() - start
                rate = checked / elapsed if elapsed > 0 else 0
                eta = (total - checked) / rate if rate > 0 else 0
                print(
                    f"  {checked}/{total} checked | {filled} filled | "
                    f"{errors} errors | ETA {eta/60:.1f}min",
                    flush=True
                )

    print(f"\nDone. {filled} ratings filled out of {total} checked.")
    print("Now exporting SQL for production...")
    export_sql()

    # Auto-push new ratings to production
    push_ratings_to_prod(newly_filled)


if __name__ == '__main__':
    main()
