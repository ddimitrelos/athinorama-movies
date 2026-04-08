"""
Re-fetch the displayed rating (span.rating-value) from every detail page
and update the local movies.db where the stored value differs.
Movies whose detail page has no span.rating-value get their rating set to NULL.

Run with:
    python fix_ratings.py [--workers N] [--dry-run]

After it finishes, commit movies.db and push to deploy the fix to Railway.
"""

import argparse
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

DB_PATH = r"C:\Users\dimitrios.dimitrelos\OneDrive - Accenture\Documents\AI Tests\Movie App\movies.db"
BASE_URL = "https://www.athinorama.gr"

_FETCH_ERROR = object()  # sentinel: fetch failed, leave DB unchanged

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'el-GR,el;q=0.9',
})


def fetch_rating(slug):
    """
    Fetch span.rating-value from the detail page.
    Returns (slug, float)  — page has a rating
            (slug, None)   — page fetched, no rating (should be NULL in DB)
            (slug, _FETCH_ERROR) — could not fetch page, leave DB unchanged
    """
    url = f"{BASE_URL}/cinema/movie/{slug}/"
    try:
        resp = SESSION.get(url, timeout=15)
        if resp.status_code != 200:
            return slug, _FETCH_ERROR
        soup = BeautifulSoup(resp.content, 'html.parser')
        el = soup.select_one('span.rating-value')
        if el:
            try:
                val = float(el.get_text(strip=True).replace(',', '.'))
                if 0 < val <= 5:
                    return slug, val
            except (ValueError, TypeError):
                pass
        return slug, None  # page OK, no rating
    except Exception:
        return slug, _FETCH_ERROR


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', type=int, default=16)
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would change without writing to DB')
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT slug, rating FROM movies WHERE detail_scraped = 1"
    ).fetchall()
    conn.close()

    slugs = {slug: rating for slug, rating in rows}
    total = len(slugs)
    print(f"Checking ratings for {total} movies with {args.workers} workers…\n")

    updated = 0
    skipped = 0
    failed = 0
    done = 0
    start = time.time()

    conn = sqlite3.connect(DB_PATH)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(fetch_rating, slug): slug for slug in slugs}
        for future in as_completed(futures):
            slug, new_rating = future.result()
            done += 1

            if new_rating is _FETCH_ERROR:
                failed += 1
            else:
                old_rating = slugs[slug]
                if new_rating != old_rating:
                    if not args.dry_run:
                        conn.execute(
                            "UPDATE movies SET rating = ? WHERE slug = ?",
                            (new_rating, slug)
                        )
                    updated += 1
                    print(f"  {'[DRY]' if args.dry_run else 'FIXED'} {slug}: {old_rating} → {new_rating}")
                else:
                    skipped += 1

            if done % 500 == 0:
                elapsed = time.time() - start
                rate = done / elapsed
                eta = (total - done) / rate
                print(f"  Progress: {done}/{total} | {updated} fixed | {failed} errors | ETA {eta/60:.1f}min")
                if not args.dry_run:
                    conn.commit()

    if not args.dry_run:
        conn.commit()
    conn.close()

    elapsed = time.time() - start
    print(f"\nDone in {elapsed/60:.1f} min.")
    print(f"  Updated  : {updated}")
    print(f"  Unchanged: {skipped}")
    print(f"  Errors   : {failed}")
    if updated and not args.dry_run:
        print("\nNow commit movies.db and push to deploy the fix to Railway:")
        print("  git add movies.db")
        print("  git commit -m 'Fix ratings from HTML span.rating-value'")
        print("  git push")


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    main()
