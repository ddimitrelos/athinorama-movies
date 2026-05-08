"""
Find every movie whose poster_url contains '/lmnts/' (a path known to return 502)
and replace it with the og:image URL from the live detail page.

Run with:
    python fix_lmnts_posters.py [--workers N] [--dry-run]

After it finishes, commit movies.db and push to deploy.
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

_FETCH_ERROR = object()

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'el-GR,el;q=0.9',
})


def fetch_og_image(slug):
    """
    Fetch og:image from the detail page.
    Returns (slug, str)    — found a poster URL
            (slug, None)   — page fetched, no og:image
            (slug, _FETCH_ERROR) — could not fetch page, leave DB unchanged
    """
    url = f"{BASE_URL}/cinema/movie/{slug}/"
    try:
        resp = SESSION.get(url, timeout=15)
        if resp.status_code != 200:
            return slug, _FETCH_ERROR
        soup = BeautifulSoup(resp.content, 'html.parser')
        og = soup.find('meta', property='og:image')
        if og and og.get('content'):
            return slug, og['content']
        return slug, None
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
        "SELECT slug, poster_url FROM movies WHERE poster_url LIKE '%/lmnts/%'"
    ).fetchall()
    conn.close()

    affected = {slug: poster_url for slug, poster_url in rows}
    total = len(affected)
    print(f"Found {total} movies with broken /lmnts/ poster URLs. "
          f"Re-fetching with {args.workers} workers…\n")

    if total == 0:
        print("Nothing to fix.")
        return

    updated = 0
    cleared = 0
    failed = 0
    done = 0
    start = time.time()

    conn = sqlite3.connect(DB_PATH)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(fetch_og_image, slug): slug for slug in affected}
        for future in as_completed(futures):
            slug, new_url = future.result()
            done += 1

            if new_url is _FETCH_ERROR:
                failed += 1
                print(f"  ERROR   {slug}")
            elif new_url is None:
                cleared += 1
                if not args.dry_run:
                    conn.execute(
                        "UPDATE movies SET poster_url = NULL WHERE slug = ?", (slug,)
                    )
                print(f"  {'[DRY]' if args.dry_run else 'CLEARED'} {slug} (no og:image found)")
            else:
                updated += 1
                if not args.dry_run:
                    conn.execute(
                        "UPDATE movies SET poster_url = ? WHERE slug = ?",
                        (new_url, slug)
                    )
                print(f"  {'[DRY]' if args.dry_run else 'FIXED'} {slug}")
                print(f"    old: {affected[slug]}")
                print(f"    new: {new_url}")

            if done % 100 == 0:
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
    print(f"\nDone in {elapsed:.1f}s.")
    print(f"  Fixed  : {updated}")
    print(f"  Cleared: {cleared}  (no og:image on page)")
    print(f"  Errors : {failed}")
    if (updated or cleared) and not args.dry_run:
        print("\nNow commit movies.db and push to deploy the fix:")
        print("  git add movies.db")
        print("  git commit -m 'Fix broken /lmnts/ poster URLs with og:image'")
        print("  git push")


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    main()
