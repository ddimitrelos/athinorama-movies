"""
Fetch YouTube trailer URLs for movies that were scraped before trailer support
was added. Skips movies that already have a trailer_url.

Run with:
    python fix_trailers.py [--workers N]

After it finishes, commit movies.db and push to deploy to Railway.
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

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'el-GR,el;q=0.9',
})


def fetch_trailer(slug):
    """Return (slug, trailer_url|None)."""
    url = f"{BASE_URL}/cinema/movie/{slug}/"
    try:
        resp = SESSION.get(url, timeout=15)
        if resp.status_code != 200:
            return slug, None
        soup = BeautifulSoup(resp.content, 'html.parser')
        el = soup.find('iframe', src=lambda s: s and 'youtube.com/embed' in s)
        if el:
            return slug, el['src'].split('?')[0]
        return slug, None
    except Exception:
        return slug, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', type=int, default=16)
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    # Ensure column exists (migration)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(movies)").fetchall()}
    if 'trailer_url' not in cols:
        conn.execute("ALTER TABLE movies ADD COLUMN trailer_url TEXT")
        conn.commit()

    rows = conn.execute(
        "SELECT slug FROM movies WHERE detail_scraped = 1 AND (trailer_url IS NULL OR trailer_url = '')"
    ).fetchall()
    conn.close()

    slugs = [r[0] for r in rows]
    total = len(slugs)
    print(f"Checking {total} movies for trailers with {args.workers} workers…\n")

    found = 0
    failed = 0
    done = 0
    start = time.time()

    conn = sqlite3.connect(DB_PATH)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(fetch_trailer, slug): slug for slug in slugs}
        for future in as_completed(futures):
            slug, trailer_url = future.result()
            done += 1

            if trailer_url:
                conn.execute(
                    "UPDATE movies SET trailer_url = ? WHERE slug = ?",
                    (trailer_url, slug)
                )
                found += 1
                print(f"  TRAILER {slug}: {trailer_url}")
            else:
                failed += 1

            if done % 500 == 0:
                elapsed = time.time() - start
                rate = done / elapsed
                eta = (total - done) / rate
                print(f"  Progress: {done}/{total} | {found} trailers | ETA {eta/60:.1f}min")
                conn.commit()

    conn.commit()
    conn.close()

    elapsed = time.time() - start
    print(f"\nDone in {elapsed/60:.1f} min.")
    print(f"  Trailers found : {found}")
    print(f"  No trailer     : {failed}")
    if found:
        print("\nNow commit movies.db and push to deploy to Railway:")
        print("  git add movies.db")
        print("  git commit -m 'Backfill YouTube trailers'")
        print("  git push")


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    main()
