"""
Athinorama Movie Archive Scraper
Uses Playwright to handle JavaScript-rendered content and lazy loading.
"""

import os
import re
import time
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup

import database

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'el-GR,el;q=0.9',
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Hardcode the Playwright browsers path so it works regardless of how
# the Flask process is launched (hidden window, service, etc.)
os.environ['PLAYWRIGHT_BROWSERS_PATH'] = (
    r'C:\Users\dimitrios.dimitrelos\AppData\Local\ms-playwright'
)

BASE_URL = "https://www.athinorama.gr"
ARCHIVE_BASE = f"{BASE_URL}/cinema/moviearchive"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# --- Shared scrape progress state (thread-safe via GIL for simple dict ops) ---
progress = {
    'running': False,
    'phase': '',
    'current': 0,
    'total': 0,
    'message': '',
    'started_at': None,
    'completed_at': None,
    'new_count': 0,
    'updated_count': 0,
    'error': None,
    'paused': False,
}

_pause_event = threading.Event()
_pause_event.set()  # Not paused initially


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _parse_duration(duration_str):
    """Convert ISO 8601 duration (PT1H40M) to minutes."""
    if not duration_str:
        return None
    h = re.search(r'(\d+)H', duration_str)
    m = re.search(r'(\d+)M', duration_str)
    total = (int(h.group(1)) * 60 if h else 0) + (int(m.group(1)) if m else 0)
    return total if total > 0 else None


def _extract_str(val):
    """Safely coerce a value to string."""
    if val is None:
        return ''
    if isinstance(val, (list, tuple)):
        return ', '.join(str(x) for x in val if x)
    return str(val).strip()


def _parse_json_ld(data, slug):
    """Turn a JSON-LD Movie object into our DB schema dict."""

    # Title
    title_gr   = _extract_str(data.get('name'))
    title_orig = _extract_str(data.get('alternateName'))
    if title_orig == title_gr:
        title_orig = ''

    # Year (skip invalid dates like "0001-01-01" which is a .NET default)
    year = None
    for field in ('dateCreated', 'datePublished', 'copyrightYear'):
        val = data.get(field)
        if val:
            try:
                y = int(str(val)[:4])
                if y >= 1880:   # Athinorama archive starts ~1916
                    year = y
                    break
            except (ValueError, TypeError):
                pass
    # Treat year=None as explicit so upsert_movie can clear stale values like 1
    # (marker picked up by upsert_movie the same way rating=None is handled)

    # Country
    cd = data.get('countryOfOrigin')
    if isinstance(cd, list):
        country = ', '.join(c.get('name', '') for c in cd if isinstance(c, dict) and c.get('name'))
    elif isinstance(cd, dict):
        country = cd.get('name', '')
    else:
        country = _extract_str(cd)

    # Director (Athinorama JSON-LD uses 'directors' plural)
    dd = data.get('directors') or data.get('director') or []
    if isinstance(dd, list):
        director = ', '.join(d.get('name', '') for d in dd if isinstance(d, dict) and d.get('name'))
    elif isinstance(dd, dict):
        director = dd.get('name', '')
    else:
        director = _extract_str(dd)

    # Cast (Athinorama JSON-LD uses 'actors' plural, limit to 20)
    ad = data.get('actors') or data.get('actor') or []
    if isinstance(ad, list):
        cast = ', '.join(a.get('name', '') for a in ad[:20] if isinstance(a, dict) and a.get('name'))
    else:
        cast = ''

    # Duration
    duration = _parse_duration(data.get('duration', ''))

    # Genre
    genre = data.get('genre', '')
    if isinstance(genre, list):
        genre = ', '.join(genre)
    genre = genre.strip()

    # Rating — Athinorama puts it inside review.reviewRating, not at top level
    rating = None
    review = data.get('review', {})
    if isinstance(review, list) and review:
        review = review[0]
    if isinstance(review, dict):
        rr = review.get('reviewRating', {})
        if isinstance(rr, dict):
            try:
                rv = rr.get('ratingValue')
                if rv is not None:
                    r_val = float(str(rv).replace(',', '.'))
                    if r_val > 0:
                        rating = r_val
            except (ValueError, TypeError):
                pass

    # Synopsis — from review.text in JSON-LD
    synopsis = ''
    if isinstance(review, dict):
        synopsis = review.get('reviewBody') or review.get('text') or ''

    # Poster
    poster = data.get('image', '')
    if isinstance(poster, dict):
        poster = poster.get('url', '')
    poster = _extract_str(poster)

    return {
        'slug':          slug,
        'title_gr':      title_gr,
        'title_orig':    title_orig,
        'year':          year,
        'country':       country,
        'director':      director,
        'cast':          cast,
        'duration':      duration,
        'genre':         genre,
        'rating':        rating,
        'synopsis':      synopsis,
        'poster_url':    poster,
        'athinorama_url': f"{BASE_URL}/cinema/movie/{slug}/",
        'detail_scraped': 1,
    }


# ---------------------------------------------------------------------------
# Fast requests-based detail scraper (no browser needed for detail pages)
# ---------------------------------------------------------------------------

def _scrape_detail_fast(slug):
    """
    Fetch a movie detail page with requests + BeautifulSoup.
    ~10x faster than Playwright for pages that don't need JS execution.
    Returns a movie_data dict or None on failure.
    """
    url = f"{BASE_URL}/cinema/movie/{slug}/"
    try:
        resp = SESSION.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.content, 'html.parser')

        # Extract JSON-LD
        json_ld = None
        for tag in soup.find_all('script', type='application/ld+json'):
            try:
                d = json.loads(tag.string or '')
                if isinstance(d, list):
                    d = next((x for x in d if x.get('@type') == 'Movie'), None)
                if d and d.get('@type') == 'Movie':
                    json_ld = d
                    break
            except (json.JSONDecodeError, AttributeError):
                pass

        if json_ld:
            movie_data = _parse_json_ld(json_ld, slug)
        else:
            # Minimal fallback from HTML
            h1 = soup.find('h1')
            movie_data = {
                'slug': slug,
                'title_gr': h1.get_text(strip=True) if h1 else slug,
                'athinorama_url': url,
                'detail_scraped': 0,
            }

        # Always prefer og:image — the JSON-LD image path (/lmnts/...) returns 502
        og_img = soup.find('meta', property='og:image')
        if og_img and og_img.get('content'):
            movie_data['poster_url'] = og_img['content']

        # Override rating with the displayed HTML rating — JSON-LD ratingValue can
        # be stale/incorrect while the page shows a different aggregated value.
        # Always set rating explicitly (even as None) so upsert_movie can clear
        # a wrong Phase-1 rating for movies that have no review on Athinorama.
        rating_el = soup.select_one('span.rating-value')
        if rating_el:
            try:
                r_val = float(rating_el.get_text(strip=True).replace(',', '.'))
                if 0 < r_val <= 5:
                    movie_data['rating'] = r_val
                else:
                    movie_data['rating'] = None
            except (ValueError, TypeError):
                movie_data['rating'] = None
        else:
            movie_data['rating'] = None

        # Trailer — grab the first YouTube embed iframe if present
        trailer_el = soup.find('iframe', src=lambda s: s and 'youtube.com/embed' in s)
        if trailer_el:
            movie_data['trailer_url'] = trailer_el['src'].split('?')[0]

        # HTML fallbacks for fields missing from JSON-LD
        if not movie_data.get('synopsis'):
            el = soup.select_one('div.summary p')
            if el:
                movie_data['synopsis'] = el.get_text(strip=True)

        if not movie_data.get('duration'):
            el = soup.select_one('span.duration')
            if el:
                m = re.search(r'(\d+)', el.get_text())
                if m:
                    movie_data['duration'] = int(m.group(1))

        return movie_data

    except Exception as exc:
        logger.warning(f"Fast scrape failed for {slug}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Page interaction helpers
# ---------------------------------------------------------------------------

def _scroll_to_bottom(page, max_rounds=80, wait_ms=700):
    """Scroll page incrementally until no new movie links appear."""
    prev = 0
    stable = 0
    for _ in range(max_rounds):
        _pause_event.wait()  # Respect pause
        count = page.locator('a[href*="/cinema/movie/"]').count()
        if count == prev:
            stable += 1
            if stable >= 3:
                break
        else:
            stable = 0
        prev = count
        page.evaluate("window.scrollBy(0, 800)")
        page.wait_for_timeout(wait_ms)


def _get_json_ld(page):
    """Extract JSON-LD Movie structured data from the current page."""
    return page.evaluate("""
        () => {
            for (const el of document.querySelectorAll('script[type="application/ld+json"]')) {
                try {
                    const d = JSON.parse(el.textContent);
                    if (Array.isArray(d)) {
                        const m = d.find(x => x['@type'] === 'Movie');
                        if (m) return m;
                    } else if (d['@type'] === 'Movie') {
                        return d;
                    }
                } catch(e) {}
            }
            return null;
        }
    """)


def _get_slugs_from_page(page):
    """Extract unique movie slugs and ratings from archive listing page."""
    entries = page.evaluate("""
        () => {
            const results = [];
            const seen = new Set();
            // Each movie block: find the closest common ancestor of link + rating
            document.querySelectorAll('a[href*="/cinema/movie/"]').forEach(a => {
                const href = a.getAttribute('href') || '';
                const slug = href.replace(/\\/$/, '').split('/').pop();
                if (!slug || seen.has(slug)) return;
                seen.add(slug);

                // Walk up to find rating nearby
                let rating = null;
                let el = a;
                for (let i = 0; i < 6 && el; i++) {
                    el = el.parentElement;
                    if (!el) break;
                    // Look for rating stars element
                    const ratingEl = el.querySelector('[class*="rating"], [class*="star"], [class*="grade"]');
                    if (ratingEl) {
                        // Try data attribute first
                        const d = ratingEl.getAttribute('data-rating') ||
                                  ratingEl.getAttribute('data-value') ||
                                  ratingEl.getAttribute('data-score');
                        if (d) { rating = parseFloat(d.replace(',', '.')); break; }
                        // Count filled star icons
                        const filled = ratingEl.querySelectorAll(
                            '.bi-star-fill, [class*="star-fill"], [class*="full"], [class*="filled"]'
                        ).length;
                        if (filled > 0) { rating = filled; break; }
                        // Try text content as number
                        const txt = ratingEl.textContent.trim();
                        const num = parseFloat(txt.replace(',', '.'));
                        if (!isNaN(num) && num >= 0.5 && num <= 5) { rating = num; break; }
                        // Count star characters
                        const stars = (txt.match(/★/g) || []).length;
                        if (stars > 0) { rating = stars; break; }
                    }
                }
                results.push({ slug, rating });
            });
            return results;
        }
    """)
    seen = set()
    slugs = []
    for entry in (entries or []):
        slug = entry.get('slug', '')
        if slug and slug not in seen:
            seen.add(slug)
            slugs.append({'slug': slug, 'rating': entry.get('rating')})
    return slugs


# ---------------------------------------------------------------------------
# Main scraping logic
# ---------------------------------------------------------------------------

def run_scrape(full_rescrape=False):
    """
    Entry point for scraping. Runs in a background thread.
    full_rescrape=True re-scrapes detail pages even for existing movies.
    """
    global progress

    progress.update({
        'running':      True,
        'paused':       False,
        'started_at':   datetime.now().isoformat(),
        'completed_at': None,
        'error':        None,
        'new_count':    0,
        'updated_count': 0,
        'phase':        'Εκκίνηση...',
        'current':      0,
        'total':        0,
        'message':      '',
    })
    _pause_event.set()

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36'
                ),
                locale='el-GR',
                viewport={'width': 1280, 'height': 900},
            )
            page = ctx.new_page()
            page.set_default_timeout(30_000)

            # ---------------------------------------------------------------
            # Phase 1 – collect all slugs from year-based archive pages
            # ---------------------------------------------------------------
            current_year = datetime.now().year
            years = list(range(1916, current_year + 1))

            progress.update({
                'phase':   'Φάση 1: Συλλογή λίστας ταινιών ανά έτος',
                'total':   len(years),
                'current': 0,
            })

            all_slugs = []
            seen_slugs = set()

            for i, year in enumerate(years):
                _pause_event.wait()
                if not progress['running']:
                    break

                progress['current'] = i + 1
                progress['message'] = f"Σάρωση έτους {year}…"

                try:
                    page.goto(f"{ARCHIVE_BASE}/{year}", wait_until='domcontentloaded')
                    _scroll_to_bottom(page)
                    entries = _get_slugs_from_page(page)
                    new_for_year = []
                    for entry in entries:
                        slug = entry['slug']
                        if slug not in seen_slugs:
                            seen_slugs.add(slug)
                            new_for_year.append(slug)
                            all_slugs.append(slug)
                            # Insert stub for new movies
                            database.upsert_movie({
                                'slug': slug,
                                'athinorama_url': f"{BASE_URL}/cinema/movie/{slug}/",
                            })
                        # Always update rating if we got one (works for new and existing)
                        if entry.get('rating') is not None:
                            database.update_rating(slug, entry['rating'])
                    logger.info(f"Year {year}: {len(entries)} movies found ({len(new_for_year)} new)")
                except PWTimeout:
                    logger.warning(f"Timeout on year {year}, skipping")
                except Exception as exc:
                    logger.error(f"Error on year {year}: {exc}")

                time.sleep(0.25)

            # ---------------------------------------------------------------
            # Phase 2 – scrape detail pages
            # ---------------------------------------------------------------
            # Determine which slugs still need full detail
            with database.get_db() as conn:
                if full_rescrape:
                    done_slugs = set()
                else:
                    rows = conn.execute(
                        "SELECT slug FROM movies WHERE detail_scraped = 1"
                    ).fetchall()
                    done_slugs = {r['slug'] for r in rows}

            todo_slugs = [s for s in all_slugs if s not in done_slugs]
            logger.info(f"Phase 2: {len(todo_slugs)} movies to scrape in detail")

            progress.update({
                'phase':   'Φάση 2: Λήψη λεπτομερειών ταινιών',
                'total':   len(todo_slugs),
                'current': 0,
            })

            # Phase 2 uses requests + thread pool (no browser needed)
            browser.close()

            WORKERS = 8

            def _scrape_one(slug):
                """Worker: scrape one detail page and save to DB."""
                _pause_event.wait()
                if not progress['running']:
                    return
                movie_data = _scrape_detail_fast(slug)
                if movie_data:
                    result = database.upsert_movie(movie_data)
                    if result == 'inserted':
                        progress['new_count'] += 1
                    elif result == 'updated':
                        progress['updated_count'] += 1

            with ThreadPoolExecutor(max_workers=WORKERS) as executor:
                futures = {executor.submit(_scrape_one, slug): slug for slug in todo_slugs}
                for i, future in enumerate(as_completed(futures)):
                    _pause_event.wait()
                    if not progress['running']:
                        executor.shutdown(wait=False, cancel_futures=True)
                        break
                    progress['current'] = i + 1
                    progress['message'] = futures[future]
                    try:
                        future.result()
                    except Exception as exc:
                        logger.error(f"Worker error for {futures[future]}: {exc}")

        progress['phase']   = 'Ολοκληρώθηκε'
        progress['message'] = (
            f"Τέλος! {progress['new_count']} νέες, "
            f"{progress['updated_count']} ενημερωμένες ταινίες."
        )
        logger.info(progress['message'])

    except Exception as exc:
        logger.exception("Fatal scraping error")
        progress['error'] = str(exc)

    finally:
        progress['running']      = False
        progress['completed_at'] = datetime.now().isoformat()


def run_ratings_scrape():
    """Re-run only Phase 1 across all years to update ratings. Fast — no detail page visits."""
    global progress

    progress.update({
        'running': True, 'paused': False,
        'started_at': datetime.now().isoformat(), 'completed_at': None,
        'error': None, 'new_count': 0, 'updated_count': 0,
        'phase': 'Ενημέρωση αξιολογήσεων…', 'current': 0, 'total': 0, 'message': '',
    })
    _pause_event.set()

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36'
                ),
                locale='el-GR',
                viewport={'width': 1280, 'height': 900},
            )
            page = ctx.new_page()
            page.set_default_timeout(30_000)

            current_year = datetime.now().year
            years = list(range(1916, current_year + 1))
            progress['total'] = len(years)

            for i, year in enumerate(years):
                _pause_event.wait()
                if not progress['running']:
                    break

                progress['current'] = i + 1
                progress['message'] = f"Σάρωση έτους {year}…"

                try:
                    page.goto(f"{ARCHIVE_BASE}/{year}", wait_until='domcontentloaded')
                    _scroll_to_bottom(page)
                    entries = _get_slugs_from_page(page)
                    for entry in entries:
                        if entry.get('rating') is not None:
                            database.update_rating(entry['slug'], entry['rating'])
                            progress['updated_count'] += 1
                except PWTimeout:
                    logger.warning(f"Timeout on year {year}")
                except Exception as exc:
                    logger.error(f"Error on year {year}: {exc}")

                time.sleep(0.25)

            browser.close()

        progress['phase'] = 'Ολοκληρώθηκε'
        progress['message'] = f"Ενημερώθηκαν {progress['updated_count']} αξιολογήσεις."

    except Exception as exc:
        logger.exception("Fatal ratings scrape error")
        progress['error'] = str(exc)
    finally:
        progress['running'] = False
        progress['completed_at'] = datetime.now().isoformat()


def pause_scrape():
    progress['paused'] = True
    _pause_event.clear()


def resume_scrape():
    progress['paused'] = False
    _pause_event.set()


def stop_scrape():
    progress['running'] = False
    _pause_event.set()  # Unblock any waiting
