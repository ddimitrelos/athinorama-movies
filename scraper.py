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
from datetime import datetime

import database

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

    # Year
    year = None
    for field in ('dateCreated', 'datePublished', 'copyrightYear'):
        val = data.get(field)
        if val:
            try:
                year = int(str(val)[:4])
                break
            except (ValueError, TypeError):
                pass

    # Country
    cd = data.get('countryOfOrigin')
    if isinstance(cd, list):
        country = ', '.join(c.get('name', '') for c in cd if isinstance(c, dict) and c.get('name'))
    elif isinstance(cd, dict):
        country = cd.get('name', '')
    else:
        country = _extract_str(cd)

    # Director
    dd = data.get('director', [])
    if isinstance(dd, list):
        director = ', '.join(d.get('name', '') for d in dd if isinstance(d, dict) and d.get('name'))
    elif isinstance(dd, dict):
        director = dd.get('name', '')
    else:
        director = _extract_str(dd)

    # Cast (limit to 20 actors)
    ad = data.get('actor', [])
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

    # Rating
    rating = None
    rr = data.get('reviewRating', {})
    if isinstance(rr, dict):
        try:
            rv = rr.get('ratingValue')
            if rv is not None:
                rating = float(rv) or None
        except (ValueError, TypeError):
            pass

    # Synopsis
    review = data.get('review', {})
    synopsis = ''
    if isinstance(review, list) and review:
        review = review[0]
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
                        if (d) { rating = parseFloat(d); break; }
                        // Count filled star icons
                        const filled = ratingEl.querySelectorAll(
                            '.bi-star-fill, [class*="star-fill"], [class*="full"], [class*="filled"]'
                        ).length;
                        if (filled > 0) { rating = filled; break; }
                        // Try text content as number
                        const txt = ratingEl.textContent.trim();
                        const num = parseFloat(txt);
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

            for i, slug in enumerate(todo_slugs):
                _pause_event.wait()
                if not progress['running']:
                    break

                progress['current'] = i + 1
                progress['message'] = slug

                try:
                    url = f"{BASE_URL}/cinema/movie/{slug}/"
                    page.goto(url, wait_until='domcontentloaded')

                    json_ld = _get_json_ld(page)

                    if json_ld:
                        movie_data = _parse_json_ld(json_ld, slug)
                    else:
                        # Minimal fallback
                        h1 = page.locator('h1').first
                        title = h1.text_content(timeout=3000).strip() if h1 else slug
                        movie_data = {
                            'slug':           slug,
                            'title_gr':       title,
                            'athinorama_url': url,
                            'detail_scraped': 0,
                        }

                    result = database.upsert_movie(movie_data)
                    if result == 'inserted':
                        progress['new_count'] += 1
                    elif result == 'updated':
                        progress['updated_count'] += 1

                except PWTimeout:
                    logger.warning(f"Timeout on movie {slug}")
                except Exception as exc:
                    logger.error(f"Error scraping {slug}: {exc}")

                time.sleep(0.35)

            browser.close()

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
