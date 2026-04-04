import sqlite3
import os
import unicodedata
from datetime import datetime


def _normalize(text):
    """Lowercase + strip Greek (and all) accent marks for accent-insensitive search."""
    if not text:
        return ''
    return ''.join(
        c for c in unicodedata.normalize('NFD', str(text).lower())
        if unicodedata.category(c) != 'Mn'
    )

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'movies.db')

SCHEMA = """
CREATE TABLE IF NOT EXISTS movies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    slug            TEXT    UNIQUE NOT NULL,
    title_gr        TEXT,
    title_orig      TEXT,
    year            INTEGER,
    country         TEXT,
    director        TEXT,
    cast            TEXT,
    duration        INTEGER,
    genre           TEXT,
    rating          REAL,
    synopsis        TEXT,
    poster_url      TEXT,
    athinorama_url  TEXT,
    detail_scraped  INTEGER DEFAULT 0,
    last_updated    TEXT
);

CREATE INDEX IF NOT EXISTS idx_movies_year     ON movies(year);
CREATE INDEX IF NOT EXISTS idx_movies_rating   ON movies(rating);
CREATE INDEX IF NOT EXISTS idx_movies_country  ON movies(country);
CREATE INDEX IF NOT EXISTS idx_movies_genre    ON movies(genre);
CREATE INDEX IF NOT EXISTS idx_movies_title_gr ON movies(title_gr);
"""


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.create_function('norm', 1, _normalize)
    return conn


def init_db():
    with get_db() as conn:
        conn.executescript(SCHEMA)
        conn.commit()


def upsert_movie(movie_data):
    """Insert or update a movie. Returns 'inserted' or 'updated'."""
    movie_data = {k: v for k, v in movie_data.items() if v is not None}
    movie_data['last_updated'] = datetime.now().isoformat()

    with get_db() as conn:
        existing = conn.execute(
            "SELECT id, detail_scraped FROM movies WHERE slug = ?",
            (movie_data['slug'],)
        ).fetchone()

        if existing:
            # Don't overwrite detail data with empty data
            if not movie_data.get('detail_scraped') and existing['detail_scraped']:
                return 'skipped'
            cols = ', '.join(f"{k} = ?" for k in movie_data if k != 'slug')
            vals = [v for k, v in movie_data.items() if k != 'slug']
            vals.append(movie_data['slug'])
            conn.execute(f"UPDATE movies SET {cols} WHERE slug = ?", vals)
            conn.commit()
            return 'updated'
        else:
            cols = ', '.join(movie_data.keys())
            placeholders = ', '.join('?' * len(movie_data))
            conn.execute(
                f"INSERT INTO movies ({cols}) VALUES ({placeholders})",
                list(movie_data.values())
            )
            conn.commit()
            return 'inserted'


def update_rating(slug, rating):
    """Update only the rating field for an existing movie."""
    with get_db() as conn:
        conn.execute(
            "UPDATE movies SET rating = ?, last_updated = ? WHERE slug = ?",
            (rating, datetime.now().isoformat(), slug)
        )
        conn.commit()


def get_movies(filters=None, page=1, per_page=24, sort_by='year', sort_dir='desc'):
    """Fetch movies with optional filters, returns paginated result."""
    where_clauses = []
    params = []

    if filters:
        if filters.get('title'):
            where_clauses.append("(norm(title_gr) LIKE ? OR norm(title_orig) LIKE ? OR norm(director) LIKE ?)")
            t = f"%{_normalize(filters['title'])}%"
            params.extend([t, t, t])

        if filters.get('year_from'):
            where_clauses.append("year >= ?")
            params.append(int(filters['year_from']))

        if filters.get('year_to'):
            where_clauses.append("year <= ?")
            params.append(int(filters['year_to']))

        if filters.get('countries'):
            countries = filters['countries'] if isinstance(filters['countries'], list) else [filters['countries']]
            countries = [c for c in countries if c]
            if countries:
                placeholders = ', '.join('?' * len(countries))
                where_clauses.append(f"country IN ({placeholders})")
                params.extend(countries)

        if filters.get('genres'):
            genres = filters['genres'] if isinstance(filters['genres'], list) else [filters['genres']]
            genres = [g for g in genres if g]
            if genres:
                genre_clauses = ' OR '.join(['genre LIKE ?' for _ in genres])
                where_clauses.append(f"({genre_clauses})")
                params.extend([f"%{g}%" for g in genres])

        if filters.get('rating_min') is not None and filters['rating_min'] != '':
            where_clauses.append("rating >= ?")
            params.append(float(filters['rating_min']))

        if filters.get('rating_max') is not None and filters['rating_max'] != '':
            where_clauses.append("rating <= ?")
            params.append(float(filters['rating_max']))

        if filters.get('director'):
            where_clauses.append("norm(director) LIKE ?")
            params.append(f"%{_normalize(filters['director'])}%")

        if filters.get('duration_min'):
            where_clauses.append("duration >= ?")
            params.append(int(filters['duration_min']))

        if filters.get('duration_max'):
            where_clauses.append("duration <= ?")
            params.append(int(filters['duration_max']))

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    valid_sorts = {'year': 'year', 'rating': 'rating', 'title': 'title_gr', 'duration': 'duration'}
    sort_col = valid_sorts.get(sort_by, 'year')
    sort_direction = 'DESC' if sort_dir == 'desc' else 'ASC'

    with get_db() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM movies WHERE {where_sql}", params
        ).fetchone()[0]

        offset = (page - 1) * per_page
        rows = conn.execute(
            f"""SELECT id, slug, title_gr, title_orig, year, country, director,
                       duration, genre, rating, poster_url, athinorama_url, "cast"
                FROM movies
                WHERE {where_sql}
                ORDER BY {sort_col} {sort_direction} NULLS LAST
                LIMIT ? OFFSET ?""",
            params + [per_page, offset]
        ).fetchall()

        return {
            'movies': [dict(r) for r in rows],
            'total': total,
            'page': page,
            'per_page': per_page,
            'pages': max(1, (total + per_page - 1) // per_page)
        }


def get_movie_detail(slug):
    """Get full movie details including synopsis."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM movies WHERE slug = ?", (slug,)).fetchone()
        return dict(row) if row else None


def get_filter_options():
    """Return distinct values for filter dropdowns."""
    with get_db() as conn:
        years = [r[0] for r in conn.execute(
            "SELECT DISTINCT year FROM movies WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

        countries_raw = [r[0] for r in conn.execute(
            "SELECT DISTINCT country FROM movies WHERE country IS NOT NULL AND country != '' ORDER BY country"
        ).fetchall()]
        # Expand comma-separated countries
        countries = sorted(set(
            c.strip() for raw in countries_raw for c in raw.split(',') if c.strip()
        ))

        genres_raw = [r[0] for r in conn.execute(
            "SELECT DISTINCT genre FROM movies WHERE genre IS NOT NULL AND genre != '' ORDER BY genre"
        ).fetchall()]
        genres = sorted(set(
            g.strip() for raw in genres_raw for g in raw.split(',') if g.strip()
        ))

        stats = conn.execute(
            """SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN detail_scraped = 1 THEN 1 END) as with_details,
                MIN(year) as min_year,
                MAX(year) as max_year,
                COUNT(DISTINCT country) as country_count
               FROM movies"""
        ).fetchone()

        return {
            'years': years,
            'countries': countries,
            'genres': genres,
            'stats': dict(stats) if stats else {}
        }


def get_scrape_stats():
    """Return current DB stats."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as total, COUNT(CASE WHEN detail_scraped=1 THEN 1 END) as detailed FROM movies"
        ).fetchone()
        return dict(row) if row else {'total': 0, 'detailed': 0}
