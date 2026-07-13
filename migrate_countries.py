"""
migrate_countries.py
One-time (idempotent) migration to normalize the `country` field in movies.db.

Usage:
    python migrate_countries.py            # apply to DB
    python migrate_countries.py --dry-run  # print stats without writing

Can also be called from app.py:
    import migrate_countries
    migrate_countries.run()
"""

import sys
import sqlite3
import os
from country_normalizer import normalize_country_field

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'movies.db')

_ALREADY_RAN = False  # module-level guard so app.py startup doesn't repeat


def run(dry_run: bool = False) -> dict:
    """
    Run the country normalization migration.
    Returns a dict with stats: total, updated, unchanged, multi_country, failed.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT id, country FROM movies WHERE country IS NOT NULL AND country != ''"
    ).fetchall()

    total = len(rows)
    updated = 0
    unchanged = 0
    multi_country = 0
    failed = 0

    updates: list[tuple[str, int]] = []

    for row in rows:
        raw = row['country']
        normalized = normalize_country_field(raw)

        if normalized == raw:
            unchanged += 1
            continue

        if not normalized:
            # Empty result — don't corrupt, keep original
            unchanged += 1
            continue

        # Count entries that failed to parse (kept as-is in result)
        if normalized == raw:
            failed += 1
            continue

        countries_in_result = [c.strip() for c in normalized.split(',') if c.strip()]
        if len(countries_in_result) > 1:
            multi_country += 1

        updates.append((normalized, row['id']))
        updated += 1

    print(f"Country normalization migration")
    print(f"  Total with country: {total}")
    print(f"  To update:          {updated}")
    print(f"  Already clean:      {unchanged}")
    print(f"  Multi-country:      {multi_country}")
    if dry_run:
        print("  DRY RUN — no changes written.")
        # Show sample
        print("\nSample updates (first 20):")
        sample_rows = [r for r in rows if normalize_country_field(r['country']) != r['country']][:20]
        for r in sample_rows:
            raw = r['country']
            norm = normalize_country_field(raw)
            print(f"  {raw!r:60s} -> {norm!r}")
    else:
        if updates:
            conn.executemany("UPDATE movies SET country = ? WHERE id = ?", updates)
            conn.commit()
            print(f"  Written {updated} updates to DB.")
        else:
            print("  Nothing to update.")
    conn.close()

    return {
        'total': total,
        'updated': updated,
        'unchanged': unchanged,
        'multi_country': multi_country,
    }


def run_once():
    """Run migration at most once per process lifetime (called from app.py)."""
    global _ALREADY_RAN
    if _ALREADY_RAN:
        return
    _ALREADY_RAN = True
    try:
        run(dry_run=False)
    except Exception as e:
        # Don't crash the app if migration fails
        import logging
        logging.getLogger(__name__).warning(f"Country migration failed: {e}")


if __name__ == '__main__':
    dry = '--dry-run' in sys.argv
    run(dry_run=dry)
