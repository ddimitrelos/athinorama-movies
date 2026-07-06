"""Fix stale year=1 values by re-reading copyrightYear from JSON-LD."""
import sqlite3, sys, json, requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.stdout.reconfigure(encoding='utf-8')

DB = r"C:\Users\dimitrios.dimitrelos\OneDrive - Accenture\Documents\AI Tests\Movie App\movies.db"
BASE = "https://www.athinorama.gr"
SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'Mozilla/5.0', 'Accept-Language': 'el-GR,el;q=0.9'})

conn = sqlite3.connect(DB)
rows = conn.execute("SELECT slug FROM movies WHERE year IS NOT NULL AND year < 1880").fetchall()
conn.close()
slugs = [r[0] for r in rows]
print(f"Fixing years for {len(slugs)} movies...")

def fix_year(slug):
    try:
        resp = SESSION.get(f"{BASE}/cinema/movie/{slug}/", timeout=15)
        if resp.status_code != 200:
            return slug, 'error'
        soup = BeautifulSoup(resp.content, 'html.parser')
        for tag in soup.find_all('script', type='application/ld+json'):
            try:
                d = json.loads(tag.string or '')
                if isinstance(d, dict) and d.get('@type') == 'Movie':
                    for field in ('copyrightYear', 'datePublished', 'dateCreated'):
                        val = d.get(field)
                        if val:
                            try:
                                y = int(str(val)[:4])
                                if y >= 1880:
                                    return slug, y
                            except Exception:
                                pass
            except Exception:
                pass
        return slug, None
    except Exception:
        return slug, 'error'

fixed = nulled = errors = 0
conn = sqlite3.connect(DB)
with ThreadPoolExecutor(max_workers=16) as ex:
    futures = {ex.submit(fix_year, s): s for s in slugs}
    for i, f in enumerate(as_completed(futures)):
        slug, year = f.result()
        if year == 'error':
            errors += 1
        elif year is None:
            conn.execute("UPDATE movies SET year=NULL WHERE slug=?", (slug,))
            nulled += 1
        else:
            conn.execute("UPDATE movies SET year=? WHERE slug=?", (year, slug))
            fixed += 1
        if (i + 1) % 200 == 0:
            conn.commit()
            print(f"  {i+1}/{len(slugs)} -- fixed:{fixed} nulled:{nulled} errors:{errors}")

conn.commit()
conn.close()
print(f"\nDone. Fixed:{fixed}  Nulled:{nulled}  Errors:{errors}")
