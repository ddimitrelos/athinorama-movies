"""
Athinorama Movie Archive - Flask Web Application
Run locally:  python app.py
Cloud deploy: CLOUD_MODE=1 gunicorn app:app
"""

import os
import threading
import logging
from flask import Flask, render_template, jsonify, request

import database

# ---------------------------------------------------------------------------
# Cloud mode: set CLOUD_MODE=1 to disable scraper (used on Railway/hosting)
# ---------------------------------------------------------------------------
CLOUD_MODE = os.environ.get('CLOUD_MODE', '0') == '1'

if not CLOUD_MODE:
    import scraper
    from apscheduler.schedulers.background import BackgroundScheduler

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = Flask(__name__)
database.init_db()

# One-time data fixes — safe to re-run (idempotent WHERE clauses)
def _run_data_migrations():
    with database.get_db() as conn:
        # dateCreated on Athinorama is the Greek release date, not production year;
        # copyrightYear=2025 is correct for this film.
        conn.execute(
            "UPDATE movies SET year=2025 WHERE slug='to_deipno_tou_franko-10091040' AND year!=2025"
        )
        conn.commit()

_run_data_migrations()

import migrate_countries
migrate_countries.run_once()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Weekly scheduler (local only)
# ---------------------------------------------------------------------------
if not CLOUD_MODE:
    import json
    from datetime import datetime, timedelta

    SCHEDULE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.scrape_schedule.json')

    def _load_schedule():
        try:
            with open(SCHEDULE_FILE) as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_schedule(data):
        try:
            with open(SCHEDULE_FILE, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.warning(f"Could not save schedule: {e}")

    def _scheduled_scrape():
        if scraper.progress['running']:
            return
        logger.info("Weekly scheduled scrape starting...")
        _save_schedule({'last_run': datetime.now().isoformat()})
        threading.Thread(target=scraper.run_scrape, daemon=True).start()

    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(_scheduled_scrape, 'interval', weeks=1, id='weekly_scrape',
                      next_run_time=datetime.now() + timedelta(weeks=1))
    scheduler.start()

    # On startup: if more than 7 days since last scrape, run immediately
    schedule_data = _load_schedule()
    last_run_str = schedule_data.get('last_run')
    if last_run_str:
        last_run = datetime.fromisoformat(last_run_str)
        days_since = (datetime.now() - last_run).days
        if days_since >= 7:
            logger.info(f"Last scrape was {days_since} days ago — running now.")
            threading.Thread(target=_scheduled_scrape, daemon=True).start()
        else:
            next_run = last_run + timedelta(weeks=1)
            # Adjust next_run_time to the correct date (not just 1 week from now)
            next_run_time = next_run if next_run > datetime.now() else datetime.now() + timedelta(seconds=5)
            scheduler.modify_job('weekly_scrape', next_run_time=next_run_time)
            logger.info(f"Next scheduled scrape: {next_run.strftime('%Y-%m-%d %H:%M')}")
    else:
        logger.info("No previous scrape recorded — weekly auto-scrape will run in 7 days.")


# ---------------------------------------------------------------------------
# Page route
# ---------------------------------------------------------------------------
@app.route('/')
def index():
    return render_template('index.html', cloud_mode=CLOUD_MODE)


# ---------------------------------------------------------------------------
# Movies API
# ---------------------------------------------------------------------------
def _parse_numeric_filters(args):
    """Parse and validate numeric filter params; return (filters_dict, error_response)."""
    int_fields   = ('year_from', 'year_to', 'duration_min', 'duration_max')
    float_fields = ('rating_min', 'rating_max')
    filters = {
        'title':        args.get('title', '').strip() or None,
        'countries':    args.getlist('country') or None,
        'genres':       args.getlist('genre') or None,
        'director':     args.get('director', '').strip() or None,
    }
    for field in int_fields:
        raw = args.get(field) or None
        if raw is not None:
            try:
                filters[field] = str(int(raw))
            except (ValueError, TypeError):
                return None, (jsonify({'error': f"Invalid value for '{field}': must be an integer"}), 400)
        else:
            filters[field] = None
    for field in float_fields:
        raw = args.get(field) or None
        if raw is not None:
            try:
                filters[field] = str(float(raw))
            except (ValueError, TypeError):
                return None, (jsonify({'error': f"Invalid value for '{field}': must be a number"}), 400)
        else:
            filters[field] = None
    return {k: v for k, v in filters.items() if v is not None}, None


@app.route('/api/movies')
def api_movies():
    filters, err = _parse_numeric_filters(request.args)
    if err:
        return err

    try:
        page     = max(1, int(request.args.get('page', 1)))
        per_page = min(100, max(12, int(request.args.get('per_page', 24))))
        sort_by  = request.args.get('sort_by', 'year')
        sort_dir = request.args.get('sort_dir', 'desc')
    except (ValueError, TypeError):
        page, per_page, sort_by, sort_dir = 1, 24, 'year', 'desc'

    result = database.get_movies(filters, page, per_page, sort_by, sort_dir)
    return jsonify(result)


@app.route('/api/movies/random')
def api_random_movie():
    filters, err = _parse_numeric_filters(request.args)
    if err:
        return err
    slug = database.get_random_movie(filters)
    if slug:
        return jsonify({'slug': slug})
    return jsonify({'error': 'No movies found'}), 404


@app.route('/api/movies/<slug>')
def api_movie_detail(slug):
    movie = database.get_movie_detail(slug)
    if movie:
        return jsonify(movie)
    return jsonify({'error': 'Not found'}), 404


@app.route('/api/filters')
def api_filters():
    return jsonify(database.get_filter_options())


@app.route('/api/stats')
def api_stats():
    return jsonify(database.get_scrape_stats())


# ---------------------------------------------------------------------------
# Internal ratings sync endpoint (available in all modes)
# ---------------------------------------------------------------------------
RATINGS_SYNC_TOKEN = 'athinorama-ratings-sync-2026'

@app.route('/api/internal/update-ratings', methods=['POST'])
def api_internal_update_ratings():
    token = request.headers.get('X-Ratings-Token', '')
    if token != RATINGS_SYNC_TOKEN:
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json(silent=True)
    if not isinstance(data, list):
        return jsonify({'error': 'Expected a JSON array'}), 400
    updated = 0
    for entry in data:
        slug = entry.get('slug')
        rating = entry.get('rating')
        if slug and rating is not None:
            database.update_rating(slug, rating)
            updated += 1
    return jsonify({'updated': updated})


# ---------------------------------------------------------------------------
# Scraper control API (local only — disabled in cloud mode)
# ---------------------------------------------------------------------------
if not CLOUD_MODE:
    @app.route('/api/scrape/ratings', methods=['POST'])
    def api_scrape_ratings():
        if scraper.progress['running']:
            return jsonify({'error': 'Η εξαγωγη δεδομενων βρισκεται ηδη σε εξελιξη.'}), 409
        threading.Thread(target=scraper.run_ratings_scrape, daemon=True).start()
        return jsonify({'message': 'Συμπληρωση κενων αξιολογησεων ξεκινησε.'})

    @app.route('/api/scrape/status')
    def api_scrape_status():
        status = dict(scraper.progress)
        try:
            job = scheduler.get_job('weekly_scrape')
            if job and job.next_run_time:
                nrt = job.next_run_time
                if hasattr(nrt, 'astimezone'):
                    nrt = nrt.astimezone(tz=None).replace(tzinfo=None)
                status['next_scheduled'] = nrt.strftime('%d/%m/%Y %H:%M')
        except Exception as e:
            logger.warning(f"Could not get next run time: {e}")
        schedule_data = _load_schedule()
        if schedule_data.get('last_run'):
            status['last_auto_scrape'] = schedule_data['last_run']
        return jsonify(status)

    @app.route('/api/scrape/start', methods=['POST'])
    def api_scrape_start():
        if scraper.progress['running']:
            return jsonify({'error': 'Η εξαγωγη δεδομενων βρισκεται ηδη σε εξελιξη.'}), 409
        full = request.json.get('full_rescrape', False) if request.json else False
        threading.Thread(
            target=scraper.run_scrape,
            kwargs={'full_rescrape': full},
            daemon=True
        ).start()
        return jsonify({'message': 'Η εξαγωγη δεδομενων ξεκινησε.'})

    @app.route('/api/scrape/pause', methods=['POST'])
    def api_scrape_pause():
        if not scraper.progress['running']:
            return jsonify({'error': 'Δεν τρεχει εξαγωγη.'}), 400
        scraper.pause_scrape()
        return jsonify({'message': 'Παυση.'})

    @app.route('/api/scrape/resume', methods=['POST'])
    def api_scrape_resume():
        scraper.resume_scrape()
        return jsonify({'message': 'Συνεχεια.'})

    @app.route('/api/scrape/stop', methods=['POST'])
    def api_scrape_stop():
        scraper.stop_scrape()
        return jsonify({'message': 'Διακοπη.'})



# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    if not CLOUD_MODE:
        print(f"\n  Athinorama Movie Archive")
        print(f"    http://localhost:{port}\n")
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
