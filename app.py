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

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Weekly scheduler (local only)
# ---------------------------------------------------------------------------
if not CLOUD_MODE:
    scheduler = BackgroundScheduler(daemon=True)

    def _scheduled_scrape():
        if not scraper.progress['running']:
            logger.info("Weekly scheduled scrape starting...")
            threading.Thread(target=scraper.run_scrape, daemon=True).start()

    scheduler.add_job(_scheduled_scrape, 'interval', weeks=1, id='weekly_scrape')
    scheduler.start()


# ---------------------------------------------------------------------------
# Page route
# ---------------------------------------------------------------------------
@app.route('/')
def index():
    return render_template('index.html', cloud_mode=CLOUD_MODE)


# ---------------------------------------------------------------------------
# Movies API
# ---------------------------------------------------------------------------
@app.route('/api/movies')
def api_movies():
    filters = {
        'title':        request.args.get('title', '').strip() or None,
        'year_from':    request.args.get('year_from') or None,
        'year_to':      request.args.get('year_to') or None,
        'countries':    request.args.getlist('country') or None,
        'genres':       request.args.getlist('genre') or None,
        'rating_min':   request.args.get('rating_min') or None,
        'rating_max':   request.args.get('rating_max') or None,
        'director':     request.args.get('director', '').strip() or None,
        'duration_min': request.args.get('duration_min') or None,
        'duration_max': request.args.get('duration_max') or None,
    }
    filters = {k: v for k, v in filters.items() if v is not None}

    try:
        page     = max(1, int(request.args.get('page', 1)))
        per_page = min(100, max(12, int(request.args.get('per_page', 24))))
        sort_by  = request.args.get('sort_by', 'year')
        sort_dir = request.args.get('sort_dir', 'desc')
    except (ValueError, TypeError):
        page, per_page, sort_by, sort_dir = 1, 24, 'year', 'desc'

    result = database.get_movies(filters, page, per_page, sort_by, sort_dir)
    return jsonify(result)


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
# Scraper control API (local only — disabled in cloud mode)
# ---------------------------------------------------------------------------
if not CLOUD_MODE:
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

    @app.route('/api/scrape/ratings', methods=['POST'])
    def api_scrape_ratings():
        if scraper.progress['running']:
            return jsonify({'error': 'Η εξαγωγη δεδομενων βρισκεται ηδη σε εξελιξη.'}), 409
        threading.Thread(target=scraper.run_ratings_scrape, daemon=True).start()
        return jsonify({'message': 'Ενημερωση αξιολογησεων ξεκινησε.'})

    @app.route('/api/scrape/status')
    def api_scrape_status():
        return jsonify(scraper.progress)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    if not CLOUD_MODE:
        print(f"\n  Athinorama Movie Archive")
        print(f"    http://localhost:{port}\n")
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
