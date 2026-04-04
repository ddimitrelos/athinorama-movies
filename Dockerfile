FROM python:3.12-slim

WORKDIR /app

# Install server dependencies only (no Playwright/Chromium)
COPY requirements-server.txt .
RUN pip install --no-cache-dir -r requirements-server.txt

# Copy app files (scraper.py intentionally excluded)
COPY app.py database.py ./
COPY templates/ templates/
COPY static/ static/
COPY movies.db ./

# Cloud mode disables scraper routes and weekly scheduler
ENV CLOUD_MODE=1

EXPOSE 8080

CMD sh -c "gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 2 --timeout 60 app:app"
