/**
 * Athinorama Movie Archive – Frontend
 */

// ── State ──────────────────────────────────────────────────────────────────
const state = {
  page: 1,
  perPage: 24,
  sortBy: 'year',
  sortDir: 'desc',
  filters: {},
  totalPages: 1,
  loading: false,
};

let debounceTimer = null;
let scrapePoller  = null;
let tsCountry     = null;
let tsGenre       = null;

// ── Bootstrap modal handles ────────────────────────────────────────────────
const movieModal  = new bootstrap.Modal(document.getElementById('movieModal'));
const scrapeModal = new bootstrap.Modal(document.getElementById('scrapeModal'));

// ── DOM refs ──────────────────────────────────────────────────────────────
const grid        = document.getElementById('movie-grid');
const pagination  = document.getElementById('pagination');
const resultCount = document.getElementById('result-count');
const loadingEl   = document.getElementById('loading-state');
const emptyEl     = document.getElementById('empty-state');
const dbStats     = document.getElementById('db-stats');


// ═══════════════════════════════════════════════════════════════════════════
// Utilities
// ═══════════════════════════════════════════════════════════════════════════

function debounce(fn, ms = 400) {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(fn, ms);
}

function starsHtml(rating, showNumber = false) {
  if (rating == null || rating === '') return '<span style="color:#555">—</span>';
  const full  = Math.floor(rating);
  const half  = (rating % 1) >= 0.5 ? 1 : 0;
  const empty = 5 - full - half;
  let html = '';
  for (let i = 0; i < full; i++)  html += '<i class="bi bi-star-fill"></i>';
  if (half)                        html += '<i class="bi bi-star-half"></i>';
  for (let i = 0; i < empty; i++) html += '<i class="bi bi-star" style="opacity:.3"></i>';
  if (showNumber) html += ` <span class="rating-num">${rating}</span>`;
  return html;
}

function starsText(rating) {
  if (rating == null || rating === '') return '';
  return `${rating} / 5`;
}

function posterSrc(url) {
  if (!url) return null;
  // Upgrade low-res archive thumbnails to decent size
  url = url.replace('/100x143/', '/300x409/');
  // Append resize params for Content/ImagesDatabase URLs
  if (url.includes('/Content/ImagesDatabase/') && !url.includes('?')) {
    url += '?w=250&h=375&mode=pad&bgcolor=191919';
  }
  return url;
}

function formatDuration(mins) {
  if (!mins) return '';
  const h = Math.floor(mins / 60);
  const m = mins % 60;
  return h > 0 ? `${h}ω ${m}λ` : `${m}λ`;
}

function collectFilters() {
  const f = {};

  const title = document.getElementById('f-title').value.trim();
  if (title) f.title = title;

  const yf = document.getElementById('f-year-from').value;
  const yt = document.getElementById('f-year-to').value;
  if (yf) f.year_from = yf;
  if (yt) f.year_to   = yt;

  const rmin = document.getElementById('f-rating-min').value;
  const rmax = document.getElementById('f-rating-max').value;
  if (rmin) f.rating_min = rmin;
  if (rmax) f.rating_max = rmax;

  const dmin = document.getElementById('f-dur-min').value;
  const dmax = document.getElementById('f-dur-max').value;
  if (dmin) f.duration_min = dmin;
  if (dmax) f.duration_max = dmax;

  if (tsCountry) {
    const countries = tsCountry.getValue();
    if (countries.length) f.country = countries;
  }
  if (tsGenre) {
    const genres = tsGenre.getValue();
    if (genres.length) f.genre = genres;
  }

  return f;
}

function buildQueryString(filters, extra = {}) {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(filters)) {
    if (Array.isArray(v)) v.forEach(x => params.append(k, x));
    else params.set(k, v);
  }
  for (const [k, v] of Object.entries(extra)) {
    params.set(k, v);
  }
  return params.toString();
}


// ═══════════════════════════════════════════════════════════════════════════
// Data loading
// ═══════════════════════════════════════════════════════════════════════════

async function loadMovies() {
  if (state.loading) return;
  state.loading = true;

  grid.style.display        = 'none';
  loadingEl.style.display   = 'block';
  emptyEl.style.display     = 'none';
  pagination.innerHTML      = '';

  const qs = buildQueryString(state.filters, {
    page:     state.page,
    per_page: state.perPage,
    sort_by:  state.sortBy,
    sort_dir: state.sortDir,
  });

  try {
    const res  = await fetch(`/api/movies?${qs}`);
    const data = await res.json();

    state.totalPages = data.pages || 1;

    loadingEl.style.display = 'none';

    if (!data.movies || data.movies.length === 0) {
      emptyEl.style.display = 'block';
      document.getElementById('empty-title').textContent =
        Object.keys(state.filters).length > 0
          ? 'Δεν βρέθηκαν ταινίες με αυτά τα φίλτρα'
          : 'Δεν υπάρχουν ταινίες στη βάση';
      document.getElementById('empty-subtitle').textContent =
        Object.keys(state.filters).length > 0
          ? 'Δοκιμάστε διαφορετικά κριτήρια.'
          : 'Πατήστε "Scrape" για να φορτώσετε τις ταινίες από το Αθηνόραμα.';
    } else {
      grid.style.display = 'grid';
      renderGrid(data.movies);
      renderPagination(data.total, data.page, data.pages);
      resultCount.textContent = `${data.total.toLocaleString('el-GR')} ταινίες`;
    }

  } catch (e) {
    loadingEl.style.display = 'none';
    emptyEl.style.display   = 'block';
    document.getElementById('empty-title').textContent    = 'Σφάλμα φόρτωσης';
    document.getElementById('empty-subtitle').textContent = e.message;
  } finally {
    state.loading = false;
  }
}

async function loadFilters() {
  try {
    const res  = await fetch('/api/filters');
    const data = await res.json();

    // Populate Tom Select dropdowns
    if (tsCountry) tsCountry.destroy();
    if (tsGenre)   tsGenre.destroy();

    tsCountry = new TomSelect('#f-country', {
      options: data.countries.map(c => ({ value: c, text: c })),
      plugins: ['remove_button', 'checkbox_options'],
      maxOptions: 300,
      placeholder: 'Επιλογή χώρας…',
    });

    tsGenre = new TomSelect('#f-genre', {
      options: data.genres.map(g => ({ value: g, text: g })),
      plugins: ['remove_button', 'checkbox_options'],
      maxOptions: 200,
      placeholder: 'Επιλογή είδους…',
    });

    tsCountry.on('change', onFilterChange);
    tsGenre.on('change',   onFilterChange);

    // Stats
    const s = data.stats || {};
    if (s.total > 0) {
      dbStats.textContent = `${s.total.toLocaleString('el-GR')} ταινίες στη βάση`;
    }

  } catch (e) {
    console.error('Filter load error', e);
  }
}

async function refreshStats() {
  try {
    const res  = await fetch('/api/stats');
    const data = await res.json();
    if (data.total > 0) {
      dbStats.textContent = `${data.total.toLocaleString('el-GR')} ταινίες (${data.detailed.toLocaleString('el-GR')} με λεπτομέρειες)`;
    } else {
      dbStats.textContent = 'Βάση κενή — εκτελέστε Scrape';
    }
  } catch (_) {}
}


// ═══════════════════════════════════════════════════════════════════════════
// Rendering
// ═══════════════════════════════════════════════════════════════════════════

function renderGrid(movies) {
  grid.innerHTML = movies.map(m => {
    const poster  = posterSrc(m.poster_url);
    const genre1  = m.genre ? m.genre.split(',')[0].trim() : '';
    const country = m.country ? m.country.split(',')[0].trim() : '';
    return `
    <div class="movie-card" onclick="openDetail('${escHtml(m.slug)}')">
      <div class="poster-wrap">
        ${poster
          ? `<img src="${escHtml(poster)}" alt="${escHtml(m.title_gr || '')}" loading="lazy"
                  onerror="this.style.display='none';this.nextElementSibling.style.display='flex'">`
          : ''}
        <div class="no-poster" ${poster ? 'style="display:none"' : ''}>
          <i class="bi bi-film"></i>
        </div>
      </div>
      <div class="card-body">
        <div class="title-gr">${escHtml(m.title_gr || '—')}</div>
        ${m.title_orig ? `<div class="title-orig">${escHtml(m.title_orig)}</div>` : ''}
        <div class="stars mt-1">${starsHtml(m.rating, true)}</div>
        <div class="meta mt-1">
          ${m.year && m.year >= 1880 ? `<span>${m.year}</span>` : ''}
          ${country ? `<span class="ms-1">· ${escHtml(country)}</span>` : ''}
          ${m.duration ? `<span class="ms-1">· ${formatDuration(m.duration)}</span>` : ''}
        </div>
        ${genre1 ? `<div class="mt-1"><span class="badge-genre">${escHtml(genre1)}</span></div>` : ''}
      </div>
    </div>`;
  }).join('');
}

function renderPagination(total, page, pages) {
  if (pages <= 1) { pagination.innerHTML = ''; return; }

  const maxVisible = 7;
  let start = Math.max(1, page - Math.floor(maxVisible / 2));
  let end   = Math.min(pages, start + maxVisible - 1);
  if (end - start < maxVisible - 1) start = Math.max(1, end - maxVisible + 1);

  let html = '';

  // Prev
  html += `<li class="page-item ${page === 1 ? 'disabled' : ''}">
    <a class="page-link" href="#" onclick="goPage(${page - 1});return false">‹</a></li>`;

  if (start > 1) {
    html += `<li class="page-item"><a class="page-link" href="#" onclick="goPage(1);return false">1</a></li>`;
    if (start > 2) html += `<li class="page-item disabled"><span class="page-link">…</span></li>`;
  }

  for (let p = start; p <= end; p++) {
    html += `<li class="page-item ${p === page ? 'active' : ''}">
      <a class="page-link" href="#" onclick="goPage(${p});return false">${p}</a></li>`;
  }

  if (end < pages) {
    if (end < pages - 1) html += `<li class="page-item disabled"><span class="page-link">…</span></li>`;
    html += `<li class="page-item"><a class="page-link" href="#" onclick="goPage(${pages});return false">${pages}</a></li>`;
  }

  // Next
  html += `<li class="page-item ${page === pages ? 'disabled' : ''}">
    <a class="page-link" href="#" onclick="goPage(${page + 1});return false">›</a></li>`;

  pagination.innerHTML = html;
}

function goPage(p) {
  if (p < 1 || p > state.totalPages || p === state.page) return;
  state.page = p;
  loadMovies();
  document.getElementById('main-content').scrollTo({ top: 0, behavior: 'smooth' });
}

function escHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}


// ═══════════════════════════════════════════════════════════════════════════
// Movie detail modal
// ═══════════════════════════════════════════════════════════════════════════

// Stop trailer playback when modal closes
document.getElementById('movieModal').addEventListener('hidden.bs.modal', () => {
  document.getElementById('modal-trailer').src = '';
  document.getElementById('modal-trailer-wrap').style.display = 'none';
});

async function openDetail(slug) {
  // Reset modal
  document.getElementById('modal-poster').style.display = 'none';
  document.getElementById('modal-poster-placeholder').style.display = 'flex';
  document.getElementById('modal-synopsis').textContent = '';
  document.getElementById('modal-cast-row').style.display = 'none';
  document.getElementById('modal-trailer-wrap').style.display = 'none';
  document.getElementById('modal-trailer').src = '';

  movieModal.show();

  try {
    const res  = await fetch(`/api/movies/${encodeURIComponent(slug)}`);
    const m    = await res.json();

    document.getElementById('modal-title-gr').textContent   = m.title_gr   || '—';
    document.getElementById('modal-title-orig').textContent = m.title_orig || '';
    document.getElementById('modal-stars').innerHTML        = starsHtml(m.rating) +
      (m.rating ? ` <small style="color:#888;font-size:.75rem">(${starsText(m.rating)})</small>` : '');
    document.getElementById('modal-year').textContent       = m.year     || '—';
    document.getElementById('modal-country').textContent    = m.country  || '—';
    document.getElementById('modal-genre').textContent      = m.genre    || '—';
    document.getElementById('modal-director').textContent   = m.director || '—';
    document.getElementById('modal-duration').textContent   = m.duration ? formatDuration(m.duration) + ` (${m.duration} λεπτά)` : '—';

    if (m.cast) {
      document.getElementById('modal-cast').textContent        = m.cast;
      document.getElementById('modal-cast-row').style.display = 'block';
    }

    if (m.synopsis) {
      document.getElementById('modal-synopsis').textContent = m.synopsis;
    }

    if (m.poster_url) {
      const img = document.getElementById('modal-poster');
      img.src = posterSrc(m.poster_url);
      img.style.display = 'block';
      document.getElementById('modal-poster-placeholder').style.display = 'none';
    }

    if (m.athinorama_url) {
      document.getElementById('modal-link').href = m.athinorama_url;
    }

    if (m.trailer_url) {
      document.getElementById('modal-trailer').src = m.trailer_url + '?autoplay=1';
      document.getElementById('modal-trailer-wrap').style.display = 'block';
    }

  } catch (e) {
    document.getElementById('modal-title-gr').textContent = 'Σφάλμα φόρτωσης';
  }
}


// ═══════════════════════════════════════════════════════════════════════════
// Scraper controls
// ═══════════════════════════════════════════════════════════════════════════

document.getElementById('btn-start-scrape').addEventListener('click', async () => {
  const full = document.getElementById('chk-full-rescrape').checked;
  try {
    await fetch('/api/scrape/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ full_rescrape: full }),
    });
    showScrapeRunning();
    startScrapePoller();
  } catch (e) {
    alert('Σφάλμα: ' + e.message);
  }
});

document.getElementById('btn-update-ratings').addEventListener('click', async () => {
  try {
    await fetch('/api/scrape/ratings', { method: 'POST' });
    showScrapeRunning();
    startScrapePoller();
  } catch (e) {
    alert('Σφάλμα: ' + e.message);
  }
});

document.getElementById('btn-pause-scrape').addEventListener('click', async () => {
  const btn = document.getElementById('btn-pause-scrape');
  if (btn.dataset.paused === '1') {
    await fetch('/api/scrape/resume', { method: 'POST' });
    btn.innerHTML = '<i class="bi bi-pause-fill me-1"></i>Παύση';
    btn.dataset.paused = '0';
  } else {
    await fetch('/api/scrape/pause', { method: 'POST' });
    btn.innerHTML = '<i class="bi bi-play-fill me-1"></i>Συνέχεια';
    btn.dataset.paused = '1';
  }
});

document.getElementById('btn-stop-scrape').addEventListener('click', async () => {
  if (!confirm('Να διακοπεί η εξαγωγή;')) return;
  await fetch('/api/scrape/stop', { method: 'POST' });
});

function showScrapeRunning() {
  document.getElementById('scrape-idle').style.display    = 'none';
  document.getElementById('scrape-running').style.display = 'block';
  document.getElementById('scrape-error').style.display   = 'none';
  const btn = document.getElementById('btn-pause-scrape');
  btn.dataset.paused = '0';
  btn.innerHTML = '<i class="bi bi-pause-fill me-1"></i>Παύση';
}

function showScrapeIdle() {
  document.getElementById('scrape-idle').style.display    = 'block';
  document.getElementById('scrape-running').style.display = 'none';
}

function startScrapePoller() {
  if (scrapePoller) return;
  scrapePoller = setInterval(pollScrapeStatus, 1500);
}

function stopScrapePoller() {
  clearInterval(scrapePoller);
  scrapePoller = null;
}

async function pollScrapeStatus() {
  try {
    const res  = await fetch('/api/scrape/status');
    const data = await res.json();

    document.getElementById('scrape-phase').textContent   = data.phase   || '—';
    document.getElementById('scrape-msg').textContent     = data.message || '—';
    document.getElementById('scrape-counts').textContent  =
      `+${data.new_count || 0} νέες, ~${data.updated_count || 0} ενημ.`;

    const pct = data.total > 0 ? Math.round((data.current / data.total) * 100) : 0;
    const bar = document.getElementById('scrape-bar');
    bar.style.width        = pct + '%';
    bar.setAttribute('aria-valuenow', pct);
    document.getElementById('scrape-counter').textContent =
      `${(data.current || 0).toLocaleString('el-GR')} / ${(data.total || 0).toLocaleString('el-GR')}`;

    if (data.error) {
      const errEl = document.getElementById('scrape-error');
      errEl.style.display = 'block';
      errEl.textContent   = 'Σφάλμα: ' + data.error;
    }

    if (!data.running) {
      stopScrapePoller();
      showScrapeIdle();
      // Reload filters and movies after scrape
      await loadFilters();
      await loadMovies();
      await refreshStats();
    }

  } catch (e) {
    console.error('Poll error', e);
  }
}

// Check if a scrape is already running on page load
async function checkScrapeOnLoad() {
  try {
    const res  = await fetch('/api/scrape/status');
    const data = await res.json();
    if (data.running) {
      showScrapeRunning();
      startScrapePoller();
    }
    // Show next scheduled scrape time
    const infoEl = document.getElementById('schedule-info');
    if (infoEl) {
      if (data.next_scheduled) {
        infoEl.textContent = `Επόμενη αυτόματη ενημέρωση: ${data.next_scheduled}`;
      }
    }
  } catch (_) {}
}


// ═══════════════════════════════════════════════════════════════════════════
// Filter event listeners
// ═══════════════════════════════════════════════════════════════════════════

function onFilterChange() {
  state.filters = collectFilters();
  state.page = 1;
  debounce(loadMovies, 350);
}

['f-title', 'f-year-from', 'f-year-to', 'f-rating-min', 'f-rating-max',
 'f-dur-min', 'f-dur-max'].forEach(id => {
  document.getElementById(id).addEventListener('input', onFilterChange);
});

document.getElementById('sort-by').addEventListener('change', e => {
  state.sortBy = e.target.value; state.page = 1; loadMovies();
});
document.getElementById('sort-dir').addEventListener('change', e => {
  state.sortDir = e.target.value; state.page = 1; loadMovies();
});
document.getElementById('per-page').addEventListener('change', e => {
  state.perPage = parseInt(e.target.value); state.page = 1; loadMovies();
});

document.getElementById('btn-random').addEventListener('click', async () => {
  const btn = document.getElementById('btn-random');
  btn.disabled = true;
  const qs = buildQueryString(state.filters);
  try {
    const res = await fetch(`/api/movies/random?${qs}`);
    if (!res.ok) return;
    const data = await res.json();
    if (data.slug) openDetail(data.slug);
  } finally {
    btn.disabled = false;
  }
});

document.getElementById('btn-reset').addEventListener('click', () => {
  document.getElementById('f-title').value      = '';
  document.getElementById('f-year-from').value  = '';
  document.getElementById('f-year-to').value    = '';
  document.getElementById('f-rating-min').value = '';
  document.getElementById('f-rating-max').value = '';
  document.getElementById('f-dur-min').value    = '';
  document.getElementById('f-dur-max').value    = '';
  if (tsCountry) tsCountry.clear();
  if (tsGenre)   tsGenre.clear();
  state.filters = {};
  state.page = 1;
  loadMovies();
});


// ═══════════════════════════════════════════════════════════════════════════
// Initialise
// ═══════════════════════════════════════════════════════════════════════════

(async () => {
  await refreshStats();
  await loadFilters();
  await loadMovies();
  await checkScrapeOnLoad();
})();
