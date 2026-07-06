#requires -Version 5.1
<#
Pull the latest movies.db (and code) from production into the local working copy.

Production data flow:
  GitHub Actions (Thursday cron) -> scrape -> commit movies.db -> push to GitHub
                                                              -> upload to PythonAnywhere

So "sync from prod" == fast-forward pull from origin/master. This script does that
safely: it backs up the current DB, WAL-checkpoints any pending writes, and refuses
to clobber un-pushed local changes.
#>

$ErrorActionPreference = 'Stop'
Set-Location -Path $PSScriptRoot

$python = Join-Path $PSScriptRoot 'python\python.exe'
if (-not (Test-Path $python)) { $python = 'python' }

Write-Host '== Sync movies.db from production ==' -ForegroundColor Cyan

# Refuse if tracked files are dirty (untracked files are fine).
$dirty = git status --porcelain | Where-Object { $_ -notmatch '^\?\?' }
if ($dirty) {
    Write-Host 'Tracked files are modified locally — refusing to pull. Resolve first:' -ForegroundColor Red
    $dirty | ForEach-Object { Write-Host "  $_" }
    exit 1
}

Write-Host '[1/4] Fetching origin/master ...'
git fetch origin master | Out-Null

$behind = (git rev-list --count HEAD..origin/master).Trim()
if ($behind -eq '0') {
    Write-Host 'Already up to date with origin/master.' -ForegroundColor Green
    exit 0
}
Write-Host "Local is $behind commit(s) behind origin/master."

Write-Host '[2/4] Checkpointing local WAL and backing up movies.db ...'
& $python -c "import sqlite3; c = sqlite3.connect('movies.db'); c.execute('PRAGMA wal_checkpoint(TRUNCATE)'); c.close()"
$ts = Get-Date -Format 'yyyyMMdd-HHmmss'
Copy-Item movies.db "movies.db.preSync-$ts"

Write-Host '[3/4] Fast-forward pulling ...'
git pull --ff-only origin master

Write-Host '[4/4] Verifying ...'
& $python -c @"
import sqlite3
c = sqlite3.connect('movies.db'); cur = c.cursor()
cur.execute('SELECT COUNT(*) FROM movies'); total = cur.fetchone()[0]
cur.execute('SELECT COUNT(*) FROM movies WHERE detail_scraped=1'); scraped = cur.fetchone()[0]
cur.execute(""SELECT slug, year FROM movies WHERE detail_scraped=1 ORDER BY CAST(SUBSTR(slug, INSTR(slug,'-10')+1) AS INTEGER) DESC LIMIT 1"")
newest = cur.fetchone()
print(f'total={total}  detail_scraped={scraped}  newest={newest[0]} ({newest[1]})')
"@

Write-Host 'Done.' -ForegroundColor Green
