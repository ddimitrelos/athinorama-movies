@echo off
echo ========================================
echo   Syncing movies.db to GitHub/Railway
echo ========================================

set PYTHON="C:\Users\dimitrios.dimitrelos\OneDrive - Accenture\Documents\AI Tests\Movie App\python\python.exe"
set APP_DIR="C:\Users\dimitrios.dimitrelos\OneDrive - Accenture\Documents\AI Tests\Movie App"

cd /d %APP_DIR%

echo.
echo [1/3] Checkpointing WAL into movies.db...
%PYTHON% -c "import sqlite3; conn = sqlite3.connect('movies.db'); conn.execute('PRAGMA wal_checkpoint(TRUNCATE)'); conn.close(); print('  Done.')"

echo.
echo [2/3] Committing to git...
git add movies.db
git commit -m "DB sync %date% %time:~0,8%"

echo.
echo [3/3] Pushing to GitHub (Railway will auto-redeploy)...
git push origin main

echo.
echo ========================================
echo   Done! Railway redeploys in ~2 minutes
echo   Check: https://railway.app/dashboard
echo ========================================
pause
