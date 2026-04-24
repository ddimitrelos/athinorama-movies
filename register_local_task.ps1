# Registers the weekly local-scrape scheduled task.
# Runs scrape_local.py every Thursday at noon local time.
# Power-conditions relaxed to avoid the silent battery-pause failure
# that broke the previous AthinoramaAutoScrapeAndPush task.

$scriptDir = 'C:\Users\dimitrios.dimitrelos\OneDrive - Accenture\Documents\AI Tests\Movie App'
$python    = Join-Path $scriptDir 'python\python.exe'
$script    = Join-Path $scriptDir 'scrape_local.py'

$action = New-ScheduledTaskAction `
    -Execute $python `
    -Argument "`"$script`"" `
    -WorkingDirectory $scriptDir

$trigger = New-ScheduledTaskTrigger `
    -Weekly -DaysOfWeek Thursday -At 12:00

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1)

Register-ScheduledTask `
    -TaskName 'AthinoramaScrapeLocal' `
    -Trigger $trigger `
    -Action $action `
    -Settings $settings `
    -Description 'Weekly local-only Athinorama scrape. Updates local movies.db without commit/push (production handled by GitHub Actions).' `
    -Force
