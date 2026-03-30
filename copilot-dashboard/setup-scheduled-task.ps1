# Copilot CLI Dashboard - Setup Script
# Creates a Windows Scheduled Task to refresh dashboard data daily at 9:00 AM

$DashboardDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonExe = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $PythonExe) { Write-Error "Python not found"; exit 1 }

$TaskName = "CopilotDashboard-DataRefresh"
$Action = New-ScheduledTaskAction -Execute $PythonExe -Argument "`"$DashboardDir\collect_data.py`"" -WorkingDirectory $DashboardDir
$Trigger = New-ScheduledTaskTrigger -Daily -At "09:00"
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable

# Remove existing if present
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Description "Refreshes Copilot CLI usage dashboard data from session-store.db"

Write-Host "`nScheduled task '$TaskName' created." -ForegroundColor Green
Write-Host "Data will refresh daily at 9:00 AM." -ForegroundColor Cyan
Write-Host "Dashboard location: $DashboardDir\dashboard.html" -ForegroundColor Cyan
Write-Host "`nTo run manually: python `"$DashboardDir\collect_data.py`"" -ForegroundColor Yellow
