@echo off
REM ==============================================
REM RS Analytics - Upload to VPS Script
REM ==============================================
REM This script uploads your project to the VPS
REM Run from: c:\Users\lowai\OneDrive\Desktop\RS_Analytics\rs_analytics\
REM ==============================================

echo ==========================================
echo RS Analytics - Upload to VPS
echo ==========================================
echo.

REM VPS Details
set VPS_IP=103.14.79.195
set VPS_USER=root
set VPS_DEST=/home/rsanalytics/rs_analytics

echo VPS IP: %VPS_IP%
echo Destination: %VPS_DEST%
echo.

echo [Step 1] This script requires SCP (comes with Git for Windows or OpenSSH)
echo.

echo [Step 2] Uploading project files...
echo Command to run manually in PowerShell or Git Bash:
echo.
echo     scp -r app etl scripts scheduler analysis data logs secrets requirements.txt .env.example .gitignore deploy %VPS_USER%@%VPS_IP%:%VPS_DEST%/
echo.
echo Or use WinSCP/FileZilla with these details:
echo     Host: %VPS_IP%
echo     User: %VPS_USER%
echo     Password: (from your VPS provider)
echo     Remote path: %VPS_DEST%
echo.

pause
