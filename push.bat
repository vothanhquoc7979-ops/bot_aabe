@echo off
echo ===== AUTO PUSH GITHUB =====

cd /d %~dp0

git add .
git commit -m "auto update code"
git branch -M main
git push origin main

echo.
echo ===== DONE =====
pause