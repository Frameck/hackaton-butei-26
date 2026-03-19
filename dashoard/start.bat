@echo off
echo Starting Data Quality Dashboard...
echo.

:: Detect python: prefer the venv, fall back to system python
set PYTHON=%~dp0hack_env\Scripts\python.exe
if not exist "%PYTHON%" set PYTHON=python

:: Start backend
echo [1/2] Starting Flask backend on http://localhost:5000 ...
start "DQ Backend" cmd /k "cd /d %~dp0backend && "%PYTHON%" app.py"

:: Wait a moment for backend to start
timeout /t 2 /nobreak >nul

:: Start frontend
echo [2/2] Starting Vite frontend on http://localhost:5173 ...
start "DQ Frontend" cmd /k "cd /d %~dp0frontend && npm run dev"

echo.
echo Dashboard will be available at: http://localhost:5173
echo Backend API running at:         http://localhost:5000
echo.
echo Press any key to exit this window (servers will keep running)...
pause >nul
