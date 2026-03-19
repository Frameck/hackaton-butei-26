@echo off
echo Starting Data Quality Dashboard...
echo.

:: Detect python: prefer conda flask env, then local venv, then system python
set PYTHON=C:\Users\Federico\miniconda3\envs\flask\python.exe
if not exist "%PYTHON%" set PYTHON=%~dp0hack_env\Scripts\python.exe
if not exist "%PYTHON%" set PYTHON=python

:: Start backend
echo [1/2] Starting Flask backend on http://localhost:5000 ...
start "DQ Backend" cmd /k "cd /d %~dp0backend && set FLASK_APP=app.py && set FLASK_DEBUG=1 && "%PYTHON%" -m flask run --port 5000 --reload"

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
