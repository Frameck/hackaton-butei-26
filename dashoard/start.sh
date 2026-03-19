#!/usr/bin/env bash
# Start Data Quality Dashboard (Unix/Mac)
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Starting Data Quality Dashboard..."
echo ""

# Use venv python if available
VENV_PYTHON="$SCRIPT_DIR/hack_env/Scripts/python.exe"
if [ -f "$VENV_PYTHON" ]; then
    PYTHON="$VENV_PYTHON"
elif command -v python3 &>/dev/null; then
    PYTHON="python3"
else
    PYTHON="python"
fi

# Start backend
echo "[1/2] Starting Flask backend on http://localhost:5000 ..."
cd "$SCRIPT_DIR/backend"
"$PYTHON" app.py &
BACKEND_PID=$!
echo "Backend PID: $BACKEND_PID"

# Wait for backend
sleep 2

# Start frontend
echo "[2/2] Starting Vite frontend on http://localhost:5173 ..."
cd "$SCRIPT_DIR/frontend"
npm run dev &
FRONTEND_PID=$!
echo "Frontend PID: $FRONTEND_PID"

echo ""
echo "Dashboard : http://localhost:5173"
echo "Backend   : http://localhost:5000"
echo ""
echo "Press Ctrl+C to stop both servers."

# Wait for either process to exit
wait $BACKEND_PID $FRONTEND_PID
