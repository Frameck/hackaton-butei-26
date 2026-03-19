# Smart Health Data Mapping

Hackathon prototype for spotting data quality issues across healthcare datasets, comparing faulty mock data against cleaner reference data, and reviewing the results in a local dashboard.

The repair/export flow now writes to a local SQLite database, so you can run the full stack without provisioning SQL Server.

## Current Repository Layout

```text
backend/            Flask API
frontend/           React + Vite dashboard
ml/                 training, prediction, and saved model files
mock_databases/     faulty datasets used by the dashboard
train_datasets/     cleaner reference datasets
pdf_to_db.py        PDF parsing helper
start.sh            start backend + frontend on macOS/Linux
start.bat           start backend + frontend on Windows
```

## What The Project Does

- Loads CSV, XLSX, and PDF healthcare exports from `mock_databases/`
- Uses `train_datasets/` as the cleaner reference set for comparison
- Detects missing values, malformed identifiers, suspicious rows, and mismatches
- Parses nursing PDF reports into structured data
- Exposes results through a Flask API
- Shows issues and repair workflows in a React dashboard
- Exports repaired datasets into local SQLite tables under `databases/`
- Includes ML utilities and a trained model under `ml/`

## Run Locally

Backend:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
python backend/app.py
```

Frontend, in a second terminal:

```bash
cd frontend
npm install
npm run dev
```

Then open `http://localhost:5173`.

SQLite export:

- Open the SQLite button in the lower-left of the dashboard
- Enter a database file such as `dashboard.sqlite`
- Relative names are created inside `databases/`
- Each dataset export replaces a table named after the dataset file

## One-Command Startup

On macOS/Linux:

```bash
chmod +x start.sh
./start.sh
```

On Windows:

```bat
start.bat
```

## Environment Variables

If you want AI-assisted features, create `backend/.env` from `backend/.env.example` and set:

```env
ANTHROPIC_API_KEY=your_api_key_here
```

If no API key is provided, the rest of the dashboard can still run locally.

## Notes

- `frontend/dist/`, `frontend/node_modules/`, runtime logs, caches, and generated database files are intentionally ignored.
- The repository has been cleaned for publishing and now keeps only the dashboard app, ML code, and datasets needed to run it.
