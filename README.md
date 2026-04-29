# Stock DW ETL + LOC Dashboard

This project builds a MySQL stock data warehouse using an XML-driven ETL pipeline and provides a Flask dashboard to visualize LOC cuboid metrics.

## Features
- XML-driven configuration (`pipeline_config.xml`, `schema_config.xml`, `window_config.xml`, `lattice_queries.xml`)
- Star-schema ETL into MySQL (`fact_stock_bar`, dimensions)
- Automatic LOC table materialization from SQL
- Flask frontend with dynamic filters by hierarchy level
- Live charting for `volume_avg` and `sma_20_avg`

## Project Structure
- `etl_pipeline.py` — main ETL runner
- `frontend_app.py` — Flask app + APIs for chart/filter data
- `config_reader.py` — typed XML config loaders
- `templates/index.html` — dashboard UI
- `data/stocks_ohlcv.csv` — input market bars
- `data/dim_stock_master.csv` — stock master enrichment

## Prerequisites
- Python 3.10+
- MySQL running on `localhost:3306`
- MySQL user with DB create/write privileges

## Quick Setup
1. Create and activate virtual environment
2. Install dependencies
3. Set DB password environment variable

### Linux/macOS
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install mysql-connector-python pandas numpy flask
export STOCK_DW_PASSWORD=your_mysql_password
```

### Windows (PowerShell)
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install mysql-connector-python pandas numpy flask
$env:STOCK_DW_PASSWORD="your_mysql_password"
```

## Run ETL
```bash
python etl_pipeline.py
```

This will:
- create/use database `stock_dw`
- create dimension/fact tables
- load CSV data and compute indicators
- materialize LOC tables

## Run Frontend
```bash
# optional
export FRONTEND_PORT=5050
python frontend_app.py
```

Open: http://127.0.0.1:5050

## Git
`.gitignore` is included for Python, virtual env, cache/log/temp files, and IDE artifacts.

## Notes
- Keep secrets in environment variables only.
- To customize DB, file paths, or indicators, update XML config files (no code change needed).
