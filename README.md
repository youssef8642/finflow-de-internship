# Finflow Internship Project

Finflow is a small Python analytics project centered on DuckDB SQL warmups, concurrency benchmarking, and shared configuration/logging.

## Prerequisites

- Python 3.9 or newer
- Git
- PowerShell on Windows, or a terminal that can run Python virtual environments

## Quick Start

Clone the repository, then run the commands below from the project root.

```powershell
git clone <your-repo-url>
cd finflow-de-internship
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python finflow\prereqs\run_sql.py
python finflow\prereqs\concurrency_benchmark.py
```

If PowerShell blocks script activation, run this once in the same session before activating the environment:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

If your system uses the Windows Python launcher, you can replace `python` with `py -3.9`.

## What the Project Does

- `finflow/prereqs/run_sql.py` opens the DuckDB database and runs the SQL warmup queries in `finflow/prereqs/sql_warmup.sql`.
- `finflow/prereqs/concurrency_benchmark.py` compares sequential vs parallel I/O and CPU workloads.
- `finflow/config/logger.py` provides a shared logging setup.
- `finflow/config/settings.py` defines a small pipeline configuration object.

## Project Structure

- `finflow/config/` contains shared settings and logging.
- `finflow/prereqs/` contains runnable utility scripts plus the sample DuckDB database and SQL file.
- `finflow/analysis/`, `finflow/detection/`, `finflow/ingestion/`, `finflow/models/`, `finflow/reports/`, and `finflow/presentation/` are planned feature areas.
- `finflow/pipeline_runner.py` is reserved for the main pipeline orchestrator.

## Repository Notes

- Generated files such as `.venv/`, `__pycache__/`, and local databases are ignored by Git.
- The prereq scripts are meant to be run from the repository root.
- The project is still a scaffold, so several package folders are intentionally empty


env file contents:FRED_API_KEY=cfaf9d1c396769e0e721088fee194288 

(.venv) C:\Users\Youss\finflow-de-internship>py -m finflow.ingestion.ingest_all_sequential
2026-08-05 15:18:17 | INFO | __main__ | Starting sequential ingestion pipeline
2026-08-05 15:18:17 | INFO | __main__ | Starting PaySim ingestion
2026-08-05 15:18:17 | INFO | __main__ | Reading PaySim CSV from C:\Users\Youss\finflow-de-internship\data\raw\paysim.csv
2026-08-05 15:20:41 | INFO | __main__ | PaySim row count: 6362620
2026-08-05 15:20:41 | INFO | __main__ | Writing 6362620 rows to C:\Users\Youss\finflow-de-internship\data\processed\transactions.parquet
2026-08-05 15:20:45 | INFO | __main__ | PaySim ingestion completed in 148.45 seconds
2026-08-05 15:20:46 | INFO | __main__ | PaySim ingestion complete: C:\Users\Youss\finflow-de-internship\data\processed\transactions.parquet
2026-08-05 15:20:46 | INFO | __main__ | Starting FRED ingestion
2026-08-05 15:20:50 | INFO | __main__ | FRED CPIAUCSL rows: 954
2026-08-05 15:20:50 | INFO | __main__ | FRED UNRATE rows: 942
2026-08-05 15:20:50 | INFO | __main__ | FRED DEXUSEU rows: 7195
2026-08-05 15:20:50 | INFO | __main__ | FRED total rows: 9091
2026-08-05 15:20:50 | INFO | __main__ | FRED data saved to C:\Users\Youss\finflow-de-internship\data\raw\macro
2026-08-05 15:20:50 | INFO | __main__ | FRED ingestion completed in 4.54 seconds
2026-08-05 15:20:50 | INFO | __main__ | FRED ingestion complete: C:\Users\Youss\finflow-de-internship\data\raw\macro
2026-08-05 15:20:50 | INFO | __main__ | Starting CFPB complaints ingestion
2026-08-05 15:20:51 | INFO | __main__ | Downloaded 1000 CFPB complaints
2026-08-05 15:20:52 | INFO | __main__ | Downloaded 2000 CFPB complaints
2026-08-05 15:20:53 | INFO | __main__ | Downloaded 3000 CFPB complaints
2026-08-05 15:20:53 | INFO | __main__ | Downloaded 4000 CFPB complaints
2026-08-05 15:20:54 | INFO | __main__ | Downloaded 5000 CFPB complaints
2026-08-05 15:20:56 | INFO | __main__ | Downloaded 6000 CFPB complaints
2026-08-05 15:20:56 | INFO | __main__ | Downloaded 7000 CFPB complaints
2026-08-05 15:20:57 | INFO | __main__ | Downloaded 8000 CFPB complaints
2026-08-05 15:20:57 | INFO | __main__ | Downloaded 9000 CFPB complaints
2026-08-05 15:20:58 | INFO | __main__ | Downloaded 10000 CFPB complaints
2026-08-05 15:20:59 | INFO | __main__ | Downloaded 11000 CFPB complaints
2026-08-05 15:20:59 | INFO | __main__ | Downloaded 12000 CFPB complaints
2026-08-05 15:21:00 | INFO | __main__ | Downloaded 13000 CFPB complaints
2026-08-05 15:21:00 | INFO | __main__ | Downloaded 14000 CFPB complaints
2026-08-05 15:21:01 | INFO | __main__ | Downloaded 15000 CFPB complaints
2026-08-05 15:21:02 | INFO | __main__ | Downloaded 16000 CFPB complaints
2026-08-05 15:21:03 | INFO | __main__ | Downloaded 17000 CFPB complaints
2026-08-05 15:21:04 | INFO | __main__ | Downloaded 18000 CFPB complaints
2026-08-05 15:21:05 | INFO | __main__ | Downloaded 19000 CFPB complaints
2026-08-05 15:21:05 | INFO | __main__ | Downloaded 20000 CFPB complaints
2026-08-05 15:21:05 | INFO | __main__ | CFPB complaint row count: 20000
2026-08-05 15:21:05 | INFO | __main__ | Writing 20000 CFPB complaints to C:\Users\Youss\finflow-de-internship\data\processed\complaints.parquet
2026-08-05 15:21:05 | INFO | __main__ | CFPB complaints ingestion completed in 15.16 seconds
2026-08-05 15:21:05 | INFO | __main__ | CFPB complaints ingestion complete: C:\Users\Youss\finflow-de-internship\data\processed\complaints.parquet
2026-08-05 15:21:05 | INFO | __main__ | Sequential ingestion pipeline completed in 168.48 seconds



CFPB was only conducted on 20,000  records for trial purposes ,not the whole dataset
for future uses,to conduct on full dataset,max limit variable will be removed,and timeout variable will be incremented accordingly