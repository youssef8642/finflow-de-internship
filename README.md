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
- The project is still a scaffold, so several package folders are intentionally empty.