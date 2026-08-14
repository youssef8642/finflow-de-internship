"""Sequential ingestion for the three FinFlow source datasets.

Ingestion is deliberately thin. Each function does four things and no more:
read the source, check the columns are the ones we expect, rename them to
snake_case, and write Parquet. Type coercion and derived columns belong to
the transformation stage (transform_parallel.py), not here.
"""

from __future__ import annotations

import time
from pathlib import Path

import duckdb
import pandas as pd
from fredapi import Fred

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig


logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG = PipelineConfig()

RAW_DIR = PROJECT_ROOT / CONFIG.raw_dir
PROCESSED_DIR = PROJECT_ROOT / CONFIG.processed_dir

# PaySim ships camelCase headers. Only the columns that actually change are
# listed -- step, type and amount are already snake_case.
PAYSIM_COLUMN_RENAMES = {
    "nameOrig": "name_orig",
    "oldbalanceOrg": "old_balance_org",
    "newbalanceOrig": "new_balance_org",
    "nameDest": "name_dest",
    "oldbalanceDest": "old_balance_dest",
    "newbalanceDest": "new_balance_dest",
    "isFraud": "is_fraud",
    "isFlaggedFraud": "is_flagged_fraud",
}

# Plain numpy dtypes on purpose. pandas' nullable Int64/Float64 parse several
# times slower and PaySim has no missing values for them to represent.
PAYSIM_DTYPES = {
    "step": "int64",
    "type": "str",
    "amount": "float64",
    "nameOrig": "str",
    "oldbalanceOrg": "float64",
    "newbalanceOrig": "float64",
    "nameDest": "str",
    "oldbalanceDest": "float64",
    "newbalanceDest": "float64",
    "isFraud": "int64",
    "isFlaggedFraud": "int64",
}

FRED_SERIES = ("CPIAUCSL", "UNRATE", "DEXUSEU")

# The CFPB export is ~9 GB and 16 columns wide. We only model 8 of them, so
# reading just those keeps the whole file out of memory.
COMPLAINT_COLUMNS = [
    "Complaint ID",
    "Date received",
    "Product",
    "Sub-product",
    "Issue",
    "Company",
    "State",
    "Company response to consumer",
]

COMPLAINT_PRODUCTS = ["Checking or savings account", "Credit card"]

COMPLAINT_CHUNK_SIZE = 100_000


class IngestionError(Exception):
    """Raised when an ingestion process fails."""


def _write_parquet(df: pd.DataFrame, output_path: Path) -> None:
    """Write a DataFrame to Parquet through DuckDB."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # as_posix() matters on Windows: backslashes inside a single-quoted SQL
    # string get read as escape sequences.
    sql_path = output_path.as_posix().replace("'", "''")

    with duckdb.connect(database=":memory:") as connection:
        connection.execute("PRAGMA enable_progress_bar=false")
        connection.register("frame_to_write", df)
        connection.execute(f"COPY frame_to_write TO '{sql_path}' (FORMAT PARQUET)")


def _validate_paysim(df: pd.DataFrame) -> None:
    """Check the raw PaySim frame has the expected columns and dtypes."""
    missing = [column for column in PAYSIM_DTYPES if column not in df.columns]
    if missing:
        raise ValueError("Missing required columns: " + ", ".join(missing))

    # "str" reads back as a string dtype whose name varies by pandas version,
    # so compare the numeric columns only.
    for column, expected in PAYSIM_DTYPES.items():
        if expected == "str":
            continue
        actual = str(df[column].dtype)
        if actual != expected:
            raise ValueError(
                f"Column {column} has dtype {actual}, expected {expected}"
            )


def ingest_paysim() -> Path:
    """Read the PaySim CSV, rename its columns, and save it as Parquet."""
    start = time.perf_counter()
    logger.info("Starting PaySim ingestion")

    try:
        source_path = RAW_DIR / "paysim.csv"
        output_path = PROCESSED_DIR / "transactions.parquet"

        if not source_path.is_file():
            raise FileNotFoundError(f"PaySim file not found: {source_path}")

        logger.info("Reading PaySim CSV from %s", source_path)
        transactions = pd.read_csv(source_path, dtype=PAYSIM_DTYPES)

        _validate_paysim(transactions)
        transactions = transactions.rename(columns=PAYSIM_COLUMN_RENAMES)

        logger.info("PaySim row count: %s", len(transactions))
        logger.info("Writing %s rows to %s", len(transactions), output_path)
        _write_parquet(transactions, output_path)

        elapsed = time.perf_counter() - start
        logger.info("PaySim ingestion completed in %.2f seconds", elapsed)
        return output_path

    except Exception as exc:
        logger.error("PaySim ingestion failed: %s", exc)
        raise IngestionError("PaySim ingestion failed") from exc


def ingest_fred() -> Path:
    """Fetch the FRED macro indicators and save each series as a CSV."""
    start = time.perf_counter()
    logger.info("Starting FRED ingestion")

    try:
        api_key = CONFIG.fred_api_key
        if not api_key:
            raise ValueError("fred_api_key is not set in PipelineConfig.")

        macro_dir = RAW_DIR / "macro"
        macro_dir.mkdir(parents=True, exist_ok=True)

        fred = Fred(api_key=api_key)
        total_rows = 0

        for series_id in FRED_SERIES:
            series = fred.get_series(series_id)
            series.to_csv(macro_dir / f"{series_id}.csv")

            total_rows += len(series)
            logger.info("FRED %s rows: %s", series_id, len(series))

        logger.info("FRED total rows: %s", total_rows)
        logger.info("FRED data saved to %s", macro_dir)

        elapsed = time.perf_counter() - start
        logger.info("FRED ingestion completed in %.2f seconds", elapsed)
        return macro_dir

    except Exception as exc:
        logger.error("FRED ingestion failed: %s", exc)
        raise IngestionError("FRED ingestion failed") from exc


def ingest_complaints() -> Path:
    """Filter the CFPB complaints export down to two products and save it."""
    start = time.perf_counter()
    logger.info("Starting CFPB complaints ingestion")

    try:
        source_path = RAW_DIR / "complaints.csv"
        output_path = PROCESSED_DIR / "complaints.parquet"

        if not source_path.is_file():
            raise FileNotFoundError(f"Complaints file not found: {source_path}")

        logger.info("Reading complaints CSV from %s", source_path)

        kept_chunks = []
        total_rows = 0
        kept_rows = 0

        reader = pd.read_csv(
            source_path,
            usecols=COMPLAINT_COLUMNS,
            chunksize=COMPLAINT_CHUNK_SIZE,
            low_memory=False,
        )

        for chunk_number, chunk in enumerate(reader, start=1):
            total_rows += len(chunk)

            chunk = chunk[chunk["Product"].isin(COMPLAINT_PRODUCTS)]
            kept_rows += len(chunk)
            kept_chunks.append(chunk)

            if chunk_number % 10 == 0:
                logger.info(
                    "Chunk %s | rows read: %s | rows kept: %s",
                    chunk_number,
                    total_rows,
                    kept_rows,
                )

        complaints = pd.concat(kept_chunks, ignore_index=True)

        logger.info("Complaint rows before filtering: %s", total_rows)
        logger.info("Complaint rows after filtering: %s", kept_rows)
        logger.info("Writing %s rows to %s", len(complaints), output_path)
        _write_parquet(complaints, output_path)

        elapsed = time.perf_counter() - start
        logger.info("CFPB complaints ingestion completed in %.2f seconds", elapsed)
        return output_path

    except Exception as exc:
        logger.error("CFPB complaints ingestion failed: %s", exc)
        raise IngestionError("CFPB complaints ingestion failed") from exc


def run_sequential() -> float:
    """Run all three ingestion functions in order and return the elapsed time."""
    start = time.perf_counter()
    logger.info("Starting sequential ingestion pipeline")

    try:
        logger.info("PaySim ingestion complete: %s", ingest_paysim())
        logger.info("FRED ingestion complete: %s", ingest_fred())
        logger.info("CFPB complaints ingestion complete: %s", ingest_complaints())

    except IngestionError:
        logger.error("Sequential ingestion pipeline failed")
        raise

    elapsed = time.perf_counter() - start
    logger.info("Sequential ingestion pipeline completed in %.2f seconds", elapsed)
    return elapsed


def main() -> None:
    """Run the sequential ingestion pipeline."""
    run_sequential()


if __name__ == "__main__":
    main()
