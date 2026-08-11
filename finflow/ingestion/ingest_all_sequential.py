"""Sequential ingestion for the PaySim dataset."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd
import requests
from dotenv import load_dotenv
from fredapi import Fred
from requests.adapters import HTTPAdapter, Retry

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Allow imports from the project root
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Project configuration and logging
from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig


logger = get_logger(__name__)

PAYSIM_COLUMN_RENAMES = {
    "step": "step",
    "type": "type",
    "amount": "amount",
    "nameOrig": "name_orig",
    "oldbalanceOrg": "old_balance_org",
    "newbalanceOrig": "new_balance_org",
    "nameDest": "name_dest",
    "oldbalanceDest": "old_balance_dest",
    "newbalanceDest": "new_balance_dest",
    "isFraud": "is_fraud",
    "isFlaggedFraud": "is_flagged_fraud",
}

REQUIRED_RAW_COLUMNS = list(PAYSIM_COLUMN_RENAMES.keys())

CONTROL_CHAR_PATTERN = r"[\x00-\x1f\x7f]"




class IngestionError(Exception):
    """Raised when an ingestion process fails."""

def ingest_paysim() -> Path:
    """Load, validate, clean, and store the PaySim dataset."""

    start = time.perf_counter()
    logger.info("Starting PaySim ingestion")

    try:
        config = PipelineConfig()

        raw_dir = PROJECT_ROOT / config.raw_dir
        processed_dir = PROJECT_ROOT / config.processed_dir

        source_path = raw_dir / "paysim.csv"
        output_path = processed_dir / "transactions.parquet"

        if not source_path.is_file():
            raise FileNotFoundError(
                f"PaySim file not found: {source_path}"
            )

        logger.info(
            "Reading PaySim CSV from %s",
            source_path,
        )

        dtype = {
            "step": "Int64",
            "type": "string",
            "amount": "Float64",
            "nameOrig": "string",
            "oldbalanceOrg": "Float64",
            "newbalanceOrig": "Float64",
            "nameDest": "string",
            "oldbalanceDest": "Float64",
            "newbalanceDest": "Float64",
            "isFraud": "Int64",
            "isFlaggedFraud": "Int64",
        }

        transactions = pd.read_csv(
            source_path,
            dtype=dtype,
            low_memory=False,
        )

        # Clean only the string columns.
        for column in ["type", "nameOrig", "nameDest"]:
            transactions[column] = (
                transactions[column]
                .str.replace(
                    CONTROL_CHAR_PATTERN,
                    "",
                    regex=True,
                )
                .str.strip()
            )

        # Remove completely empty rows.
        transactions = transactions.dropna(how="all")

        # Validate required columns.
        missing_columns = [
            column
            for column in REQUIRED_RAW_COLUMNS
            if column not in transactions.columns
        ]

        if missing_columns:
            raise ValueError(
                "Missing required columns: "
                + ", ".join(missing_columns)
            )

        # Rename PaySim columns to snake_case.
        transactions = transactions.rename(
            columns=PAYSIM_COLUMN_RENAMES
        )

        logger.info(
            "PaySim row count: %s",
            len(transactions),
        )

        logger.info(
            "Writing %s rows to %s",
            len(transactions),
            output_path,
        )

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        connection = duckdb.connect(
            database=":memory:"
        )

        try:
            connection.register(
                "transactions_df",
                transactions,
            )

            sql_path = str(
                output_path
            ).replace("'", "''")

            connection.execute(
                f"""
                COPY transactions_df
                TO '{sql_path}'
                (FORMAT PARQUET)
                """
            )

        finally:
            connection.close()

        elapsed = time.perf_counter() - start

        logger.info(
            "PaySim ingestion completed in %.2f seconds",
            elapsed,
        )

        return output_path

    except Exception as exc:
        logger.error(
            "PaySim ingestion failed: %s",
            exc,
        )
        raise IngestionError(
            "PaySim ingestion failed"
        ) from exc
    

def ingest_fred() -> Path:
    """Fetch FRED economic indicators and save them as CSV files."""

    start_time = time.perf_counter()

    logger.info("Starting FRED ingestion")

    try:
        load_dotenv()

        raw_macro_dir = (
            PROJECT_ROOT
            / "data"
            / "raw"
            / "macro"
        )

        raw_macro_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        api_key = os.getenv("FRED_API_KEY")

        if not api_key:
            raise ValueError(
                "FRED_API_KEY is not set."
            )

        fred = Fred(
            api_key=api_key
        )

        cpi = fred.get_series(
            "CPIAUCSL"
        )

        unemployment = fred.get_series(
            "UNRATE"
        )

        fx_rate = fred.get_series(
            "DEXUSEU"
        )

        cpi.to_csv(
            raw_macro_dir / "CPIAUCSL.csv"
        )

        unemployment.to_csv(
            raw_macro_dir / "UNRATE.csv"
        )

        fx_rate.to_csv(
            raw_macro_dir / "DEXUSEU.csv"
        )

        logger.info(
            "FRED CPIAUCSL rows: %s",
            len(cpi),
        )

        logger.info(
            "FRED UNRATE rows: %s",
            len(unemployment),
        )

        logger.info(
            "FRED DEXUSEU rows: %s",
            len(fx_rate),
        )

        logger.info(
            "FRED total rows: %s",
            len(cpi) + len(unemployment) + len(fx_rate),
        )

        logger.info(
            "FRED data saved to %s",
            raw_macro_dir,
        )

        elapsed = (
            time.perf_counter()
            - start_time
        )

        logger.info(
            "FRED ingestion completed in %.2f seconds",
            elapsed,
        )

        return raw_macro_dir

    except Exception as exc:
        logger.error(
            "FRED ingestion failed: %s",
            exc,
        )

        raise IngestionError(
            "FRED ingestion failed"
        ) from exc


def _write_parquet(
    df: pd.DataFrame,
    output_path: Path,
) -> None:
    """Write a DataFrame to Parquet through DuckDB."""

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    connection = duckdb.connect(
        database=":memory:"
    )

    try:
        connection.register(
            "frame_to_write",
            df,
        )

        sql_path = (
            output_path
            .as_posix()
            .replace("'", "''")
        )

        connection.execute(
            f"COPY frame_to_write "
            f"TO '{sql_path}' "
            f"(FORMAT PARQUET)"
        )

    finally:
        connection.close()


def ingest_complaints() -> Path:
    """Fetch CFPB complaints data and save it as a Parquet file."""

    start_time = time.perf_counter()

    logger.info(
        "Starting CFPB complaints ingestion"
    )

    config = PipelineConfig()

    raw_dir = PROJECT_ROOT / config.raw_dir
    processed_dir = PROJECT_ROOT / config.processed_dir

    source_path = raw_dir / "complaints.csv"
    output_path = processed_dir / "complaints.parquet"

    if not source_path.is_file():
        raise FileNotFoundError(
            f"Complaints file not found: {source_path}"
        )

    logger.info(
        "Reading Complaints CSV from %s",
        source_path,
    )

    chunk_size = 100_000
    filtered_chunks = []
    total_rows = 0
    filtered_rows = 0
    chunk_number = 0

    for complaints in pd.read_csv(
        source_path,
        low_memory=False,
        chunksize=chunk_size,
    ):
        chunk_number += 1
        total_rows += len(complaints)

        complaints = complaints[
            complaints["Product"].isin(
                [
                    "Checking or savings account",
                    "Credit card",
                ]
            )
        ]

        filtered_rows += len(complaints)

        filtered_chunks.append(complaints)

        logger.info(
            "Processed chunk %s | Rows read: %s | Rows kept: %s",
            chunk_number,
            total_rows,
            filtered_rows,
        )

    complaints = pd.concat(
        filtered_chunks,
        ignore_index=True,
    )

    logger.info(
        "CFPB complaints row count before filtering: %s",
        total_rows,
    )

    logger.info(
        "CFPB complaints row count after filtering: %s",
        filtered_rows,
    )

    logger.info(
        "Writing %s rows to %s",
        len(complaints),
        output_path,
    )

    _write_parquet(
        complaints,
        output_path,
    )

    elapsed_time = time.perf_counter() - start_time

    logger.info(
        "CFPB complaints ingestion completed in %.2f seconds",
        elapsed_time,
    )

    return output_path


def run_sequential() -> None:
    """Run all ingestion processes sequentially."""

    start_time = time.perf_counter()

    logger.info(
        "Starting sequential ingestion pipeline"
    )

    try:
        paysim_output = ingest_paysim()

        logger.info(
            "PaySim ingestion complete: %s",
            paysim_output,
        )

        fred_output = ingest_fred()

        logger.info(
            "FRED ingestion complete: %s",
            fred_output,
        )


        complaints_output = ingest_complaints()

        logger.info(
            "CFPB complaints ingestion complete: %s",
            complaints_output,
        )

    except Exception:
        logger.error(
            "Sequential ingestion pipeline failed"
        )
        raise

    finally:
        elapsed = (
            time.perf_counter()
            - start_time
        )

        logger.info(
            "Sequential ingestion pipeline completed in %.2f seconds",
            elapsed,
        )



def main() -> None:
    """Run the sequential ingestion pipeline."""

    run_sequential()


if __name__ == "__main__":
    main()