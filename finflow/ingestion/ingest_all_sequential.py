"""Sequential ingestion for the PaySim, FRED, and CFPB datasets."""

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


# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Allow imports from the project root
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# Project configuration and logging
from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig


logger = get_logger(__name__)


class IngestionError(Exception):
    """Raised when an ingestion process fails."""


# PaySim column definitions
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

INTEGER_COLUMNS = [
    "step",
    "isFraud",
    "isFlaggedFraud",
]

FLOAT_COLUMNS = [
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
]

STRING_COLUMNS = [
    "type",
    "nameOrig",
    "nameDest",
]

CONTROL_CHAR_PATTERN = r"[\x00-\x1f\x7f]"


def _resolve_source_path(raw_dir: Path, filename: str) -> Path:
    """Return the full path to a source file in the raw data directory."""
    source_path = raw_dir / filename

    if not source_path.is_file():
        raise FileNotFoundError(
            f"Source file not found: {source_path}"
        )

    return source_path


def _validate_columns(
    df: pd.DataFrame,
    required_columns: list[str],
) -> None:
    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Missing required columns: " + ", ".join(missing_columns)
        )


def _normalize_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Convert empty strings and whitespace-only fields to null values."""
    return df.replace(
        r"^\s*$",
        pd.NA,
        regex=True,
    )


def _normalize_control_characters(df: pd.DataFrame) -> pd.DataFrame:
    """Remove embedded control characters that commonly appear in CSV exports."""
    normalized = df.copy()

    for column in normalized.columns:
        normalized[column] = (
            normalized[column]
            .astype("string")
            .str.replace(
                CONTROL_CHAR_PATTERN,
                "",
                regex=True,
            )
            .str.strip()
        )

    return normalized


def _raise_invalid_numeric_values(
    column: str,
    invalid_values: pd.Series,
) -> None:
    sample_values = invalid_values.head(5).tolist()

    raise ValueError(
        f"Column {column!r} contains invalid numeric values: "
        f"{sample_values}"
    )


def _validate_and_coerce_types(
    df: pd.DataFrame,
    integer_columns: list[str],
    float_columns: list[str],
    string_columns: list[str],
) -> pd.DataFrame:
    validated = df.copy()

    for column in integer_columns:
        source = validated[column]
        non_missing_mask = source.notna()
        coerced = pd.to_numeric(
            source,
            errors="coerce",
        )

        invalid_numeric_mask = (
            non_missing_mask
            & coerced.isna()
        )

        if invalid_numeric_mask.any():
            _raise_invalid_numeric_values(
                column,
                source[invalid_numeric_mask],
            )

        fractional_mask = (
            non_missing_mask
            & coerced.notna()
            & (coerced % 1 != 0)
        )

        if fractional_mask.any():
            _raise_invalid_numeric_values(
                column,
                source[fractional_mask],
            )

        validated[column] = (
            coerced
            .round()
            .astype("Int64")
        )

    for column in float_columns:
        source = validated[column]
        non_missing_mask = source.notna()
        coerced = pd.to_numeric(
            source,
            errors="coerce",
        )

        invalid_numeric_mask = (
            non_missing_mask
            & coerced.isna()
        )

        if invalid_numeric_mask.any():
            _raise_invalid_numeric_values(
                column,
                source[invalid_numeric_mask],
            )

        validated[column] = coerced.astype("Float64")

    for column in string_columns:
        validated[column] = (
            validated[column].astype("string")
        )

    return validated


def _write_parquet(
    df: pd.DataFrame,
    output_path: Path,
) -> None:
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
            df,
        )

        sql_path = (
            output_path
            .as_posix()
            .replace("'", "''")
        )

        connection.execute(
            f"COPY transactions_df "
            f"TO '{sql_path}' "
            f"(FORMAT PARQUET)"
        )

    finally:
        connection.close()


def ingest_paysim() -> Path:
    """Load, validate, clean, and store the PaySim dataset."""

    start_time = time.perf_counter()

    logger.info("Starting PaySim ingestion")

    try:
        config = PipelineConfig()

        raw_dir = PROJECT_ROOT / config.raw_dir
        processed_dir = PROJECT_ROOT / config.processed_dir

        source_path = _resolve_source_path(
            raw_dir,
            "paysim.csv",
        )

        output_path = (
            processed_dir
            / "transactions.parquet"
        )

        logger.info(
            "Reading PaySim CSV from %s",
            source_path,
        )

        transactions = pd.read_csv(
            source_path,
            low_memory=False,
        )

        transactions = _normalize_control_characters(
            transactions
        )

        transactions = _normalize_missing_values(
            transactions
        )

        transactions = transactions.dropna(
            how="all"
        )

        _validate_columns(
            transactions,
            REQUIRED_RAW_COLUMNS,
        )

        transactions = _validate_and_coerce_types(
            transactions,
            INTEGER_COLUMNS,
            FLOAT_COLUMNS,
            STRING_COLUMNS,
        )

        transactions = transactions.rename(
            columns=PAYSIM_COLUMN_RENAMES
        )

        row_count = len(transactions)

        logger.info(
            "PaySim row count: %s",
            row_count,
        )

        logger.info(
            "Writing %s rows to %s",
            row_count,
            output_path,
        )

        _write_parquet(
            transactions,
            output_path,
        )

        elapsed = (
            time.perf_counter()
            - start_time
        )

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
    """Fetch FRED indicators and save them as CSV files."""

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

        cpi.to_csv(
            raw_macro_dir
            / "CPIAUCSL.csv"
        )

        unemployment = fred.get_series(
            "UNRATE"
        )

        unemployment.to_csv(
            raw_macro_dir
            / "UNRATE.csv"
        )

        fx_rate = fred.get_series(
            "DEXUSEU"
        )

        fx_rate.to_csv(
            raw_macro_dir
            / "DEXUSEU.csv"
        )

        total_rows = (
            len(cpi)
            + len(unemployment)
            + len(fx_rate)
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
            total_rows,
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


def ingest_complaints() -> Path:
    """Download and save a sample of CFPB complaints."""

    start_time = time.perf_counter()

    logger.info(
        "Starting CFPB complaints ingestion"
    )

    try:
        products = [
            "Credit card",
            "Checking or savings account",
        ]

        api_url = (
            "https://www.consumerfinance.gov/"
            "data-research/consumer-complaints/"
            "search/api/v1/"
        )

        batch_size = 1000
        max_complaints = 20_000

        output_path = (
            PROJECT_ROOT
            / "data"
            / "processed"
            / "complaints.parquet"
        )

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        all_complaints = []
        offset = 0

        while len(all_complaints) < max_complaints:

            params = {
                "size": batch_size,
                "from": offset,
                "product": products,
            }

            response = requests.get(
                api_url,
                params=params,
                timeout=180,
            )

            response.raise_for_status()

            data = response.json()
            hits = data["hits"]["hits"]

            if not hits:
                break

            remaining = (
                max_complaints
                - len(all_complaints)
            )

            for complaint in hits[:remaining]:
                all_complaints.append(
                    complaint["_source"]
                )

            offset += len(hits)

            logger.info(
                "Downloaded %s CFPB complaints",
                len(all_complaints),
            )

        complaints_df = pd.DataFrame(
            all_complaints
        )

        complaints_df = complaints_df[
            complaints_df["product"].isin(products)
        ]

        row_count = len(complaints_df)

        logger.info(
            "CFPB complaint row count: %s",
            row_count,
        )

        logger.info(
            "Writing %s CFPB complaints to %s",
            row_count,
            output_path,
        )

        _write_parquet(
            complaints_df,
            output_path,
        )

        elapsed = (
            time.perf_counter()
            - start_time
        )

        logger.info(
            "CFPB complaints ingestion "
            "completed in %.2f seconds",
            elapsed,
        )

        return output_path

    except Exception as exc:
        logger.error(
            "CFPB complaints ingestion failed: %s",
            exc,
        )

        raise IngestionError(
            "CFPB complaints ingestion failed"
        ) from exc


def run_sequential() -> None:
    """Run all three ingestion functions sequentially."""

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

    except IngestionError:
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
            "Sequential ingestion pipeline "
            "completed in %.2f seconds",
            elapsed,
        )


def main() -> None:
    """Run the sequential ingestion pipeline."""
    run_sequential()


if __name__ == "__main__":
    main()