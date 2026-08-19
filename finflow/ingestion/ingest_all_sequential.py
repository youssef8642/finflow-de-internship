"""Milestone 1.2 - Sequential ingestion (baseline).

Order of this file follows the milestone bullets:
    1. ingest_paysim()
    2. ingest_fred()
    3. ingest_complaints()
    4. run_sequential()
"""

import time

import duckdb
import pandas as pd
from fredapi import Fred

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig

logger = get_logger(__name__)
config = PipelineConfig()

PAYSIM_RENAMES = {
    "nameOrig": "name_orig",
    "oldbalanceOrg": "old_balance_org",
    "newbalanceOrig": "new_balance_org",
    "nameDest": "name_dest",
    "oldbalanceDest": "old_balance_dest",
    "newbalanceDest": "new_balance_dest",
    "isFraud": "is_fraud",
    "isFlaggedFraud": "is_flagged_fraud",
}

PAYSIM_COLUMNS = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
]

FRED_SERIES = ["CPIAUCSL", "UNRATE", "DEXUSEU"]

COMPLAINT_COLUMNS = [
    "Complaint ID", "Date received", "Product", "Sub-product",
    "Issue", "Company", "State", "Company response to consumer",
]

COMPLAINT_PRODUCTS = ["Checking or savings account", "Credit card"]


class IngestionError(Exception):
    """Raised when one of the ingestion steps fails."""


def save_parquet(df: pd.DataFrame, path: str) -> None:
    """Write a DataFrame to Parquet using DuckDB."""
    connection = duckdb.connect()
    connection.register("df", df)
    connection.execute("COPY df TO '" + path + "' (FORMAT PARQUET)")
    connection.close()


def ingest_paysim() -> str:
    """Load the PaySim CSV, rename columns to snake_case, save as Parquet."""
    start = time.perf_counter()
    logger.info("Starting PaySim ingestion")

    source = config.raw_dir + "/paysim.csv"
    target = config.processed_dir + "/transactions.parquet"

    try:
        transactions = pd.read_csv(source)

        for column in PAYSIM_COLUMNS:
            if column not in transactions.columns:
                raise ValueError("Missing column in PaySim CSV: " + column)

        transactions = transactions.rename(columns=PAYSIM_RENAMES)

        logger.info("PaySim rows: %s", len(transactions))
        save_parquet(transactions, target)

        logger.info("PaySim ingestion took %.2f seconds", time.perf_counter() - start)
        return target

    except Exception as error:
        logger.error("PaySim ingestion failed: %s", error)
        raise IngestionError("PaySim ingestion failed") from error


def ingest_fred() -> str:
    """Fetch CPI, unemployment and the USD/EUR rate, save each as a CSV."""
    start = time.perf_counter()
    logger.info("Starting FRED ingestion")

    target_dir = config.raw_dir + "/macro"

    try:
        fred = Fred(api_key=config.fred_api_key)

        for series_id in FRED_SERIES:
            series = fred.get_series(series_id)
            series.to_csv(target_dir + "/" + series_id + ".csv", header=["value"])
            logger.info("FRED %s rows: %s", series_id, len(series))

        logger.info("FRED ingestion took %.2f seconds", time.perf_counter() - start)
        return target_dir

    except Exception as error:
        logger.error("FRED ingestion failed: %s", error)
        raise IngestionError("FRED ingestion failed") from error


def ingest_complaints() -> str:
    """Load the CFPB complaints, keep two products, save as Parquet."""
    start = time.perf_counter()
    logger.info("Starting CFPB complaints ingestion")

    source = config.raw_dir + "/complaints.csv"
    target = config.processed_dir + "/complaints.parquet"

    try:
        kept_chunks = []
        total_rows = 0

        for chunk in pd.read_csv(source, usecols=COMPLAINT_COLUMNS, chunksize=100_000):
            total_rows = total_rows + len(chunk)
            chunk = chunk[chunk["Product"].isin(COMPLAINT_PRODUCTS)]
            kept_chunks.append(chunk)

        complaints = pd.concat(kept_chunks, ignore_index=True)

        logger.info("Complaint rows read: %s", total_rows)
        logger.info("Complaint rows kept: %s", len(complaints))
        save_parquet(complaints, target)

        logger.info("Complaints ingestion took %.2f seconds", time.perf_counter() - start)
        return target

    except Exception as error:
        logger.error("CFPB complaints ingestion failed: %s", error)
        raise IngestionError("CFPB complaints ingestion failed") from error


def run_sequential() -> float:
    """Run the three ingest functions one after another and time the whole run."""
    logger.info("Starting sequential ingestion")
    start = time.perf_counter()

    ingest_paysim()
    ingest_fred()
    ingest_complaints()

    elapsed = time.perf_counter() - start
    logger.info("Sequential ingestion took %.2f seconds", elapsed)
    return elapsed


def main() -> None:
    """Entry point for Milestone 1.2."""
    run_sequential()


if __name__ == "__main__":
    main()
