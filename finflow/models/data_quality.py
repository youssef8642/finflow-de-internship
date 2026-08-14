"""Data quality checks that gate the pipeline before analysis runs."""

from __future__ import annotations

from pathlib import Path

import duckdb

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig


logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG = PipelineConfig()

DATABASE_PATH = PROJECT_ROOT / CONFIG.db_path

# Every foreign key in the fact table, with the dimension it has to resolve to.
FOREIGN_KEYS = [
    ("transaction_type_id", "dim_transaction_type", "transaction type"),
    ("sender_account_id", "dim_account", "sender account"),
    ("receiver_account_id", "dim_account", "receiver account"),
    ("step", "dim_time", "time"),
]


class DataQualityError(Exception):
    """Raised when a data quality check fails."""


def _count(connection: duckdb.DuckDBPyConnection, sql: str) -> int:
    """Run a scalar COUNT query and return the result."""
    return connection.execute(sql).fetchone()[0]


def check_duplicate_transaction_ids(connection: duckdb.DuckDBPyConnection) -> None:
    """Fail if the fact table primary key is not unique."""
    duplicates = _count(
        connection,
        """
        SELECT COUNT(*) FROM (
            SELECT transaction_id
            FROM fact_transactions
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        )
        """,
    )

    if duplicates:
        raise DataQualityError(
            f"Duplicate transaction_id values found: {duplicates}"
        )

    logger.info("Duplicate transaction_id check passed")


def check_fraud_nulls(connection: duckdb.DuckDBPyConnection) -> None:
    """Fail if is_fraud has any nulls -- the label has to be complete."""
    nulls = _count(
        connection,
        "SELECT COUNT(*) FROM fact_transactions WHERE is_fraud IS NULL",
    )

    if nulls:
        raise DataQualityError(f"is_fraud contains {nulls} NULL values")

    logger.info("is_fraud NULL check passed")


def check_foreign_keys(connection: duckdb.DuckDBPyConnection) -> None:
    """Fail if any fact table foreign key has no row in its dimension."""
    for column, dimension, label in FOREIGN_KEYS:
        dimension_key = "step" if dimension == "dim_time" else "id"

        orphans = _count(
            connection,
            f"""
            SELECT COUNT(*)
            FROM fact_transactions AS ft
            LEFT JOIN {dimension} AS d ON ft.{column} = d.{dimension_key}
            WHERE d.{dimension_key} IS NULL
            """,
        )

        if orphans:
            raise DataQualityError(
                f"Missing {label} foreign keys: {orphans}"
            )

        logger.info("%s foreign key check passed", label.capitalize())


def check_negative_amounts(connection: duckdb.DuckDBPyConnection) -> None:
    """Fail if any transaction has a negative amount."""
    negatives = _count(
        connection,
        "SELECT COUNT(*) FROM fact_transactions WHERE amount < 0",
    )

    if negatives:
        raise DataQualityError(f"Negative transaction amounts found: {negatives}")

    logger.info("Negative amount check passed")


def run_quality_checks(connection: duckdb.DuckDBPyConnection) -> None:
    """Run every quality check, raising DataQualityError on the first failure."""
    logger.info("Starting data quality checks")

    check_duplicate_transaction_ids(connection)
    check_fraud_nulls(connection)
    check_foreign_keys(connection)
    check_negative_amounts(connection)

    logger.info("All data quality checks passed")


def main() -> None:
    """Run the quality checks against the project database."""
    with duckdb.connect(str(DATABASE_PATH)) as connection:
        connection.execute("PRAGMA enable_progress_bar=false")

        try:
            run_quality_checks(connection)
        except DataQualityError as error:
            logger.error("Data quality check failed: %s", error)
            raise


if __name__ == "__main__":
    main()
