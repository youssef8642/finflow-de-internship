"""Milestone 2.3 - Data quality checks.

Order of this file follows the milestone bullets:
    1. no duplicated transaction_id in fact_transactions
    2. is_fraud null rate is 0%
    3. every foreign key has a match in its dimension table
    4. amount has no negative values

The pipeline should not continue to the analysis stage if a check fails,
so every check raises DataQualityError instead of just logging a warning.
"""

import duckdb

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig

logger = get_logger(__name__)
config = PipelineConfig()


class DataQualityError(Exception):
    """Raised when one of the quality checks fails."""


def check_duplicate_ids(connection) -> None:
    """Check 1: transaction_id must be unique."""
    duplicates = connection.execute("""
        SELECT COUNT(*) FROM (
            SELECT transaction_id
            FROM fact_transactions
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    if duplicates > 0:
        raise DataQualityError(f"Found {duplicates} duplicated transaction_id values")

    logger.info("Check 1 passed: transaction_id is unique")


def check_fraud_nulls(connection) -> None:
    """Check 2: is_fraud must never be null."""
    nulls = connection.execute("""
        SELECT COUNT(*)
        FROM fact_transactions
        WHERE is_fraud IS NULL
    """).fetchone()[0]

    if nulls > 0:
        raise DataQualityError(f"is_fraud has {nulls} null values")

    logger.info("Check 2 passed: is_fraud has no nulls")


def check_foreign_keys(connection) -> None:
    """Check 3: every foreign key must exist in its dimension table."""
    checks = [
        ("transaction_type_id", "dim_transaction_type", "id"),
        ("sender_account_id", "dim_account", "id"),
        ("receiver_account_id", "dim_account", "id"),
        ("step", "dim_time", "step"),
    ]

    for column, dimension, dimension_key in checks:
        missing = connection.execute(f"""
            SELECT COUNT(*)
            FROM fact_transactions AS f
            LEFT JOIN {dimension} AS d
                ON f.{column} = d.{dimension_key}
            WHERE d.{dimension_key} IS NULL
        """).fetchone()[0]

        if missing > 0:
            raise DataQualityError(f"{column} has {missing} rows with no matching dimension row")

        logger.info("Check 3 passed for %s", column)


def check_negative_amounts(connection) -> None:
    """Check 4: amount must never be negative."""
    negatives = connection.execute("""
        SELECT COUNT(*)
        FROM fact_transactions
        WHERE amount < 0
    """).fetchone()[0]

    if negatives > 0:
        raise DataQualityError(f"Found {negatives} negative amounts")

    logger.info("Check 4 passed: no negative amounts")


def run_quality_checks(connection) -> None:
    """Run all four checks. Stops at the first failure."""
    logger.info("Running data quality checks")

    check_duplicate_ids(connection)
    check_fraud_nulls(connection)
    check_foreign_keys(connection)
    check_negative_amounts(connection)

    logger.info("All data quality checks passed")


def main() -> None:
    """Entry point for Milestone 2.3."""
    connection = duckdb.connect(config.db_path)
    run_quality_checks(connection)
    connection.close()


if __name__ == "__main__":
    main()
