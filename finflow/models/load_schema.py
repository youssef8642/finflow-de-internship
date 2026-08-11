"""Load FinFlow data into DuckDB."""

from __future__ import annotations

import argparse
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import duckdb
import pandas as pd

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig


logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def get_paths() -> tuple[Path, Path, Path]:
    """Get database and input paths."""

    config = PipelineConfig()

    db_path = PROJECT_ROOT / config.db_path

    transactions_path = (
        PROJECT_ROOT
        / config.processed_dir
        / "transactions_transformed_1000000.parquet"
    )

    complaints_path = (
        PROJECT_ROOT
        / config.processed_dir
        / "complaints.parquet"
    )

    return (
        db_path,
        transactions_path,
        complaints_path,
    )


def create_database(
    connection: duckdb.DuckDBPyConnection,
    reload: bool,
) -> None:
    """Create the database tables."""

    schema_path = (
        PROJECT_ROOT
        / "finflow"
        / "models"
        / "schema.sql"
    )

    logger.info(
        "Creating schema from %s",
        schema_path,
    )

    schema_sql = schema_path.read_text(
        encoding="utf-8"
    )

    connection.execute(schema_sql)

    if reload:
        logger.info(
            "Reload requested - clearing existing data"
        )

        connection.execute(
            "DELETE FROM fact_transactions"
        )

        connection.execute(
            "DELETE FROM complaints"
        )

        connection.execute(
            "DELETE FROM dim_transaction_type"
        )

        connection.execute(
            "DELETE FROM dim_account"
        )

        connection.execute(
            "DELETE FROM dim_time"
        )


def load_transaction_types(
    connection: duckdb.DuckDBPyConnection,
    parquet_path: Path,
) -> None:
    """Load transaction types."""

    logger.info(
        "Loading dim_transaction_type"
    )

    connection.execute(
        """
        INSERT OR IGNORE INTO dim_transaction_type (
            id,
            type_name
        )
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY type
            ),
            type
        FROM (
            SELECT DISTINCT type
            FROM read_parquet(?)
        )
        """,
        [str(parquet_path)],
    )

    count = connection.execute(
        """
        SELECT COUNT(*)
        FROM dim_transaction_type
        """
    ).fetchone()[0]

    logger.info(
        "Transaction types loaded: %s",
        count,
    )


def load_accounts(
    connection: duckdb.DuckDBPyConnection,
    parquet_path: Path,
) -> None:
    """Load unique sender and receiver accounts."""

    logger.info(
        "Loading dim_account"
    )

    connection.execute(
        """
        INSERT OR IGNORE INTO dim_account (
            id,
            name
        )
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY name
            ),
            name
        FROM (
            SELECT name_orig AS name
            FROM read_parquet(?)

            UNION

            SELECT name_dest AS name
            FROM read_parquet(?)
        )
        """,
        [
            str(parquet_path),
            str(parquet_path),
        ],
    )

    count = connection.execute(
        """
        SELECT COUNT(*)
        FROM dim_account
        """
    ).fetchone()[0]

    logger.info(
        "Accounts loaded: %s",
        count,
    )


def load_time(
    connection: duckdb.DuckDBPyConnection,
    parquet_path: Path,
) -> None:
    """Load time dimension."""

    logger.info(
        "Loading dim_time"
    )

    connection.execute(
        """
        INSERT OR IGNORE INTO dim_time (
            step,
            sim_day,
            sim_week,
            hour_of_day
        )
        SELECT
            step,
            step / 24,
            step / (24 * 7),
            step % 24
        FROM (
            SELECT DISTINCT step
            FROM read_parquet(?)
        )
        """,
        [str(parquet_path)],
    )

    count = connection.execute(
        """
        SELECT COUNT(*)
        FROM dim_time
        """
    ).fetchone()[0]

    logger.info(
        "Time dimension loaded: %s",
        count,
    )


def load_complaints(
    connection: duckdb.DuckDBPyConnection,
    complaints_path: Path,
) -> None:
    """Load CFPB complaints."""

    logger.info(
        "Loading complaints"
    )

    connection.execute(
        """
        INSERT OR IGNORE INTO complaints (
            complaint_id,
            date_received,
            product,
            sub_product,
            issue,
            company,
            state,
            resolution
        )
        SELECT
            "Complaint ID",
            CAST("Date received" AS DATE),
            "Product",
            "Sub-product",
            "Issue",
            "Company",
            "State",
            "Company public response"
        FROM read_parquet(?)
        """,
        [str(complaints_path)],
    )

    count = connection.execute(
        """
        SELECT COUNT(*)
        FROM complaints
        """
    ).fetchone()[0]

    logger.info(
        "Complaints loaded: %s",
        count,
    )


def create_chunks(
    parquet_path: Path,
    chunk_size: int,
) -> list[pd.DataFrame]:
    """Read the transactions and split them into chunks."""

    logger.info(
        "Creating transaction chunks of %s rows",
        chunk_size,
    )

    connection = duckdb.connect(
        database=":memory:"
    )

    try:
        result = connection.execute(
            """
            SELECT *
            FROM read_parquet(?)
            """,
            [str(parquet_path)],
        )

        columns = [
            column[0]
            for column in result.description
        ]

        chunks = []
        chunk_number = 0

        while True:

            rows = result.fetchmany(
                chunk_size
            )

            if not rows:
                break

            chunk = pd.DataFrame(
                rows,
                columns=columns,
            )

            chunks.append(chunk)

            chunk_number += 1

            logger.info(
                "Created chunk %s | Rows: %s",
                chunk_number,
                len(chunk),
            )

        logger.info(
            "Total chunks created: %s",
            len(chunks),
        )

        return chunks

    finally:
        connection.close()


def transform_chunk(
    arguments: tuple[
        pd.DataFrame,
        int,
        str,
    ],
) -> str:
    """Prepare one transaction chunk."""

    chunk, start_id, temp_directory = arguments

    chunk = chunk.copy()

    # Create a unique transaction ID.
    chunk["transaction_id"] = range(
        start_id,
        start_id + len(chunk),
    )

    # Rename the actual transformed Parquet
    # columns to the names used by the fact table.
    chunk = chunk.rename(
        columns={
            "name_orig": "sender_name",
            "name_dest": "receiver_name",
            "type": "type_name",
            "old_balance_org": "old_balance_sender",
            "new_balance_org": "new_balance_sender",
            "old_balance_dest": "old_balance_receiver",
            "new_balance_dest": "new_balance_receiver",
        }
    )

    fact_columns = [
        "transaction_id",
        "step",
        "type_name",
        "amount",
        "log_amount",
        "balance_drain",
        "sender_name",
        "receiver_name",
        "is_fraud",
        "is_flagged_fraud",
        "old_balance_sender",
        "new_balance_sender",
        "old_balance_receiver",
        "new_balance_receiver",
    ]

    chunk = chunk[fact_columns]

    output_path = (
        Path(temp_directory)
        / f"fact_chunk_{start_id}.parquet"
    )

    connection = duckdb.connect(
        database=":memory:"
    )

    try:
        connection.register(
            "chunk_df",
            chunk,
        )

        connection.execute(
            """
            COPY chunk_df
            TO ?
            (FORMAT PARQUET)
            """,
            [str(output_path)],
        )

    finally:
        connection.close()

    return str(output_path)


def load_fact_transactions(
    connection: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    config: PipelineConfig,
) -> None:
    """Load fact_transactions using ProcessPoolExecutor."""

    logger.info(
        "Starting fact_transactions loading"
    )

    source_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM read_parquet(?)
        """,
        [str(parquet_path)],
    ).fetchone()[0]

    logger.info(
        "Source transaction rows: %s",
        source_count,
    )

    chunks = create_chunks(
        parquet_path,
        config.chunk_size,
    )

    temp_directory = (
        PROJECT_ROOT
        / "data"
        / "temp_fact_chunks"
    )

    temp_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    worker_arguments = []

    start_id = 1

    for chunk in chunks:

        worker_arguments.append(
            (
                chunk,
                start_id,
                str(temp_directory),
            )
        )

        start_id += len(chunk)

    logger.info(
        "Starting ProcessPoolExecutor with %s workers",
        config.max_workers,
    )

    start_time = time.perf_counter()

    with ProcessPoolExecutor(
        max_workers=config.max_workers
    ) as executor:

        temp_files = list(
            executor.map(
                transform_chunk,
                worker_arguments,
            )
        )

    elapsed = (
        time.perf_counter()
        - start_time
    )

    logger.info(
        "Parallel chunk processing completed in %.2f seconds",
        elapsed,
    )

    logger.info(
        "Inserting processed chunks into fact_transactions"
    )

    for temp_file in temp_files:

        connection.execute(
            """
            INSERT OR IGNORE INTO fact_transactions
            SELECT
                fact.transaction_id,
                fact.step,
                transaction_type.id,
                fact.amount,
                fact.log_amount,
                fact.balance_drain,
                sender.id,
                receiver.id,
                fact.is_fraud,
                fact.is_flagged_fraud,
                fact.old_balance_sender,
                fact.new_balance_sender,
                fact.old_balance_receiver,
                fact.new_balance_receiver

            FROM read_parquet(?) AS fact

            JOIN dim_transaction_type AS transaction_type
                ON fact.type_name =
                   transaction_type.type_name

            JOIN dim_account AS sender
                ON fact.sender_name =
                   sender.name

            JOIN dim_account AS receiver
                ON fact.receiver_name =
                   receiver.name
            """,
            [temp_file],
        )

    final_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM fact_transactions
        """
    ).fetchone()[0]

    logger.info(
        "Final fact_transactions row count: %s",
        final_count,
    )

    if final_count != source_count:
        raise RuntimeError(
            "Fact transaction row count mismatch: "
            f"source={source_count}, "
            f"database={final_count}"
        )

    logger.info(
        "Fact transaction row count verified"
    )

    # Delete temporary files.
    for temp_file in temp_files:
        Path(temp_file).unlink(
            missing_ok=True
        )

    try:
        temp_directory.rmdir()
    except OSError:
        pass


def verify_counts(
    connection: duckdb.DuckDBPyConnection,
    transactions_path: Path,
) -> None:
    """Verify the final fact table row count."""

    source_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM read_parquet(?)
        """,
        [str(transactions_path)],
    ).fetchone()[0]

    database_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM fact_transactions
        """
    ).fetchone()[0]

    logger.info(
        "Source transaction rows: %s",
        source_count,
    )

    logger.info(
        "Database transaction rows: %s",
        database_count,
    )

    if source_count != database_count:
        raise RuntimeError(
            "Source and database row counts do not match"
        )

    logger.info(
        "Final row count verification passed"
    )


def main() -> None:
    """Run the complete DuckDB loading pipeline."""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--reload",
        action="store_true",
        help="Clear existing data before loading",
    )

    args = parser.parse_args()

    config = PipelineConfig()

    (
        db_path,
        transactions_path,
        complaints_path,
    ) = get_paths()

    start_time = time.perf_counter()

    logger.info(
        "Starting DuckDB loading"
    )

    if not transactions_path.is_file():
        raise FileNotFoundError(
            f"Transformed transactions file not found: "
            f"{transactions_path}"
        )

    if not complaints_path.is_file():
        raise FileNotFoundError(
            f"Complaints file not found: "
            f"{complaints_path}"
        )

    # Creates the data directory if necessary.
    db_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # DuckDB automatically creates the database
    # if finflow.duckdb does not exist.
    connection = duckdb.connect(
        str(db_path)
    )

    try:

        # Create tables.
        create_database(
            connection,
            args.reload,
        )

        # Load dimensions first.
        load_transaction_types(
            connection,
            transactions_path,
        )

        load_accounts(
            connection,
            transactions_path,
        )

        load_time(
            connection,
            transactions_path,
        )

        # Load complaints.
        load_complaints(
            connection,
            complaints_path,
        )

        # Load fact table.
        load_fact_transactions(
            connection,
            transactions_path,
            config,
        )

        # Verify final row count.
        verify_counts(
            connection,
            transactions_path,
        )

    finally:
        connection.close()

    elapsed = (
        time.perf_counter()
        - start_time
    )

    logger.info(
        "DuckDB loading completed in %.2f seconds",
        elapsed,
    )

    logger.info(
        "Database saved to %s",
        db_path,
    )


if __name__ == "__main__":
    main()