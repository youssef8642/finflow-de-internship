"""Load the transformed dataset into the DuckDB star schema.

Run with --reload to drop and recreate the tables from schema.sql. Without
it the tables are truncated before loading, so either way running the script
twice leaves the database in the same state.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import duckdb
import pandas as pd

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig
from finflow.models.data_quality import run_quality_checks


logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG = PipelineConfig()

DATABASE_PATH = PROJECT_ROOT / CONFIG.db_path
SCHEMA_PATH = PROJECT_ROOT / "finflow" / "models" / "schema.sql"
VIEWS_PATH = PROJECT_ROOT / "finflow" / "models" / "views.sql"

PROCESSED_DIR = PROJECT_ROOT / CONFIG.processed_dir
TRANSACTIONS_PATH = PROCESSED_DIR / "transactions_transformed.parquet"
COMPLAINTS_PATH = PROCESSED_DIR / "complaints.parquet"

# fact_transactions holds the foreign keys, so it has to go first.
TABLES_IN_TEARDOWN_ORDER = [
    "fact_transactions",
    "complaints",
    "dim_time",
    "dim_account",
    "dim_transaction_type",
]

FACT_COLUMNS = [
    "transaction_id",
    "step",
    "transaction_type_id",
    "amount",
    "log_amount",
    "balance_drain",
    "name_orig",
    "name_dest",
    "is_fraud",
    "is_flagged_fraud",
    "old_balance_sender",
    "new_balance_sender",
    "old_balance_receiver",
    "new_balance_receiver",
]

BALANCE_RENAMES = {
    "old_balance_org": "old_balance_sender",
    "new_balance_org": "new_balance_sender",
    "old_balance_dest": "old_balance_receiver",
    "new_balance_dest": "new_balance_receiver",
}


def open_database() -> duckdb.DuckDBPyConnection:
    """Open the DuckDB database with the progress bar turned off."""
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(DATABASE_PATH))
    connection.execute("PRAGMA enable_progress_bar=false")
    return connection


def create_tables(connection: duckdb.DuckDBPyConnection, reload: bool) -> None:
    """Create the schema, optionally dropping the existing tables first."""
    if reload:
        logger.info("Reload requested, dropping existing tables")
        for table in TABLES_IN_TEARDOWN_ORDER:
            connection.execute(f"DROP TABLE IF EXISTS {table}")

    connection.execute(SCHEMA_PATH.read_text())
    logger.info("Schema loaded from %s", SCHEMA_PATH)


def truncate_tables(connection: duckdb.DuckDBPyConnection) -> None:
    """Empty every table so a repeated run cannot duplicate rows."""
    for table in TABLES_IN_TEARDOWN_ORDER:
        connection.execute(f"DELETE FROM {table}")

    logger.info("Existing rows cleared from all tables")


def load_transaction_types(connection: duckdb.DuckDBPyConnection) -> None:
    """Populate dim_transaction_type from the distinct types in the source."""
    logger.info("Loading dim_transaction_type")

    connection.execute(
        f"""
        INSERT INTO dim_transaction_type (id, type_name)
        SELECT ROW_NUMBER() OVER (ORDER BY type) AS id, type AS type_name
        FROM (
            SELECT DISTINCT type
            FROM read_parquet('{TRANSACTIONS_PATH.as_posix()}')
        )
        """
    )

    count = connection.execute("SELECT COUNT(*) FROM dim_transaction_type").fetchone()[0]
    logger.info("Transaction types loaded: %s", count)


def transaction_type_map(connection: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Read the transaction type surrogate keys back out of the dimension.

    The workers need to map type names to ids. Reading the mapping back
    instead of hardcoding it means the two can never drift apart.
    """
    rows = connection.execute(
        "SELECT type_name, id FROM dim_transaction_type"
    ).fetchall()

    return dict(rows)


def load_accounts(connection: duckdb.DuckDBPyConnection) -> None:
    """Populate dim_account from both the sender and receiver name columns."""
    logger.info("Loading dim_account")

    source = TRANSACTIONS_PATH.as_posix()

    connection.execute(
        f"""
        INSERT INTO dim_account (id, name)
        SELECT ROW_NUMBER() OVER (ORDER BY name) AS id, name
        FROM (
            SELECT name_orig AS name FROM read_parquet('{source}')
            UNION
            SELECT name_dest AS name FROM read_parquet('{source}')
        )
        """
    )

    count = connection.execute("SELECT COUNT(*) FROM dim_account").fetchone()[0]
    logger.info("Accounts loaded: %s", count)


def load_time(connection: duckdb.DuckDBPyConnection) -> None:
    """Populate dim_time by deriving calendar fields from the PaySim step.

    Steps are 1-indexed hours, so subtract 1 before dividing. The division
    has to be integer division -- DuckDB's / returns a DOUBLE, which then
    gets rounded on insert into an INTEGER column and shifts every day
    boundary by half a day.
    """
    logger.info("Loading dim_time")

    connection.execute(
        f"""
        INSERT INTO dim_time (step, sim_day, sim_week, hour_of_day)
        SELECT
            step,
            ((step - 1) // 24) + 1 AS sim_day,
            ((step - 1) // 168) + 1 AS sim_week,
            ((step - 1) % 24) AS hour_of_day
        FROM read_parquet('{TRANSACTIONS_PATH.as_posix()}')
        GROUP BY step
        """
    )

    count = connection.execute("SELECT COUNT(*) FROM dim_time").fetchone()[0]
    logger.info("Time records loaded: %s", count)


def load_complaints(connection: duckdb.DuckDBPyConnection) -> None:
    """Populate the complaints table from the ingested CFPB Parquet."""
    logger.info("Loading complaints")

    connection.execute(
        f"""
        INSERT INTO complaints (
            complaint_id, date_received, product, sub_product,
            issue, company, state, resolution
        )
        SELECT
            "Complaint ID",
            "Date received",
            Product,
            "Sub-product",
            Issue,
            Company,
            State,
            "Company response to consumer"
        FROM read_parquet('{COMPLAINTS_PATH.as_posix()}')
        """
    )

    count = connection.execute("SELECT COUNT(*) FROM complaints").fetchone()[0]
    logger.info("Complaints loaded: %s", count)


def create_chunks(connection: duckdb.DuckDBPyConnection, chunk_size: int) -> list[tuple[int, int]]:
    """Split the transaction id range into inclusive (start, end) windows."""
    min_id, max_id, total = connection.execute(
        f"""
        SELECT MIN(transaction_id), MAX(transaction_id), COUNT(*)
        FROM read_parquet('{TRANSACTIONS_PATH.as_posix()}')
        """
    ).fetchone()

    chunks = [
        (start, min(start + chunk_size - 1, max_id))
        for start in range(min_id, max_id + 1, chunk_size)
    ]

    logger.info("Source transaction rows: %s", total)
    logger.info("Chunk size: %s", chunk_size)
    logger.info("Total chunks created: %s", len(chunks))
    return chunks


def read_chunk(source: str, start_id: int, end_id: int) -> pd.DataFrame:
    """Read one id window out of the transformed Parquet.

    Filtering on transaction_id rather than using LIMIT/OFFSET keeps the
    chunk boundaries deterministic -- DuckDB does not guarantee Parquet scan
    order -- and lets row group statistics skip most of the file.
    """
    with duckdb.connect() as connection:
        connection.execute("PRAGMA enable_progress_bar=false")

        return connection.execute(
            f"""
            SELECT *
            FROM read_parquet('{source}')
            WHERE transaction_id BETWEEN {start_id} AND {end_id}
            """
        ).df()


def prepare_chunk(chunk: pd.DataFrame, type_map: dict[str, int]) -> pd.DataFrame:
    """Map the type name to its surrogate key and align the column names."""
    chunk = chunk.copy()
    chunk["transaction_type_id"] = chunk["type"].map(type_map)
    chunk = chunk.rename(columns=BALANCE_RENAMES)
    return chunk[FACT_COLUMNS]


def process_chunk(arguments: tuple[str, int, int, dict[str, int], str]) -> str:
    """Worker entry point: read one chunk, prepare it, stage it as Parquet."""
    source, start_id, end_id, type_map, staging_dir = arguments

    chunk = prepare_chunk(read_chunk(source, start_id, end_id), type_map)
    temp_path = Path(staging_dir) / f"temp_chunk_{start_id}.parquet"

    with duckdb.connect() as connection:
        connection.execute("PRAGMA enable_progress_bar=false")
        connection.register("chunk", chunk)
        connection.execute(
            f"COPY chunk TO '{temp_path.as_posix()}' (FORMAT PARQUET)"
        )

    return str(temp_path)


def insert_chunks(
    connection: duckdb.DuckDBPyConnection,
    temp_files: list[str],
) -> int:
    """Insert every staged chunk into fact_transactions, resolving the FKs."""
    logger.info("Inserting processed chunks into fact_transactions")
    total_rows = 0

    for temp_file in temp_files:
        source = Path(temp_file).as_posix()

        rows = connection.execute(
            f"SELECT COUNT(*) FROM read_parquet('{source}')"
        ).fetchone()[0]

        connection.execute(
            f"""
            INSERT INTO fact_transactions (
                transaction_id, step, transaction_type_id, amount, log_amount,
                balance_drain, sender_account_id, receiver_account_id,
                is_fraud, is_flagged_fraud, old_balance_sender,
                new_balance_sender, old_balance_receiver, new_balance_receiver
            )
            SELECT
                chunk.transaction_id,
                chunk.step,
                chunk.transaction_type_id,
                chunk.amount,
                chunk.log_amount,
                chunk.balance_drain,
                sender.id,
                receiver.id,
                chunk.is_fraud,
                chunk.is_flagged_fraud,
                chunk.old_balance_sender,
                chunk.new_balance_sender,
                chunk.old_balance_receiver,
                chunk.new_balance_receiver
            FROM read_parquet('{source}') AS chunk
            JOIN dim_account AS sender ON chunk.name_orig = sender.name
            JOIN dim_account AS receiver ON chunk.name_dest = receiver.name
            """
        )

        total_rows += rows

    logger.info("Rows inserted into fact_transactions: %s", total_rows)
    return total_rows


def create_views(connection: duckdb.DuckDBPyConnection) -> None:
    """Create the analytical views from views.sql."""
    connection.execute(VIEWS_PATH.read_text())
    logger.info("Views created from %s", VIEWS_PATH)


def verify_row_count(connection: duckdb.DuckDBPyConnection) -> None:
    """Check the fact table matches the source Parquet row for row."""
    loaded = connection.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0]
    expected = connection.execute(
        f"SELECT COUNT(*) FROM read_parquet('{TRANSACTIONS_PATH.as_posix()}')"
    ).fetchone()[0]

    logger.info("Final fact_transactions row count: %s", loaded)

    if loaded != expected:
        raise ValueError(
            f"Row count mismatch: loaded {loaded}, source has {expected}"
        )

    logger.info("Row count matches the source Parquet")


def parse_arguments() -> argparse.Namespace:
    """Parse the command line arguments."""
    parser = argparse.ArgumentParser(description="Load FinFlow data into DuckDB.")
    parser.add_argument(
        "--reload",
        action="store_true",
        help="drop and recreate the tables instead of truncating them",
    )
    return parser.parse_args()


def main() -> None:
    """Load every table, create the views, and run the quality checks."""
    arguments = parse_arguments()

    logger.info("Starting DuckDB loading")
    logger.info("Database path: %s", DATABASE_PATH)
    logger.info("Chunk size: %s", CONFIG.chunk_size)
    logger.info("Max workers: %s", CONFIG.max_workers)

    if not TRANSACTIONS_PATH.is_file():
        raise FileNotFoundError(
            f"Transformed transactions not found: {TRANSACTIONS_PATH}. "
            "Run the transformation stage first."
        )

    connection = open_database()
    temp_files: list[str] = []

    try:
        create_tables(connection, reload=arguments.reload)
        truncate_tables(connection)

        load_transaction_types(connection)
        load_accounts(connection)
        load_time(connection)
        load_complaints(connection)

        type_map = transaction_type_map(connection)
        chunks = create_chunks(connection, CONFIG.chunk_size)

        # The workers open their own connections, so the parent one has to be
        # closed while they run -- DuckDB allows a single writer per file.
        connection.close()

        arguments_list = [
            (
                TRANSACTIONS_PATH.as_posix(),
                start_id,
                end_id,
                type_map,
                str(PROCESSED_DIR),
            )
            for start_id, end_id in chunks
        ]

        logger.info("Starting ProcessPoolExecutor with %s workers", CONFIG.max_workers)

        with ProcessPoolExecutor(max_workers=CONFIG.max_workers) as executor:
            temp_files = list(executor.map(process_chunk, arguments_list))

        logger.info("Parallel chunk processing completed")

        connection = open_database()
        total_rows = insert_chunks(connection, temp_files)

        verify_row_count(connection)
        create_views(connection)

        logger.info("Starting data quality checks")
        run_quality_checks(connection)

        logger.info("Total rows inserted: %s", total_rows)
        logger.info("DuckDB loading completed")

    finally:
        connection.close()

        for temp_file in temp_files:
            Path(temp_file).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
