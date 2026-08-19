"""Milestone 2.2 - Load the transformed data into DuckDB.

Order of this file follows the milestone bullets:
    1. create the database and all tables from schema.sql
    2. load the dimension tables first
    3. load fact_transactions in chunks with a ProcessPoolExecutor
    4. verify the row count matches the source parquet
    5. idempotent: running it twice does not duplicate rows
    6. log the total load time

Run:
    python -m finflow.models.load_schema
    python -m finflow.models.load_schema --reload
"""

import argparse
import os
import time
from concurrent.futures import ProcessPoolExecutor

import duckdb

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig
from finflow.models.data_quality import run_quality_checks

logger = get_logger(__name__)
config = PipelineConfig()

TRANSACTIONS_PATH = config.processed_dir + "/transactions_transformed.parquet"
COMPLAINTS_PATH = config.processed_dir + "/complaints.parquet"

SCHEMA_PATH = "finflow/models/schema.sql"
VIEWS_PATH = "finflow/models/views.sql"

TABLES = [
    "fact_transactions",
    "complaints",
    "dim_time",
    "dim_account",
    "dim_transaction_type",
]


def create_tables(connection, reload_tables: bool) -> None:
    """Create every table from schema.sql, then empty them.

    Emptying the tables before loading is what makes this script idempotent:
    running it twice gives the same row counts instead of doubling them.
    """
    if reload_tables:
        for table in TABLES:
            connection.execute(f"DROP TABLE IF EXISTS {table}")
        logger.info("Dropped existing tables")

    schema_file = open(SCHEMA_PATH)
    connection.execute(schema_file.read())
    schema_file.close()

    for table in TABLES:
        connection.execute(f"DELETE FROM {table}")

    logger.info("Tables created and emptied")


def load_transaction_types(connection) -> None:
    """Load dim_transaction_type from the distinct types in the source."""
    connection.execute(f"""
        INSERT INTO dim_transaction_type (id, type_name)
        SELECT ROW_NUMBER() OVER (ORDER BY type), type
        FROM (SELECT DISTINCT type FROM read_parquet('{TRANSACTIONS_PATH}'))
    """)

    count = connection.execute("SELECT COUNT(*) FROM dim_transaction_type").fetchone()[0]
    logger.info("dim_transaction_type rows: %s", count)


def load_accounts(connection) -> None:
    """Load dim_account from both the sender and the receiver name columns."""
    connection.execute(f"""
        INSERT INTO dim_account (id, name)
        SELECT ROW_NUMBER() OVER (ORDER BY name), name
        FROM (
            SELECT name_orig AS name FROM read_parquet('{TRANSACTIONS_PATH}')
            UNION
            SELECT name_dest AS name FROM read_parquet('{TRANSACTIONS_PATH}')
        )
    """)

    count = connection.execute("SELECT COUNT(*) FROM dim_account").fetchone()[0]
    logger.info("dim_account rows: %s", count)


def load_time(connection) -> None:
    """Load dim_time by turning the PaySim step into day, week and hour.

    A step is one hour and steps start at 1, so subtract 1 before dividing.
    The division has to be // and not /, because / returns a DOUBLE in DuckDB
    and that gets rounded when it is inserted into an INTEGER column, which
    would shift every day boundary by half a day.
    """
    connection.execute(f"""
        INSERT INTO dim_time (step, sim_day, sim_week, hour_of_day)
        SELECT
            step,
            ((step - 1) // 24) + 1,
            ((step - 1) // 168) + 1,
            (step - 1) % 24
        FROM read_parquet('{TRANSACTIONS_PATH}')
        GROUP BY step
    """)

    count = connection.execute("SELECT COUNT(*) FROM dim_time").fetchone()[0]
    logger.info("dim_time rows: %s", count)


def load_complaints(connection) -> None:
    """Load the complaints table from the ingested CFPB parquet."""
    connection.execute(f"""
        INSERT INTO complaints
        SELECT
            "Complaint ID",
            "Date received",
            Product,
            "Sub-product",
            Issue,
            Company,
            State,
            "Company response to consumer"
        FROM read_parquet('{COMPLAINTS_PATH}')
    """)

    count = connection.execute("SELECT COUNT(*) FROM complaints").fetchone()[0]
    logger.info("complaints rows: %s", count)


def build_chunks(total_rows: int, chunk_size: int) -> list:
    """Split the transaction ids into a list of (first_id, last_id) pairs."""
    chunks = []
    start = 1

    while start <= total_rows:
        end = start + chunk_size - 1
        if end > total_rows:
            end = total_rows
        chunks.append((start, end))
        start = start + chunk_size

    logger.info("Created %s chunks of %s rows", len(chunks), chunk_size)
    return chunks


def write_chunk(chunk: tuple) -> str:
    """Worker function: copy one id range out into its own small parquet file.

    Each worker opens its own DuckDB connection. DuckDB only allows one writer
    on a database file, so the workers write parquet files and the parent
    process does the inserts afterwards.
    """
    first_id = chunk[0]
    last_id = chunk[1]
    temp_path = f"{config.processed_dir}/temp_chunk_{first_id}.parquet"

    connection = duckdb.connect()
    connection.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{TRANSACTIONS_PATH}')
            WHERE transaction_id BETWEEN {first_id} AND {last_id}
        )
        TO '{temp_path}' (FORMAT PARQUET)
    """)
    connection.close()

    return temp_path


def insert_chunk(connection, temp_path: str) -> None:
    """Insert one staged chunk, swapping the names for their dimension ids."""
    connection.execute(f"""
        INSERT INTO fact_transactions
        SELECT
            c.transaction_id,
            c.step,
            t.id,
            c.amount,
            c.log_amount,
            c.balance_drain,
            sender.id,
            receiver.id,
            c.is_fraud,
            c.is_flagged_fraud,
            c.old_balance_org,
            c.new_balance_org,
            c.old_balance_dest,
            c.new_balance_dest
        FROM read_parquet('{temp_path}') AS c
        JOIN dim_transaction_type AS t ON c.type = t.type_name
        JOIN dim_account AS sender ON c.name_orig = sender.name
        JOIN dim_account AS receiver ON c.name_dest = receiver.name
    """)


def create_views(connection) -> None:
    """Create the Milestone 2.3 views from views.sql."""
    views_file = open(VIEWS_PATH)
    connection.execute(views_file.read())
    views_file.close()
    logger.info("Views created")


def main() -> None:
    """Entry point for Milestone 2.2."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--reload", action="store_true", help="drop and recreate the tables")
    arguments = parser.parse_args()

    start = time.perf_counter()
    connection = duckdb.connect(config.db_path)

    create_tables(connection, arguments.reload)

    load_transaction_types(connection)
    load_accounts(connection)
    load_time(connection)
    load_complaints(connection)

    total_rows = connection.execute(
        f"SELECT COUNT(*) FROM read_parquet('{TRANSACTIONS_PATH}')"
    ).fetchone()[0]

    chunks = build_chunks(total_rows, config.chunk_size)

    connection.close()

    executor = ProcessPoolExecutor(max_workers=config.max_workers)
    temp_paths = list(executor.map(write_chunk, chunks))
    executor.shutdown()
    logger.info("All chunks written by the workers")

    connection = duckdb.connect(config.db_path)
    for temp_path in temp_paths:
        insert_chunk(connection, temp_path)

    loaded = connection.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0]
    logger.info("fact_transactions rows: %s (source has %s)", loaded, total_rows)

    if loaded != total_rows:
        raise ValueError("Row count does not match the source parquet")

    create_views(connection)
    run_quality_checks(connection)
    connection.close()

    for temp_path in temp_paths:
        os.remove(temp_path)

    logger.info("Loading took %.2f seconds", time.perf_counter() - start)


if __name__ == "__main__":
    main()
