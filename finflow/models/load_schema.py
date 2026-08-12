import duckdb
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig
from finflow.models.data_quality import run_quality_checks


# --------------------------------------------------
# Logger
# --------------------------------------------------

logger = get_logger(__name__)


# --------------------------------------------------
# Configuration
# --------------------------------------------------

config = PipelineConfig()


# --------------------------------------------------
# Paths
# --------------------------------------------------

database_path = Path(
    config.db_path
)

schema_path = Path(
    "finflow/models/schema.sql"
)

parquet_path = Path(
    config.processed_dir
) / "transactions_transformed_1000000.parquet"

complaints_path = Path(
    config.processed_dir
) / "complaints.parquet"


# --------------------------------------------------
# Create / open DuckDB database
# --------------------------------------------------

def create_database():

    connection = duckdb.connect(
        str(database_path)
    )

    connection.execute(
        "PRAGMA enable_progress_bar=false"
    )

    schema_sql = schema_path.read_text()

    connection.execute(
        schema_sql
    )

    logger.info(
        "DuckDB database opened"
    )

    logger.info(
        "Schema loaded from %s",
        schema_path
    )

    return connection


# --------------------------------------------------
# Load transaction type dimension
# --------------------------------------------------

def load_transaction_types(connection):

    logger.info(
        "Loading dim_transaction_type"
    )

    connection.execute(
        f"""
        INSERT INTO dim_transaction_type (
            id,
            type_name
        )

        SELECT
            ROW_NUMBER() OVER (
                ORDER BY type
            ) AS id,

            type AS type_name

        FROM (
            SELECT DISTINCT type

            FROM read_parquet(
                '{parquet_path}'
            )
        )
        """
    )

    count = connection.execute(
        """
        SELECT COUNT(*)
        FROM dim_transaction_type
        """
    ).fetchone()[0]

    logger.info(
        "Transaction types loaded: %s",
        count
    )


# --------------------------------------------------
# Load account dimension
# --------------------------------------------------

def load_accounts(connection):

    logger.info(
        "Loading dim_account"
    )

    connection.execute(
        f"""
        INSERT INTO dim_account (
            id,
            name
        )

        SELECT
            ROW_NUMBER() OVER (
                ORDER BY name
            ) AS id,

            name

        FROM (
            SELECT name_orig AS name

            FROM read_parquet(
                '{parquet_path}'
            )

            UNION

            SELECT name_dest AS name

            FROM read_parquet(
                '{parquet_path}'
            )
        )
        """
    )

    count = connection.execute(
        """
        SELECT COUNT(*)
        FROM dim_account
        """
    ).fetchone()[0]

    logger.info(
        "Accounts loaded: %s",
        count
    )


# --------------------------------------------------
# Load time dimension
# --------------------------------------------------

def load_time(connection):

    logger.info(
        "Loading dim_time"
    )

    connection.execute(
        f"""
        INSERT INTO dim_time (
            step,
            sim_day,
            sim_week,
            hour_of_day
        )

        SELECT
            step,

            ((step - 1) / 24) + 1
                AS sim_day,

            ((step - 1) / 168) + 1
                AS sim_week,

            ((step - 1) % 24)
                AS hour_of_day

        FROM read_parquet(
            '{parquet_path}'
        )

        GROUP BY step
        """
    )

    count = connection.execute(
        """
        SELECT COUNT(*)
        FROM dim_time
        """
    ).fetchone()[0]

    logger.info(
        "Time records loaded: %s",
        count
    )


# --------------------------------------------------
# Load complaints
# --------------------------------------------------

def load_complaints(connection):

    logger.info(
        "Loading complaints"
    )

    connection.execute(
        f"""
        INSERT INTO complaints (
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
            "Date received",
            Product,
            "Sub-product",
            Issue,
            Company,
            State,
            "Company response to consumer"

        FROM read_parquet(
            '{complaints_path}'
        )
        """
    )

    count = connection.execute(
        """
        SELECT COUNT(*)
        FROM complaints
        """
    ).fetchone()[0]

    logger.info(
        "Complaints loaded: %s",
        count
    )


# --------------------------------------------------
# Create transaction chunks
# --------------------------------------------------

def create_chunks(
    parquet_path,
    chunk_size
):

    connection = duckdb.connect()

    connection.execute(
        "PRAGMA enable_progress_bar=false"
    )

    total_rows = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{parquet_path}'
        )
        """
    ).fetchone()[0]

    connection.close()

    chunks = []

    for start in range(
        0,
        total_rows,
        chunk_size
    ):

        end = min(
            start + chunk_size,
            total_rows
        )

        chunks.append(
            (
                start,
                end
            )
        )

    logger.info(
        "Source transaction rows: %s",
        total_rows
    )

    logger.info(
        "Chunk size: %s",
        chunk_size
    )

    logger.info(
        "Total chunks created: %s",
        len(chunks)
    )

    return chunks


# --------------------------------------------------
# Read one transaction chunk
# --------------------------------------------------

def read_chunk(
    parquet_path,
    start,
    end
):

    connection = duckdb.connect()

    connection.execute(
        "PRAGMA enable_progress_bar=false"
    )

    chunk = connection.execute(
        f"""
        SELECT *
        FROM read_parquet(
            '{parquet_path}'
        )

        LIMIT {end - start}

        OFFSET {start}
        """
    ).df()

    connection.close()

    return chunk


# --------------------------------------------------
# Transform one chunk
# --------------------------------------------------

def transform_chunk(
    chunk,
    start
):

    chunk = chunk.copy()

    chunk["transaction_id"] = (
        range(
            start + 1,
            start + len(chunk) + 1
        )
    )

    chunk["transaction_type_id"] = (
        chunk["type"]
        .map(
            {
                "CASH_IN": 1,
                "CASH_OUT": 2,
                "DEBIT": 3,
                "PAYMENT": 4,
                "TRANSFER": 5,
            }
        )
    )

    chunk = chunk.rename(
        columns={
            "old_balance_org":
                "old_balance_sender",

            "new_balance_org":
                "new_balance_sender",

            "old_balance_dest":
                "old_balance_receiver",

            "new_balance_dest":
                "new_balance_receiver",
        }
    )

    return chunk[
        [
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
    ]


# --------------------------------------------------
# Process one transaction chunk
# --------------------------------------------------

def process_chunk(arguments):

    (
        parquet_path,
        start,
        end
    ) = arguments

    chunk = read_chunk(
        parquet_path,
        start,
        end
    )

    chunk = transform_chunk(
        chunk,
        start
    )

    temp_path = Path(
        "data/processed"
    ) / f"temp_chunk_{start}.parquet"

    connection = duckdb.connect()

    connection.execute(
        "PRAGMA enable_progress_bar=false"
    )

    connection.register(
        "chunk",
        chunk
    )

    connection.execute(
        f"""
        COPY chunk
        TO '{temp_path}'
        (
            FORMAT PARQUET
        )
        """
    )

    connection.close()

    return str(temp_path)


# --------------------------------------------------
# Insert processed chunks into DuckDB
# --------------------------------------------------

def insert_chunks(
    connection,
    temp_files
):

    total_rows = 0

    logger.info(
        "Inserting processed chunks into fact_transactions"
    )

    for temp_file in temp_files:

        rows = connection.execute(
            f"""
            SELECT COUNT(*)
            FROM read_parquet(
                '{temp_file}'
            )
            """
        ).fetchone()[0]

        connection.execute(
            f"""
            INSERT INTO fact_transactions (
                transaction_id,
                step,
                transaction_type_id,
                amount,
                log_amount,
                balance_drain,
                sender_account_id,
                receiver_account_id,
                is_fraud,
                is_flagged_fraud,
                old_balance_sender,
                new_balance_sender,
                old_balance_receiver,
                new_balance_receiver
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

            FROM read_parquet(
                '{temp_file}'
            ) AS chunk

            JOIN dim_account AS sender
                ON chunk.name_orig = sender.name

            JOIN dim_account AS receiver
                ON chunk.name_dest = receiver.name
            """
        )

        total_rows += rows

    logger.info(
        "Rows inserted into fact_transactions: %s",
        total_rows
    )

    return total_rows


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    logger.info(
        "Starting DuckDB loading"
    )

    logger.info(
        "Database path: %s",
        database_path
    )

    logger.info(
        "Chunk size: %s",
        config.chunk_size
    )

    logger.info(
        "Max workers: %s",
        config.max_workers
    )


    # ----------------------------------------------
    # Create database and load schema
    # ----------------------------------------------

    connection = create_database()


    # ----------------------------------------------
    # Load dimensions
    # ----------------------------------------------

    load_transaction_types(
        connection
    )

    load_accounts(
        connection
    )

    load_time(
        connection
    )


    # ----------------------------------------------
    # Load complaints
    # ----------------------------------------------

    load_complaints(
        connection
    )


    # ----------------------------------------------
    # Create transaction chunks
    # ----------------------------------------------

    chunks = create_chunks(
        parquet_path,
        config.chunk_size
    )


    # ----------------------------------------------
    # Close parent connection
    # ----------------------------------------------

    connection.close()


    # ----------------------------------------------
    # Prepare worker arguments
    # ----------------------------------------------

    arguments = []

    for start, end in chunks:

        arguments.append(
            (
                str(parquet_path),
                start,
                end
            )
        )


    # ----------------------------------------------
    # Process chunks in parallel
    # ----------------------------------------------

    logger.info(
        "Starting ProcessPoolExecutor with %s workers",
        config.max_workers
    )

    with ProcessPoolExecutor(
        max_workers=config.max_workers
    ) as executor:

        temp_files = list(
            executor.map(
                process_chunk,
                arguments
            )
        )


    logger.info(
        "Parallel chunk processing completed"
    )


    # ----------------------------------------------
    # Open DuckDB for final insertion
    # ----------------------------------------------

    connection = duckdb.connect(
        str(database_path)
    )

    connection.execute(
        "PRAGMA enable_progress_bar=false"
    )


    # ----------------------------------------------
    # Insert all processed chunks
    # ----------------------------------------------

    total_rows = insert_chunks(
        connection,
        temp_files
    )


    # ----------------------------------------------
    # Verify final row count
    # ----------------------------------------------

    database_rows = connection.execute(
        """
        SELECT COUNT(*)
        FROM fact_transactions
        """
    ).fetchone()[0]

    logger.info(
        "Final fact_transactions row count: %s",
        database_rows
    )


    # ----------------------------------------------
    # Run data quality checks
    # ----------------------------------------------

    logger.info(
        "Starting data quality checks"
    )

    run_quality_checks(
        connection
    )

    logger.info(
        "Data quality checks passed"
    )


    # ----------------------------------------------
    # Close database
    # ----------------------------------------------

    connection.close()


    # ----------------------------------------------
    # Delete temporary files
    # ----------------------------------------------

    for temp_file in temp_files:

        Path(
            temp_file
        ).unlink()


    # ----------------------------------------------
    # Final result
    # ----------------------------------------------

    logger.info(
        "Total rows inserted: %s",
        total_rows
    )

    logger.info(
        "DuckDB loading completed"
    )


# --------------------------------------------------
# Run program
# --------------------------------------------------

if __name__ == "__main__":

    main()