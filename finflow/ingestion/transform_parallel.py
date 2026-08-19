"""Milestone 1.4 - Parallel transformation with ProcessPoolExecutor.

Order of this file follows the milestone bullets:
    1. split the parquet into chunks
    2. transform_chunk() - type coercion plus the two derived columns
    3. transform_parallel() using ProcessPoolExecutor
    4. benchmark against the single-process version
"""

import time
from concurrent.futures import ProcessPoolExecutor

import duckdb
import numpy as np
import pandas as pd

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig
from finflow.ingestion.ingest_all_sequential import save_parquet

logger = get_logger(__name__)
config = PipelineConfig()

INPUT_PATH = config.processed_dir + "/transactions.parquet"
OUTPUT_PATH = config.processed_dir + "/transactions_transformed.parquet"


def read_transactions() -> pd.DataFrame:
    """Read the ingested transactions Parquet into a DataFrame."""
    connection = duckdb.connect()
    df = connection.execute("SELECT * FROM read_parquet('" + INPUT_PATH + "')").fetchdf()
    connection.close()
    logger.info("Loaded %s rows", len(df))
    return df


def split_into_chunks(df: pd.DataFrame, chunk_size: int) -> list:
    """Cut the DataFrame into a list of smaller DataFrames."""
    chunks = []
    start = 0
    while start < len(df):
        chunks.append(df.iloc[start:start + chunk_size].copy())
        start = start + chunk_size

    logger.info("Split into %s chunks of %s rows", len(chunks), chunk_size)
    return chunks


def transform_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """Apply type coercion and add the two derived columns to one chunk."""
    df["is_fraud"] = df["is_fraud"].astype(bool)
    df["is_flagged_fraud"] = df["is_flagged_fraud"].astype(bool)
    df["step"] = df["step"].astype(int)

    df["balance_drain"] = df["old_balance_org"] - df["new_balance_org"] - df["amount"]

    df["log_amount"] = np.log1p(df["amount"])

    return df


def transform_sequential(df: pd.DataFrame) -> pd.DataFrame:
    """Single-process version, used as the benchmark baseline."""
    return transform_chunk(df)


def transform_parallel(n_workers: int = 4, chunk_size: int = 1_000_000) -> pd.DataFrame:
    """Transform every chunk in a separate process, then join them back together.

    ProcessPoolExecutor is used here because this is CPU-bound work. Each
    process has its own interpreter, so the GIL does not block them.
    """
    df = read_transactions()
    chunks = split_into_chunks(df, chunk_size)

    executor = ProcessPoolExecutor(max_workers=n_workers)
    results = list(executor.map(transform_chunk, chunks))
    executor.shutdown()

    return pd.concat(results, ignore_index=True)


def add_transaction_id(df: pd.DataFrame) -> pd.DataFrame:
    """Give every row a unique id, used as the fact table primary key.

    This happens after the chunks are joined back together, which is the last
    point where the rows are guaranteed to be in their original order.
    """
    df.insert(0, "transaction_id", range(1, len(df) + 1))
    return df


def benchmark_transform(n_workers: int = 4, chunk_size: int = 1_000_000) -> None:
    """Time the single-process version against the parallel one."""
    df = read_transactions()

    start = time.perf_counter()
    transform_sequential(df)
    sequential_time = time.perf_counter() - start

    df = read_transactions()
    chunks = split_into_chunks(df, chunk_size)

    start = time.perf_counter()
    executor = ProcessPoolExecutor(max_workers=n_workers)
    list(executor.map(transform_chunk, chunks))
    executor.shutdown()
    parallel_time = time.perf_counter() - start

    speedup = sequential_time / parallel_time

    print("")
    print("  Method         | Time (s) | Speedup")
    print("  ---------------|----------|---------")
    print("  Sequential     | %8.2f | 1.0x" % sequential_time)
    print("  ProcessPool(%s) | %8.2f | %.1fx" % (n_workers, parallel_time, speedup))
    print("")


def main() -> None:
    """Entry point for Milestone 1.4."""
    start = time.perf_counter()

    transformed = transform_parallel(config.max_workers, 1_000_000)
    transformed = add_transaction_id(transformed)

    save_parquet(transformed, OUTPUT_PATH)

    logger.info("Transformation took %.2f seconds", time.perf_counter() - start)
    logger.info("Saved to %s", OUTPUT_PATH)


if __name__ == "__main__":
    main()
