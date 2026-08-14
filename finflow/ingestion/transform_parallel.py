"""Parallel transformation of the ingested PaySim dataset.

This is where the raw ingested Parquet becomes something the warehouse can
use. Ingestion only renamed columns, so every semantic decision lives here:
the fraud flags become real booleans, step becomes a narrow int, the two
derived columns are computed, and each row gets its surrogate key.
"""

from __future__ import annotations

import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig


logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG = PipelineConfig()

PROCESSED_DIR = PROJECT_ROOT / CONFIG.processed_dir
INPUT_PATH = PROCESSED_DIR / "transactions.parquet"
OUTPUT_PATH = PROCESSED_DIR / "transactions_transformed.parquet"

BALANCE_COLUMNS = [
    "amount",
    "old_balance_org",
    "new_balance_org",
    "old_balance_dest",
    "new_balance_dest",
]


def create_chunks(df: pd.DataFrame, chunk_size: int) -> list[pd.DataFrame]:
    """Split a DataFrame into fixed-size chunks."""
    chunks = [
        df.iloc[start:start + chunk_size].copy()
        for start in range(0, len(df), chunk_size)
    ]

    logger.info("Created %s chunks of up to %s rows", len(chunks), chunk_size)
    return chunks


def transform_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce types and add the derived columns for one chunk."""
    transformed = df.copy()

    # PaySim stores the two fraud flags as 0/1 integers, but the warehouse
    # models them as BOOLEAN -- convert once here rather than at load time.
    transformed["is_fraud"] = transformed["is_fraud"].astype("bool")
    transformed["is_flagged_fraud"] = transformed["is_flagged_fraud"].astype("bool")

    # 743 steps fits comfortably in int32.
    transformed["step"] = transformed["step"].astype("int32")

    for column in BALANCE_COLUMNS:
        transformed[column] = transformed[column].astype("float64")

    # How much of the sender's balance the transaction failed to account for.
    # Should be ~0 for a consistent ledger; Week 3 looks at where it isn't.
    transformed["balance_drain"] = (
        transformed["old_balance_org"]
        - transformed["new_balance_org"]
        - transformed["amount"]
    )

    # log1p rather than log because amount can legitimately be 0.
    transformed["log_amount"] = np.log1p(transformed["amount"])

    return transformed


def transform_sequential(df: pd.DataFrame) -> pd.DataFrame:
    """Single-process baseline used for the Milestone 1.4 benchmark."""
    return transform_chunk(df)


def _write_parquet(df: pd.DataFrame, output_path: Path) -> None:
    """Write a DataFrame to Parquet through DuckDB."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    sql_path = output_path.as_posix().replace("'", "''")

    with duckdb.connect(database=":memory:") as connection:
        connection.execute("PRAGMA enable_progress_bar=false")
        connection.register("transformed_df", df)
        connection.execute(f"COPY transformed_df TO '{sql_path}' (FORMAT PARQUET)")


def read_transactions() -> pd.DataFrame:
    """Load the ingested transactions Parquet into memory."""
    logger.info("Reading %s", INPUT_PATH)

    if not INPUT_PATH.is_file():
        raise FileNotFoundError(
            f"Ingested transactions not found: {INPUT_PATH}. Run ingestion first."
        )

    df = duckdb.read_parquet(INPUT_PATH.as_posix()).df()
    logger.info("Loaded %s rows", len(df))
    return df


def add_transaction_id(df: pd.DataFrame) -> pd.DataFrame:
    """Assign the surrogate key used as the fact table primary key.

    This runs in the parent process after the chunks are concatenated, which
    is the last point in the pipeline where global row order is guaranteed.
    Assigning it inside a worker would tie the key to Parquet scan order,
    which DuckDB does not promise to keep stable.
    """
    df.insert(0, "transaction_id", np.arange(1, len(df) + 1, dtype="int64"))
    return df


def transform_parallel(n_workers: int = 4, chunk_size: int = 1_000_000) -> pd.DataFrame:
    """Transform the dataset across several worker processes."""
    df = read_transactions()
    chunks = create_chunks(df, chunk_size)

    logger.info("Starting parallel transformation with %s workers", n_workers)
    start = time.perf_counter()

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        # executor.map preserves input order, so concat rebuilds the original
        # row order and the surrogate key stays deterministic.
        transformed_chunks = list(executor.map(transform_chunk, chunks))

    transformed = pd.concat(transformed_chunks, ignore_index=True)

    elapsed = time.perf_counter() - start
    logger.info("Parallel transformation completed in %.2f seconds", elapsed)
    return transformed


def benchmark_transform(n_workers: int = 4, chunk_size: int = 1_000_000) -> None:
    """Compare single-process and multi-process transformation."""
    df = read_transactions()

    logger.info("Running single-process transformation")
    sequential_start = time.perf_counter()
    transform_sequential(df)
    sequential_time = time.perf_counter() - sequential_start

    chunks = create_chunks(df, chunk_size)

    logger.info("Running parallel transformation with %s workers", n_workers)
    parallel_start = time.perf_counter()

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        list(executor.map(transform_chunk, chunks))

    parallel_time = time.perf_counter() - parallel_start
    speedup = sequential_time / parallel_time

    print("\n" + "=" * 52)
    print(f"TRANSFORMATION BENCHMARK (chunk size {chunk_size:,})")
    print("=" * 52)
    print(f"{'Method':<24}{'Time (s)':>14}{'Speedup':>14}")
    print("-" * 52)
    print(f"{'Sequential':<24}{sequential_time:>14.2f}{'1.0x':>14}")
    print(f"{f'ProcessPool({n_workers})':<24}{parallel_time:>14.2f}{f'{speedup:.2f}x':>14}")
    print("=" * 52 + "\n")

    logger.info(
        "Sequential: %.2fs | Parallel: %.2fs | Speedup: %.2fx",
        sequential_time,
        parallel_time,
        speedup,
    )


def main() -> None:
    """Transform the dataset and save it for the modeling stage."""
    transformed = add_transaction_id(
        transform_parallel(n_workers=CONFIG.max_workers, chunk_size=1_000_000)
    )

    logger.info("Writing transformed dataset to %s", OUTPUT_PATH)
    _write_parquet(transformed, OUTPUT_PATH)
    logger.info("Transformed dataset saved to %s", OUTPUT_PATH)


if __name__ == "__main__":
    main()
