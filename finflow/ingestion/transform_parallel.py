"""Parallel transformation of the PaySim dataset."""

from __future__ import annotations

import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from finflow.config.logger import get_logger


logger = get_logger(__name__)


def create_chunks(
    df: pd.DataFrame,
    chunk_size: int,
) -> list[pd.DataFrame]:
    """Split the DataFrame into fixed-size chunks."""

    chunks = []

    for start in range(0, len(df), chunk_size):
        end = start + chunk_size

        chunks.append(
            df.iloc[start:end].copy()
        )

    logger.info(
        "Created %s chunks of approximately %s rows",
        len(chunks),
        chunk_size,
    )

    return chunks


def transform_chunk(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Transform one chunk of the PaySim dataset."""

    transformed = df.copy()

    transformed["step"] = (
        transformed["step"].astype("Int64")
    )

    transformed["is_fraud"] = (
        transformed["is_fraud"].astype("Int64")
    )

    transformed["is_flagged_fraud"] = (
        transformed["is_flagged_fraud"].astype("Int64")
    )

    float_columns = [
        "amount",
        "old_balance_org",
        "new_balance_org",
        "old_balance_dest",
        "new_balance_dest",
    ]

    for column in float_columns:
        transformed[column] = (
            transformed[column].astype("Float64")
        )

    transformed["balance_drain"] = (
        transformed["old_balance_org"]
        - transformed["new_balance_org"]
        - transformed["amount"]
    )

    transformed["log_amount"] = np.log1p(
        transformed["amount"]
    )

    return transformed


def _write_parquet(
    df: pd.DataFrame,
    output_path: Path,
) -> None:
    """Write a DataFrame to Parquet using DuckDB."""

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    connection = duckdb.connect(
        database=":memory:"
    )

    try:
        connection.register(
            "transformed_df",
            df,
        )

        sql_path = str(
            output_path
        ).replace("'", "''")

        connection.execute(
            f"""
            COPY transformed_df
            TO '{sql_path}'
            (FORMAT PARQUET)
            """
        )

    finally:
        connection.close()


def transform_parallel(
    n_workers: int = 4,
    chunk_size: int = 500_000,
) -> Path:
    """Transform PaySim chunks using multiple processes."""

    input_path = (
        Path("data")
        / "processed"
        / "transactions.parquet"
    )

    output_path = (
        Path("data")
        / "processed"
        / f"transactions_transformed_{chunk_size}.parquet"
    )

    logger.info(
        "Reading %s",
        input_path,
    )

    df = duckdb.read_parquet(
        str(input_path)
    ).df()

    logger.info(
        "Loaded %s rows",
        len(df),
    )

    chunks = create_chunks(
        df,
        chunk_size,
    )

    logger.info(
        "Starting parallel transformation with %s workers",
        n_workers,
    )

    start = time.perf_counter()

    with ProcessPoolExecutor(
        max_workers=n_workers,
    ) as executor:

        transformed_chunks = list(
            executor.map(
                transform_chunk,
                chunks,
            )
        )

    transformed = pd.concat(
        transformed_chunks,
        ignore_index=True,
    )

    elapsed = (
        time.perf_counter()
        - start
    )

    logger.info(
        "Parallel transformation completed in %.2f seconds",
        elapsed,
    )

    logger.info(
        "Writing transformed dataset to %s",
        output_path,
    )

    _write_parquet(
        transformed,
        output_path,
    )

    logger.info(
        "Transformed dataset saved to %s",
        output_path,
    )

    return output_path


def main() -> None:
    """Run the parallel transformation."""

    transform_parallel(
        n_workers=4,
        chunk_size=1_000_000,
    )


if __name__ == "__main__":
    main()