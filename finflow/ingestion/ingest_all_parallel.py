"""Parallel ingestion and benchmarking."""

from __future__ import annotations

import time
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
from typing import Callable

from finflow.config.logger import get_logger
from finflow.ingestion.ingest_all_sequential import (
    ingest_complaints,
    ingest_fred,
    ingest_paysim,
    run_sequential,
)


logger = get_logger(__name__)


def run_parallel(max_workers: int = 3) -> None:
    """Run all three ingestion functions concurrently."""

    ingestion_functions: list[Callable] = [
        ingest_paysim,
        ingest_fred,
        ingest_complaints,
    ]

    logger.info(
        "Starting parallel ingestion with %s workers",
        max_workers,
    )

    with ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:

        futures = {
            executor.submit(function): function.__name__
            for function in ingestion_functions
        }

        for future in as_completed(futures):
            function_name = futures[future]

            try:
                result = future.result()

                logger.info(
                    "%s completed successfully: %s",
                    function_name,
                    result,
                )

            except Exception as exc:
                logger.error(
                    "%s failed: %s",
                    function_name,
                    exc,
                )

    logger.info("Parallel ingestion completed")


def benchmark_ingestion() -> None:
    """Compare sequential and parallel ingestion times."""

    print("\nRunning sequential ingestion...")

    sequential_start = time.perf_counter()

    run_sequential()

    sequential_time = (
        time.perf_counter()
        - sequential_start
    )

    print("\nRunning parallel ingestion...")

    parallel_start = time.perf_counter()

    run_parallel()

    parallel_time = (
        time.perf_counter()
        - parallel_start
    )

    speedup = (
        sequential_time / parallel_time
        if parallel_time > 0
        else 0
    )

    print("\n" + "=" * 60)
    print("INGESTION BENCHMARK")
    print("=" * 60)

    print(
        f"{'Method':<20}"
        f"{'Time (seconds)':>20}"
    )

    print("-" * 60)

    print(
        f"{'Sequential':<20}"
        f"{sequential_time:>20.2f}"
    )

    print(
        f"{'Parallel':<20}"
        f"{parallel_time:>20.2f}"
    )

    print("-" * 60)

    print(
        f"{'Speedup':<20}"
        f"{speedup:>20.2f}x"
    )

    print("=" * 60)


def main() -> None:
    """Run the ingestion benchmark."""

    benchmark_ingestion()


if __name__ == "__main__":
    main()