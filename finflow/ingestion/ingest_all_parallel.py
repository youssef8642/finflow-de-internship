"""Parallel ingestion and the sequential-vs-parallel benchmark."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

from finflow.config.logger import get_logger
from finflow.ingestion.ingest_all_sequential import (
    IngestionError,
    ingest_complaints,
    ingest_fred,
    ingest_paysim,
    run_sequential,
)


logger = get_logger(__name__)

INGESTION_FUNCTIONS: list[Callable[[], object]] = [
    ingest_paysim,
    ingest_fred,
    ingest_complaints,
]


def run_parallel(max_workers: int = 3) -> float:
    """Run all three ingestion functions concurrently and return elapsed time.

    A failure in one ingest must not cancel the other two, so exceptions are
    collected per future instead of propagating immediately. They are still
    raised at the end -- a half-ingested pipeline should not look successful
    to the caller.
    """
    logger.info("Starting parallel ingestion with %s workers", max_workers)
    start = time.perf_counter()
    failures: list[str] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(function): function.__name__
            for function in INGESTION_FUNCTIONS
        }

        for future in as_completed(futures):
            name = futures[future]

            try:
                logger.info("%s completed successfully: %s", name, future.result())
            # Deliberately broad: whatever one ingest raises, the other two
            # still have to be allowed to finish.
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.error("%s failed: %s", name, exc)
                failures.append(f"{name}: {exc}")

    elapsed = time.perf_counter() - start

    if failures:
        raise IngestionError(
            "Parallel ingestion failed for: " + "; ".join(failures)
        )

    logger.info("Parallel ingestion completed in %.2f seconds", elapsed)
    return elapsed


def benchmark_ingestion(max_workers: int = 3) -> None:
    """Run both pipelines back to back and print the comparison table."""
    logger.info("Running sequential ingestion for the benchmark baseline")
    sequential_time = run_sequential()

    logger.info("Running parallel ingestion for the benchmark")
    parallel_time = run_parallel(max_workers=max_workers)

    speedup = sequential_time / parallel_time

    print("\n" + "=" * 52)
    print("INGESTION BENCHMARK")
    print("=" * 52)
    print(f"{'Method':<24}{'Time (s)':>14}{'Speedup':>14}")
    print("-" * 52)
    print(f"{'Sequential':<24}{sequential_time:>14.2f}{'1.0x':>14}")
    print(f"{f'ThreadPool({max_workers})':<24}{parallel_time:>14.2f}{f'{speedup:.2f}x':>14}")
    print("=" * 52 + "\n")

    logger.info(
        "Sequential: %.2fs | Parallel: %.2fs | Speedup: %.2fx",
        sequential_time,
        parallel_time,
        speedup,
    )


def main() -> None:
    """Run the ingestion benchmark."""
    benchmark_ingestion()


if __name__ == "__main__":
    main()
