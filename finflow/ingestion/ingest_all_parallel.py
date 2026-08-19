"""Milestone 1.3 - Parallel ingestion and benchmark.

Order of this file follows the milestone bullets:
    1. the three ingest functions are imported from Milestone 1.2 (not copied)
    2. run_parallel() using ThreadPoolExecutor
    3. exceptions handled per future with as_completed()
    4. benchmark_ingestion() comparing sequential and parallel
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from finflow.config.logger import get_logger
from finflow.ingestion.ingest_all_sequential import (
    ingest_complaints,
    ingest_fred,
    ingest_paysim,
    run_sequential,
)

logger = get_logger(__name__)


def run_parallel(max_workers: int = 3) -> float:
    """Run the three ingest functions at the same time using threads.

    ThreadPoolExecutor is used because ingestion waits on disk and network,
    which is I/O-bound work. Threads can overlap that waiting even with the GIL.
    """
    logger.info("Starting parallel ingestion with %s workers", max_workers)
    start = time.perf_counter()

    executor = ThreadPoolExecutor(max_workers=max_workers)

    futures = {}
    futures[executor.submit(ingest_paysim)] = "ingest_paysim"
    futures[executor.submit(ingest_fred)] = "ingest_fred"
    futures[executor.submit(ingest_complaints)] = "ingest_complaints"

    for future in as_completed(futures):
        name = futures[future]
        try:
            future.result()
            logger.info("%s finished", name)
        except Exception as error:
            logger.error("%s failed: %s", name, error)

    executor.shutdown()

    elapsed = time.perf_counter() - start
    logger.info("Parallel ingestion took %.2f seconds", elapsed)
    return elapsed


def benchmark_ingestion() -> None:
    """Run both versions and print the comparison table."""
    sequential_time = run_sequential()
    parallel_time = run_parallel()
    speedup = sequential_time / parallel_time

    print("")
    print("  Method        | Time (s) | Speedup")
    print("  --------------|----------|---------")
    print("  Sequential    | %8.2f | 1.0x" % sequential_time)
    print("  ThreadPool(3) | %8.2f | %.1fx" % (parallel_time, speedup))
    print("")


def main() -> None:
    """Entry point for Milestone 1.3."""
    benchmark_ingestion()


if __name__ == "__main__":
    main()
