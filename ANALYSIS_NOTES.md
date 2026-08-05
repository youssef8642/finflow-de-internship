# Analysis Notes

## Milestone 1.3 — Parallel Ingestion

### ThreadPoolExecutor Design Choice

`ThreadPoolExecutor` was selected for parallel ingestion because the three ingestion tasks are primarily I/O-bound.

The pipeline performs operations such as reading the PaySim CSV from disk and making network requests to FRED and the CFPB API. Threads allow these I/O operations to overlap while one task is waiting for disk or network activity.

The benchmark showed that the sequential pipeline took 173.38 seconds, while the parallel pipeline took 145.71 seconds. This resulted in a measured speedup of 1.19x.

### What Would Happen With ProcessPoolExecutor?

Using `ProcessPoolExecutor` would create separate Python processes instead of threads. This would provide separate Python interpreters and separate memory spaces for each ingestion task.

For this ingestion workload, processes would add additional overhead because the tasks are mainly I/O-bound. Creating processes and managing separate memory spaces would not provide a major benefit for waiting on network or disk I/O.

`ThreadPoolExecutor` is therefore simpler and more appropriate for this workload.

### When Would ProcessPoolExecutor Be Used?

`ProcessPoolExecutor` would become more appropriate if an ingestion task contained significant CPU-bound processing.

For example, if the pipeline performed expensive data transformations, complex calculations, CPU-heavy parsing, or computational feature engineering on large datasets, multiple processes could execute those CPU-intensive operations in parallel.

Processes would be especially useful for CPU-bound work because separate processes can use multiple CPU cores without being limited by Python's Global Interpreter Lock (GIL).

### Conclusion

For the current ingestion pipeline, `ThreadPoolExecutor` is the appropriate design because the workload is dominated by disk and network I/O.

The measured 1.19x speedup confirms that concurrent execution reduced the overall wall-clock time, although PaySim remained the primary bottleneck.
