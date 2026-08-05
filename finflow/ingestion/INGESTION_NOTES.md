# Ingestion Notes

## Benchmark Results

The ingestion pipeline was tested using both sequential and parallel execution.

| Method     | Time (seconds) |
| ---------- | -------------: |
| Sequential |         173.38 |
| Parallel   |         145.71 |
| Speedup    |          1.19x |

The parallel implementation reduced the total wall-clock time by approximately 16%.

## Why Parallel Ingestion Was Faster

The ingestion tasks involve I/O operations such as reading the PaySim CSV file, requesting data from FRED, and downloading CFPB complaints from the internet.

Using `ThreadPoolExecutor` allowed these operations to overlap instead of waiting for each ingestion task to finish before starting the next one.

The biggest workload was PaySim, which processed 6,362,620 rows and took approximately 145 seconds during the parallel run. FRED and CFPB could run while PaySim was being processed, reducing the overall wall-clock time.

The speedup was therefore limited because PaySim remained the main bottleneck.

## Race Conditions and Shared State

No significant race conditions were encountered.

Each ingestion function writes to a separate output:

* PaySim → `data/processed/transactions.parquet`
* FRED → `data/raw/macro/`
* CFPB → `data/processed/complaints.parquet`

Because the ingestion functions use separate output files and do not modify shared in-memory data structures, they can safely run concurrently.

The main consideration is that repeated benchmark runs access the same output files. Each ingestion function writes its own output, so there is no conflict between the three different ingestion tasks.

## Conclusion

Thread-based parallelism was appropriate because the ingestion pipeline contains significant I/O activity. The measured result was a 1.19x speedup, reducing execution time from 173.38 seconds to 145.71 seconds.
