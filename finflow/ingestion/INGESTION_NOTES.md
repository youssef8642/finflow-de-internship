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

# Ingestion Notes

## Parallel Transformation — Chunk Size Analysis

The PaySim dataset contains 6,362,620 rows. The transformation was performed using `ProcessPoolExecutor` with 4 workers.

The transformation adds two derived columns:

* `balance_drain = old_balance_org - new_balance_org - amount`
* `log_amount = log1p(amount)`

### Chunk Size Experiments

| Chunk Size | Number of Chunks |   Run 1 |   Run 2 |   Best Time |
| ---------: | ---------------: | ------: | ------: | ----------: |
|    500,000 |               13 | 18.59 s |       — |     18.59 s |
|  1,000,000 |                7 | 17.87 s | 16.62 s | **16.62 s** |
|  2,000,000 |                4 | 18.77 s | 18.33 s |     18.33 s |

The 1,000,000-row chunk size produced the fastest observed transformation time at **16.62 seconds**.

### Why 1,000,000 Rows Was Chosen

A chunk size of 1,000,000 provides a good balance between parallelism and processing overhead.

With 500,000-row chunks, the dataset is divided into 13 chunks. This provides more individual tasks, but also creates additional overhead when transferring DataFrames between the main process and worker processes.

With 2,000,000-row chunks, only 4 chunks are created. This reduces task-management overhead, but each process must handle a much larger DataFrame, increasing memory usage and reducing flexibility in distributing work.

The 1,000,000-row configuration creates 7 chunks, providing enough work for the 4 worker processes while avoiding the larger memory requirements of 2,000,000-row chunks.

### Memory vs CPU Trade-Off

Smaller chunks generally use less memory per individual task and provide more opportunities for the worker processes to receive work. However, too many small chunks increase process communication, serialization, and scheduling overhead.

Larger chunks reduce the number of tasks and therefore reduce scheduling and serialization overhead. However, they require more memory per worker and can reduce the benefits of parallelism because there are fewer tasks to distribute.

Therefore:

* **500,000 rows:** lower per-task memory usage, but more chunks and more overhead.
* **1,000,000 rows:** balanced memory usage, task count, and CPU utilization.
* **2,000,000 rows:** fewer chunks and less scheduling overhead, but higher memory requirements and less task granularity.

Based on the measured results, **1,000,000 rows per chunk was selected** for the final configuration.

### Final Configuration

```text
ProcessPoolExecutor workers: 4
Chunk size: 1,000,000 rows
Number of chunks: 7
Best measured transformation time: 16.62 seconds
```

The transformed dataset is saved as:

```text
data/processed/transactions_transformed_1000000.parquet
```
