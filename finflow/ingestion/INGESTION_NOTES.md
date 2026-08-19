# Ingestion Notes

Milestones 1.2, 1.3 and 1.4. All timings below are from runs on this machine
(16 logical cores, Windows, local SSD) and are reproducible with:

```
python -m finflow.ingestion.ingest_all_parallel
```

---

## Milestone 1.3 — Parallel Ingestion

### Design Choice A — why ThreadPoolExecutor

The three ingestion tasks are I/O-bound. `ingest_paysim` reads a 493 MB CSV from
disk, `ingest_fred` makes three HTTPS calls to the FRED API, and
`ingest_complaints` reads a 9 GB CSV from disk. In all three cases most of the
elapsed time is spent waiting for bytes to arrive rather than executing Python
bytecode.

Python releases the GIL while a thread is blocked on disk or network I/O, so
threads genuinely overlap that waiting. `ThreadPoolExecutor` is therefore the
right tool, and it is far cheaper than processes: threads share one memory space,
so nothing has to be serialised between them.

### Measured benchmark

| Method | Time (s) | Speedup |
|---|---:|---:|
| Sequential | 95.53 | 1.0x |
| ThreadPool(3) | 88.62 | 1.08x |

### Why the speedup is only 1.08x

The per-task timings from inside the parallel run explain it completely:

| Task | Time (s) |
|---|---:|
| `ingest_fred` | 4.34 |
| `ingest_paysim` | 18.20 |
| `ingest_complaints` | 88.53 |
| **Whole parallel run** | **88.62** |

The parallel wall-clock time is 88.62 seconds and the slowest single task is
88.53 seconds. FRED and PaySim finished entirely inside the window that
complaints was still running in, so they became free — but the total can never
drop below the longest single task. This is Amdahl's law: the achievable speedup
is capped by the part that cannot be divided, and here one task is 93% of the
sequential work.

The complaints file is the bottleneck because it is 9 GB and contains 17,004,291
rows, of which only 718,623 survive the product filter. Everything else is
rounding error next to it.

An earlier version of this pipeline measured a *higher* speedup of 1.19x, and it
went down after PaySim ingestion was optimised from 148 seconds to 18. That looks
backwards until you look at Amdahl's law again: making a non-bottleneck task
faster reduces the sequential baseline without changing the parallel floor, so
the ratio between them shrinks. The pipeline got much faster in absolute terms
(the whole sequential run fell from about 173 seconds to 95) while the speedup
number got worse. The speedup ratio on its own is a misleading measure of
improvement.

To actually improve this further, the complaints ingest itself would have to be
split — for example by reading disjoint row ranges of the CSV in parallel — since
no amount of task-level threading can beat a single 88-second task.

### What would happen with ProcessPoolExecutor instead?

Each ingest function would run in a separate OS process with its own interpreter
and its own memory. Three things would get worse:

1. **Startup cost.** Each process has to be spawned and has to re-import pandas,
   duckdb and fredapi, which costs on the order of a second per worker before any
   work begins.
2. **Serialisation.** Return values cross a process boundary by being pickled.
   These functions return short path strings so that is cheap here, but if they
   returned DataFrames it would be very expensive.
3. **Memory.** Three processes each holding their own copy of the interpreter and
   libraries, instead of three threads sharing one.

And nothing would get better, because the GIL was never the constraint — the
threads are blocked on I/O, not competing for the interpreter.

### When would you switch to processes for ingestion?

When the ingest functions start doing real CPU work rather than waiting. If a
step had to decompress and parse a large file, decrypt it, or run heavy
validation or feature engineering on every row, that work holds the GIL and
threads would serialise on it. At that point processes become worth their
overhead. The rule of thumb: threads for waiting, processes for computing.

### Race conditions and shared state

No race conditions occurred, and this is by construction rather than luck. Each
ingest function writes to a different destination:

- `ingest_paysim` to `data/processed/transactions.parquet`
- `ingest_fred` to `data/raw/macro/*.csv`
- `ingest_complaints` to `data/processed/complaints.parquet`

They share no mutable in-memory state, and no two of them touch the same file. The
only shared object is the module-level `config`, which is read-only.

The one real hazard is DuckDB. `save_parquet` opens a fresh in-memory DuckDB
connection each time it is called rather than sharing one, because a DuckDB
connection is not safe to use from several threads at once. Sharing a single
connection across the three threads would be a genuine race.

The logger is safe to share — the standard library `logging` module is
thread-safe, which is why interleaved log lines from the three tasks never
corrupt each other.

---

## Milestone 1.4 — Parallel Transformation

### Design Choice B — chunk size

Three chunk sizes were measured, 4 worker processes each, on the full 6,362,620
rows:

| Chunk size | Chunks | Time (s) |
|---:|---:|---:|
| 500,000 | 13 | 16.48 |
| **1,000,000** | **7** | **15.88** |
| 2,000,000 | 4 | 19.57 |

**1,000,000 rows was chosen**, since it was fastest, but the margin over 500,000
is only 3.6% and is close to run-to-run noise. The 2,000,000 result is clearly
worse for a structural reason: 4 chunks across 4 workers means a single straggler
delays the whole batch, and there is no spare work to fill the gap.

### Memory versus CPU trade-off

Smaller chunks use less memory per worker and spread the work more evenly, so a
slow chunk is less able to hold up the batch. The cost is more tasks, and every
task has to be pickled out to a worker and its result pickled back, so overhead
grows with the number of chunks.

Larger chunks cut that per-task overhead but raise peak memory — each worker holds
a bigger DataFrame, and the parent process holds all of the returned chunks at
once before concatenating. Fewer chunks also means coarser scheduling, which is
what hurt the 2,000,000 case.

### The result that matters: parallelism made this slower

| Method | Time (s) | Speedup |
|---|---:|---:|
| Sequential, single process | **0.14** | 1.0x |
| ProcessPool(4), 1M chunks | 15.77 | **0.01x** |

Running the transformation across 4 processes is roughly **113 times slower**
than doing it in one.

The milestone's premise is that "applying column transformations sequentially is
slow" on 6.3 million rows. Measured, it is not. The transformation is three
`astype` calls, one subtraction and one `np.log1p` — all vectorised numpy
operations that run as compiled C loops over contiguous arrays, releasing the GIL
and using SIMD instructions. The entire job over 6.3 million rows takes 0.14
seconds.

Against that, the cost of parallelising is enormous. Every chunk must be pickled
from the parent process into a worker, and every transformed chunk pickled back —
several gigabytes of DataFrame crossing process boundaries in both directions. The
15.77 seconds is almost entirely serialisation. There is only 0.14 seconds of
actual work available to divide, so there is nothing to win and a great deal to
lose.

The general lesson is that `ProcessPoolExecutor` only pays off when the work per
item is large relative to the cost of moving that item between processes. Here
the ratio is roughly 100:1 in the wrong direction. Parallelism is not free, and
applying it to already-vectorised numpy code is a reliable way to make a program
slower.

The implementation is kept because the milestone requires it and because the
benchmark demonstrating this is itself the deliverable. In production the correct
choice would be the single-process version.

### Where the time in `main()` actually goes

A full `transform_parallel` run takes about 24 seconds end to end, and almost none
of it is the transformation:

- reading the 265 MB Parquet into pandas: ~3 s
- splitting into chunks (a copy per chunk): ~1 s
- pickling to workers, transforming, pickling back: ~16 s
- concatenating and writing the output Parquet: ~4 s

Replacing the process pool with a direct call would cut this to roughly 8 seconds.
