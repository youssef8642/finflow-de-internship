import os
import time
import requests
import psutil
from typing import Union
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor


URLS = [
    "https://api.stlouisfed.org/fred/category?category_id=125&api_key=abcdefghijklmnopqrstuvwxyz123456&file_type=json",
    "https://api.stlouisfed.org/fred/category/children?category_id=13&api_key=abcdefghijklmnopqrstuvwxyz123456&file_type=json",
    "https://api.stlouisfed.org/fred/category/related?category_id=32073&api_key=abcdefghijklmnopqrstuvwxyz123456&file_type=json",
    "https://api.stlouisfed.org/fred/category/series?category_id=125&api_key=abcdefghijklmnopqrstuvwxyz123456&file_type=json",
    "https://api.stlouisfed.org/fred/series?series_id=GNPCA&api_key=abcdefghijklmnopqrstuvwxyz123456&file_type=json"
]

inputs = [20_000_000, 20_000_000, 20_000_000, 20_000_000]


def fetch_sequential(urls):
    sizes = []

    for url in urls:
        response = requests.get(url)
        sizes.append(len(response.content))  # bytes
    return sizes


def fetch_parallel(urls):
    with ThreadPoolExecutor(max_workers=5) as executor:
        responses = list(executor.map(requests.get, urls))
    sizes = [len(response.content) for response in responses]
    return sizes


def compute_heavy(n: int) -> int:
    if n < 2:
        return 0

    primes = bytearray(b"\x01") * n
    primes[0:2] = b"\x00\x00"

    limit = int(n ** 0.5) + 1
    for p in range(2, limit):
        if primes[p]:
            start = p * p
            count = ((n - 1 - start) // p) + 1
            primes[start:n:p] = b"\x00" * count

    return sum(index for index, is_prime in enumerate(primes) if is_prime)


def _cpu_seconds(cpu_times) -> float:
    return cpu_times.user + cpu_times.system


def _ctx_switches(ctx_switches) -> int:
    return ctx_switches.voluntary + ctx_switches.involuntary


def run_sequential(inputs: list[int]) -> list[int]:
    return [compute_heavy(n) for n in inputs]


def run_parallel(inputs: list[int]) -> tuple[list[int], dict[str, Union[float, int]]]:
    process = psutil.Process()
    worker_count = min(len(inputs), os.cpu_count() or 1)

    startup_start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        executor.submit(os.getpid).result()
        startup_time = time.perf_counter() - startup_start

        children_before = process.children(recursive=False)
        parent_cpu_before = _cpu_seconds(process.cpu_times())
        parent_ctx_before = _ctx_switches(process.num_ctx_switches())
        child_cpu_before = {
            child.pid: _cpu_seconds(child.cpu_times()) for child in children_before
        }
        child_ctx_before = {
            child.pid: _ctx_switches(child.num_ctx_switches()) for child in children_before
        }
        mem_before = process.memory_info().rss

        compute_start = time.perf_counter()
        results = list(executor.map(compute_heavy, inputs))
        compute_time = time.perf_counter() - compute_start

        children_after = process.children(recursive=False)
        parent_cpu_after = _cpu_seconds(process.cpu_times())
        parent_ctx_after = _ctx_switches(process.num_ctx_switches())
        child_cpu_after = {
            child.pid: _cpu_seconds(child.cpu_times()) for child in children_after
        }
        child_ctx_after = {
            child.pid: _ctx_switches(child.num_ctx_switches()) for child in children_after
        }
        mem_after = process.memory_info().rss

    total_child_cpu = sum(
        child_cpu_after.get(pid, 0.0) - child_cpu_before.get(pid, 0.0)
        for pid in child_cpu_after
    )
    total_child_ctx = sum(
        child_ctx_after.get(pid, 0) - child_ctx_before.get(pid, 0)
        for pid in child_ctx_after
    )

    total_cpu_time = (parent_cpu_after - parent_cpu_before) + total_child_cpu
    total_ctx_switches = (parent_ctx_after - parent_ctx_before) + total_child_ctx
    logical_cores = psutil.cpu_count(logical=True) or 1
    cpu_utilization = (
        (total_cpu_time / (compute_time * logical_cores)) * 100
        if compute_time > 0
        else 0.0
    )

    metrics = {
        "total_time": startup_time + compute_time,
        "startup_time": startup_time,
        "compute_time": compute_time,
        "cpu_utilization": cpu_utilization,
        "cpu_cores": logical_cores,
        "context_switches": total_ctx_switches,
        "memory_before_mb": mem_before / (1024 * 1024),
        "memory_after_mb": mem_after / (1024 * 1024),
        "process_count": 1 + len(children_after),
    }
    return results, metrics


if __name__ == "__main__":
    seq_start = time.perf_counter()
    seq_sizes = fetch_sequential(URLS)
    seq_time = time.perf_counter() - seq_start

    par_start = time.perf_counter()
    par_sizes = fetch_parallel(URLS)
    par_time = time.perf_counter() - par_start

    speedup = seq_time / par_time if par_time > 0 else float("inf")

    print("Part A - I/O-bound benchmark")
    print("URLs:")
    for url in URLS:
        print(" -", url)
    print("Sequential response sizes (bytes):", seq_sizes)
    print("Parallel response sizes (bytes):", par_sizes)
    print("Method | Time (s) | Speedup")
    print("--------------|----------|---------")
    print(f"Sequential | {seq_time:.2f} | 1.0x")
    print(f"ThreadPool(5) | {par_time:.2f} | {speedup:.1f}x")

    if seq_sizes != par_sizes:
        print("Warning: sequential and parallel response sizes differ.")

    print()
    print("Part B - CPU-bound benchmark")
    print("Inputs:", inputs)

    seq_start = time.perf_counter()
    seq_results = run_sequential(inputs)
    seq_time = time.perf_counter() - seq_start

    par_results, par_metrics = run_parallel(inputs)
    par_time = par_metrics["total_time"]

    speedup = seq_time / par_time if par_time > 0 else float("inf")

    print("Sequential results:", seq_results)
    print("Parallel results:", par_results)
    print("Method | Time (s) | Speedup")
    print("--------------|----------|---------")
    print(f"Sequential | {seq_time:.2f} | 1.0x")
    print(f"ProcessPool | {par_time:.2f} | {speedup:.1f}x")

    print()
    print("Parallel breakdown")
    print(f"Pool startup time: {par_metrics['startup_time']:.4f} s")
    print(f"Actual computation time: {par_metrics['compute_time']:.4f} s")
    print(f"CPU utilization: {par_metrics['cpu_utilization']:.1f}%")
    print(f"CPU cores: {par_metrics['cpu_cores']}")
    print(f"Context switches: {par_metrics['context_switches']}")
    print(
        f"Memory usage: {par_metrics['memory_before_mb']:.1f} MB -> "
        f"{par_metrics['memory_after_mb']:.1f} MB"
    )
    print(f"Process count: {par_metrics['process_count']}")

    if seq_results != par_results:
        print("Warning: sequential and parallel results differ.")

# ThreadPoolExecutor works well in Part A because the requests spend most of their time waiting on network I/O instead of using the CPU.
# That waiting time lets other threads run, so the total download time drops even though the work is still done in one Python process.
# Part B is CPU-bound, so the Global Interpreter Lock (GIL) keeps only one thread executing Python bytecode at a time, which is why ProcessPoolExecutor is needed to use multiple cores.