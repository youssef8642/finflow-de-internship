"""Milestone 3.1 - Transaction distribution and time series analysis.

Order of this file follows the milestone bullets:
    1. time series, computed in parallel, plotted on a 3-panel figure
    2. amount distribution, log(amount + 1), TRANSFER vs CASH_OUT
    3. balance drain distribution
"""

import time
from concurrent.futures import ThreadPoolExecutor

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from scipy.stats import norm

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig

logger = get_logger(__name__)
config = PipelineConfig()

REPORTS_DIR = "finflow/reports"


def run_query(sql: str):
    """Run one query on its own connection and return a DataFrame.

    Each call opens its own connection because these queries run at the same
    time in threads, and one connection cannot be shared between threads.
    """
    connection = duckdb.connect(config.db_path)
    result = connection.execute(sql).fetchdf()
    connection.close()
    return result


def daily_volume():
    """(a) Daily transaction count for each transaction type."""
    return run_query("""
        SELECT t.sim_day, tt.type_name, COUNT(*) AS transaction_count
        FROM fact_transactions AS f
        JOIN dim_time AS t ON f.step = t.step
        JOIN dim_transaction_type AS tt ON f.transaction_type_id = tt.id
        GROUP BY t.sim_day, tt.type_name
        ORDER BY t.sim_day
    """)


def daily_fraud():
    """(b) Daily count of fraudulent transactions."""
    return run_query("""
        SELECT t.sim_day, COUNT(*) AS fraud_count
        FROM fact_transactions AS f
        JOIN dim_time AS t ON f.step = t.step
        WHERE f.is_fraud = TRUE
        GROUP BY t.sim_day
        ORDER BY t.sim_day
    """)


def daily_mean_amount():
    """(c) Daily mean transaction amount for each transaction type."""
    return run_query("""
        SELECT t.sim_day, tt.type_name, AVG(f.amount) AS mean_amount
        FROM fact_transactions AS f
        JOIN dim_time AS t ON f.step = t.step
        JOIN dim_transaction_type AS tt ON f.transaction_type_id = tt.id
        GROUP BY t.sim_day, tt.type_name
        ORDER BY t.sim_day
    """)


def run_sequential():
    """Run the three queries one after another."""
    start = time.perf_counter()

    volume = daily_volume()
    fraud = daily_fraud()
    mean_amount = daily_mean_amount()

    elapsed = time.perf_counter() - start
    logger.info("Sequential took %.4f seconds", elapsed)
    return volume, fraud, mean_amount, elapsed


def run_parallel():
    """Run the three queries at the same time using threads."""
    start = time.perf_counter()

    executor = ThreadPoolExecutor(max_workers=3)
    volume_future = executor.submit(daily_volume)
    fraud_future = executor.submit(daily_fraud)
    mean_future = executor.submit(daily_mean_amount)

    volume = volume_future.result()
    fraud = fraud_future.result()
    mean_amount = mean_future.result()
    executor.shutdown()

    elapsed = time.perf_counter() - start
    logger.info("Parallel took %.4f seconds", elapsed)
    return volume, fraud, mean_amount, elapsed


def plot_time_series(volume, fraud, mean_amount) -> None:
    """Plot the three daily series on one figure with three panels."""
    figure, axes = plt.subplots(3, 1, figsize=(12, 14))

    for type_name in volume["type_name"].unique():
        rows = volume[volume["type_name"] == type_name]
        axes[0].plot(rows["sim_day"], rows["transaction_count"], label=type_name)

    axes[0].set_title("Daily Transaction Volume by Type")
    axes[0].set_xlabel("Simulation Day")
    axes[0].set_ylabel("Transaction Count")
    axes[0].legend()

    axes[1].plot(fraud["sim_day"], fraud["fraud_count"], label="Fraudulent transactions")
    axes[1].set_title("Daily Fraud Count")
    axes[1].set_xlabel("Simulation Day")
    axes[1].set_ylabel("Fraud Count")
    axes[1].legend()

    for type_name in mean_amount["type_name"].unique():
        rows = mean_amount[mean_amount["type_name"] == type_name]
        axes[2].plot(rows["sim_day"], rows["mean_amount"], label=type_name)

    axes[2].set_title("Daily Mean Transaction Amount by Type")
    axes[2].set_xlabel("Simulation Day")
    axes[2].set_ylabel("Mean Amount")
    axes[2].legend()

    figure.tight_layout()
    figure.savefig(REPORTS_DIR + "/transaction_time_series.png", dpi=150)
    plt.close(figure)
    logger.info("Saved transaction_time_series.png")


def amount_distribution_data():
    """Get log_amount for TRANSFER and CASH_OUT."""
    return run_query("""
        SELECT f.log_amount, tt.type_name
        FROM fact_transactions AS f
        JOIN dim_transaction_type AS tt ON f.transaction_type_id = tt.id
        WHERE tt.type_name IN ('TRANSFER', 'CASH_OUT')
    """)


def plot_amount_distribution(data) -> None:
    """Plot the two KDE curves and overlay a fitted normal on each.

    Is the data log-normal? If amount were log-normal then log(amount + 1)
    would be normal, so the dashed line and the filled curve would sit on top
    of each other. They do not: both curves lean to the left and are taller
    and narrower than the normal. That matters for Milestone 4.1, because the
    z-score rule R02 assumes a normal shape when it picks a cut-off of 3
    standard deviations. If the real shape is not normal then that cut-off
    does not catch the share of transactions we think it does, so the
    threshold should be checked against the real percentiles per type.
    """
    figure, axes = plt.subplots(figsize=(12, 7))
    colours = {"TRANSFER": "tab:blue", "CASH_OUT": "tab:orange"}

    for type_name in ["TRANSFER", "CASH_OUT"]:
        values = data[data["type_name"] == type_name]["log_amount"]

        sample = values.sample(n=150_000, random_state=42)

        sns.kdeplot(x=sample, ax=axes, color=colours[type_name], fill=True,
                    alpha=0.3, label=type_name + " (actual)")

        mean = values.mean()
        sd = values.std()
        grid = np.linspace(values.min(), values.max(), 200)

        axes.plot(grid, norm.pdf(grid, mean, sd), color=colours[type_name],
                  linestyle="--", label=type_name + " normal fit")

        logger.info("%s: mean=%.2f sd=%.2f skew=%.2f", type_name, mean, sd, values.skew())

    axes.set_title("Log Transaction Amount: TRANSFER vs CASH_OUT")
    axes.set_xlabel("log(amount + 1)")
    axes.set_ylabel("Density")
    axes.legend()

    figure.tight_layout()
    figure.savefig(REPORTS_DIR + "/amount_distribution.png", dpi=150)
    plt.close(figure)
    logger.info("Saved amount_distribution.png")


def balance_drain_data():
    """Get every balance_drain value."""
    return run_query("SELECT balance_drain FROM fact_transactions")


def balance_drain_percentage() -> float:
    """Percentage of transactions where |balance_drain| is larger than 1.

    This is the data inconsistency indicator the milestone asks for. A drain
    of 0 means the sender balance moved by exactly the transaction amount.
    """
    result = run_query("""
        SELECT
            100.0 * SUM(CASE WHEN ABS(balance_drain) > 1 THEN 1 ELSE 0 END) / COUNT(*)
                AS pct_inconsistent
        FROM fact_transactions
    """)

    percentage = result["pct_inconsistent"][0]
    logger.info("Transactions with |balance_drain| > 1: %.2f%%", percentage)
    return percentage


def plot_balance_drain(data, percentage: float) -> None:
    """Plot the balance_drain distribution as a histogram."""
    figure, axes = plt.subplots(figsize=(12, 6))

    lower = np.percentile(data["balance_drain"], 1)
    clipped = data[data["balance_drain"] >= lower]["balance_drain"]

    axes.hist(clipped, bins=100, label="Transactions")
    axes.set_yscale("log")
    axes.axvline(0, color="black", linestyle="--", label="Consistent (drain = 0)")

    axes.set_title("Distribution of balance_drain (%.2f%% exceed the tolerance)" % percentage)
    axes.set_xlabel("balance_drain")
    axes.set_ylabel("Transaction Count (log scale)")
    axes.legend()

    figure.tight_layout()
    figure.savefig(REPORTS_DIR + "/balance_drain_distribution.png", dpi=150)
    plt.close(figure)
    logger.info("Saved balance_drain_distribution.png")


def main() -> None:
    """Entry point for Milestone 3.1."""
    logger.info("Starting transaction analysis")

    volume, fraud, mean_amount, sequential_time = run_sequential()
    volume, fraud, mean_amount, parallel_time = run_parallel()
    logger.info("Speedup: %.2fx", sequential_time / parallel_time)
    plot_time_series(volume, fraud, mean_amount)

    plot_amount_distribution(amount_distribution_data())

    percentage = balance_drain_percentage()
    plot_balance_drain(balance_drain_data(), percentage)

    logger.info("Transaction analysis completed")


if __name__ == "__main__":
    main()
