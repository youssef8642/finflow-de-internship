"""Milestone 3.4 - Complaint trend analysis (stretch goal).

Order of this file follows the milestone bullets:
    1. dual-line monthly complaint chart, Credit card vs Checking/savings
    2. top 5 issues per product using a RANK() window function
    3. flag spike months: volume > mean + 2 * std of a 6-month rolling window
    4. cross-reference the spike months with the macro indicators from 3.3
"""

import duckdb
import matplotlib.pyplot as plt
import pandas as pd

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig

logger = get_logger(__name__)
config = PipelineConfig()

REPORTS_DIR = "finflow/reports"
MACRO_DIR = config.raw_dir + "/macro"


def run_query(sql: str):
    """Run one query and return a DataFrame."""
    connection = duckdb.connect(config.db_path)
    result = connection.execute(sql).fetchdf()
    connection.close()
    return result


def monthly_complaints():
    """Complaint count per product per month."""
    return run_query("""
        SELECT
            product,
            DATE_TRUNC('month', date_received) AS month,
            COUNT(*) AS complaint_count
        FROM complaints
        GROUP BY product, DATE_TRUNC('month', date_received)
        ORDER BY month
    """)


def plot_monthly_complaints(complaints) -> None:
    """Dual-line chart, one line per product."""
    figure, axes = plt.subplots(figsize=(13, 6))

    for product in complaints["product"].unique():
        rows = complaints[complaints["product"] == product]
        axes.plot(rows["month"], rows["complaint_count"], label=product)

    axes.set_title("Monthly CFPB Complaints by Product")
    axes.set_xlabel("Month")
    axes.set_ylabel("Complaint Count")
    axes.legend()

    figure.tight_layout()
    figure.savefig(REPORTS_DIR + "/complaints_monthly.png", dpi=150)
    plt.close(figure)
    logger.info("Saved complaints_monthly.png")


def top_issues():
    """Top 5 issues for each product, using RANK() in a window function."""
    return run_query("""
        WITH issue_counts AS (
            SELECT product, issue, COUNT(*) AS complaint_count
            FROM complaints
            GROUP BY product, issue
        ),
        ranked AS (
            SELECT
                product,
                issue,
                complaint_count,
                RANK() OVER (PARTITION BY product ORDER BY complaint_count DESC) AS issue_rank
            FROM issue_counts
        )
        SELECT product, issue_rank, issue, complaint_count
        FROM ranked
        WHERE issue_rank <= 5
        ORDER BY product, issue_rank
    """)


def find_spike_months(complaints):
    """Flag months where complaints exceed the rolling mean plus 2 std.

    The window is the previous 6 months, so a spike is judged against what
    was normal just before it rather than against the whole history.
    """
    spikes = []

    for product in complaints["product"].unique():
        rows = complaints[complaints["product"] == product].copy()
        rows = rows.sort_values("month")

        rolling_mean = rows["complaint_count"].rolling(6).mean()
        rolling_std = rows["complaint_count"].rolling(6).std()

        rows["threshold"] = rolling_mean + 2 * rolling_std
        rows["is_spike"] = rows["complaint_count"] > rows["threshold"]

        spikes.append(rows[rows["is_spike"]])

    result = pd.concat(spikes, ignore_index=True)
    logger.info("Found %s spike months", len(result))
    return result


def add_macro_context(spikes):
    """Attach the CPI and unemployment values for each spike month."""
    cpi = pd.read_csv(MACRO_DIR + "/CPIAUCSL.csv", names=["month", "cpi"], skiprows=1)
    unrate = pd.read_csv(MACRO_DIR + "/UNRATE.csv", names=["month", "unrate"], skiprows=1)

    cpi["month"] = pd.to_datetime(cpi["month"])
    unrate["month"] = pd.to_datetime(unrate["month"])

    spikes = spikes.copy()
    spikes["month"] = pd.to_datetime(spikes["month"])

    spikes = spikes.merge(cpi, on="month", how="left")
    spikes = spikes.merge(unrate, on="month", how="left")

    return spikes[["product", "month", "complaint_count", "threshold", "cpi", "unrate"]]


def main() -> None:
    """Entry point for Milestone 3.4."""
    logger.info("Starting complaint trend analysis")

    complaints = monthly_complaints()
    plot_monthly_complaints(complaints)

    issues = top_issues()
    print(issues.to_string(index=False))
    issues.to_csv(REPORTS_DIR + "/complaints_top_issues.csv", index=False)

    spikes = find_spike_months(complaints)
    spikes_with_macro = add_macro_context(spikes)

    print(spikes_with_macro.to_string(index=False))
    spikes_with_macro.to_csv(REPORTS_DIR + "/complaints_spike_months.csv", index=False)

    logger.info("Complaint trend analysis completed")


if __name__ == "__main__":
    main()
