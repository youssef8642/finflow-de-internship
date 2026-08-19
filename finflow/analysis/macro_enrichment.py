"""Milestone 3.3 - Macro enrichment with FRED indicators.

Design Choice D: the two correlations investigated here are

    D1 - CPI vs TRANSFER volume
         hypothesis: inflation pushes people to move money electronically faster
    D2 - Unemployment rate vs CASH_OUT volume
         hypothesis: higher unemployment means more cash withdrawals as
         people draw down their savings

Order of this file follows the milestone bullets:
    1. map PaySim steps to calendar months
    2. join to the FRED data by month
    3. Pearson, Spearman and an OLS regression for each correlation
    4. plots, and the note on causal vs spurious at the bottom of the file
"""

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
from scipy.stats import linregress, pearsonr, spearmanr

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig

logger = get_logger(__name__)
config = PipelineConfig()

REPORTS_DIR = "finflow/reports"
MACRO_DIR = config.raw_dir + "/macro"

START_MONTH = "2019-01-01"


def monthly_volume():
    """Monthly transaction counts for TRANSFER and CASH_OUT.

    dim_time.sim_day is already ((step - 1) // 24) + 1, and the milestone
    maps 24 steps to one month, so sim_day is the month number 1 to 31.
    """
    connection = duckdb.connect(config.db_path)
    volume = connection.execute("""
        SELECT
            t.sim_day AS month_number,
            SUM(CASE WHEN tt.type_name = 'TRANSFER' THEN 1 ELSE 0 END) AS transfer_count,
            SUM(CASE WHEN tt.type_name = 'CASH_OUT' THEN 1 ELSE 0 END) AS cash_out_count,
            COUNT(*) AS total_count
        FROM fact_transactions AS f
        JOIN dim_time AS t ON f.step = t.step
        JOIN dim_transaction_type AS tt ON f.transaction_type_id = tt.id
        GROUP BY t.sim_day
        ORDER BY t.sim_day
    """).fetchdf()
    connection.close()

    volume["month"] = pd.date_range(START_MONTH, periods=len(volume), freq="MS")

    logger.info("Built %s months of transaction volume", len(volume))
    return volume


def read_fred(series_id: str):
    """Read one FRED series CSV into a DataFrame with month and value."""
    path = MACRO_DIR + "/" + series_id + ".csv"

    series = pd.read_csv(path, names=["month", series_id], skiprows=1)
    series["month"] = pd.to_datetime(series["month"])

    return series


def build_dataset():
    """Join the monthly transaction volume to CPI and unemployment."""
    volume = monthly_volume()

    dataset = volume.merge(read_fred("CPIAUCSL"), on="month", how="inner")
    dataset = dataset.merge(read_fred("UNRATE"), on="month", how="inner")

    logger.info("Joined dataset has %s rows", len(dataset))

    if len(dataset) != len(volume):
        raise ValueError("The FRED join lost rows, check the month alignment")

    return dataset


def correlate(dataset, x_column: str, y_column: str, label: str):
    """Print Pearson, Spearman and the OLS line for one pair of columns."""
    x_values = dataset[x_column]
    y_values = dataset[y_column]

    pearson_r, pearson_p = pearsonr(x_values, y_values)
    spearman_r, spearman_p = spearmanr(x_values, y_values)
    regression = linregress(x_values, y_values)

    logger.info("--- %s ---", label)
    logger.info("Pearson  r = %.4f (p = %.4f)", pearson_r, pearson_p)
    logger.info("Spearman r = %.4f (p = %.4f)", spearman_r, spearman_p)
    logger.info("OLS slope = %.2f, intercept = %.2f, r_squared = %.4f",
                regression.slope, regression.intercept, regression.rvalue ** 2)

    return {
        "correlation": label,
        "pearson_r": pearson_r,
        "pearson_p": pearson_p,
        "spearman_r": spearman_r,
        "spearman_p": spearman_p,
        "ols_slope": regression.slope,
        "ols_intercept": regression.intercept,
        "r_squared": regression.rvalue ** 2,
    }


def plot_side_by_side(dataset, macro_column: str, volume_column: str,
                      macro_label: str, volume_label: str, filename: str) -> None:
    """Plot the macro series and the transaction series next to each other."""
    figure, axes = plt.subplots(1, 2, figsize=(14, 5))

    axes[0].plot(dataset["month"], dataset[macro_column], color="tab:red", label=macro_label)
    axes[0].set_title(macro_label)
    axes[0].set_xlabel("Month")
    axes[0].set_ylabel(macro_label)
    axes[0].legend()

    axes[1].plot(dataset["month"], dataset[volume_column], color="tab:blue", label=volume_label)
    axes[1].set_title(volume_label)
    axes[1].set_xlabel("Month")
    axes[1].set_ylabel("Transaction Count")
    axes[1].legend()

    figure.tight_layout()
    figure.savefig(REPORTS_DIR + "/" + filename, dpi=150)
    plt.close(figure)
    logger.info("Saved %s", filename)


def main() -> None:
    """Entry point for Milestone 3.3."""
    logger.info("Starting macro enrichment")

    dataset = build_dataset()

    d1 = correlate(dataset, "CPIAUCSL", "transfer_count", "D1 CPI vs TRANSFER volume")
    plot_side_by_side(dataset, "CPIAUCSL", "transfer_count",
                      "CPI (index)", "TRANSFER volume", "macro_d1_cpi_transfer.png")

    d2 = correlate(dataset, "UNRATE", "cash_out_count", "D2 Unemployment vs CASH_OUT volume")
    plot_side_by_side(dataset, "UNRATE", "cash_out_count",
                      "Unemployment rate (%)", "CASH_OUT volume", "macro_d2_unrate_cashout.png")

    early = dataset[dataset["month_number"] <= 18]
    logger.info("=== months 1-18 only ===")
    correlate(early, "CPIAUCSL", "transfer_count", "D1 (months 1-18)")
    correlate(early, "UNRATE", "cash_out_count", "D2 (months 1-18)")

    results = pd.DataFrame([d1, d2])
    results.to_csv(REPORTS_DIR + "/macro_correlations.csv", index=False)
    print(results.to_string(index=False))

    logger.info("Macro enrichment completed")


if __name__ == "__main__":
    main()
