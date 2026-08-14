import duckdb
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import time

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from scipy.stats import norm

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig


# --------------------------------------------------
# Logger
# --------------------------------------------------

logger = get_logger(__name__)


# --------------------------------------------------
# Configuration
# --------------------------------------------------

config = PipelineConfig()


# --------------------------------------------------
# Paths
# --------------------------------------------------

database_path = Path(
    config.db_path
)

reports_dir = (
    Path(__file__).resolve().parents[1]
    / "reports"
)


# --------------------------------------------------
# Daily transaction volume by type
# --------------------------------------------------

def daily_transaction_volume():

    connection = duckdb.connect(
        str(database_path)
    )

    connection.execute(
        "PRAGMA enable_progress_bar=false"
    )

    result = connection.execute(
        """
        SELECT
            t.sim_day,
            tt.type_name,
            COUNT(*) AS transaction_count

        FROM fact_transactions AS f

        JOIN dim_time AS t
            ON f.step = t.step

        JOIN dim_transaction_type AS tt
            ON f.transaction_type_id = tt.id

        GROUP BY
            t.sim_day,
            tt.type_name

        ORDER BY
            t.sim_day,
            tt.type_name
        """
    ).fetchall()

    connection.close()

    return result


# --------------------------------------------------
# Daily fraud count
# --------------------------------------------------

def daily_fraud_count():

    connection = duckdb.connect(
        str(database_path)
    )

    connection.execute(
        "PRAGMA enable_progress_bar=false"
    )

    result = connection.execute(
        """
        SELECT
            t.sim_day,
            COUNT(*) AS fraud_count

        FROM fact_transactions AS f

        JOIN dim_time AS t
            ON f.step = t.step

        WHERE
            f.is_fraud = TRUE

        GROUP BY
            t.sim_day

        ORDER BY
            t.sim_day
        """
    ).fetchall()

    connection.close()

    return result


# --------------------------------------------------
# Daily mean amount by type
# --------------------------------------------------

def daily_mean_amount():

    connection = duckdb.connect(
        str(database_path)
    )

    connection.execute(
        "PRAGMA enable_progress_bar=false"
    )

    result = connection.execute(
        """
        SELECT
            t.sim_day,
            tt.type_name,
            AVG(f.amount) AS mean_amount

        FROM fact_transactions AS f

        JOIN dim_time AS t
            ON f.step = t.step

        JOIN dim_transaction_type AS tt
            ON f.transaction_type_id = tt.id

        GROUP BY
            t.sim_day,
            tt.type_name

        ORDER BY
            t.sim_day,
            tt.type_name
        """
    ).fetchall()

    connection.close()

    return result


# --------------------------------------------------
# Sequential execution
# --------------------------------------------------

def run_sequential():

    logger.info(
        "Starting sequential time-series analysis"
    )

    start_time = time.perf_counter()

    volume = daily_transaction_volume()

    fraud = daily_fraud_count()

    mean_amount = daily_mean_amount()

    elapsed_time = (
        time.perf_counter()
        - start_time
    )

    logger.info(
        "Sequential execution completed in %.4f seconds",
        elapsed_time
    )

    return (
        volume,
        fraud,
        mean_amount,
        elapsed_time
    )


# --------------------------------------------------
# Parallel execution
# --------------------------------------------------

def run_parallel():

    logger.info(
        "Starting parallel time-series analysis"
    )

    start_time = time.perf_counter()

    with ThreadPoolExecutor(
        max_workers=3
    ) as executor:

        volume_future = executor.submit(
            daily_transaction_volume
        )

        fraud_future = executor.submit(
            daily_fraud_count
        )

        mean_future = executor.submit(
            daily_mean_amount
        )

        volume = volume_future.result()

        fraud = fraud_future.result()

        mean_amount = mean_future.result()

    elapsed_time = (
        time.perf_counter()
        - start_time
    )

    logger.info(
        "Parallel execution completed in %.4f seconds",
        elapsed_time
    )

    return (
        volume,
        fraud,
        mean_amount,
        elapsed_time
    )


# --------------------------------------------------
# Compare results
# --------------------------------------------------

def check_results(
    sequential,
    parallel
):

    if sequential == parallel:

        return True

    rounded_sequential = [
        (
            row[0],
            row[1],
            round(row[2], 6)
        )

        for row in sequential
    ]

    rounded_parallel = [
        (
            row[0],
            row[1],
            round(row[2], 6)
        )

        for row in parallel
    ]

    return (
        rounded_sequential
        == rounded_parallel
    )


# --------------------------------------------------
# Create time-series chart
# --------------------------------------------------

def create_time_series_chart(
    volume_data,
    fraud_data,
    mean_amount_data
):

    logger.info(
        "Creating transaction time-series chart"
    )


    # --------------------------------------------------
    # Prepare volume data
    # --------------------------------------------------

    volume = {}

    for day, transaction_type, count in volume_data:

        if transaction_type not in volume:

            volume[transaction_type] = []

        volume[transaction_type].append(
            (
                day,
                count
            )
        )


    # --------------------------------------------------
    # Prepare fraud data
    # --------------------------------------------------

    fraud_days = [
        row[0]
        for row in fraud_data
    ]

    fraud_counts = [
        row[1]
        for row in fraud_data
    ]


    # --------------------------------------------------
    # Prepare mean amount data
    # --------------------------------------------------

    mean_amount = {}

    for day, transaction_type, amount in mean_amount_data:

        if transaction_type not in mean_amount:

            mean_amount[transaction_type] = []

        mean_amount[transaction_type].append(
            (
                day,
                amount
            )
        )


    # --------------------------------------------------
    # Create figure
    # --------------------------------------------------

    figure, axes = plt.subplots(
        3,
        1,
        figsize=(12, 14)
    )


    # --------------------------------------------------
    # Panel 1
    # --------------------------------------------------

    for transaction_type, values in volume.items():

        days = [
            row[0]
            for row in values
        ]

        counts = [
            row[1]
            for row in values
        ]

        axes[0].plot(
            days,
            counts,
            label=transaction_type
        )

    axes[0].set_title(
        "Daily Transaction Volume by Type"
    )

    axes[0].set_xlabel(
        "Simulation Day"
    )

    axes[0].set_ylabel(
        "Transaction Count"
    )

    axes[0].legend()


    # --------------------------------------------------
    # Panel 2
    # --------------------------------------------------

    axes[1].plot(
        fraud_days,
        fraud_counts
    )

    axes[1].set_title(
        "Daily Fraud Count"
    )

    axes[1].set_xlabel(
        "Simulation Day"
    )

    axes[1].set_ylabel(
        "Fraud Count"
    )


    # --------------------------------------------------
    # Panel 3
    # --------------------------------------------------

    for transaction_type, values in mean_amount.items():

        days = [
            row[0]
            for row in values
        ]

        amounts = [
            row[1]
            for row in values
        ]

        axes[2].plot(
            days,
            amounts,
            label=transaction_type
        )

    axes[2].set_title(
        "Daily Mean Transaction Amount by Type"
    )

    axes[2].set_xlabel(
        "Simulation Day"
    )

    axes[2].set_ylabel(
        "Mean Amount"
    )

    axes[2].legend()


    # --------------------------------------------------
    # Save figure
    # --------------------------------------------------

    figure.tight_layout()

    output_path = (
        reports_dir
        / "transaction_time_series.png"
    )

    figure.savefig(
        output_path,
        dpi=300
    )

    plt.close(
        figure
    )

    logger.info(
        "Time-series chart saved to %s",
        output_path
    )


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    logger.info(
        "Starting transaction analysis"
    )


    # --------------------------------------------------
    # Sequential
    # --------------------------------------------------

    (
        volume_sequential,
        fraud_sequential,
        mean_sequential,
        sequential_time
    ) = run_sequential()


    # --------------------------------------------------
    # Parallel
    # --------------------------------------------------

    (
        volume_parallel,
        fraud_parallel,
        mean_parallel,
        parallel_time
    ) = run_parallel()


    # --------------------------------------------------
    # Speedup
    # --------------------------------------------------

    speedup = (
        sequential_time
        / parallel_time
    )

    logger.info(
        "Sequential time: %.4f seconds",
        sequential_time
    )

    logger.info(
        "Parallel time: %.4f seconds",
        parallel_time
    )

    logger.info(
        "Parallel speedup: %.2fx",
        speedup
    )


    # --------------------------------------------------
    # Check results
    # --------------------------------------------------

    if volume_sequential != volume_parallel:

        raise RuntimeError(
            "Transaction volume results do not match"
        )


    if fraud_sequential != fraud_parallel:

        raise RuntimeError(
            "Fraud count results do not match"
        )


    if not check_results(
        mean_sequential,
        mean_parallel
    ):

        raise RuntimeError(
            "Mean amount results do not match"
        )


    logger.info(
        "Sequential and parallel results match"
    )


    # --------------------------------------------------
    # Create chart
    # --------------------------------------------------

    create_time_series_chart(
        volume_parallel,
        fraud_parallel,
        mean_parallel
    )


    # --------------------------------------------------
    # Amount distribution
    # --------------------------------------------------

    create_amount_distribution_chart(
        amount_distribution_data(),
        amount_distribution_stats()
    )


    # --------------------------------------------------
    # Balance drain distribution
    # --------------------------------------------------

    overall_rate, by_type = balance_drain_inconsistency()

    create_balance_drain_chart(
        balance_drain_values(),
        overall_rate,
        by_type
    )


    logger.info(
        "Transaction analysis completed"
    )


# --------------------------------------------------
# Distribution settings
# --------------------------------------------------

# Estimating a KDE over 2.8 million points is far slower than it is
# informative, so each curve is drawn from a fixed random sample. The fitted
# normal curves still use the full population statistics.
KDE_SAMPLE_SIZE = 150_000

DISTRIBUTION_COLOURS = {
    "TRANSFER": "tab:blue",
    "CASH_OUT": "tab:orange",
}

# A transaction is treated as internally inconsistent when the sender balance
# does not move by the transaction amount. One currency unit of slack absorbs
# floating point noise.
DRAIN_TOLERANCE = 1.0


# --------------------------------------------------
# Amount distribution data
# --------------------------------------------------

def amount_distribution_data():

    connection = duckdb.connect(
        str(database_path)
    )

    connection.execute(
        "PRAGMA enable_progress_bar=false"
    )

    df = connection.execute(
        """
        SELECT
            f.log_amount,
            tt.type_name

        FROM fact_transactions AS f

        JOIN dim_transaction_type AS tt
            ON f.transaction_type_id = tt.id

        WHERE tt.type_name IN ('TRANSFER', 'CASH_OUT')
        """
    ).df()

    connection.close()

    return df


# --------------------------------------------------
# Amount distribution statistics
# --------------------------------------------------

def amount_distribution_stats():

    connection = duckdb.connect(
        str(database_path)
    )

    connection.execute(
        "PRAGMA enable_progress_bar=false"
    )

    rows = connection.execute(
        """
        SELECT
            tt.type_name,
            COUNT(*) AS row_count,
            AVG(f.log_amount) AS mean_log_amount,
            STDDEV_SAMP(f.log_amount) AS sd_log_amount,
            SKEWNESS(f.log_amount) AS skewness,
            KURTOSIS(f.log_amount) AS excess_kurtosis

        FROM fact_transactions AS f

        JOIN dim_transaction_type AS tt
            ON f.transaction_type_id = tt.id

        WHERE tt.type_name IN ('TRANSFER', 'CASH_OUT')

        GROUP BY tt.type_name
        """
    ).fetchall()

    connection.close()

    stats = {}

    for type_name, row_count, mean, sd, skewness, excess_kurtosis in rows:

        stats[type_name] = {
            "row_count": row_count,
            "mean": mean,
            "sd": sd,
            "skewness": skewness,
            "excess_kurtosis": excess_kurtosis,
        }

        logger.info(
            "%s log_amount: n=%s mean=%.4f sd=%.4f skew=%.4f excess_kurtosis=%.4f",
            type_name,
            row_count,
            mean,
            sd,
            skewness,
            excess_kurtosis,
        )

    return stats


# --------------------------------------------------
# Amount distribution chart
# --------------------------------------------------

# Is the empirical distribution consistent with log-normality?
#
# If amount were log-normal, then log(amount + 1) would be normal, which means
# skewness 0 and excess kurtosis 0. The measured values are:
#
#     TRANSFER   skew -0.71   excess kurtosis 2.20
#     CASH_OUT   skew -1.52   excess kurtosis 4.29
#
# So neither is normal in the strict sense. Both are left skewed and both have
# excess kurtosis, meaning more mass in the tails and a sharper peak than a
# normal of the same mean and standard deviation. TRANSFER is much closer to
# the log-normal ideal than CASH_OUT is.
#
# Both are left skewed, which means the excess kurtosis is carried mostly by
# the lower tail, not the upper one. That distinction matters, because a fraud
# rule only ever looks at the upper tail.
#
# Why this matters for detection thresholds: a z-score rule such as R02
# assumes the distribution is roughly normal, because that is what makes
# "3 standard deviations" correspond to a known tail probability. Under a
# normal distribution z > 3 covers about 0.135% of the upper tail. Measured
# against the real data:
#
#     TRANSFER   z > 3 fires on 0.1368% of rows  (normal predicts 0.135%)
#     CASH_OUT   z > 3 fires on 0.0261% of rows  (normal predicts 0.135%)
#
# So the normal assumption is close enough to harmless for TRANSFER, but for
# CASH_OUT it is wrong by a factor of five in the direction that costs recall:
# the rule fires far less often than the assumption implies, so it under-flags
# rather than over-flags. The reason is that the CASH_OUT upper tail is
# compressed -- its 99.9th percentile of log_amount is 13.67 against 17.03 for
# TRANSFER -- so a cut-off placed three standard deviations out lands beyond
# almost all of the data. A threshold set from the normal assumption should
# therefore be checked against the empirical quantiles per transaction type
# rather than trusted directly.

def create_amount_distribution_chart(df, stats):

    logger.info(
        "Creating amount distribution chart"
    )

    figure, axes = plt.subplots(
        figsize=(12, 7)
    )

    for type_name in ("TRANSFER", "CASH_OUT"):

        values = df.loc[
            df["type_name"] == type_name,
            "log_amount"
        ]

        sample = values.sample(
            n=min(KDE_SAMPLE_SIZE, len(values)),
            random_state=42,
        )

        colour = DISTRIBUTION_COLOURS[type_name]

        sns.kdeplot(
            x=sample,
            ax=axes,
            color=colour,
            fill=True,
            alpha=0.3,
            label=f"{type_name} (empirical)",
        )

        mean = stats[type_name]["mean"]
        sd = stats[type_name]["sd"]

        grid = np.linspace(
            values.min(),
            values.max(),
            500,
        )

        axes.plot(
            grid,
            norm.pdf(grid, mean, sd),
            color=colour,
            linestyle="--",
            linewidth=2,
            label=f"{type_name} normal fit (mu={mean:.2f}, sigma={sd:.2f})",
        )

    axes.set_title(
        "Log Transaction Amount Distribution: TRANSFER vs CASH_OUT"
    )

    axes.set_xlabel(
        "log(amount + 1)"
    )

    axes.set_ylabel(
        "Density"
    )

    axes.legend()

    figure.tight_layout()

    output_path = (
        reports_dir
        / "amount_distribution.png"
    )

    figure.savefig(
        output_path,
        dpi=300
    )

    plt.close(
        figure
    )

    logger.info(
        "Amount distribution chart saved to %s",
        output_path
    )

    return output_path


# --------------------------------------------------
# Balance drain data
# --------------------------------------------------

def balance_drain_values():

    connection = duckdb.connect(
        str(database_path)
    )

    connection.execute(
        "PRAGMA enable_progress_bar=false"
    )

    values = connection.execute(
        """
        SELECT balance_drain
        FROM fact_transactions
        """
    ).df()["balance_drain"]

    connection.close()

    return values


# --------------------------------------------------
# Balance drain inconsistency rate
# --------------------------------------------------

def balance_drain_inconsistency():

    connection = duckdb.connect(
        str(database_path)
    )

    connection.execute(
        "PRAGMA enable_progress_bar=false"
    )

    overall_rate = connection.execute(
        f"""
        SELECT
            100.0
            * SUM(
                CASE
                    WHEN ABS(balance_drain) > {DRAIN_TOLERANCE}
                    THEN 1
                    ELSE 0
                END
            )
            / COUNT(*)

        FROM fact_transactions
        """
    ).fetchone()[0]

    by_type = connection.execute(
        f"""
        SELECT
            tt.type_name,

            100.0
            * SUM(
                CASE
                    WHEN ABS(f.balance_drain) > {DRAIN_TOLERANCE}
                    THEN 1
                    ELSE 0
                END
            )
            / COUNT(*) AS inconsistency_rate

        FROM fact_transactions AS f

        JOIN dim_transaction_type AS tt
            ON f.transaction_type_id = tt.id

        GROUP BY tt.type_name

        ORDER BY inconsistency_rate DESC
        """
    ).fetchall()

    connection.close()

    logger.info(
        "Transactions with |balance_drain| > %s: %.2f%%",
        DRAIN_TOLERANCE,
        overall_rate,
    )

    for type_name, rate in by_type:

        logger.info(
            "  %s: %.2f%%",
            type_name,
            rate,
        )

    return overall_rate, by_type


# --------------------------------------------------
# Balance drain chart
# --------------------------------------------------

def create_balance_drain_chart(values, overall_rate, by_type):

    logger.info(
        "Creating balance drain chart"
    )

    figure, axes = plt.subplots(
        2,
        1,
        figsize=(12, 10)
    )


    # --------------------------------------------------
    # Panel 1 - distribution
    # --------------------------------------------------

    # balance_drain never goes above 0, and the lower tail reaches -92 million,
    # so the histogram is clipped at the 1st percentile to stay readable. The
    # y axis is logarithmic because the bar at 0 dwarfs everything else.

    lower_bound = np.percentile(values, 1)

    clipped = values[values >= lower_bound]

    axes[0].hist(
        clipped,
        bins=120,
        color="tab:blue",
    )

    axes[0].set_yscale("log")

    axes[0].axvline(
        0,
        color="black",
        linestyle="--",
        linewidth=1,
        label="Consistent ledger (drain = 0)",
    )

    axes[0].set_title(
        "Distribution of balance_drain "
        f"(clipped at 1st percentile, {overall_rate:.2f}% exceed the tolerance)"
    )

    axes[0].set_xlabel(
        "balance_drain (old sender balance - new sender balance - amount)"
    )

    axes[0].set_ylabel(
        "Transaction Count (log scale)"
    )

    axes[0].legend()


    # --------------------------------------------------
    # Panel 2 - inconsistency rate by type
    # --------------------------------------------------

    type_names = [
        row[0]
        for row in by_type
    ]

    rates = [
        row[1]
        for row in by_type
    ]

    bars = axes[1].bar(
        type_names,
        rates,
        color="tab:orange",
    )

    axes[1].bar_label(
        bars,
        fmt="%.1f%%",
    )

    axes[1].set_ylim(0, 110)

    axes[1].set_title(
        f"Share of Transactions with |balance_drain| > {DRAIN_TOLERANCE:.0f} by Type"
    )

    axes[1].set_xlabel(
        "Transaction Type"
    )

    axes[1].set_ylabel(
        "Percent of Transactions"
    )

    figure.tight_layout()

    output_path = (
        reports_dir
        / "balance_drain_distribution.png"
    )

    figure.savefig(
        output_path,
        dpi=300
    )

    plt.close(
        figure
    )

    logger.info(
        "Balance drain chart saved to %s",
        output_path
    )

    return output_path


# --------------------------------------------------
# Run program
# --------------------------------------------------

if __name__ == "__main__":

    main()
