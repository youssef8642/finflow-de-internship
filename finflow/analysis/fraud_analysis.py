"""Milestone 3.2 - Fraud prevalence and statistical analysis.

Order of this file follows the milestone bullets:
    1. summary table (overall rate, rate by type, amount stats, zero balances)
    2. z-score analysis with a box plot
    3. conditional probability table, written in DuckDB SQL
    4. Bayes' theorem applied to the R01 proxy
"""

import duckdb
import matplotlib.pyplot as plt
import pandas as pd

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig

logger = get_logger(__name__)
config = PipelineConfig()

REPORTS_DIR = "finflow/reports"


def run_query(sql: str):
    """Run one query and return a DataFrame."""
    connection = duckdb.connect(config.db_path)
    result = connection.execute(sql).fetchdf()
    connection.close()
    return result


def overall_fraud_rate():
    """Overall fraud rate across every transaction."""
    return run_query("""
        SELECT
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_transactions,
            100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) / COUNT(*) AS fraud_rate
        FROM fact_transactions
    """)


def fraud_rate_by_type():
    """P(fraud | transaction_type) for each transaction type."""
    return run_query("""
        SELECT
            tt.type_name,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_transactions,
            100.0 * SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) / COUNT(*) AS fraud_rate
        FROM fact_transactions AS f
        JOIN dim_transaction_type AS tt ON f.transaction_type_id = tt.id
        GROUP BY tt.type_name
        ORDER BY fraud_rate DESC
    """)


def amount_statistics():
    """Mean, median and 95th percentile of amount, fraud vs non-fraud."""
    return run_query("""
        SELECT
            CASE WHEN is_fraud THEN 'fraud' ELSE 'non_fraud' END AS group_name,
            AVG(amount) AS mean_amount,
            MEDIAN(amount) AS median_amount,
            QUANTILE_CONT(amount, 0.95) AS p95_amount
        FROM fact_transactions
        GROUP BY is_fraud
        ORDER BY group_name
    """)


def zero_balance_rates():
    """Share of fraud cases where the sender or the receiver balance is 0.

    A receiver balance that never moves is a well known money laundering
    pattern: the money arrives and is swept straight out again.
    """
    return run_query("""
        SELECT
            100.0 * SUM(CASE WHEN new_balance_sender = 0 THEN 1 ELSE 0 END) / COUNT(*)
                AS pct_sender_zero,
            100.0 * SUM(CASE WHEN new_balance_receiver = 0 THEN 1 ELSE 0 END) / COUNT(*)
                AS pct_receiver_zero
        FROM fact_transactions
        WHERE is_fraud = TRUE
    """)


def fraud_z_scores():
    """Z-score of each fraudulent amount, measured inside its own type.

    The mean and standard deviation come from all transactions of that type,
    not only the fraudulent ones, because that is the population a detection
    rule would compare a new transaction against.
    """
    return run_query("""
        WITH type_stats AS (
            SELECT
                transaction_type_id,
                AVG(amount) AS mean_amount,
                STDDEV_SAMP(amount) AS sd_amount
            FROM fact_transactions
            GROUP BY transaction_type_id
        )
        SELECT
            tt.type_name,
            (f.amount - s.mean_amount) / s.sd_amount AS z_score
        FROM fact_transactions AS f
        JOIN type_stats AS s ON f.transaction_type_id = s.transaction_type_id
        JOIN dim_transaction_type AS tt ON f.transaction_type_id = tt.id
        WHERE f.is_fraud = TRUE
    """)


def plot_z_scores(z_scores) -> None:
    """Box plot comparing the z-score distributions across transaction types."""
    figure, axes = plt.subplots(figsize=(10, 6))

    type_names = sorted(z_scores["type_name"].unique())
    values = []
    for type_name in type_names:
        values.append(z_scores[z_scores["type_name"] == type_name]["z_score"])

    axes.boxplot(values, tick_labels=type_names)
    axes.axhline(3, color="red", linestyle="--", label="z = 3 (rule R02 cut-off)")

    axes.set_title("Z-Scores of Fraudulent Amounts by Transaction Type")
    axes.set_xlabel("Transaction Type")
    axes.set_ylabel("Z-score")
    axes.legend()

    figure.tight_layout()
    figure.savefig(REPORTS_DIR + "/fraud_zscore_boxplot.png", dpi=150)
    plt.close(figure)
    logger.info("Saved fraud_zscore_boxplot.png")


def conditional_probabilities():
    """P(fraud | |balance_drain| > threshold) at four percentile cut-offs.

    This is the empirical basis for rule R01. Written in SQL, not pandas,
    because the milestone asks for it that way.
    """
    return run_query("""
        WITH cutoffs AS (
            SELECT
                QUANTILE_CONT(ABS(balance_drain), 0.75) AS p75,
                QUANTILE_CONT(ABS(balance_drain), 0.90) AS p90,
                QUANTILE_CONT(ABS(balance_drain), 0.95) AS p95,
                QUANTILE_CONT(ABS(balance_drain), 0.99) AS p99
            FROM fact_transactions
        ),
        levels AS (
            SELECT '75th' AS level_name, p75 AS cutoff FROM cutoffs
            UNION ALL SELECT '90th', p90 FROM cutoffs
            UNION ALL SELECT '95th', p95 FROM cutoffs
            UNION ALL SELECT '99th', p99 FROM cutoffs
        )
        SELECT
            l.level_name,
            l.cutoff,
            COUNT(*) AS transactions_above,
            SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_above,
            100.0 * SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) / COUNT(*) AS fraud_probability
        FROM fact_transactions AS f
        JOIN levels AS l ON ABS(f.balance_drain) > l.cutoff
        GROUP BY l.level_name, l.cutoff
        ORDER BY l.cutoff
    """)


def bayes_for_r01():
    """Posterior probability that a transaction is fraud once R01 fires.

    R01 proxy: new_balance_sender = 0 AND amount > median amount.

    Bayes' theorem:

        P(fraud | R01) = P(R01 | fraud) * P(fraud) / P(R01)

    The three inputs are counted in SQL, then the formula is applied below
    and checked against the direct count of P(fraud | R01).
    """
    counts = run_query("""
        WITH median_amount AS (
            SELECT MEDIAN(amount) AS value FROM fact_transactions
        )
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud,
            SUM(CASE WHEN f.new_balance_sender = 0 AND f.amount > m.value
                     THEN 1 ELSE 0 END) AS r01_fired,
            SUM(CASE WHEN f.is_fraud AND f.new_balance_sender = 0 AND f.amount > m.value
                     THEN 1 ELSE 0 END) AS fraud_and_r01
        FROM fact_transactions AS f
        CROSS JOIN median_amount AS m
    """)

    total = counts["total"][0]
    fraud = counts["fraud"][0]
    r01_fired = counts["r01_fired"][0]
    fraud_and_r01 = counts["fraud_and_r01"][0]

    p_fraud = fraud / total

    p_r01_given_fraud = fraud_and_r01 / fraud

    p_r01 = r01_fired / total

    p_fraud_given_r01 = p_r01_given_fraud * p_fraud / p_r01

    logger.info("P(fraud)          = %.6f%%", p_fraud * 100)
    logger.info("P(R01 | fraud)    = %.4f%%", p_r01_given_fraud * 100)
    logger.info("P(R01)            = %.4f%%", p_r01 * 100)
    logger.info("P(fraud | R01)    = %.4f%%  (Bayes)", p_fraud_given_r01 * 100)
    logger.info("P(fraud | R01)    = %.4f%%  (direct count)", fraud_and_r01 / r01_fired * 100)

    return {
        "p_fraud": p_fraud * 100,
        "p_r01_given_fraud": p_r01_given_fraud * 100,
        "p_r01": p_r01 * 100,
        "p_fraud_given_r01": p_fraud_given_r01 * 100,
    }


def save_summary(overall, by_type, amounts, zero_balances, conditional, bayes) -> None:
    """Collect every number into one table, print it and save it as CSV."""
    rows = []

    rows.append(("overall", "fraud_rate_pct", overall["fraud_rate"][0]))
    rows.append(("overall", "fraud_transactions", overall["fraud_transactions"][0]))
    rows.append(("overall", "total_transactions", overall["total_transactions"][0]))

    for i in range(len(by_type)):
        rows.append(("fraud_by_type", by_type["type_name"][i], by_type["fraud_rate"][i]))

    for i in range(len(amounts)):
        group = amounts["group_name"][i]
        rows.append(("amount", group + "_mean", amounts["mean_amount"][i]))
        rows.append(("amount", group + "_median", amounts["median_amount"][i]))
        rows.append(("amount", group + "_p95", amounts["p95_amount"][i]))

    rows.append(("zero_balance", "pct_fraud_sender_zero", zero_balances["pct_sender_zero"][0]))
    rows.append(("zero_balance", "pct_fraud_receiver_zero", zero_balances["pct_receiver_zero"][0]))

    for i in range(len(conditional)):
        level = conditional["level_name"][i]
        rows.append(("conditional", "P(fraud|drain>" + level + ")",
                     conditional["fraud_probability"][i]))

    for key in bayes:
        rows.append(("bayes", key, bayes[key]))

    summary = pd.DataFrame(rows, columns=["section", "metric", "value"])

    print(summary.to_string(index=False))

    summary.to_csv(REPORTS_DIR + "/fraud_summary.csv", index=False)
    logger.info("Saved fraud_summary.csv")


def main() -> None:
    """Entry point for Milestone 3.2."""
    logger.info("Starting fraud analysis")

    overall = overall_fraud_rate()
    by_type = fraud_rate_by_type()
    amounts = amount_statistics()
    zero_balances = zero_balance_rates()

    z_scores = fraud_z_scores()
    plot_z_scores(z_scores)

    conditional = conditional_probabilities()
    bayes = bayes_for_r01()

    save_summary(overall, by_type, amounts, zero_balances, conditional, bayes)

    logger.info("Fraud analysis completed")


if __name__ == "__main__":
    main()
