import duckdb
import pandas as pd
import matplotlib.pyplot as plt

from pathlib import Path

from finflow.config.logger import get_logger


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

DB_PATH = "data/finflow.duckdb"
REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"

SUMMARY_PATH = REPORTS_DIR / "fraud_summary.csv"
ZSCORE_PLOT_PATH = REPORTS_DIR / "fraud_zscore_boxplot.png"
NOTES_PATH = REPORTS_DIR / "fraud_analysis_notes.md"

logger = get_logger(__name__)


def get_connection():
    connection = duckdb.connect(DB_PATH)

    connection.execute(
        "PRAGMA enable_progress_bar=false"
    )

    return connection


# ---------------------------------------------------------------------
# 1. Overall fraud percentage
# ---------------------------------------------------------------------

def get_fraud_percentage():
    connection = get_connection()

    try:
        result = connection.execute(
            """
            SELECT
                COUNT(*) FILTER (
                    WHERE is_fraud = TRUE
                ) AS fraud_count,

                COUNT(*) AS total_count

            FROM fact_transactions;
            """
        ).fetchone()

        fraud_count = result[0]
        total_count = result[1]

        fraud_percentage = (
            fraud_count / total_count * 100
            if total_count > 0
            else 0
        )

        logger.info(
            f"Fraud count: {fraud_count}, "
            f"Total count: {total_count}"
        )

        logger.info(
            f"Fraud percentage: "
            f"{fraud_percentage:.2f}%"
        )

        return {
            "fraud_count": fraud_count,
            "total_count": total_count,
            "fraud_percentage": fraud_percentage
        }

    finally:
        connection.close()


# ---------------------------------------------------------------------
# 2. Fraud percentage by transaction type
# ---------------------------------------------------------------------

def get_fraud_percentage_by_transaction_type():
    connection = get_connection()

    try:
        result = connection.execute(
            """
            SELECT
                tt.type_name AS transaction_type,

                COUNT(*) AS total_transactions,

                COUNT(*) FILTER (
                    WHERE ft.is_fraud = TRUE
                ) AS fraud_count,

                ROUND(
                    100.0 *
                    COUNT(*) FILTER (
                        WHERE ft.is_fraud = TRUE
                    )
                    / COUNT(*),
                    2
                ) AS fraud_percentage

            FROM fact_transactions AS ft

            JOIN dim_transaction_type AS tt
                ON ft.transaction_type_id = tt.id

            GROUP BY tt.type_name

            ORDER BY fraud_percentage DESC;
            """
        ).fetchdf()

        logger.info(
            "Fraud percentage by transaction type:"
        )

        for _, row in result.iterrows():
            logger.info(
                f"Transaction Type: {row['transaction_type']}, "
                f"Total Transactions: "
                f"{int(row['total_transactions'])}, "
                f"Fraud Count: {int(row['fraud_count'])}, "
                f"Fraud Percentage: "
                f"{row['fraud_percentage']:.2f}%"
            )

        return result

    finally:
        connection.close()


# ---------------------------------------------------------------------
# 3. Mean, median and 95th percentile amount
#    for fraud vs non-fraud
# ---------------------------------------------------------------------

def get_transaction_statistics_by_fraud_status():
    connection = get_connection()

    try:
        result = connection.execute(
            """
            SELECT
                CASE
                    WHEN is_fraud = TRUE
                        THEN 'Fraud'
                    ELSE 'Non-Fraud'
                END AS fraud_status,

                COUNT(*) AS transaction_count,

                ROUND(
                    AVG(amount),
                    2
                ) AS mean_amount,

                ROUND(
                    MEDIAN(amount),
                    2
                ) AS median_amount,

                ROUND(
                    QUANTILE_CONT(amount, 0.95),
                    2
                ) AS percentile_95_amount

            FROM fact_transactions

            GROUP BY is_fraud

            ORDER BY is_fraud DESC;
            """
        ).fetchdf()

        logger.info(
            "Transaction statistics by fraud status:"
        )

        for _, row in result.iterrows():
            logger.info(
                f"Fraud Status: {row['fraud_status']}, "
                f"Transaction Count: "
                f"{int(row['transaction_count'])}, "
                f"Mean Amount: "
                f"{row['mean_amount']:.2f}, "
                f"Median Amount: "
                f"{row['median_amount']:.2f}, "
                f"95th Percentile Amount: "
                f"{row['percentile_95_amount']:.2f}"
            )

        return result

    finally:
        connection.close()


# ---------------------------------------------------------------------
# 4. Zero balance analysis
# ---------------------------------------------------------------------

def get_zero_balance_fraud_percentage():
    connection = get_connection()

    try:
        result = connection.execute(
            """
            SELECT
                COUNT(*) AS fraud_count,

                COUNT(*) FILTER (
                    WHERE new_balance_sender = 0
                ) AS fraud_zero_sender_balance,

                ROUND(
                    100.0 *
                    COUNT(*) FILTER (
                        WHERE new_balance_sender = 0
                    )
                    / COUNT(*),
                    2
                ) AS pct_fraud_zero_sender_balance,

                COUNT(*) FILTER (
                    WHERE new_balance_receiver = 0
                ) AS fraud_zero_receiver_balance,

                ROUND(
                    100.0 *
                    COUNT(*) FILTER (
                        WHERE new_balance_receiver = 0
                    )
                    / COUNT(*),
                    2
                ) AS pct_fraud_zero_receiver_balance

            FROM fact_transactions

            WHERE is_fraud = TRUE;
            """
        ).fetchone()

        logger.info(
            f"Fraud count: {result[0]}"
        )

        logger.info(
            f"Fraud with zero sender balance: "
            f"{result[1]} ({result[2]:.2f}%)"
        )

        logger.info(
            f"Fraud with zero receiver balance: "
            f"{result[3]} ({result[4]:.2f}%)"
        )

        return {
            "fraud_count": result[0],
            "zero_sender_count": result[1],
            "zero_sender_percentage": result[2],
            "zero_receiver_count": result[3],
            "zero_receiver_percentage": result[4]
        }

    finally:
        connection.close()


# ---------------------------------------------------------------------
# 5. Z-score analysis for fraudulent transaction amounts
# ---------------------------------------------------------------------

def get_fraud_z_scores():
    connection = get_connection()

    try:
        result = connection.execute(
            """
            WITH transaction_type_statistics AS (
                SELECT
                    transaction_type_id,

                    AVG(amount) AS mean_amount,

                    STDDEV_SAMP(amount) AS std_amount

                FROM fact_transactions

                GROUP BY transaction_type_id
            )

            SELECT
                tt.type_name AS transaction_type,

                ft.transaction_id,

                ft.amount,

                stats.mean_amount,

                stats.std_amount,

                (
                    ft.amount - stats.mean_amount
                )
                /
                NULLIF(stats.std_amount, 0)
                AS z_score

            FROM fact_transactions AS ft

            JOIN transaction_type_statistics AS stats
                ON ft.transaction_type_id =
                   stats.transaction_type_id

            JOIN dim_transaction_type AS tt
                ON ft.transaction_type_id = tt.id

            WHERE ft.is_fraud = TRUE

            ORDER BY
                tt.type_name,
                z_score;
            """
        ).fetchdf()

        logger.info(
            "Fraudulent transaction z-score statistics:"
        )

        summary = (
            result
            .groupby("transaction_type")["z_score"]
            .agg(
                count="count",
                mean="mean",
                median="median",
                min="min",
                max="max"
            )
            .reset_index()
        )

        for _, row in summary.iterrows():
            logger.info(
                f"Transaction Type: "
                f"{row['transaction_type']}, "
                f"Fraud Count: {int(row['count'])}, "
                f"Mean Z-score: {row['mean']:.2f}, "
                f"Median Z-score: {row['median']:.2f}, "
                f"Min Z-score: {row['min']:.2f}, "
                f"Max Z-score: {row['max']:.2f}"
            )

        # -------------------------------------------------------------
        # Box plot
        # -------------------------------------------------------------

        plt.figure(figsize=(10, 6))

        result.boxplot(
            column="z_score",
            by="transaction_type"
        )

        plt.title(
            "Fraudulent Transaction Amount Z-Scores "
            "by Transaction Type"
        )

        plt.suptitle("")

        plt.xlabel("Transaction Type")
        plt.ylabel("Z-score")

        plt.axhline(
            y=0,
            linestyle="--"
        )

        plt.tight_layout()

        plt.savefig(
            ZSCORE_PLOT_PATH,
            dpi=300
        )

        plt.close()

        logger.info(
            f"Z-score box plot saved to "
            f"{ZSCORE_PLOT_PATH}"
        )

        return result, summary

    finally:
        connection.close()


# ---------------------------------------------------------------------
# 6. Conditional probability:
#
# P(Fraud | |balance_drain| > threshold)
#
# Thresholds:
# 75th, 90th, 95th and 99th percentile
# ---------------------------------------------------------------------

def get_balance_drain_conditional_probabilities():
    connection = get_connection()

    try:
        result = connection.execute(
            """
            WITH thresholds AS (
                SELECT
                    QUANTILE_CONT(
                        ABS(balance_drain),
                        0.75
                    ) AS p75,

                    QUANTILE_CONT(
                        ABS(balance_drain),
                        0.90
                    ) AS p90,

                    QUANTILE_CONT(
                        ABS(balance_drain),
                        0.95
                    ) AS p95,

                    QUANTILE_CONT(
                        ABS(balance_drain),
                        0.99
                    ) AS p99

                FROM fact_transactions
            ),

            threshold_values AS (
                SELECT
                    '75th' AS threshold_name,
                    p75 AS threshold
                FROM thresholds

                UNION ALL

                SELECT
                    '90th',
                    p90
                FROM thresholds

                UNION ALL

                SELECT
                    '95th',
                    p95
                FROM thresholds

                UNION ALL

                SELECT
                    '99th',
                    p99
                FROM thresholds
            )

            SELECT
                tv.threshold_name,

                tv.threshold,

                COUNT(*) FILTER (
                    WHERE ABS(ft.balance_drain)
                          > tv.threshold
                ) AS transactions_above_threshold,

                COUNT(*) FILTER (
                    WHERE ABS(ft.balance_drain)
                          > tv.threshold

                      AND ft.is_fraud = TRUE
                ) AS fraud_above_threshold,

                ROUND(
                    100.0 *

                    COUNT(*) FILTER (
                        WHERE ABS(ft.balance_drain)
                              > tv.threshold

                          AND ft.is_fraud = TRUE
                    )

                    /

                    NULLIF(
                        COUNT(*) FILTER (
                            WHERE ABS(ft.balance_drain)
                                  > tv.threshold
                        ),
                        0
                    ),

                    4
                ) AS fraud_probability

            FROM fact_transactions AS ft

            CROSS JOIN threshold_values AS tv

            GROUP BY
                tv.threshold_name,
                tv.threshold

            ORDER BY
                tv.threshold;
            """
        ).fetchdf()

        logger.info(
            "Conditional probability "
            "P(fraud | |balance_drain| > threshold):"
        )

        for _, row in result.iterrows():
            logger.info(
                f"Threshold: {row['threshold_name']}, "
                f"Balance Drain > {row['threshold']:.2f}, "
                f"Transactions: "
                f"{int(row['transactions_above_threshold'])}, "
                f"Fraud: "
                f"{int(row['fraud_above_threshold'])}, "
                f"P(Fraud | Threshold): "
                f"{row['fraud_probability']:.4f}%"
            )

        return result

    finally:
        connection.close()


# ---------------------------------------------------------------------
# 7. Bayes theorem / R01 proxy
#
# R01 proxy:
#
# new_balance_sender = 0
# AND amount > median_amount
# ---------------------------------------------------------------------

def get_rule_r01_bayes_probability():
    connection = get_connection()

    try:
        result = connection.execute(
            """
            WITH median_value AS (
                SELECT
                    MEDIAN(amount) AS median_amount

                FROM fact_transactions
            ),

            rule_results AS (
                SELECT

                    ft.is_fraud,

                    CASE
                        WHEN
                            ft.new_balance_sender = 0
                            AND ft.amount >
                                mv.median_amount
                        THEN TRUE

                        ELSE FALSE
                    END AS rule_r01

                FROM fact_transactions AS ft

                CROSS JOIN median_value AS mv
            )

            SELECT

                COUNT(*) AS total_transactions,

                COUNT(*) FILTER (
                    WHERE is_fraud = TRUE
                ) AS fraud_count,

                COUNT(*) FILTER (
                    WHERE rule_r01 = TRUE
                ) AS r01_count,

                COUNT(*) FILTER (
                    WHERE
                        is_fraud = TRUE
                        AND rule_r01 = TRUE
                ) AS fraud_and_r01_count,

                ROUND(
                    100.0 *

                    COUNT(*) FILTER (
                        WHERE is_fraud = TRUE
                    )

                    / COUNT(*),

                    6
                ) AS p_fraud,

                ROUND(
                    100.0 *

                    COUNT(*) FILTER (
                        WHERE
                            is_fraud = TRUE
                            AND rule_r01 = TRUE
                    )

                    /

                    NULLIF(
                        COUNT(*) FILTER (
                            WHERE is_fraud = TRUE
                        ),
                        0
                    ),

                    6
                ) AS p_r01_given_fraud,

                ROUND(
                    100.0 *

                    COUNT(*) FILTER (
                        WHERE rule_r01 = TRUE
                    )

                    / COUNT(*),

                    6
                ) AS p_r01,

                ROUND(
                    100.0 *

                    COUNT(*) FILTER (
                        WHERE
                            is_fraud = TRUE
                            AND rule_r01 = TRUE
                    )

                    /

                    NULLIF(
                        COUNT(*) FILTER (
                            WHERE rule_r01 = TRUE
                        ),
                        0
                    ),

                    6
                ) AS p_fraud_given_r01

            FROM rule_results;
            """
        ).fetchone()

        total_transactions = result[0]
        fraud_count = result[1]
        r01_count = result[2]
        fraud_and_r01_count = result[3]

        p_fraud = result[4]
        p_r01_given_fraud = result[5]
        p_r01 = result[6]
        p_fraud_given_r01 = result[7]

        logger.info(
            "Bayes analysis for Rule R01 proxy:"
        )

        logger.info(
            f"Total transactions: "
            f"{total_transactions}"
        )

        logger.info(
            f"Fraud count: {fraud_count}"
        )

        logger.info(
            f"R01 fired: {r01_count}"
        )

        logger.info(
            f"Fraud AND R01: "
            f"{fraud_and_r01_count}"
        )

        logger.info(
            f"P(Fraud): {p_fraud:.6f}%"
        )

        logger.info(
            f"P(R01 | Fraud): "
            f"{p_r01_given_fraud:.6f}%"
        )

        logger.info(
            f"P(R01): {p_r01:.6f}%"
        )

        logger.info(
            f"P(Fraud | R01): "
            f"{p_fraud_given_r01:.6f}%"
        )

        # -------------------------------------------------------------
        # Bayes theorem:
        #
        # P(Fraud | R01)
        #
        #       P(R01 | Fraud) * P(Fraud)
        # =     ----------------------------
        #               P(R01)
        #
        # Since the SQL values above are percentages, convert them
        # to probabilities before performing the multiplication.
        # -------------------------------------------------------------

        bayes_probability = (
            (p_r01_given_fraud / 100.0)
            *
            (p_fraud / 100.0)
            /
            (p_r01 / 100.0)
        ) * 100.0

        logger.info(
            f"Bayes calculated P(Fraud | R01): "
            f"{bayes_probability:.6f}%"
        )

        return {
            "total_transactions": total_transactions,
            "fraud_count": fraud_count,
            "r01_count": r01_count,
            "fraud_and_r01_count": fraud_and_r01_count,
            "p_fraud": p_fraud,
            "p_r01_given_fraud": p_r01_given_fraud,
            "p_r01": p_r01,
            "p_fraud_given_r01": p_fraud_given_r01,
            "bayes_probability": bayes_probability
        }

    finally:
        connection.close()


# ---------------------------------------------------------------------
# 8. Create summary CSV
# ---------------------------------------------------------------------

def save_fraud_summary(
    overall_results,
    type_results,
    amount_results,
    zero_balance_results,
    zscore_summary,
    conditional_results,
    bayes_results
):
    summary_rows = []

    # -------------------------------------------------------------
    # Overall fraud
    # -------------------------------------------------------------

    summary_rows.append({
        "analysis": "overall_fraud",
        "category": "overall",
        "metric": "fraud_count",
        "value": overall_results["fraud_count"]
    })

    summary_rows.append({
        "analysis": "overall_fraud",
        "category": "overall",
        "metric": "total_count",
        "value": overall_results["total_count"]
    })

    summary_rows.append({
        "analysis": "overall_fraud",
        "category": "overall",
        "metric": "fraud_percentage",
        "value": overall_results["fraud_percentage"]
    })

    # -------------------------------------------------------------
    # Fraud percentage by type
    # -------------------------------------------------------------

    for _, row in type_results.iterrows():

        summary_rows.append({
            "analysis": "fraud_by_type",
            "category": row["transaction_type"],
            "metric": "fraud_percentage",
            "value": row["fraud_percentage"]
        })

    # -------------------------------------------------------------
    # Amount statistics
    # -------------------------------------------------------------

    for _, row in amount_results.iterrows():

        category = row["fraud_status"]

        summary_rows.append({
            "analysis": "amount_statistics",
            "category": category,
            "metric": "mean_amount",
            "value": row["mean_amount"]
        })

        summary_rows.append({
            "analysis": "amount_statistics",
            "category": category,
            "metric": "median_amount",
            "value": row["median_amount"]
        })

        summary_rows.append({
            "analysis": "amount_statistics",
            "category": category,
            "metric": "95th_percentile_amount",
            "value": row["percentile_95_amount"]
        })

    # -------------------------------------------------------------
    # Zero balance
    # -------------------------------------------------------------

    summary_rows.extend([
        {
            "analysis": "zero_balance",
            "category": "sender",
            "metric": "fraud_count_zero_balance",
            "value": zero_balance_results[
                "zero_sender_count"
            ]
        },
        {
            "analysis": "zero_balance",
            "category": "sender",
            "metric": "percentage",
            "value": zero_balance_results[
                "zero_sender_percentage"
            ]
        },
        {
            "analysis": "zero_balance",
            "category": "receiver",
            "metric": "fraud_count_zero_balance",
            "value": zero_balance_results[
                "zero_receiver_count"
            ]
        },
        {
            "analysis": "zero_balance",
            "category": "receiver",
            "metric": "percentage",
            "value": zero_balance_results[
                "zero_receiver_percentage"
            ]
        }
    ])

    # -------------------------------------------------------------
    # Z-score
    # -------------------------------------------------------------

    for _, row in zscore_summary.iterrows():

        summary_rows.extend([
            {
                "analysis": "fraud_zscore",
                "category": row["transaction_type"],
                "metric": "mean_z_score",
                "value": row["mean"]
            },
            {
                "analysis": "fraud_zscore",
                "category": row["transaction_type"],
                "metric": "median_z_score",
                "value": row["median"]
            },
            {
                "analysis": "fraud_zscore",
                "category": row["transaction_type"],
                "metric": "min_z_score",
                "value": row["min"]
            },
            {
                "analysis": "fraud_zscore",
                "category": row["transaction_type"],
                "metric": "max_z_score",
                "value": row["max"]
            }
        ])

    # -------------------------------------------------------------
    # Balance drain conditional probability
    # -------------------------------------------------------------

    for _, row in conditional_results.iterrows():

        summary_rows.append({
            "analysis": "balance_drain_probability",
            "category": row["threshold_name"],
            "metric": "threshold",
            "value": row["threshold"]
        })

        summary_rows.append({
            "analysis": "balance_drain_probability",
            "category": row["threshold_name"],
            "metric": "transactions_above_threshold",
            "value": row[
                "transactions_above_threshold"
            ]
        })

        summary_rows.append({
            "analysis": "balance_drain_probability",
            "category": row["threshold_name"],
            "metric": "fraud_above_threshold",
            "value": row[
                "fraud_above_threshold"
            ]
        })

        summary_rows.append({
            "analysis": "balance_drain_probability",
            "category": row["threshold_name"],
            "metric": "P(fraud | threshold)",
            "value": row["fraud_probability"]
        })

    # -------------------------------------------------------------
    # Bayes / R01
    # -------------------------------------------------------------

    summary_rows.extend([
        {
            "analysis": "rule_r01_bayes",
            "category": "R01",
            "metric": "P(fraud)",
            "value": bayes_results["p_fraud"]
        },
        {
            "analysis": "rule_r01_bayes",
            "category": "R01",
            "metric": "P(R01 | fraud)",
            "value": bayes_results[
                "p_r01_given_fraud"
            ]
        },
        {
            "analysis": "rule_r01_bayes",
            "category": "R01",
            "metric": "P(R01)",
            "value": bayes_results["p_r01"]
        },
        {
            "analysis": "rule_r01_bayes",
            "category": "R01",
            "metric": "P(fraud | R01)",
            "value": bayes_results[
                "p_fraud_given_r01"
            ]
        }
    ])

    summary = pd.DataFrame(summary_rows)

    # -------------------------------------------------------------
    # Print summary
    # -------------------------------------------------------------

    print("\n")
    print("=" * 80)
    print("FRAUD ANALYSIS SUMMARY")
    print("=" * 80)

    print(
        summary.to_string(
            index=False
        )
    )

    print("=" * 80)

    # -------------------------------------------------------------
    # Save CSV
    # -------------------------------------------------------------

    summary.to_csv(
        SUMMARY_PATH,
        index=False
    )

    logger.info(
        f"Fraud summary saved to {SUMMARY_PATH}"
    )

    return summary


# ---------------------------------------------------------------------
# 9. Analysis notes
# ---------------------------------------------------------------------

def save_analysis_notes(
    overall_results,
    type_results,
    amount_results,
    zero_balance_results,
    conditional_results,
    bayes_results
):
    fraud_percentage = overall_results[
        "fraud_percentage"
    ]

    fraud_amount = amount_results[
        amount_results["fraud_status"] == "Fraud"
    ].iloc[0]

    non_fraud_amount = amount_results[
        amount_results["fraud_status"] == "Non-Fraud"
    ].iloc[0]

    highest_risk_type = type_results.iloc[0]

    notes = f"""# Fraud Analysis Notes

## Overall Fraud Rate

The dataset contains {overall_results["total_count"]:,}
transactions, of which {overall_results["fraud_count"]:,}
are fraudulent.

The overall fraud rate is
**{fraud_percentage:.2f}%**.

This indicates a highly imbalanced fraud classification problem.

## Fraud by Transaction Type

The transaction type with the highest fraud percentage is
**{highest_risk_type["transaction_type"]}**, with a fraud rate of
**{highest_risk_type["fraud_percentage"]:.2f}%**.

## Transaction Amount Statistics

### Fraudulent Transactions

Mean amount:

**{fraud_amount["mean_amount"]:,.2f}**

Median amount:

**{fraud_amount["median_amount"]:,.2f}**

95th percentile:

**{fraud_amount["percentile_95_amount"]:,.2f}**

### Non-Fraudulent Transactions

Mean amount:

**{non_fraud_amount["mean_amount"]:,.2f}**

Median amount:

**{non_fraud_amount["median_amount"]:,.2f}**

95th percentile:

**{non_fraud_amount["percentile_95_amount"]:,.2f}**

The fraudulent transactions therefore exhibit substantially
different transaction amount distributions from non-fraudulent
transactions.

## Zero Balance Analysis

Among fraudulent transactions:

- Zero sender balance: **{zero_balance_results["zero_sender_percentage"]:.2f}%**
- Zero receiver balance: **{zero_balance_results["zero_receiver_percentage"]:.2f}%**

These are descriptive characteristics of the PaySim dataset and
should not independently be interpreted as proof of fraudulent
behavior in real-world transactions.

## Balance Drain Conditional Probability

The conditional probability

**P(fraud | |balance_drain| > threshold)**

was calculated at the 75th, 90th, 95th and 99th percentiles.

All percentile thresholds and conditional probabilities were
calculated directly using DuckDB SQL.

"""

    notes += "\n| Threshold | P(Fraud \\| Drain > Threshold) |\n"
    notes += "|---|---:|\n"

    for _, row in conditional_results.iterrows():
        notes += (
            f"| {row['threshold_name']} | "
            f"{row['fraud_probability']:.4f}% |\n"
        )

    notes += f"""

## Rule R01 Proxy

The temporary Rule R01 proxy is:

`new_balance_sender = 0 AND amount > median_amount`

The base fraud probability is:

**P(Fraud) = {bayes_results["p_fraud"]:.6f}%**

The probability that R01 fires given fraud is:

**P(R01 | Fraud) = {bayes_results["p_r01_given_fraud"]:.6f}%**

The probability that R01 fires overall is:

**P(R01) = {bayes_results["p_r01"]:.6f}%**

Using Bayes' theorem:

P(Fraud | R01)
=
P(R01 | Fraud) × P(Fraud)
/
P(R01)

The resulting posterior probability is:

**P(Fraud | R01) = {bayes_results["p_fraud_given_r01"]:.6f}%**

This R01 definition is a proxy for the future formal rule and
should be revisited when the formal rule is defined.
"""

    NOTES_PATH.write_text(
        notes,
        encoding="utf-8"
    )

    logger.info(
        f"Analysis notes saved to {NOTES_PATH}"
    )


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():

    REPORTS_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    logger.info(
        "Starting fraud analysis"
    )

    # 1. Overall fraud
    overall_results = get_fraud_percentage()

    logger.info(
        "Fraud analysis completed."
    )

    # 2. Fraud by transaction type
    type_results = (
        get_fraud_percentage_by_transaction_type()
    )

    logger.info(
        "Fraud analysis by category completed."
    )

    # 3. Amount statistics
    amount_results = (
        get_transaction_statistics_by_fraud_status()
    )

    logger.info(
        "Transaction statistics analysis completed."
    )

    # 4. Zero balances
    zero_balance_results = (
        get_zero_balance_fraud_percentage()
    )

    logger.info(
        "Zero balance fraud analysis completed."
    )

    # 5. Z-score analysis + box plot
    zscore_result, zscore_summary = (
        get_fraud_z_scores()
    )

    logger.info(
        "Fraud z-score analysis completed."
    )

    # 6. Balance drain conditional probabilities
    conditional_results = (
        get_balance_drain_conditional_probabilities()
    )

    logger.info(
        "Balance drain conditional probability "
        "analysis completed."
    )

    # 7. Bayes / R01
    bayes_results = (
        get_rule_r01_bayes_probability()
    )

    logger.info(
        "Rule R01 Bayes analysis completed."
    )

    # 8. Save summary
    save_fraud_summary(
        overall_results,
        type_results,
        amount_results,
        zero_balance_results,
        zscore_summary,
        conditional_results,
        bayes_results
    )

    # 9. Save analysis notes
    save_analysis_notes(
        overall_results,
        type_results,
        amount_results,
        zero_balance_results,
        conditional_results,
        bayes_results
    )

    logger.info(
        "Fraud analysis completed successfully."
    )


if __name__ == "__main__":
    main()