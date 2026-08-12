CREATE OR REPLACE VIEW v_monthly_volume AS
SELECT
    FLOOR((dt.sim_day - 1) / 30) + 1 AS sim_month,
    tt.type_name AS transaction_type,
    COUNT(*) AS transaction_count,
    SUM(ft.amount) AS total_amount
FROM fact_transactions ft
JOIN dim_time dt
    ON ft.step = dt.step
JOIN dim_transaction_type tt
    ON ft.transaction_type_id = tt.id
GROUP BY
    sim_month,
    tt.type_name;


CREATE OR REPLACE VIEW v_fraud_by_type AS
SELECT
    tt.type_name AS transaction_type,
    SUM(CASE WHEN ft.is_fraud THEN 1 ELSE 0 END) AS fraud_count,
    COUNT(*) AS total_count,
    SUM(CASE WHEN ft.is_fraud THEN 1 ELSE 0 END) * 1.0
        / COUNT(*) AS fraud_rate
FROM fact_transactions ft
JOIN dim_transaction_type tt
    ON ft.transaction_type_id = tt.id
GROUP BY
    tt.type_name;


CREATE OR REPLACE VIEW v_monthly_complaints AS
SELECT
    product,
    YEAR(date_received) AS year,
    MONTH(date_received) AS month,
    COUNT(*) AS complaint_count
FROM complaints
GROUP BY
    product,
    YEAR(date_received),
    MONTH(date_received);


CREATE OR REPLACE VIEW v_balance_anomalies AS
SELECT
    transaction_id,
    step,
    amount,
    balance_drain,
    is_fraud
FROM fact_transactions
WHERE
    is_fraud = FALSE
    AND ABS(balance_drain) > 0.01;