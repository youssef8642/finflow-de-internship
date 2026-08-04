-- query 1

SELECT type, SUM(amount) AS total_amount
FROM transactions
GROUP BY type;


-- query 2

SELECT type,QUANTILE_CONT(amount, 0.9) AS ninteeth_percentile
FROM transactions
GROUP BY type;

-- query 3

SELECT nameOrig,COUNT(*) AS transaction_count
FROM transactions
GROUP BY nameOrig
HAVING COUNT(*) > 3;  

-- query 4

SELECT step,
	amount AS transfer_amount,
	SUM(amount) OVER (ORDER BY step) AS running_transfer_total
FROM transactions
WHERE type = 'TRANSFER'
ORDER BY step;


-- query 5

WITH TempTable AS (
	SELECT * FROM transactions WHERE newbalanceOrig = 0 AND oldbalanceOrg > 0
)
SELECT
	COUNT(*) AS total_count,
	SUM(isFraud) AS fraud_count,
	ROUND(100.0 * SUM(isFraud) / COUNT(*), 2) AS fraud_percentage
FROM TempTable;