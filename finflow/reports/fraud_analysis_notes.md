# Fraud Analysis Notes

## Overall Fraud Rate

The dataset contains 6,362,620
transactions, of which 8,213
are fraudulent.

The overall fraud rate is
**0.13%**.

This indicates a highly imbalanced fraud classification problem.

## Fraud by Transaction Type

The transaction type with the highest fraud percentage is
**TRANSFER**, with a fraud rate of
**0.77%**.

## Transaction Amount Statistics

### Fraudulent Transactions

Mean amount:

**1,467,967.30**

Median amount:

**441,423.44**

95th percentile:

**8,006,429.04**

### Non-Fraudulent Transactions

Mean amount:

**178,197.04**

Median amount:

**74,684.72**

95th percentile:

**515,610.42**

The fraudulent transactions therefore exhibit substantially
different transaction amount distributions from non-fraudulent
transactions.

## Zero Balance Analysis

Among fraudulent transactions:

- Zero sender balance: **98.05%**
- Zero receiver balance: **49.81%**

These are descriptive characteristics of the PaySim dataset and
should not independently be interpreted as proof of fraudulent
behavior in real-world transactions.

## Balance Drain Conditional Probability

The conditional probability

**P(fraud | |balance_drain| > threshold)**

was calculated at the 75th, 90th, 95th and 99th percentiles.

All percentile thresholds and conditional probabilities were
calculated directly using DuckDB SQL.


| Threshold | P(Fraud \| Drain > Threshold) |
|---|---:|
| 75th | 0.0017% |
| 90th | 0.0030% |
| 95th | 0.0050% |
| 99th | 0.0204% |


## Rule R01 Proxy

The temporary Rule R01 proxy is:

`new_balance_sender = 0 AND amount > median_amount`

The base fraud probability is:

**P(Fraud) = 0.129082%**

The probability that R01 fires given fraud is:

**P(R01 | Fraud) = 80.944844%**

The probability that R01 fires overall is:

**P(R01) = 31.928985%**

Using Bayes' theorem:

P(Fraud | R01)
=
P(R01 | Fraud) × P(Fraud)
/
P(R01)

The resulting posterior probability is:

**P(Fraud | R01) = 0.327243%**