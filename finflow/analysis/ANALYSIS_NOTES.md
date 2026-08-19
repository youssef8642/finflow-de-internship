# Analysis Notes

Week 3 analysis, Milestones 3.1 to 3.4. Every number below comes from a run of
the scripts in `finflow/analysis/`; the charts are written to `finflow/reports/`.

The dataset is 6,362,620 PaySim transactions spanning 743 hourly steps, which
`dim_time` groups into 31 simulated days.

---

## Milestone 3.1 — Transaction Distribution & Time Series

Script: `transaction_analysis.py` · Charts: `transaction_time_series.png`,
`amount_distribution.png`, `balance_drain_distribution.png`

Computing the three daily series concurrently with `ThreadPoolExecutor` instead
of one after another gave a measured speedup of **1.91x** (0.205s to 0.107s).
Threads help here because the work is spent inside DuckDB rather than in Python
bytecode, and DuckDB releases the GIL while a query runs.

### Chart 1 — Daily time series

CASH_OUT and PAYMENT dominate daily volume at roughly 150,000 transactions per
day at peak, TRANSFER runs about a quarter of that, and DEBIT is negligible
throughout. The striking feature is the collapse after day 18: days 1–18 hold
**6,008,416** of the 6,362,620 transactions, leaving only **354,204** across the
remaining thirteen days.

Daily fraud count barely moves. It stays between 216 and 320 for the whole month
and shows no response at all to the volume collapse. The consequence is that the
fraud rate is not stationary:

| Period | Transactions | Fraud | Fraud rate |
|---|---:|---:|---:|
| Days 1–18 | 6,008,416 | 4,857 | 0.0808% |
| Days 19–31 | 354,204 | 3,356 | 0.9475% |

That is a twelvefold jump in the fraud rate produced entirely by the denominator
falling away while the numerator held steady. Economically this is what a fixed
adversarial campaign looks like: the fraudster population attempts a roughly
constant number of transactions per day regardless of how much legitimate
activity is around to hide inside, rather than scaling with market volume. For
detection this is the single most important property in the chart, because a
threshold calibrated on the busy period faces a base rate an order of magnitude
different in the quiet one.

The mean-amount panel adds a second point. TRANSFER carries a mean transaction
size an order of magnitude above every other type, and the spike to roughly 2.5
million on day 13 shows how heavily that mean is pulled by a handful of very
large transfers. A mean that moves that violently in one day is a warning that
the mean is the wrong summary statistic here, and that median or quantile
measures should be preferred when setting any amount-based rule.

### Chart 2 — Amount distribution, and is it log-normal?

TRANSFER centres at a log amount of 12.97 (about 430,000 currency units) against
11.68 for CASH_OUT (about 118,000), so transfers are systematically larger than
cash withdrawals.

If `amount` were log-normal then `log(amount + 1)` would be normal, which means
skewness 0 and excess kurtosis 0. The measured values are:

| Type | Mean | Std dev | Skewness | Excess kurtosis |
|---|---:|---:|---:|---:|
| TRANSFER | 12.97 | 1.30 | −0.71 | 2.20 |
| CASH_OUT | 11.68 | 1.08 | −1.52 | 4.29 |

**Neither is log-normal.** On the chart the dashed normal fit and the filled
empirical curve visibly separate: both empirical curves lean left and are taller
and narrower than the normal of the same mean and standard deviation.

Both are *left* skewed, which means the excess kurtosis sits mostly in the lower
tail rather than the upper one. That distinction matters, because a fraud rule
only ever inspects the upper tail. Testing the upper tail directly:

| Type | Share above z = 3 | A normal predicts |
|---|---:|---:|
| TRANSFER | 0.1368% | 0.135% |
| CASH_OUT | 0.0261% | 0.135% |

**Why this matters for detection thresholds.** Rule R02 in Milestone 4.1 uses a
z-score cut-off of 3, and that number only corresponds to a known tail
probability if the distribution is roughly normal. For TRANSFER the normal
assumption is close enough to harmless. For CASH_OUT it is wrong by a factor of
five, and it errs in the direction that costs recall: the rule fires far *less*
often than the assumption implies, so it under-flags rather than over-flags. The
cause is that the CASH_OUT upper tail is compressed — its 99.9th percentile log
amount is 13.67 against 17.03 for TRANSFER — so a cut-off placed three standard
deviations out lands beyond nearly all of the data. Any threshold taken from the
normal assumption should be checked against the empirical percentiles for that
transaction type instead.

### Chart 3 — Balance drain distribution

`balance_drain = old_balance_org − new_balance_org − amount`. It never rises
above zero (maximum exactly 0.0, minimum −92,445,516.6), so it measures only
unaccounted-for outflow, never a surplus.

**78.60% of all transactions have `|balance_drain| > 1`**, which sounds like a
near-total data integrity failure until it is split by type:

| Type | Share exceeding tolerance |
|---|---:|
| CASH_IN | 100.00% |
| TRANSFER | 95.24% |
| CASH_OUT | 88.54% |
| PAYMENT | 51.18% |
| DEBIT | 28.44% |

The CASH_IN figure is a formula artifact, not a data defect. CASH_IN is an
*inflow*, so a correct record satisfies `new_balance = old_balance + amount`,
which makes `old − new − amount` equal to `−2 × amount`. In fact **92.78% of
CASH_IN rows satisfy that inflow identity exactly** — the type with the worst
apparent score has the cleanest records. The formula in the milestone is written
for outflows and simply does not apply to CASH_IN.

For the genuine outflow types the driver is different: 53.06% of TRANSFER rows,
45.85% of CASH_OUT rows and 35.99% of PAYMENT rows carry
`old_balance = new_balance = 0` alongside a positive amount, which mechanically
forces `balance_drain = −amount`. This is PaySim's known balance-field
incompleteness.

The practical conclusion is that `balance_drain` cannot be used as a raw fraud
signal — a rule firing on it would flag roughly four transactions in five. It is
only informative conditioned on transaction type and on the balance fields
actually being populated.

---

## Milestone 3.2 — Fraud Prevalence & Statistical Analysis

Script: `fraud_analysis.py` · Outputs: `fraud_summary.csv`,
`fraud_zscore_boxplot.png`

### Summary table

Overall fraud rate: **0.1291%** — 8,213 fraudulent transactions out of
6,362,620. This is a severely imbalanced classification problem: a model that
predicted "never fraud" would be 99.87% accurate and completely useless, which
is why Milestone 4.2 evaluates precision, recall and F1 rather than accuracy.

P(fraud | transaction_type):

| Type | Fraud rate |
|---|---:|
| TRANSFER | 0.7688% |
| CASH_OUT | 0.1840% |
| PAYMENT | 0% |
| CASH_IN | 0% |
| DEBIT | 0% |

Fraud exists in exactly two of the five types. That matches the PaySim paper's
design, where the fraudulent agent takes control of an account, moves the
balance out by TRANSFER and then cashes it out. It also means any rule applied
to PAYMENT, CASH_IN or DEBIT can only generate false positives.

Transaction amounts, fraud versus non-fraud:

| Statistic | Fraud | Non-fraud | Ratio |
|---|---:|---:|---:|
| Mean | 1,467,967 | 178,197 | 8.2× |
| Median | 441,423 | 74,685 | 5.9× |
| 95th percentile | 8,006,429 | 515,610 | 15.5× |

Fraudulent transactions are far larger at every point of the distribution, and
the gap widens toward the tail. That is consistent with the economics of the
attack: the cost of compromising an account is roughly fixed, so the return is
maximised by draining as much as possible per attempt.

Zero-balance characteristics of fraud cases:

- **98.05%** end with the sender balance at exactly 0
- **49.81%** end with the receiver balance at exactly 0

The first is the full-drain signature that motivates rule R01. The second is the
classic laundering pattern the milestone refers to — funds arrive at the
destination account and the balance is not credited because the money is swept
straight back out.

### Z-score analysis

Z-scores are computed within each transaction type, using the mean and standard
deviation of *all* transactions of that type rather than only the fraudulent
ones, because that is the population a live detection rule would compare a new
transaction against. Only TRANSFER and CASH_OUT appear on the box plot, since
the other three types contain no fraud.

### Conditional probability table

P(fraud | `|balance_drain|` > threshold), thresholds taken at percentiles of
`|balance_drain|`, computed in DuckDB SQL:

| Threshold | P(fraud) | Versus base rate of 0.1291% |
|---|---:|---|
| 75th percentile | 0.0017% | 76× lower |
| 90th percentile | 0.0030% | 43× lower |
| 95th percentile | 0.0050% | 26× lower |
| 99th percentile | 0.0204% | 6× lower |

**This inverts the assumption in the milestone.** The brief describes this table
as "the empirical basis for Rule R01", implying that a large balance drain
signals fraud. In this dataset it signals the opposite: conditioning on a large
drain makes fraud *less* likely at every threshold tested.

Checking the reverse direction:

| Condition | P(fraud) | Lift |
|---|---:|---:|
| Base rate | 0.1291% | 1.0× |
| `\|balance_drain\| ≤ 1` | 0.5998% | 4.6× |

**99.45% of fraudulent transactions have `|balance_drain| ≤ 1`**, against 21.3%
of legitimate ones. The median drain for fraud is exactly 0.0; for non-fraud it
is −69,049.

The reason is mechanical. Fraud drains the account completely, so
`old_balance = amount` and `new_balance = 0`, which makes
`old − new − amount = 0` exactly. Fraudulent rows have a *perfectly consistent*
ledger. It is the legitimate rows, with PaySim's zeroed balance fields, that
look inconsistent. The predictive signal is therefore a consistent ledger
combined with a zero ending balance, not a large drain — which is the direction
Rule R05 should be built in.

### Bayes' theorem applied to the R01 proxy

R01 proxy: `new_balance_sender = 0 AND amount > median_amount`.

```
P(fraud | R01) = P(R01 | fraud) × P(fraud) / P(R01)
```

Measured inputs:

- P(fraud) = 0.129082%
- P(R01 | fraud) = 80.9448%
- P(R01) = 31.9290%

Substituting:

```
P(fraud | R01) = 0.809448 × 0.00129082 / 0.319290 = 0.003272 = 0.3272%
```

The direct count gives **0.3272%** as well, so the theorem and the raw counts
agree exactly.

Interpretation: firing R01 raises the probability of fraud from 0.129% to
0.327%, a lift of about 2.5×. That is a real improvement on the base rate, but
it also means **99.67% of the transactions R01 flags are not fraud**. R01 fires
on 31.93% of all transactions, so on its own it is far too broad to act on. It
is only useful as one input to a composite rule, which is exactly why Milestone
4.2 evaluates a `flag_count >= 2` combination rather than any single rule.

---

## Milestone 3.3 — Macro Enrichment

Script: `macro_enrichment.py` · Charts: `macro_d1_cpi_transfer.png`,
`macro_d2_unrate_cashout.png` · Table: `macro_correlations.csv`

### Design Choice D — the two correlations chosen

**D1: CPI vs TRANSFER volume.** Hypothesis: inflation pushes people to move
money electronically faster, because holding cash loses value.

**D2: Unemployment rate vs CASH_OUT volume.** Hypothesis: higher unemployment
means more cash withdrawals as households draw down savings.

These two were chosen because they test opposite sides of the same behaviour —
one predicts money moving faster *between* accounts, the other predicts money
leaving the system entirely — and because CPI and unemployment are the two
series with the cleanest monthly coverage of the window.

### The step-to-month mapping

The milestone specifies step 1 = January 2019 and 24 steps = one month. In
PaySim a step is really one *hour*, so 24 steps is one day. This mapping is a
convention the project imposes, not a property of the dataset, and it exists for
a good reason: under the literal reading the whole simulation covers about one
month, which is a single observation and cannot be correlated with anything.
Stretching each simulated day into a calendar month produces 31 monthly
observations, running January 2019 to July 2021.

Because `dim_time.sim_day` is already `((step − 1) // 24) + 1`, it *is* the month
number under this mapping, so no separate derivation is needed. Both FRED series
have exactly 31 monthly observations in that window, so the join is 1-to-1 and
loses no rows.

### Results

| Correlation | Sample | Pearson r | p | Spearman r | p | OLS slope | R² |
|---|---|---:|---:|---:|---:|---:|---:|
| D1 CPI vs TRANSFER | All 31 months | −0.5704 | 0.0008 | −0.4942 | 0.0047 | −1974.10 | 0.325 |
| D1 CPI vs TRANSFER | Months 1–18 | +0.0785 | 0.7568 | −0.1487 | 0.5560 | +625.82 | 0.006 |
| D2 UNRATE vs CASH_OUT | All 31 months | −0.2478 | 0.1790 | −0.3615 | 0.0457 | −6171.04 | 0.061 |
| D2 UNRATE vs CASH_OUT | Months 1–18 | −0.0803 | 0.7515 | −0.0106 | 0.9665 | −1441.38 | 0.006 |

### Causal or spurious?

**Both are spurious, and the second row of each pair proves it.**

On the full 31 months D1 looks like a strong result: r = −0.57 with p = 0.0008,
comfortably significant, explaining a third of the variance. Restricted to
months 1–18 the coefficient collapses to +0.08 with p = 0.76 and **changes
sign**. D2 behaves the same way, falling from −0.25 to −0.08.

The reason is the volume collapse documented in Milestone 3.1. Ninety-four
percent of the transactions fall in the first 18 months, so both volume series
fall off a cliff at month 19 for reasons entirely internal to the simulation.
CPI rises steadily across the whole window. The "significant" full-sample
correlation is therefore just a rising macro series measured against a
collapsing volume series, and it disappears the moment the artificial collapse
is removed.

There is also a stronger, prior reason no causal reading is available: the
PaySim transactions are simulated, and were generated without any reference to
US CPI or US unemployment. There is no channel through which one could move the
other. Whatever coefficient we measure is a coincidence between two series that
happen to move over the same 31 months.

Finally, D1 is mis-specified as stated. `CPIAUCSL` is a price *index* — a level,
not a rate. Inflation is the rate of change of that index. Two series that both
trend upward will correlate almost by construction, which is the classic
spurious regression problem (Granger and Newbold, 1974). Testing the stated
hypothesis properly would mean regressing volume on the month-over-month percent
change in CPI rather than on the index level.

---

## Milestone 3.4 — Complaint Trend Analysis (stretch goal)

Script: `complaints_analysis.py` · Chart: `complaints_monthly.png` · Tables:
`complaints_top_issues.csv`, `complaints_spike_months.csv`

718,623 CFPB complaints across the two products retained at ingestion.

### Top 5 issues per product

Computed with `RANK() OVER (PARTITION BY product ORDER BY complaint_count DESC)`.

**Checking or savings account**

| Rank | Issue | Complaints |
|---:|---|---:|
| 1 | Managing an account | 225,374 |
| 2 | Closing an account | 49,613 |
| 3 | Problem with a lender or other company charging your account | 42,884 |
| 4 | Opening an account | 35,533 |
| 5 | Problem caused by your funds being low | 29,776 |

**Credit card**

| Rank | Issue | Complaints |
|---:|---|---:|
| 1 | Problem with a purchase shown on your statement | 57,630 |
| 2 | Incorrect information on your report | 31,937 |
| 3 | Getting a credit card | 30,125 |
| 4 | Problem with a company's investigation into an existing problem | 26,284 |
| 5 | Other features, terms, or problems | 24,341 |

"Managing an account" dominates checking and savings by a factor of four and a
half over the next issue, which is a category-design artifact as much as a
finding — it is a broad bucket that absorbs many distinct underlying problems.
The credit card list is more evenly spread and more specific, led by disputed
transactions, which is the complaint type most directly adjacent to fraud.

### Spike months

A month is flagged when its complaint count exceeds the mean plus two standard
deviations of the preceding 6-month rolling window, so a spike is judged against
what was normal immediately before it rather than against the whole history.

| Product | Month | Complaints | Threshold | CPI | Unemployment |
|---|---|---:|---:|---:|---:|
| Checking or savings account | 2023-01 | 6,395 | 6,382.5 | 300.420 | 3.5% |
| Checking or savings account | 2025-01 | 18,367 | 18,152.3 | 318.961 | 4.0% |

Only two months clear the threshold, and both only barely — 6,395 against 6,382
and 18,367 against 18,152, margins well under 2%. Both fall in January, which
points at a seasonal filing pattern rather than a macro shock.

Cross-referencing against the 3.3 indicators finds nothing: unemployment sat at
3.5% and 4.0%, both unremarkable, and CPI was on its ordinary upward path in
each month. Neither spike coincides with macro stress. Given the thin margins
and the shared calendar month, the honest reading is that the rolling-window
rule is picking up January seasonality, and a seasonally adjusted baseline would
probably remove both flags.
