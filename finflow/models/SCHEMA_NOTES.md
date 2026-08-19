
For mielstone 2, I chose Star Schema option

I chose this design because the main purpose of the project is to work with and analyze financial transaction data. A star schema keeps the structure simple by having one central fact table and several dimension tables around it.

The main table is `fact_transactions`. It contains the actual transaction data such as the transaction amount, balances, fraud indicators, and the IDs connecting the transaction to the dimension tables.

The dimension tables provide additional information about the transaction:

* `dim_transaction_type` stores the different transaction types such as PAYMENT, TRANSFER, CASH_OUT, DEBIT, and DEPOSIT.
* `dim_account` stores the account IDs and account names used by transactions.
* `dim_time` stores the simulated time information based on the PaySim `step` value.
* `complaints` stores the filtered CFPB complaint data separately.

## Why I Chose a Star Schema

I chose the star schema mainly because it is simpler and suitable for analytical queries.

Instead of storing information such as the transaction type or account name repeatedly inside every transaction, the fact table stores IDs that reference the dimension tables. This reduces repeated data and makes it easier to query the data based on transaction type, account, or time.

For example, `fact_transactions.transaction_type_id` references `dim_transaction_type.id`, while `sender_account_id` and `receiver_account_id` both reference `dim_account.id`.

Another reason for choosing this design is that it keeps the number of joins relatively small compared with a more normalized design. This is useful for the analytical queries that will be performed later in the project.

## Why I Did Not Choose the Snowflake Design

The alternative was to use a snowflake schema by splitting the account dimension into multiple tables, such as `dim_account` and `dim_account_type`.

I decided not to use this approach because it would add another table and another join without providing a major benefit for this project. The account information required by the current dataset is simple, so keeping it in a single `dim_account` table makes the schema easier to understand and work with.

Overall, the pure star schema provides a good balance between simplicity and efficient analytical querying for this project.

---

## Milestone 2.2 — Loading Notes

These notes cover implementation decisions in `load_schema.py` that are not
visible from the schema itself.

### Deriving dim_time: why `//` and not `/`

`dim_time` is built from the PaySim `step`, which is a 1-indexed hour counter:

```sql
((step - 1) // 24) + 1   AS sim_day
((step - 1) // 168) + 1  AS sim_week
(step - 1) % 24          AS hour_of_day
```

Two details matter here.

The `- 1` is needed because steps start at 1, not 0. Without it, steps 24 and 25
both land in day 1 (`24 // 24 = 1` and `25 // 24 = 1`) and every day boundary
ends up shifted by 23 hours.

The division must be `//` and not `/`. In DuckDB, `/` performs **float**
division and returns a `DOUBLE` — `743 / 24` gives 30.9583, not 30. Because
`sim_day` is declared `INTEGER`, that DOUBLE gets **rounded** on insert rather
than truncated. The visible symptom is that day 1 covers only steps 1–12 instead
of 1–24, every later boundary sits half a day out, and a phantom 32nd day
appears at the end of a 31-day dataset. Every daily aggregation in Week 3 is
built on `sim_day`, so this single operator silently corrupts all of it.

With `//` the result is correct: 31 days of exactly 24 steps each, 5 weeks of
168 steps (the last partial), and `hour_of_day` spanning 0–23.

### Idempotency

The milestone requires that running the load twice does not duplicate rows. The
approach here is truncate-then-load: `create_tables()` runs `DELETE FROM` on
every table before any insert happens, so a second run rebuilds the same state
rather than appending to it. The `--reload` flag additionally drops the tables
and recreates them from `schema.sql`, which is what to use after changing the
schema itself.

Tables are emptied in reverse dependency order — `fact_transactions` first —
because it holds the foreign keys pointing at the dimensions.

Verified: two consecutive runs both produced exactly 6,362,620 rows in
`fact_transactions`, and all four data quality checks passed each time.

### Why the workers write Parquet instead of inserting

The milestone describes each worker inserting its own chunk. DuckDB permits only
one writer per database file, so parallel inserts into the same `.duckdb` are not
possible. The workaround is that each worker opens its own in-memory connection,
copies its id range out to a small temporary Parquet file, and the parent process
inserts those files afterwards. The read and the row selection are parallel; only
the final insert is serialised.

Chunks are selected with `WHERE transaction_id BETWEEN x AND y` rather than
`LIMIT/OFFSET`. Two reasons: DuckDB does not guarantee a stable row order when
scanning Parquet, so `OFFSET` boundaries are not reproducible; and `OFFSET`
forces the engine to scan and discard every preceding row, which makes the last
chunk read almost the entire file. Filtering on `transaction_id` is deterministic
and lets Parquet row-group statistics skip most of the file.

`transaction_id` itself is assigned in the transformation stage, right after the
chunks are concatenated, which is the last point in the pipeline where global row
order is guaranteed.

### Measured load

| Step | Result |
|---|---|
| dim_transaction_type | 5 rows |
| dim_account | 9,073,900 rows |
| dim_time | 743 rows |
| complaints | 718,623 rows |
| fact_transactions | 6,362,620 rows (matches source Parquet) |
| Total load time | 140.11 seconds |
