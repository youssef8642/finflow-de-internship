
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
