CREATE TABLE dim_transaction_type (
    id INTEGER PRIMARY KEY,
    type_name VARCHAR NOT NULL UNIQUE
);


CREATE TABLE dim_account (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE
);


CREATE TABLE dim_time (
    step INTEGER PRIMARY KEY,
    sim_day INTEGER NOT NULL,
    sim_week INTEGER NOT NULL,
    hour_of_day INTEGER NOT NULL
);


CREATE TABLE fact_transactions (
    transaction_id INTEGER PRIMARY KEY,

    step INTEGER NOT NULL,

    transaction_type_id INTEGER NOT NULL,

    amount DOUBLE NOT NULL,

    log_amount DOUBLE,

    balance_drain DOUBLE,

    sender_account_id INTEGER NOT NULL,

    receiver_account_id INTEGER NOT NULL,

    is_fraud BOOLEAN NOT NULL,

    is_flagged_fraud BOOLEAN NOT NULL,

    old_balance_sender DOUBLE,

    new_balance_sender DOUBLE,

    old_balance_receiver DOUBLE,

    new_balance_receiver DOUBLE,

    FOREIGN KEY (step)
        REFERENCES dim_time(step),

    FOREIGN KEY (transaction_type_id)
        REFERENCES dim_transaction_type(id),

    FOREIGN KEY (sender_account_id)
        REFERENCES dim_account(id),

    FOREIGN KEY (receiver_account_id)
        REFERENCES dim_account(id)
);


CREATE TABLE complaints (
    complaint_id INTEGER PRIMARY KEY,

    date_received DATE,

    product VARCHAR,

    sub_product VARCHAR,

    issue VARCHAR,

    company VARCHAR,

    state VARCHAR,

    resolution VARCHAR
);