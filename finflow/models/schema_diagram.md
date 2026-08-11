---
config:
  layout: elk
---
erDiagram

    DIM_TRANSACTION_TYPE ||--o{ FACT_TRANSACTIONS : "type of transaction(joined on transaction_type_id)"

    DIM_TIME ||--o{ FACT_TRANSACTIONS : "time(joined on step)"

    DIM_ACCOUNT ||--o{ FACT_TRANSACTIONS : "sender(joined on sender_account_id)"

    DIM_ACCOUNT ||--o{ FACT_TRANSACTIONS : "receiver(joined on reciever_account_id)"


    DIM_TRANSACTION_TYPE {
        INTEGER id PK
        VARCHAR type_name
    }

    DIM_ACCOUNT {
        INTEGER id PK
        VARCHAR name
    }

    DIM_TIME {
        INTEGER step PK
        INTEGER sim_day
        INTEGER sim_week
        INTEGER hour_of_day
    }

    FACT_TRANSACTIONS {
        INTEGER transaction_id PK
        INTEGER step FK
        INTEGER transaction_type_id FK
        DOUBLE amount
        DOUBLE log_amount
        DOUBLE balance_drain
        INTEGER sender_account_id FK
        INTEGER receiver_account_id FK
        BOOLEAN is_fraud
        BOOLEAN is_flagged_fraud
        DOUBLE old_balance_sender
        DOUBLE new_balance_sender
        DOUBLE old_balance_receiver
        DOUBLE new_balance_receiver
    }

    COMPLAINTS {
        INTEGER complaint_id PK
        DATE date_received
        VARCHAR product
        VARCHAR sub_product
        VARCHAR issue
        VARCHAR company
        VARCHAR state
        VARCHAR resolution
    }