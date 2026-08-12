import duckdb

from finflow.config.logger import get_logger


logger = get_logger(__name__)


class DataQualityError(Exception):
    pass


def check_duplicate_transaction_ids(connection):

    result = connection.execute(
        """
        SELECT transaction_id
        FROM fact_transactions
        GROUP BY transaction_id
        HAVING COUNT(*) > 1
        """
    ).fetchall()

    if result:
        raise DataQualityError(
            "Duplicate transaction_id values found"
        )

    logger.info(
        "Duplicate transaction_id check passed"
    )


def check_fraud_nulls(connection):

    result = connection.execute(
        """
        SELECT COUNT(*)
        FROM fact_transactions
        WHERE is_fraud IS NULL
        """
    ).fetchone()[0]

    if result > 0:
        raise DataQualityError(
            f"is_fraud contains {result} NULL values"
        )

    logger.info(
        "is_fraud NULL check passed"
    )


def check_transaction_type_foreign_keys(connection):

    result = connection.execute(
        """
        SELECT COUNT(*)
        FROM fact_transactions ft

        LEFT JOIN dim_transaction_type tt
            ON ft.transaction_type_id = tt.id

        WHERE tt.id IS NULL
        """
    ).fetchone()[0]

    if result > 0:
        raise DataQualityError(
            f"Missing transaction type foreign keys: {result}"
        )

    logger.info(
        "Transaction type foreign key check passed"
    )


def check_sender_foreign_keys(connection):

    result = connection.execute(
        """
        SELECT COUNT(*)
        FROM fact_transactions ft

        LEFT JOIN dim_account a
            ON ft.sender_account_id = a.id

        WHERE a.id IS NULL
        """
    ).fetchone()[0]

    if result > 0:
        raise DataQualityError(
            f"Missing sender account foreign keys: {result}"
        )

    logger.info(
        "Sender account foreign key check passed"
    )


def check_receiver_foreign_keys(connection):

    result = connection.execute(
        """
        SELECT COUNT(*)
        FROM fact_transactions ft

        LEFT JOIN dim_account a
            ON ft.receiver_account_id = a.id

        WHERE a.id IS NULL
        """
    ).fetchone()[0]

    if result > 0:
        raise DataQualityError(
            f"Missing receiver account foreign keys: {result}"
        )

    logger.info(
        "Receiver account foreign key check passed"
    )


def check_negative_amounts(connection):

    result = connection.execute(
        """
        SELECT COUNT(*)
        FROM fact_transactions
        WHERE amount < 0
        """
    ).fetchone()[0]

    if result > 0:
        raise DataQualityError(
            f"Negative transaction amounts found: {result}"
        )

    logger.info(
        "Negative amount check passed"
    )

def run_quality_checks(connection):

    logger.info(
        "Starting data quality checks"
    )

    check_duplicate_transaction_ids(
        connection
    )

    check_fraud_nulls(
        connection
    )

    check_transaction_type_foreign_keys(
        connection
    )

    check_sender_foreign_keys(
        connection
    )

    check_receiver_foreign_keys(
        connection
    )

    check_negative_amounts(
        connection
    )

    logger.info(
        "All data quality checks passed"
    )



def main():

    connection = duckdb.connect(
        "data/finflow.duckdb"
    )

    try:

        run_quality_checks(
            connection
        )

    except DataQualityError as error:

        logger.error(
            f"Data quality check failed: {error}"
        )

        raise

    finally:

        connection.close()


if __name__ == "__main__":
    main()