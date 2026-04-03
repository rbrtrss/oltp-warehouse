from __future__ import annotations

from dataclasses import dataclass

from oltp_warehouse.config import DatabaseConfig
from oltp_warehouse.seed_data import SeedBundle, build_seed_bundle

DDL_STATEMENTS = (
    """
    DROP TABLE IF EXISTS payments;
    DROP TABLE IF EXISTS transfers;
    DROP TABLE IF EXISTS transactions;
    DROP TABLE IF EXISTS accounts;
    """,
    """
    CREATE TABLE accounts (
        account_id BIGINT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        account_type TEXT NOT NULL,
        status TEXT NOT NULL,
        currency_code CHAR(3) NOT NULL,
        balance NUMERIC(14, 2) NOT NULL,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL
    );
    """,
    """
    CREATE TABLE transactions (
        transaction_id BIGINT PRIMARY KEY,
        account_id BIGINT NOT NULL REFERENCES accounts(account_id),
        transaction_type TEXT NOT NULL,
        amount NUMERIC(14, 2) NOT NULL,
        status TEXT NOT NULL,
        description TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL
    );
    """,
    """
    CREATE TABLE transfers (
        transfer_id BIGINT PRIMARY KEY,
        source_account_id BIGINT NOT NULL REFERENCES accounts(account_id),
        destination_account_id BIGINT NOT NULL REFERENCES accounts(account_id),
        amount NUMERIC(14, 2) NOT NULL,
        status TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL,
        CHECK (source_account_id <> destination_account_id)
    );
    """,
    """
    CREATE TABLE payments (
        payment_id BIGINT PRIMARY KEY,
        account_id BIGINT NOT NULL REFERENCES accounts(account_id),
        merchant_name TEXT NOT NULL,
        category TEXT NOT NULL,
        amount NUMERIC(14, 2) NOT NULL,
        status TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL
    );
    """,
)


@dataclass(frozen=True)
class BootstrapConfig:
    seed: int = 42
    accounts: int = 25
    transactions: int = 120
    transfers: int = 40
    payments: int = 60


def bootstrap_database(config: BootstrapConfig) -> dict[str, int]:
    from psycopg import connect

    db_config = DatabaseConfig.from_env()
    bundle = build_seed_bundle(
        seed=config.seed,
        account_count=config.accounts,
        transaction_count=config.transactions,
        transfer_count=config.transfers,
        payment_count=config.payments,
    )

    with connect(
        host=db_config.host,
        port=db_config.port,
        dbname=db_config.dbname,
        user=db_config.user,
        password=db_config.password,
    ) as connection:
        connection.autocommit = False
        with connection.cursor() as cursor:
            recreate_schema(cursor)
            insert_seed_data(cursor, bundle)
        connection.commit()

    return {
        "accounts": len(bundle.accounts),
        "transactions": len(bundle.transactions),
        "transfers": len(bundle.transfers),
        "payments": len(bundle.payments),
    }


def recreate_schema(cursor) -> None:
    for statement in DDL_STATEMENTS:
        cursor.execute(statement)


def insert_seed_data(cursor, bundle: SeedBundle) -> None:
    cursor.executemany(
        """
        INSERT INTO accounts (
            account_id, customer_id, account_type, status,
            currency_code, balance, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        bundle.accounts,
    )
    cursor.executemany(
        """
        INSERT INTO transactions (
            transaction_id, account_id, transaction_type, amount, status,
            description, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        bundle.transactions,
    )
    cursor.executemany(
        """
        INSERT INTO transfers (
            transfer_id, source_account_id, destination_account_id, amount,
            status, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        bundle.transfers,
    )
    cursor.executemany(
        """
        INSERT INTO payments (
            payment_id, account_id, merchant_name, category, amount, status,
            created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        bundle.payments,
    )
