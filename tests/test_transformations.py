from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def test_silver_transactions_keeps_latest_row_per_key(tmp_path: Path):
    bronze_dir = tmp_path / "raw" / "cdc" / "transactions"
    bronze_dir.mkdir(parents=True)

    pq.write_table(
        pa.table(
            {
                "transaction_id": [5000],
                "account_id": [1000],
                "transaction_type": ["card_purchase"],
                "amount": [25.50],
                "status": ["pending"],
                "description": ["card purchase #1"],
                "created_at": ["2026-01-01T09:00:00"],
                "updated_at": ["2026-01-01T09:05:00"],
            }
        ),
        bronze_dir / "older.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "transaction_id": [5000],
                "account_id": [1000],
                "transaction_type": ["card_purchase"],
                "amount": [25.50],
                "status": ["completed"],
                "description": ["card purchase #1"],
                "created_at": ["2026-01-01T09:00:00"],
                "updated_at": ["2026-01-01T09:15:00"],
            }
        ),
        bronze_dir / "newer.parquet",
    )

    query = f"""
        with ranked as (
            select
                cast(transaction_id as bigint) as transaction_id,
                cast(account_id as bigint) as account_id,
                cast(transaction_type as varchar) as transaction_type,
                cast(amount as decimal(14, 2)) as amount,
                cast(status as varchar) as status,
                cast(description as varchar) as description,
                cast(created_at as timestamp) as created_at,
                cast(updated_at as timestamp) as updated_at,
                filename as bronze_file_path,
                row_number() over (
                    partition by transaction_id
                    order by updated_at desc, filename desc
                ) as row_num
            from read_parquet('{bronze_dir}/*.parquet', filename=true)
        )
        select transaction_id, status, bronze_file_path
        from ranked
        where row_num = 1
    """

    row = duckdb.connect().execute(query).fetchone()

    assert row[0] == 5000
    assert row[1] == "completed"
    assert row[2].endswith("newer.parquet")
