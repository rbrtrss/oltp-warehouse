{{ config(location="data/silver/silver_transactions.parquet") }}

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
    from {{ source('bronze', 'transactions_cdc') }}
)
select
    transaction_id,
    account_id,
    transaction_type,
    amount,
    status,
    description,
    created_at,
    updated_at,
    bronze_file_path
from ranked
where row_num = 1
