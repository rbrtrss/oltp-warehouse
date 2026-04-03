{{ config(location="data/silver/silver_payments.parquet") }}

with ranked as (
    select
        cast(payment_id as bigint) as payment_id,
        cast(account_id as bigint) as account_id,
        cast(merchant_name as varchar) as merchant_name,
        cast(category as varchar) as category,
        cast(amount as decimal(14, 2)) as amount,
        cast(status as varchar) as status,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        filename as bronze_file_path,
        row_number() over (
            partition by payment_id
            order by updated_at desc, filename desc
        ) as row_num
    from {{ source('bronze', 'payments_cdc') }}
)
select
    payment_id,
    account_id,
    merchant_name,
    category,
    amount,
    status,
    created_at,
    updated_at,
    bronze_file_path
from ranked
where row_num = 1
