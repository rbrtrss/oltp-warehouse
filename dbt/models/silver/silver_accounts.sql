{{ config(location="data/silver/silver_accounts.parquet") }}

with ranked as (
    select
        cast(account_id as bigint) as account_id,
        cast(customer_id as varchar) as customer_id,
        cast(account_type as varchar) as account_type,
        cast(status as varchar) as status,
        cast(currency_code as varchar) as currency_code,
        cast(balance as decimal(14, 2)) as balance,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        filename as bronze_file_path,
        row_number() over (
            partition by account_id
            order by updated_at desc, filename desc
        ) as row_num
    from {{ source('bronze', 'accounts_cdc') }}
)
select
    account_id,
    customer_id,
    account_type,
    status,
    currency_code,
    balance,
    created_at,
    updated_at,
    bronze_file_path
from ranked
where row_num = 1
