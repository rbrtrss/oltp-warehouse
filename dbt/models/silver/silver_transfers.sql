{{ config(location="data/silver/silver_transfers.parquet") }}

with ranked as (
    select
        cast(transfer_id as bigint) as transfer_id,
        cast(source_account_id as bigint) as source_account_id,
        cast(destination_account_id as bigint) as destination_account_id,
        cast(amount as decimal(14, 2)) as amount,
        cast(status as varchar) as status,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        filename as bronze_file_path,
        row_number() over (
            partition by transfer_id
            order by updated_at desc, filename desc
        ) as row_num
    from {{ source('bronze', 'transfers_cdc') }}
)
select
    transfer_id,
    source_account_id,
    destination_account_id,
    amount,
    status,
    created_at,
    updated_at,
    bronze_file_path
from ranked
where row_num = 1
