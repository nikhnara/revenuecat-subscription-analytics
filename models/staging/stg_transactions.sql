{{ config(materialized='view') }}

with combined as (

    select *, 'v1' as source_release
    from {{ source('kkbox_raw', 'transactions_v1') }}

    union all

    select *, 'v2' as source_release
    from {{ source('kkbox_raw', 'transactions_v2') }}

)

select
    msno as customer_id,
    payment_method_id,
    safe_cast(payment_plan_days as int64) as plan_days,
    safe_cast(plan_list_price as numeric) as list_price_ntd,
    safe_cast(actual_amount_paid as numeric) as recorded_amount_paid_ntd,
    safe_cast(is_auto_renew as int64) as auto_renew_flag,
    safe.parse_date('%Y%m%d', transaction_date) as transaction_date,
    safe.parse_date('%Y%m%d', membership_expire_date) as expiry_date,
    safe_cast(is_cancel as int64) as cancellation_flag,
    source_release
from combined
