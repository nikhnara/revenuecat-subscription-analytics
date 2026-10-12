-- Expensive manual source diagnostic: this groups the full 410M-row daily
-- staging model. It is intentionally outside tests/ and never runs as a dbt
-- production test. NULL keys/dates remain visible as separate audit counts.
with customer_days as (
    select
        customer_id,
        activity_date,
        count(*) as source_row_count
    from {{ ref('stg_user_logs') }}
    group by customer_id, activity_date
)

select
    coalesce(sum(if(customer_id is null, source_row_count, 0)), 0)
        as rows_without_customer_id,
    coalesce(sum(if(activity_date is null, source_row_count, 0)), 0)
        as rows_with_missing_or_invalid_activity_date,
    countif(
        customer_id is not null
        and activity_date is not null
        and source_row_count > 1
    ) as duplicate_customer_day_groups,
    coalesce(sum(if(
        customer_id is not null
        and activity_date is not null
        and source_row_count > 1,
        source_row_count - 1,
        0
    )), 0) as excess_duplicate_customer_day_rows
from customer_days
