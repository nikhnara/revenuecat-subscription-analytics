-- Phase 3 manual source diagnostic. This scans the large daily table when run;
-- it is intentionally outside tests/ and does not run in a normal dbt test.
-- NULL parsed dates remain a separate invalid-date audit concern.
with customer_days as (
    select
        customer_id,
        activity_date,
        count(*) as source_row_count
    from {{ ref('stg_user_logs') }}
    where customer_id is not null and activity_date is not null
    group by customer_id, activity_date
)

select
    count(*) as duplicate_customer_day_groups,
    coalesce(sum(source_row_count - 1), 0) as excess_source_rows
from customer_days
where source_row_count > 1
