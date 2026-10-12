-- Read-only event-relative mart audit. event_count is repeated on four relative
-- rows, so it is summed only after collapsing to one event customer-month.
with row_keys as (
    select
        count(*) as total_rows,
        count(distinct event_relative_month_key)
            as unique_event_relative_month_keys,
        countif(event_relative_month_key is null)
            as null_event_relative_month_keys
    from {{ ref('mart_lifecycle_event_engagement') }}
),
per_event as (
    select
        event_type,
        customer_id,
        event_month,
        event_customer_month_key,
        count(*) as relative_rows,
        count(distinct relative_month) as distinct_relative_months,
        countif(behavior_month_observable_flag = 1) as observable_months,
        min(event_count) as min_event_count,
        max(event_count) as max_event_count,
        min(balanced_panel_flag) as min_balanced_panel_flag,
        max(balanced_panel_flag) as max_balanced_panel_flag,
        countif(balanced_panel_flag = 1 and relative_month = -3)
            as balanced_minus_3_rows,
        countif(balanced_panel_flag = 1 and relative_month = -2)
            as balanced_minus_2_rows,
        countif(balanced_panel_flag = 1 and relative_month = -1)
            as balanced_minus_1_rows,
        countif(balanced_panel_flag = 1 and relative_month = 0)
            as balanced_month_0_rows
    from {{ ref('mart_lifecycle_event_engagement') }}
    group by event_type, customer_id, event_month, event_customer_month_key
),
per_type as (
    select
        event_type,
        count(*) as event_customer_months,
        sum(min_event_count) as underlying_lifecycle_events,
        countif(min_event_count is distinct from max_event_count)
            as event_count_conflict_customer_months,
        countif(relative_rows != 4 or distinct_relative_months != 4)
            as malformed_four_month_event_customer_months,
        countif(max_balanced_panel_flag = 1)
            as balanced_event_customer_months,
        sum(balanced_minus_3_rows) as balanced_minus_3_rows,
        sum(balanced_minus_2_rows) as balanced_minus_2_rows,
        sum(balanced_minus_1_rows) as balanced_minus_1_rows,
        sum(balanced_month_0_rows) as balanced_month_0_rows,
        countif(
            max_balanced_panel_flag = 1
            and (
                min_balanced_panel_flag != 1
                or relative_rows != 4
                or distinct_relative_months != 4
                or observable_months != 4
                or balanced_minus_3_rows != 1
                or balanced_minus_2_rows != 1
                or balanced_minus_1_rows != 1
                or balanced_month_0_rows != 1
            )
        ) as invalid_balanced_event_customer_months
    from per_event
    group by event_type
)
select
    p.event_type,
    k.total_rows,
    k.unique_event_relative_month_keys,
    k.total_rows - k.null_event_relative_month_keys
        - k.unique_event_relative_month_keys
        as duplicate_event_relative_rows,
    k.null_event_relative_month_keys,
    p.event_customer_months,
    p.underlying_lifecycle_events,
    p.event_count_conflict_customer_months,
    p.malformed_four_month_event_customer_months,
    p.balanced_event_customer_months,
    p.balanced_minus_3_rows,
    p.balanced_minus_2_rows,
    p.balanced_minus_1_rows,
    p.balanced_month_0_rows,
    p.invalid_balanced_event_customer_months
from per_type as p
cross join row_keys as k
order by p.event_type
