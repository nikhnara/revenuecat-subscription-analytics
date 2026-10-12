-- Read-only check of the materialized customer-month mart. Grouping by customer
-- allows row totals, grain checks, and spine endpoints to share one mart scan.
with per_customer as (
    select
        customer_id,
        count(*) as mart_rows,
        count(distinct calendar_month) as distinct_calendar_months,
        min(calendar_month) as first_calendar_month,
        max(calendar_month) as last_calendar_month,
        min(first_observed_month) as min_declared_start,
        max(first_observed_month) as max_declared_start,
        min(observation_end_month) as min_declared_end,
        max(observation_end_month) as max_declared_end,
        countif(has_engagement_flag = 1) as engagement_months,
        countif(zero_engagement_flag = 1) as zero_engagement_months,
        countif(has_transaction_flag = 1) as transaction_months,
        countif(
            has_engagement_flag + zero_engagement_flag is distinct from 1
        ) as inconsistent_engagement_presence_flags,
        countif(
            zero_engagement_flag = 1
            and (
                has_engagement_flag is distinct from 0
                or active_days is distinct from 0
                or total_listening_seconds is distinct from 0
                or total_plays is distinct from 0
                or avg_listening_seconds_per_active_day is not null
                or avg_plays_per_active_day is not null
                or avg_daily_unique_songs is not null
                or listening_metrics_valid_flag is not null
                or play_metrics_valid_flag is not null
                or unique_song_metrics_valid_flag is not null
            )
        ) as zero_engagement_semantic_violations,
        countif(
            has_engagement_flag = 1
            and invalid_listening_day_count > 0
            and (
                total_listening_seconds is not null
                or avg_listening_seconds_per_active_day is not null
            )
        ) as invalid_listening_months_with_populated_metrics
    from {{ ref('mart_customer_month') }}
    group by customer_id
)
select
    sum(mart_rows) as total_mart_rows,
    count(*) as distinct_customers,
    sum(engagement_months) as engagement_customer_months,
    sum(zero_engagement_months) as zero_engagement_customer_months,
    sum(transaction_months) as transaction_customer_months,
    sum(mart_rows - distinct_calendar_months)
        as duplicate_customer_month_rows,
    countif(
        first_calendar_month is distinct from min_declared_start
        or min_declared_start is distinct from max_declared_start
    ) as customers_not_starting_at_first_observed_month,
    countif(
        last_calendar_month is distinct from min_declared_end
        or min_declared_end is distinct from max_declared_end
    ) as customers_not_reaching_common_observation_end,
    countif(
        distinct_calendar_months
        != date_diff(last_calendar_month, first_calendar_month, month) + 1
    ) as customers_with_internal_month_gaps,
    sum(inconsistent_engagement_presence_flags)
        as inconsistent_engagement_presence_rows,
    sum(zero_engagement_semantic_violations)
        as zero_engagement_semantic_violations,
    sum(invalid_listening_months_with_populated_metrics)
        as invalid_listening_months_with_populated_metrics
from per_customer
