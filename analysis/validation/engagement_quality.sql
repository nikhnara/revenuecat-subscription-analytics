-- Read-only quality summary over the materialized monthly engagement table.
-- Zero-engagement months are generated in mart_customer_month, not here.
-- An invalid listening month still has observed activity but NULL listening metrics.
select
    count(*) as engagement_monthly_model_rows,
    countif(activity_month is not null) as dated_engagement_customer_months,
    countif(activity_month is null) as undated_activity_month_buckets,
    countif(listening_metrics_valid_flag = 1) as valid_listening_months,
    countif(listening_metrics_valid_flag = 0) as invalid_listening_months,
    sum(invalid_listening_day_count) as invalid_listening_days,
    countif(play_metrics_valid_flag = 0) as invalid_play_months,
    countif(unique_song_metrics_valid_flag = 0)
        as invalid_unique_song_months,
    countif(
        listening_metrics_valid_flag = 0
        and (
            total_listening_seconds is not null
            or avg_listening_seconds_per_active_day is not null
            or avg_weekday_listening_seconds is not null
            or avg_weekend_listening_seconds is not null
        )
    ) as invalid_listening_months_with_populated_listening_metrics,
    countif(
        listening_metrics_valid_flag = 1
        and avg_listening_seconds_per_active_day > 86400
    ) as valid_months_above_86400_seconds_per_active_day,
    countif(
        listening_metrics_valid_flag = 0 and active_days <= 0
    ) as invalid_listening_months_without_observed_activity
from {{ ref('int_customer_engagement_monthly') }}
