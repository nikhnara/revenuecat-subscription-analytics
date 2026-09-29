select customer_id, activity_month
from {{ ref('int_customer_engagement_monthly') }}
where listening_metrics_valid_flag is null
    or listening_metrics_valid_flag not in (0, 1)
    or play_metrics_valid_flag is null
    or play_metrics_valid_flag not in (0, 1)
    or unique_song_metrics_valid_flag is null
    or unique_song_metrics_valid_flag not in (0, 1)
