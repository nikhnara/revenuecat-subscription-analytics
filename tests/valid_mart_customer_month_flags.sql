select customer_id, calendar_month
from {{ ref('mart_customer_month') }}
where has_engagement_flag is null
    or has_engagement_flag not in (0, 1)
    or zero_engagement_flag is null
    or zero_engagement_flag not in (0, 1)
    or has_transaction_flag is null
    or has_transaction_flag not in (0, 1)
    or first_observed_month_flag is null
    or first_observed_month_flag not in (0, 1)
    or observation_end_month_flag is null
    or observation_end_month_flag not in (0, 1)
