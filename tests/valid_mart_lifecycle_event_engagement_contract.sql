-- One narrow mart scan checks the event key parts, domains, and binary flags.
select event_relative_month_key
from {{ ref('mart_lifecycle_event_engagement') }}
where event_customer_month_key is null
    or customer_id is null
    or event_month is null
    or event_type is null
    or event_type not in ('cancellation', 'late_renewal', 'reactivation')
    or relative_month is null
    or relative_month not in (-3, -2, -1, 0)
    or behavior_month_observable_flag is null
    or behavior_month_observable_flag not in (0, 1)
    or balanced_panel_flag is null
    or balanced_panel_flag not in (0, 1)
    or (has_engagement_flag is not null and has_engagement_flag not in (0, 1))
    or (zero_engagement_flag is not null and zero_engagement_flag not in (0, 1))
