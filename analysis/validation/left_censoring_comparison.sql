-- Descriptive comparison only: changing observable-history coverage can make
-- unrestricted cancellation trajectories misleading. This is not causal proof.
-- Month 0 includes behavior before and after the actual cancellation date.
with balanced_cancellation_events as (
    select event_customer_month_key
    from {{ ref('mart_lifecycle_event_engagement') }}
    where event_type = 'cancellation'
        and relative_month in (-3, -2, -1, 0)
    group by event_customer_month_key
    having count(*) = 4
        and count(distinct relative_month) = 4
        and countif(
            balanced_panel_flag = 1
            and behavior_month_observable_flag = 1
        ) = 4
),
cancellation_rows as (
    select
        m.event_customer_month_key,
        m.relative_month,
        m.behavior_month_observable_flag,
        m.zero_engagement_flag,
        m.active_days,
        m.total_listening_seconds,
        b.event_customer_month_key is not null as in_balanced_cohort
    from {{ ref('mart_lifecycle_event_engagement') }} as m
    left join balanced_cancellation_events as b
        on m.event_customer_month_key = b.event_customer_month_key
    where m.event_type = 'cancellation'
        and m.relative_month in (-3, -2, -1, 0)
),
cohort_rows as (
    select
        scope,
        relative_month,
        behavior_month_observable_flag,
        zero_engagement_flag,
        active_days,
        total_listening_seconds
    from cancellation_rows
    cross join unnest(
        if(
            in_balanced_cohort,
            ['unrestricted', 'balanced'],
            ['unrestricted']
        )
    ) as scope
),
trajectories as (
    select
        scope,
        relative_month,
        count(*) as event_customer_months,
        countif(behavior_month_observable_flag = 1)
            as observable_event_customer_months,
        safe_divide(
            countif(behavior_month_observable_flag = 1), count(*)
        ) as observable_history_coverage_rate,
        avg(if(
            behavior_month_observable_flag = 1,
            cast(zero_engagement_flag as float64),
            null
        )) as zero_engagement_rate,
        avg(if(
            behavior_month_observable_flag = 1, active_days, null
        )) as avg_active_days,
        avg(if(
            behavior_month_observable_flag = 1,
            total_listening_seconds,
            null
        )) as avg_monthly_listening_seconds
    from cohort_rows
    group by scope, relative_month
),
comparison as (
    select
        relative_month,
        max(if(scope = 'unrestricted', event_customer_months, null))
            as unrestricted_event_customer_months,
        max(if(scope = 'unrestricted', observable_event_customer_months, null))
            as unrestricted_observable_event_customer_months,
        max(if(scope = 'unrestricted', observable_history_coverage_rate, null))
            as unrestricted_coverage_rate,
        max(if(scope = 'balanced', event_customer_months, null))
            as balanced_event_customer_months,
        max(if(scope = 'balanced', observable_history_coverage_rate, null))
            as balanced_coverage_rate,
        max(if(scope = 'unrestricted', zero_engagement_rate, null))
            as unrestricted_zero_engagement_rate,
        max(if(scope = 'balanced', zero_engagement_rate, null))
            as balanced_zero_engagement_rate,
        max(if(scope = 'unrestricted', avg_active_days, null))
            as unrestricted_avg_active_days,
        max(if(scope = 'balanced', avg_active_days, null))
            as balanced_avg_active_days,
        max(if(scope = 'unrestricted', avg_monthly_listening_seconds, null))
            as unrestricted_avg_monthly_listening_seconds,
        max(if(scope = 'balanced', avg_monthly_listening_seconds, null))
            as balanced_avg_monthly_listening_seconds
    from trajectories
    group by relative_month
)
select
    *,
    unrestricted_zero_engagement_rate
        - max(if(relative_month = -3, unrestricted_zero_engagement_rate, null))
            over () as unrestricted_zero_rate_change_from_minus_3,
    balanced_zero_engagement_rate
        - max(if(relative_month = -3, balanced_zero_engagement_rate, null))
            over () as balanced_zero_rate_change_from_minus_3,
    unrestricted_avg_active_days
        - max(if(relative_month = -3, unrestricted_avg_active_days, null))
            over () as unrestricted_active_days_change_from_minus_3,
    balanced_avg_active_days
        - max(if(relative_month = -3, balanced_avg_active_days, null))
            over () as balanced_active_days_change_from_minus_3
from comparison
order by relative_month
