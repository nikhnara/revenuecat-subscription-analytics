-- Observed associations, not causal effects. The cohort CTE fixes the same
-- event customer-months at -3, -2, -1, and 0 for each lifecycle category.
-- Month 0 mixes behavior before and after the actual event date.
with balanced_event_cohort as (
    select
        event_type,
        event_customer_month_key
    from {{ ref('mart_lifecycle_event_engagement') }}
    where event_type in ('cancellation', 'late_renewal', 'reactivation')
        and relative_month in (-3, -2, -1, 0)
    group by event_type, event_customer_month_key
    having count(*) = 4
        and count(distinct relative_month) = 4
        and countif(
            balanced_panel_flag = 1
            and behavior_month_observable_flag = 1
        ) = 4
)
select
    m.event_type,
    m.relative_month,
    count(*) as cohort_event_customer_months,
    avg(cast(m.zero_engagement_flag as float64))
        as zero_engagement_rate,
    avg(m.active_days) as avg_active_days,
    avg(m.total_listening_seconds)
        as avg_monthly_listening_seconds,
    count(m.total_listening_seconds)
        as months_with_valid_listening_total,
    avg(m.avg_listening_seconds_per_active_day)
        as avg_listening_seconds_per_active_day,
    avg(m.total_plays) as avg_monthly_plays,
    avg(m.avg_plays_per_active_day) as avg_plays_per_active_day,
    avg(m.avg_daily_unique_songs) as avg_daily_unique_songs,
    avg(m.plays_under_25_share) as avg_under_25_share,
    avg(m.plays_over_985_share) as avg_over_985_share
from {{ ref('mart_lifecycle_event_engagement') }} as m
inner join balanced_event_cohort as c
    on m.event_type = c.event_type
    and m.event_customer_month_key = c.event_customer_month_key
where m.relative_month in (-3, -2, -1, 0)
group by m.event_type, m.relative_month
order by m.event_type, m.relative_month
