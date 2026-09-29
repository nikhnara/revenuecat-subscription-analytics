{{ config(
    materialized='table',
    partition_by={
        "field": "calendar_month",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=["customer_id"]
) }}

/*
Primary customer-month analytical mart.

Grain:
    One row per customer x every calendar month in the customer's
    observable window.

Observation window:
    Starts at the earliest month in which the customer appears in either:
        - subscription transactions
        - engagement logs

    Ends at the common observation end shared by both domains.

Semantics:
    - Missing engagement row inside the spine = zero observed engagement.
    - Missing transaction row = zero observed transaction/lifecycle events.
    - Undefined averages remain NULL.
    - Invalid listening-time months remain NULL for listening metrics.
    - Cancellation is NOT automatically interpreted as churn.
    - No present-day subscription status is inferred.
*/

WITH customer_start_months AS (

    SELECT
        customer_id,
        MIN(observed_month) AS first_observed_month

    FROM (

        SELECT
            customer_id,
            MIN(activity_month) AS observed_month
        FROM {{ ref('int_customer_engagement_monthly') }}
        GROUP BY customer_id

        UNION ALL

        SELECT
            customer_id,
            MIN(transaction_month) AS observed_month
        FROM {{ ref('int_customer_subscription_monthly') }}
        GROUP BY customer_id

    )

    GROUP BY customer_id

),

observation_end AS (

    /*
    Use the COMMON observation end.

    If one domain extends beyond the other in a future dataset,
    months outside the shared coverage must not be interpreted
    as genuine zero engagement or zero transaction activity.
    */

    SELECT
        LEAST(
            (
                SELECT MAX(activity_month)
                FROM {{ ref('int_customer_engagement_monthly') }}
            ),
            (
                SELECT MAX(transaction_month)
                FROM {{ ref('int_customer_subscription_monthly') }}
            )
        ) AS observation_end_month

),

customer_month_spine AS (

    SELECT
        c.customer_id,
        calendar_month,
        c.first_observed_month,
        o.observation_end_month

    FROM customer_start_months AS c

    CROSS JOIN observation_end AS o

    CROSS JOIN UNNEST(
        GENERATE_DATE_ARRAY(
            c.first_observed_month,
            o.observation_end_month,
            INTERVAL 1 MONTH
        )
    ) AS calendar_month

)

SELECT

    /* ============================================================
       GRAIN / CALENDAR
       ============================================================ */

    s.customer_id,
    s.calendar_month,

    LAST_DAY(
        s.calendar_month,
        MONTH
    ) AS calendar_month_end,

    s.first_observed_month,

    DATE_DIFF(
        s.calendar_month,
        s.first_observed_month,
        MONTH
    ) + 1 AS customer_month_number,

    s.observation_end_month,

    CASE
        WHEN s.calendar_month = s.first_observed_month
        THEN 1
        ELSE 0
    END AS first_observed_month_flag,

    CASE
        WHEN s.calendar_month = s.observation_end_month
        THEN 1
        ELSE 0
    END AS observation_end_month_flag,


    /* ============================================================
       DOMAIN PRESENCE
       ============================================================ */

    CASE
        WHEN e.customer_id IS NOT NULL
        THEN 1
        ELSE 0
    END AS has_engagement_flag,

    CASE
        WHEN e.customer_id IS NULL
        THEN 1
        ELSE 0
    END AS zero_engagement_flag,

    CASE
        WHEN t.customer_id IS NOT NULL
        THEN 1
        ELSE 0
    END AS has_transaction_flag,

    CASE
        WHEN m.customer_id IS NOT NULL
        THEN 1
        ELSE 0
    END AS has_member_profile_flag,

    CASE
        WHEN cs.customer_id IS NOT NULL
        THEN 1
        ELSE 0
    END AS has_subscription_history_flag,


    /* ============================================================
       MEMBER ATTRIBUTES
       ============================================================ */

    m.registration_date,

    DATE_TRUNC(
        m.registration_date,
        MONTH
    ) AS registration_month,

    m.city_code,
    m.age_raw,
    m.age_clean,
    m.age_valid_flag,
    m.gender,

    m.registration_channel_id,
    m.registration_channel_valid_flag,


    /* ============================================================
       ENGAGEMENT — PRESENCE
       ============================================================ */

    COALESCE(
        e.active_days,
        0
    ) AS active_days,

    COALESCE(
        e.weekday_active_days,
        0
    ) AS weekday_active_days,

    COALESCE(
        e.weekend_active_days,
        0
    ) AS weekend_active_days,


    /* ============================================================
       ENGAGEMENT — DATA QUALITY

       Counts become zero for a synthetic zero-engagement month.

       Validity flags remain NULL when there was no engagement data,
       because there was nothing to validate.
       ============================================================ */

    COALESCE(
        e.invalid_listening_day_count,
        0
    ) AS invalid_listening_day_count,

    COALESCE(
        e.invalid_play_day_count,
        0
    ) AS invalid_play_day_count,

    COALESCE(
        e.invalid_unique_song_day_count,
        0
    ) AS invalid_unique_song_day_count,

    e.listening_metrics_valid_flag,
    e.play_metrics_valid_flag,
    e.unique_song_metrics_valid_flag,


    /* ============================================================
       ENGAGEMENT — TIMELINE
       ============================================================ */

    e.first_observed_user_log_date,
    e.first_observed_user_log_month,
    e.engagement_month_number,

    e.previous_observed_activity_month,
    e.inactive_months_since_previous_activity,


    /* ============================================================
       ENGAGEMENT — LISTENING

       Zero-engagement month:
           additive total = 0

       Engagement month with invalid listening data:
           metrics remain NULL

       This preserves the difference between:
           "no activity"
       and
           "activity observed, listening measurement unreliable"
       ============================================================ */

    CASE
        WHEN e.customer_id IS NULL
        THEN 0
        ELSE e.total_listening_seconds
    END AS total_listening_seconds,

    e.avg_listening_seconds_per_active_day,

    e.avg_weekday_listening_seconds,
    e.avg_weekend_listening_seconds,


    /* ============================================================
       ENGAGEMENT — PLAY VOLUME
       ============================================================ */

    CASE
        WHEN e.customer_id IS NULL
        THEN 0
        ELSE e.total_plays
    END AS total_plays,

    e.avg_plays_per_active_day,

    e.avg_weekday_plays,
    e.avg_weekend_plays,


    /* ============================================================
       ENGAGEMENT — MUSIC DIVERSITY
       ============================================================ */

    e.avg_daily_unique_songs,
    e.avg_weekday_unique_songs,
    e.avg_weekend_unique_songs,


    /* ============================================================
       ENGAGEMENT — COMPLETION-BAND TOTALS
       ============================================================ */

    CASE
        WHEN e.customer_id IS NULL
        THEN 0
        ELSE e.plays_under_25_pct
    END AS plays_under_25_pct,

    CASE
        WHEN e.customer_id IS NULL
        THEN 0
        ELSE e.plays_25_to_50_pct
    END AS plays_25_to_50_pct,

    CASE
        WHEN e.customer_id IS NULL
        THEN 0
        ELSE e.plays_50_to_75_pct
    END AS plays_50_to_75_pct,

    CASE
        WHEN e.customer_id IS NULL
        THEN 0
        ELSE e.plays_75_to_985_pct
    END AS plays_75_to_985_pct,

    CASE
        WHEN e.customer_id IS NULL
        THEN 0
        ELSE e.plays_over_985_pct
    END AS plays_over_985_pct,


    /* ============================================================
       ENGAGEMENT — COMPLETION SHARES

       Zero-engagement months remain NULL because 0 / 0 does not
       describe listening behavior.
       ============================================================ */

    e.plays_under_25_share,
    e.plays_25_to_50_share,
    e.plays_50_to_75_share,
    e.plays_75_to_985_share,
    e.plays_over_985_share,

    e.source_release
        AS engagement_source_release,


    /* ============================================================
       MONTHLY TRANSACTION ACTIVITY
       ============================================================ */

    t.first_transaction_date_in_month,
    t.last_transaction_date_in_month,

    COALESCE(
        t.transaction_record_count,
        0
    ) AS transaction_record_count,

    COALESCE(
        t.transaction_day_count,
        0
    ) AS transaction_day_count,

    COALESCE(
        t.cancellation_record_count,
        0
    ) AS cancellation_record_count,

    COALESCE(
        t.non_cancellation_record_count,
        0
    ) AS non_cancellation_record_count,


    /* ============================================================
       MONTHLY LIFECYCLE EVENTS
       ============================================================ */

    COALESCE(
        t.ordered_event_group_count,
        0
    ) AS ordered_event_group_count,

    COALESCE(
        t.first_observed_subscription_event_count,
        0
    ) AS first_observed_subscription_event_count,

    COALESCE(
        t.renewal_event_count,
        0
    ) AS renewal_event_count,

    COALESCE(
        t.late_renewal_event_count,
        0
    ) AS late_renewal_event_count,

    COALESCE(
        t.reactivation_event_count,
        0
    ) AS reactivation_event_count,

    COALESCE(
        t.cancellation_event_count,
        0
    ) AS cancellation_event_count,

    COALESCE(
        t.unclassified_event_count,
        0
    ) AS unclassified_event_count,


    /* ============================================================
       MONTHLY TRANSACTION QUALITY
       ============================================================ */

    COALESCE(
        t.unresolved_order_record_count,
        0
    ) AS unresolved_order_record_count,

    COALESCE(
        t.ordering_key_tie_record_count,
        0
    ) AS ordering_key_tie_record_count,

    COALESCE(
        t.expiry_before_transaction_record_count,
        0
    ) AS expiry_before_transaction_record_count,

    COALESCE(
        t.cancellation_expiry_above_reference_count,
        0
    ) AS cancellation_expiry_above_reference_count,

    COALESCE(
        t.reported_state_unknown_event_count,
        0
    ) AS reported_state_unknown_event_count,

    COALESCE(
        t.renewal_reference_unknown_event_count,
        0
    ) AS renewal_reference_unknown_event_count,

    COALESCE(
        t.attribute_conflict_event_count,
        0
    ) AS attribute_conflict_event_count,


    /* ============================================================
       OBSERVED FINANCIAL FIELDS

       No transaction that month:
           observed amount = 0

       Transaction exists but amount information is incomplete:
           preserve NULL

       These fields are observed recorded amounts, not asserted
       revenue or LTV.
       ============================================================ */

    COALESCE(
        t.missing_recorded_amount_count,
        0
    ) AS missing_recorded_amount_count,

    CASE
        WHEN t.customer_id IS NULL
        THEN 0
        ELSE t.observed_recorded_amount_paid_ntd
    END AS observed_recorded_amount_paid_ntd,

    CASE
        WHEN t.customer_id IS NULL
        THEN 0
        ELSE t.observed_non_cancellation_recorded_amount_ntd
    END AS observed_non_cancellation_recorded_amount_ntd,


    /* ============================================================
       LATEST ORDERED EVENT WITHIN MONTH
       ============================================================ */

    t.latest_event_group_sequence,
    t.latest_event_date,

    t.latest_event_group_source_row_count,

    t.latest_event_type,
    t.latest_classification_reason,

    t.latest_reported_expiry_date,
    t.latest_renewal_reference_expiry_date,

    t.latest_reported_state_unknown_flag,
    t.latest_renewal_reference_unknown_flag,
    t.latest_attribute_conflict_flag,

    t.latest_plan_days,
    t.latest_list_price_ntd,
    t.latest_recorded_amount_paid_ntd,

    t.latest_payment_method_id,
    t.latest_auto_renew_flag,
    t.latest_cancellation_flag,

    t.latest_source_release
        AS transaction_source_release,


    /* ============================================================
       CUSTOMER-LEVEL SUBSCRIPTION HISTORY

       These describe the customer's complete observed transaction
       history and therefore repeat across customer-month rows.
       ============================================================ */

    cs.first_transaction_date
        AS lifetime_first_transaction_date,

    cs.last_transaction_date
        AS lifetime_last_transaction_date,

    cs.total_transaction_count
        AS lifetime_transaction_record_count,

    cs.observed_transaction_day_count
        AS lifetime_transaction_day_count,

    cs.ordered_event_group_count
        AS lifetime_ordered_event_group_count,

    cs.first_observed_subscription_event_group_count
        AS lifetime_first_observed_subscription_event_count,

    cs.renewal_event_group_count
        AS lifetime_renewal_event_count,

    cs.late_renewal_event_group_count
        AS lifetime_late_renewal_event_count,

    cs.reactivation_event_group_count
        AS lifetime_reactivation_event_count,

    cs.cancellation_event_group_count
        AS lifetime_cancellation_event_count,

    cs.ever_cancelled_flag,
    cs.ever_reactivated_flag,

    cs.last_ordered_subscription_date,
    cs.last_ordered_subscription_recorded_expiry,

    cs.most_frequent_payment_method_id,
    cs.payment_method_mode_tie_flag


FROM customer_month_spine AS s


LEFT JOIN {{ ref('int_customer_engagement_monthly') }} AS e
    ON s.customer_id = e.customer_id
    AND s.calendar_month = e.activity_month


LEFT JOIN {{ ref('int_customer_subscription_monthly') }} AS t
    ON s.customer_id = t.customer_id
    AND s.calendar_month = t.transaction_month


LEFT JOIN {{ ref('stg_members') }} AS m
    ON s.customer_id = m.customer_id


LEFT JOIN {{ ref('int_customer_subscription_summary') }} AS cs
    ON s.customer_id = cs.customer_id
