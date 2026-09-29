{{ config(
    materialized='table',
    partition_by={
        "field": "event_month",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=["event_type", "customer_id"]
) }}

/*
=====================================================================
MART: mart_lifecycle_event_engagement
=====================================================================

Primary purpose:
    Support Looker analysis of customer engagement around subscription
    lifecycle events.

Grain:
    One customer
    x one lifecycle event type
    x one event month
    x one relative behavior month

Example:

    customer_id   event_type     event_month   relative_month
    abc           cancellation   2017-01-01          -3
    abc           cancellation   2017-01-01          -2
    abc           cancellation   2017-01-01          -1
    abc           cancellation   2017-01-01           0

Supported lifecycle events:
    - cancellation
    - late_renewal
    - reactivation

Relative-month semantics:
    -3 = three calendar months before event month
    -2 = two calendar months before event month
    -1 = final complete calendar month before event month
     0 = event calendar month

IMPORTANT:
    Relative month 0 is NOT strictly pre-event behavior.

    Because engagement is aggregated monthly, month 0 can contain
    activity occurring both before and after the actual transaction
    event.

Balanced-panel semantics:
    balanced_panel_flag = 1 when all four relative months
    (-3, -2, -1, 0) exist inside the customer's observable
    customer-month spine.

    This does NOT require engagement in all four months.

    A balanced customer may legitimately have:
        zero_engagement_flag = 1

    Balanced therefore means:
        "observable"

    not:
        "active"

Why preserve unbalanced rows:
    They remain useful for:
        - cohort coverage diagnostics
        - left-censoring analysis
        - auditing
        - comparing unrestricted vs balanced populations

    Looker should default to:
        balanced_panel_flag = 1

Quality semantics:
    - zero-engagement months remain valid observed months.
    - invalid listening months retain activity presence but have
      NULL listening metrics.
    - no invalid listening values are imputed.
=====================================================================
*/

WITH event_months AS (

    /*
    ---------------------------------------------------------------
    One row per customer x event type x event month.

    We intentionally model an event CUSTOMER-MONTH rather than
    expanding individual event groups here.

    event_count tells us when multiple events of the same type
    occurred for the customer inside the same month.
    ---------------------------------------------------------------
    */

    SELECT
        customer_id,
        transaction_month AS event_month,
        'cancellation' AS event_type,
        cancellation_event_count AS event_count

    FROM {{ ref('int_customer_subscription_monthly') }}

    WHERE cancellation_event_count > 0


    UNION ALL


    SELECT
        customer_id,
        transaction_month AS event_month,
        'late_renewal' AS event_type,
        late_renewal_event_count AS event_count

    FROM {{ ref('int_customer_subscription_monthly') }}

    WHERE late_renewal_event_count > 0


    UNION ALL


    SELECT
        customer_id,
        transaction_month AS event_month,
        'reactivation' AS event_type,
        reactivation_event_count AS event_count

    FROM {{ ref('int_customer_subscription_monthly') }}

    WHERE reactivation_event_count > 0

),

relative_months AS (

    /*
    Analysis window intentionally starts at -3.

    We can extend this later without changing the grain.
    */

    SELECT relative_month

    FROM UNNEST([-3, -2, -1, 0]) AS relative_month

),

aligned AS (

    SELECT

        /* =========================================================
           EVENT IDENTITY
           ========================================================= */

        e.customer_id,
        e.event_month,
        e.event_type,
        e.event_count,

        r.relative_month,

        DATE_ADD(
            e.event_month,
            INTERVAL r.relative_month MONTH
        ) AS expected_behavior_month,


        /* =========================================================
           OBSERVATION
           ========================================================= */

        m.calendar_month AS behavior_month,

        CASE
            WHEN m.calendar_month IS NOT NULL
            THEN 1
            ELSE 0
        END AS behavior_month_observable_flag,


        /* =========================================================
           ENGAGEMENT PRESENCE
           ========================================================= */

        m.has_engagement_flag,
        m.zero_engagement_flag,

        m.active_days,
        m.weekday_active_days,
        m.weekend_active_days,


        /* =========================================================
           LISTENING QUALITY
           ========================================================= */

        m.invalid_listening_day_count,
        m.invalid_play_day_count,
        m.invalid_unique_song_day_count,

        m.listening_metrics_valid_flag,
        m.play_metrics_valid_flag,
        m.unique_song_metrics_valid_flag,


        /* =========================================================
           LISTENING INTENSITY
           ========================================================= */

        m.total_listening_seconds,

        m.avg_listening_seconds_per_active_day,
        m.avg_weekday_listening_seconds,
        m.avg_weekend_listening_seconds,


        /* =========================================================
           PLAY INTENSITY
           ========================================================= */

        m.total_plays,

        m.avg_plays_per_active_day,
        m.avg_weekday_plays,
        m.avg_weekend_plays,


        /* =========================================================
           MUSIC DIVERSITY
           ========================================================= */

        m.avg_daily_unique_songs,
        m.avg_weekday_unique_songs,
        m.avg_weekend_unique_songs,


        /* =========================================================
           LISTENING DEPTH
           ========================================================= */

        m.plays_under_25_pct,
        m.plays_25_to_50_pct,
        m.plays_50_to_75_pct,
        m.plays_75_to_985_pct,
        m.plays_over_985_pct,

        m.plays_under_25_share,
        m.plays_25_to_50_share,
        m.plays_50_to_75_share,
        m.plays_75_to_985_share,
        m.plays_over_985_share,


        /* =========================================================
           CUSTOMER / MEMBER ATTRIBUTES

           These are stable profile attributes and allow Looker
           cohort slicing without requiring another join.
           ========================================================= */

        m.registration_date,
        m.registration_month,

        m.city_code,

        m.age_raw,
        m.age_clean,
        m.age_valid_flag,

        m.gender,

        m.registration_channel_id,
        m.registration_channel_valid_flag

    FROM event_months AS e

    CROSS JOIN relative_months AS r

    LEFT JOIN {{ ref('mart_customer_month') }} AS m
        ON e.customer_id = m.customer_id

        AND m.calendar_month = DATE_ADD(
            e.event_month,
            INTERVAL r.relative_month MONTH
        )

),

panel_status AS (

    SELECT
        *,

        /*
        Exactly four observable rows means the customer has complete
        observable history across:

            -3, -2, -1, 0

        Zero-engagement months still count as observable.
        */

        COUNTIF(
            behavior_month_observable_flag = 1
        ) OVER (
            PARTITION BY
                customer_id,
                event_month,
                event_type
        ) AS observable_relative_month_count

    FROM aligned

)

SELECT

    /* =============================================================
       KEYS
       ============================================================= */

    TO_HEX(
        MD5(
            CONCAT(
                customer_id,
                '|',
                CAST(event_month AS STRING),
                '|',
                event_type
            )
        )
    ) AS event_customer_month_key,

    TO_HEX(
        MD5(
            CONCAT(
                customer_id,
                '|',
                CAST(event_month AS STRING),
                '|',
                event_type,
                '|',
                CAST(relative_month AS STRING)
            )
        )
    ) AS event_relative_month_key,


    /* =============================================================
       EVENT CONTEXT
       ============================================================= */

    customer_id,
    event_month,
    event_type,

    event_count,

    relative_month,

    expected_behavior_month,
    behavior_month,


    /* =============================================================
       COHORT / OBSERVABILITY
       ============================================================= */

    behavior_month_observable_flag,

    observable_relative_month_count,

    CASE
        WHEN observable_relative_month_count = 4
        THEN 1
        ELSE 0
    END AS balanced_panel_flag,


    /*
    Human-readable categorization useful in Looker.

    Numeric relative_month remains the canonical field.
    */

    CASE relative_month

        WHEN -3
            THEN '3 months before'

        WHEN -2
            THEN '2 months before'

        WHEN -1
            THEN '1 month before'

        WHEN 0
            THEN 'Event month'

    END AS relative_month_label,


    /* =============================================================
       ENGAGEMENT PRESENCE
       ============================================================= */

    has_engagement_flag,
    zero_engagement_flag,

    active_days,
    weekday_active_days,
    weekend_active_days,


    /* =============================================================
       QUALITY
       ============================================================= */

    invalid_listening_day_count,
    invalid_play_day_count,
    invalid_unique_song_day_count,

    listening_metrics_valid_flag,
    play_metrics_valid_flag,
    unique_song_metrics_valid_flag,


    /* =============================================================
       LISTENING
       ============================================================= */

    total_listening_seconds,

    avg_listening_seconds_per_active_day,

    avg_weekday_listening_seconds,
    avg_weekend_listening_seconds,


    /* =============================================================
       PLAYS
       ============================================================= */

    total_plays,

    avg_plays_per_active_day,

    avg_weekday_plays,
    avg_weekend_plays,


    /* =============================================================
       MUSIC DIVERSITY
       ============================================================= */

    avg_daily_unique_songs,

    avg_weekday_unique_songs,
    avg_weekend_unique_songs,


    /* =============================================================
       LISTENING DEPTH
       ============================================================= */

    plays_under_25_pct,
    plays_25_to_50_pct,
    plays_50_to_75_pct,
    plays_75_to_985_pct,
    plays_over_985_pct,

    plays_under_25_share,
    plays_25_to_50_share,
    plays_50_to_75_share,
    plays_75_to_985_share,
    plays_over_985_share,


    /* =============================================================
       MEMBER ATTRIBUTES
       ============================================================= */

    registration_date,
    registration_month,

    city_code,

    age_raw,
    age_clean,
    age_valid_flag,

    gender,

    registration_channel_id,
    registration_channel_valid_flag

FROM panel_status
