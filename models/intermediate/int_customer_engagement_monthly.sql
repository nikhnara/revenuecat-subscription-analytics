{{ config(
    materialized='table',
    partition_by={
        "field": "activity_month",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=["customer_id"]
) }}

/*
Grain:
    One row per customer x observed activity month.

Purpose:
    Aggregate daily KKBOX engagement into monthly behavioral features.

Quality policy:
    - Staging preserves original source values.
    - Active-day presence remains valid even when a behavioral measure is invalid.
    - total_listening_seconds is valid only when:
          0 <= total_listening_seconds <= 86,400
      because one source row represents one customer-day.
    - Negative/null play counts are invalid.
    - Negative/null daily unique-song counts are invalid.
    - If any active day in a month is invalid for a metric family,
      that month's metric family is returned as NULL rather than partially summed.
    - Invalid source rows are NOT deleted.
*/

WITH daily_features AS (

    SELECT
        customer_id,
        activity_date,
        activity_month,
        day_type,

        plays_under_25_pct,
        plays_25_to_50_pct,
        plays_50_to_75_pct,
        plays_75_to_985_pct,
        plays_over_985_pct,

        unique_songs_played,
        total_listening_seconds,

        source_release,

        /* =====================================================
           Listening-time quality

           One staging row = one customer-day.

           Valid:
               0 to 86,400 seconds

           Invalid:
               NULL
               negative values
               more than 24 hours
           ===================================================== */

        CASE
            WHEN total_listening_seconds IS NOT NULL
                 AND total_listening_seconds >= 0
                 AND total_listening_seconds <= 86400
            THEN 1
            ELSE 0
        END AS listening_day_valid_flag,

        /* =====================================================
           Play-count quality
           ===================================================== */

        CASE
            WHEN plays_under_25_pct IS NOT NULL
                 AND plays_under_25_pct >= 0

                 AND plays_25_to_50_pct IS NOT NULL
                 AND plays_25_to_50_pct >= 0

                 AND plays_50_to_75_pct IS NOT NULL
                 AND plays_50_to_75_pct >= 0

                 AND plays_75_to_985_pct IS NOT NULL
                 AND plays_75_to_985_pct >= 0

                 AND plays_over_985_pct IS NOT NULL
                 AND plays_over_985_pct >= 0
            THEN 1
            ELSE 0
        END AS play_day_valid_flag,

        /* =====================================================
           Daily unique-song quality
           ===================================================== */

        CASE
            WHEN unique_songs_played IS NOT NULL
                 AND unique_songs_played >= 0
            THEN 1
            ELSE 0
        END AS unique_song_day_valid_flag,

        /* =====================================================
           Total plays for the observed day
           ===================================================== */

        CASE
            WHEN plays_under_25_pct IS NOT NULL
                 AND plays_under_25_pct >= 0

                 AND plays_25_to_50_pct IS NOT NULL
                 AND plays_25_to_50_pct >= 0

                 AND plays_50_to_75_pct IS NOT NULL
                 AND plays_50_to_75_pct >= 0

                 AND plays_75_to_985_pct IS NOT NULL
                 AND plays_75_to_985_pct >= 0

                 AND plays_over_985_pct IS NOT NULL
                 AND plays_over_985_pct >= 0
            THEN
                plays_under_25_pct
                + plays_25_to_50_pct
                + plays_50_to_75_pct
                + plays_75_to_985_pct
                + plays_over_985_pct
        END AS daily_total_plays

    FROM {{ ref('stg_user_logs') }}

    WHERE customer_id IS NOT NULL

),

monthly_engagement AS (

    SELECT
        customer_id,
        activity_month,

        /* =====================================================
           First observed active day within month
           ===================================================== */

        MIN(activity_date)
            AS first_activity_date_in_month,

        /* =====================================================
           Presence
           ===================================================== */

        COUNT(*)
            AS active_days,

        COUNTIF(day_type = 'weekday')
            AS weekday_active_days,

        COUNTIF(day_type = 'weekend')
            AS weekend_active_days,

        /* =====================================================
           Quality diagnostics
           ===================================================== */

        COUNTIF(listening_day_valid_flag = 0)
            AS invalid_listening_day_count,

        COUNTIF(play_day_valid_flag = 0)
            AS invalid_play_day_count,

        COUNTIF(unique_song_day_valid_flag = 0)
            AS invalid_unique_song_day_count,

        /* =====================================================
           Listening-time metrics

           Invalid days are excluded from the temporary
           aggregation here.

           In the final SELECT, the whole monthly listening
           metric family is NULL if any invalid day exists.
           ===================================================== */

        SUM(
            IF(
                listening_day_valid_flag = 1,
                total_listening_seconds,
                NULL
            )
        ) AS valid_total_listening_seconds,

        AVG(
            IF(
                listening_day_valid_flag = 1,
                total_listening_seconds,
                NULL
            )
        ) AS valid_avg_listening_seconds_per_active_day,

        AVG(
            IF(
                listening_day_valid_flag = 1
                AND day_type = 'weekday',
                total_listening_seconds,
                NULL
            )
        ) AS valid_avg_weekday_listening_seconds,

        AVG(
            IF(
                listening_day_valid_flag = 1
                AND day_type = 'weekend',
                total_listening_seconds,
                NULL
            )
        ) AS valid_avg_weekend_listening_seconds,

        /* =====================================================
           Play volume
           ===================================================== */

        SUM(
            IF(
                play_day_valid_flag = 1,
                daily_total_plays,
                NULL
            )
        ) AS valid_total_plays,

        AVG(
            IF(
                play_day_valid_flag = 1,
                daily_total_plays,
                NULL
            )
        ) AS valid_avg_plays_per_active_day,

        AVG(
            IF(
                play_day_valid_flag = 1
                AND day_type = 'weekday',
                daily_total_plays,
                NULL
            )
        ) AS valid_avg_weekday_plays,

        AVG(
            IF(
                play_day_valid_flag = 1
                AND day_type = 'weekend',
                daily_total_plays,
                NULL
            )
        ) AS valid_avg_weekend_plays,

        /* =====================================================
           Completion-band totals
           ===================================================== */

        SUM(
            IF(
                play_day_valid_flag = 1,
                plays_under_25_pct,
                NULL
            )
        ) AS valid_plays_under_25_pct,

        SUM(
            IF(
                play_day_valid_flag = 1,
                plays_25_to_50_pct,
                NULL
            )
        ) AS valid_plays_25_to_50_pct,

        SUM(
            IF(
                play_day_valid_flag = 1,
                plays_50_to_75_pct,
                NULL
            )
        ) AS valid_plays_50_to_75_pct,

        SUM(
            IF(
                play_day_valid_flag = 1,
                plays_75_to_985_pct,
                NULL
            )
        ) AS valid_plays_75_to_985_pct,

        SUM(
            IF(
                play_day_valid_flag = 1,
                plays_over_985_pct,
                NULL
            )
        ) AS valid_plays_over_985_pct,

        /* =====================================================
           Daily unique-song behavior

           num_unq is daily unique songs.

           We therefore calculate average unique songs across
           active days rather than claiming a true monthly
           distinct-song count.
           ===================================================== */

        AVG(
            IF(
                unique_song_day_valid_flag = 1,
                unique_songs_played,
                NULL
            )
        ) AS valid_avg_daily_unique_songs,

        AVG(
            IF(
                unique_song_day_valid_flag = 1
                AND day_type = 'weekday',
                unique_songs_played,
                NULL
            )
        ) AS valid_avg_weekday_unique_songs,

        AVG(
            IF(
                unique_song_day_valid_flag = 1
                AND day_type = 'weekend',
                unique_songs_played,
                NULL
            )
        ) AS valid_avg_weekend_unique_songs,

        /* =====================================================
           Provenance

           user_logs_v1 ends 2017-02
           user_logs_v2 begins 2017-03

           Therefore one calendar month belongs to one release.
           ===================================================== */

        MAX(source_release)
            AS source_release

    FROM daily_features

    GROUP BY
        customer_id,
        activity_month

),

customer_timeline AS (

    SELECT
        *,

        MIN(first_activity_date_in_month) OVER (
            PARTITION BY customer_id
        ) AS first_observed_user_log_date,

        MIN(activity_month) OVER (
            PARTITION BY customer_id
        ) AS first_observed_user_log_month,

        LAG(activity_month) OVER (
            PARTITION BY customer_id
            ORDER BY activity_month
        ) AS previous_observed_activity_month

    FROM monthly_engagement

)

SELECT
    customer_id,
    activity_month,

    /* =========================================================
       ENGAGEMENT TIMELINE
       ========================================================= */

    first_observed_user_log_date,
    first_observed_user_log_month,

    DATE_DIFF(
        activity_month,
        first_observed_user_log_month,
        MONTH
    ) + 1 AS engagement_month_number,

    previous_observed_activity_month,

    CASE
        WHEN previous_observed_activity_month IS NULL
            THEN NULL

        ELSE GREATEST(
            DATE_DIFF(
                activity_month,
                previous_observed_activity_month,
                MONTH
            ) - 1,
            0
        )
    END AS inactive_months_since_previous_activity,

    /* =========================================================
       PRESENCE
       ========================================================= */

    active_days,
    weekday_active_days,
    weekend_active_days,

    /* =========================================================
       QUALITY DIAGNOSTICS
       ========================================================= */

    invalid_listening_day_count,
    invalid_play_day_count,
    invalid_unique_song_day_count,

    CASE
        WHEN invalid_listening_day_count = 0
        THEN 1
        ELSE 0
    END AS listening_metrics_valid_flag,

    CASE
        WHEN invalid_play_day_count = 0
        THEN 1
        ELSE 0
    END AS play_metrics_valid_flag,

    CASE
        WHEN invalid_unique_song_day_count = 0
        THEN 1
        ELSE 0
    END AS unique_song_metrics_valid_flag,

    /* =========================================================
       LISTENING INTENSITY
       ========================================================= */

    CASE
        WHEN invalid_listening_day_count = 0
        THEN valid_total_listening_seconds
    END AS total_listening_seconds,

    CASE
        WHEN invalid_listening_day_count = 0
        THEN valid_avg_listening_seconds_per_active_day
    END AS avg_listening_seconds_per_active_day,

    CASE
        WHEN invalid_listening_day_count = 0
        THEN valid_avg_weekday_listening_seconds
    END AS avg_weekday_listening_seconds,

    CASE
        WHEN invalid_listening_day_count = 0
        THEN valid_avg_weekend_listening_seconds
    END AS avg_weekend_listening_seconds,

    /* =========================================================
       PLAY INTENSITY
       ========================================================= */

    CASE
        WHEN invalid_play_day_count = 0
        THEN valid_total_plays
    END AS total_plays,

    CASE
        WHEN invalid_play_day_count = 0
        THEN valid_avg_plays_per_active_day
    END AS avg_plays_per_active_day,

    CASE
        WHEN invalid_play_day_count = 0
        THEN valid_avg_weekday_plays
    END AS avg_weekday_plays,

    CASE
        WHEN invalid_play_day_count = 0
        THEN valid_avg_weekend_plays
    END AS avg_weekend_plays,

    /* =========================================================
       MUSIC DIVERSITY
       ========================================================= */

    CASE
        WHEN invalid_unique_song_day_count = 0
        THEN valid_avg_daily_unique_songs
    END AS avg_daily_unique_songs,

    CASE
        WHEN invalid_unique_song_day_count = 0
        THEN valid_avg_weekday_unique_songs
    END AS avg_weekday_unique_songs,

    CASE
        WHEN invalid_unique_song_day_count = 0
        THEN valid_avg_weekend_unique_songs
    END AS avg_weekend_unique_songs,

    /* =========================================================
       COMPLETION-BAND TOTALS
       ========================================================= */

    CASE
        WHEN invalid_play_day_count = 0
        THEN valid_plays_under_25_pct
    END AS plays_under_25_pct,

    CASE
        WHEN invalid_play_day_count = 0
        THEN valid_plays_25_to_50_pct
    END AS plays_25_to_50_pct,

    CASE
        WHEN invalid_play_day_count = 0
        THEN valid_plays_50_to_75_pct
    END AS plays_50_to_75_pct,

    CASE
        WHEN invalid_play_day_count = 0
        THEN valid_plays_75_to_985_pct
    END AS plays_75_to_985_pct,

    CASE
        WHEN invalid_play_day_count = 0
        THEN valid_plays_over_985_pct
    END AS plays_over_985_pct,

    /* =========================================================
       COMPLETION-BAND SHARES
       ========================================================= */

    CASE
        WHEN invalid_play_day_count = 0
        THEN SAFE_DIVIDE(
            valid_plays_under_25_pct,
            valid_total_plays
        )
    END AS plays_under_25_share,

    CASE
        WHEN invalid_play_day_count = 0
        THEN SAFE_DIVIDE(
            valid_plays_25_to_50_pct,
            valid_total_plays
        )
    END AS plays_25_to_50_share,

    CASE
        WHEN invalid_play_day_count = 0
        THEN SAFE_DIVIDE(
            valid_plays_50_to_75_pct,
            valid_total_plays
        )
    END AS plays_50_to_75_share,

    CASE
        WHEN invalid_play_day_count = 0
        THEN SAFE_DIVIDE(
            valid_plays_75_to_985_pct,
            valid_total_plays
        )
    END AS plays_75_to_985_share,

    CASE
        WHEN invalid_play_day_count = 0
        THEN SAFE_DIVIDE(
            valid_plays_over_985_pct,
            valid_total_plays
        )
    END AS plays_over_985_share,

    source_release

FROM customer_timeline
