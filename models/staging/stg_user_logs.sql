{{ config(
    materialized='table',
    partition_by={
        "field": "activity_date",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=["customer_id"]
) }}

WITH combined AS (

    SELECT
        *,
        'v1' AS source_release
    FROM {{ source('kkbox_raw', 'user_logs_v1') }}

    UNION ALL

    SELECT
        *,
        'v2' AS source_release
    FROM {{ source('kkbox_raw', 'user_logs_v2') }}

),

parsed AS (

    SELECT
        msno AS customer_id,

        SAFE.PARSE_DATE(
            '%Y%m%d',
            CAST(date AS STRING)
        ) AS activity_date,

        num_25,
        num_50,
        num_75,
        num_985,
        num_100,
        num_unq,
        total_secs,

        source_release

    FROM combined

)

SELECT
    customer_id,
    activity_date,

    DATE_TRUNC(
        activity_date,
        MONTH
    ) AS activity_month,

    CASE
        WHEN EXTRACT(DAYOFWEEK FROM activity_date) IN (1, 7)
            THEN 'weekend'
        ELSE 'weekday'
    END AS day_type,

    num_25 AS plays_under_25_pct,
    num_50 AS plays_25_to_50_pct,
    num_75 AS plays_50_to_75_pct,
    num_985 AS plays_75_to_985_pct,
    num_100 AS plays_over_985_pct,

    num_unq AS unique_songs_played,

    total_secs AS total_listening_seconds,

    source_release

FROM parsed
