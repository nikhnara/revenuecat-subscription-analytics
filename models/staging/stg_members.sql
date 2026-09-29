{{ config(
    materialized='view'
) }}

SELECT
    msno AS customer_id,

    city AS city_code,

    bd AS age_raw,

    CASE
        WHEN bd BETWEEN 1 AND 100
        THEN bd
        ELSE NULL
    END AS age_clean,

    CASE
        WHEN bd BETWEEN 1 AND 100
        THEN 1
        ELSE 0
    END AS age_valid_flag,

    CASE
        WHEN LOWER(TRIM(gender)) IN ('male', 'female')
        THEN LOWER(TRIM(gender))
        ELSE NULL
    END AS gender,

    registered_via AS registration_channel_id,

    CASE
        WHEN registered_via >= 0
        THEN 1
        ELSE 0
    END AS registration_channel_valid_flag,

    SAFE.PARSE_DATE(
        '%Y%m%d',
        CAST(registration_init_time AS STRING)
    ) AS registration_date,

    registration_init_time AS registration_date_raw

FROM {{ source('kkbox_raw', 'members') }}
