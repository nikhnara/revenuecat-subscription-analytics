{{ config(
    materialized='table',
    partition_by={
        "field": "transaction_month",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=["customer_id"]
) }}

/*
Grain:
    One row per customer x observed transaction month.

Important:
    - Raw transaction records and lifecycle event groups are different grains.
    - Lifecycle counts use event_group_sequence, preventing tied KKBOX source
      rows from being double-counted as multiple lifecycle events.
    - Months with no transaction are NOT generated here.
    - No monthly active/churned status is inferred.
    - Rows without a usable transaction date cannot belong to a calendar month
      and remain represented in customer-level transaction models instead.
*/

WITH dated_transactions AS (

    SELECT
        customer_id,
        transaction_date,
        DATE_TRUNC(transaction_date, MONTH) AS transaction_month,

        event_group_sequence,
        event_type,
        classification_reason,

        reported_expiry_after_event,
        renewal_reference_expiry_after_event,

        plan_days,
        list_price_ntd,
        recorded_amount_paid_ntd,
        payment_method_id,
        auto_renew_flag,
        cancellation_flag,

        source_release,

        ordering_key_tie_flag,
        expiry_before_transaction_flag,
        cancellation_expiry_above_reference_flag,
        reported_state_unknown_flag,
        renewal_reference_unknown_flag

    FROM {{ ref('int_transaction') }}

    WHERE
        customer_id IS NOT NULL
        AND transaction_date IS NOT NULL

),

/*
Raw-record metrics.

These preserve the number of source transaction rows and are intentionally
kept separate from lifecycle event-group counts.
*/
monthly_records AS (

    SELECT
        customer_id,
        transaction_month,

        MIN(transaction_date)
            AS first_transaction_date_in_month,

        MAX(transaction_date)
            AS last_transaction_date_in_month,

        COUNT(*)
            AS transaction_record_count,

        COUNT(DISTINCT transaction_date)
            AS transaction_day_count,

        COUNTIF(cancellation_flag = 1)
            AS cancellation_record_count,

        COUNTIF(cancellation_flag = 0)
            AS non_cancellation_record_count,

        COUNTIF(event_group_sequence IS NULL)
            AS unresolved_order_record_count,

        COUNTIF(ordering_key_tie_flag = 1)
            AS ordering_key_tie_record_count,

        COUNTIF(expiry_before_transaction_flag = 1)
            AS expiry_before_transaction_record_count,

        COUNTIF(cancellation_expiry_above_reference_flag = 1)
            AS cancellation_expiry_above_reference_count,

        /*
        Recorded amount is preserved as an observed source measure,
        not labelled as revenue.
        */
        COUNTIF(recorded_amount_paid_ntd IS NULL)
            AS missing_recorded_amount_count,

        CASE
            WHEN COUNTIF(recorded_amount_paid_ntd IS NULL) = 0
            THEN SUM(recorded_amount_paid_ntd)
        END AS observed_recorded_amount_paid_ntd,

        CASE
            WHEN COUNTIF(
                cancellation_flag = 0
                AND recorded_amount_paid_ntd IS NULL
            ) = 0
            THEN SUM(
                IF(
                    cancellation_flag = 0,
                    recorded_amount_paid_ntd,
                    0
                )
            )
        END AS observed_non_cancellation_recorded_amount_ntd

    FROM dated_transactions

    GROUP BY
        customer_id,
        transaction_month

),

/*
Collapse source rows to the KKBOX lifecycle event-group grain.

This is where tied ordering-key rows become one lifecycle event while
retaining a source-row count and checking whether commercial attributes
agree within the group.
*/
event_groups AS (

    SELECT
        customer_id,
        transaction_month,
        event_group_sequence,

        MIN(transaction_date)
            AS event_date,

        COUNT(*)
            AS event_group_source_row_count,

        MAX(event_type)
            AS event_type,

        MAX(classification_reason)
            AS classification_reason,

        MAX(reported_expiry_after_event)
            AS reported_expiry_after_event,

        MAX(renewal_reference_expiry_after_event)
            AS renewal_reference_expiry_after_event,

        MAX(reported_state_unknown_flag)
            AS reported_state_unknown_flag,

        MAX(renewal_reference_unknown_flag)
            AS renewal_reference_unknown_flag,

        /*
        Only expose attributes when every source row in the ordering group
        agrees. Otherwise leave the attribute NULL rather than choosing one.
        */

        CASE
            WHEN COUNT(DISTINCT plan_days) <= 1
                 AND NOT (
                     COUNTIF(plan_days IS NULL) > 0
                     AND COUNTIF(plan_days IS NOT NULL) > 0
                 )
            THEN MAX(plan_days)
        END AS plan_days,

        CASE
            WHEN COUNT(DISTINCT list_price_ntd) <= 1
                 AND NOT (
                     COUNTIF(list_price_ntd IS NULL) > 0
                     AND COUNTIF(list_price_ntd IS NOT NULL) > 0
                 )
            THEN MAX(list_price_ntd)
        END AS list_price_ntd,

        CASE
            WHEN COUNT(DISTINCT recorded_amount_paid_ntd) <= 1
                 AND NOT (
                     COUNTIF(recorded_amount_paid_ntd IS NULL) > 0
                     AND COUNTIF(recorded_amount_paid_ntd IS NOT NULL) > 0
                 )
            THEN MAX(recorded_amount_paid_ntd)
        END AS recorded_amount_paid_ntd,

        CASE
            WHEN COUNT(DISTINCT payment_method_id) <= 1
                 AND NOT (
                     COUNTIF(payment_method_id IS NULL) > 0
                     AND COUNTIF(payment_method_id IS NOT NULL) > 0
                 )
            THEN MAX(payment_method_id)
        END AS payment_method_id,

        CASE
            WHEN COUNT(DISTINCT auto_renew_flag) <= 1
                 AND NOT (
                     COUNTIF(auto_renew_flag IS NULL) > 0
                     AND COUNTIF(auto_renew_flag IS NOT NULL) > 0
                 )
            THEN MAX(auto_renew_flag)
        END AS auto_renew_flag,

        CASE
            WHEN COUNT(DISTINCT cancellation_flag) <= 1
                 AND NOT (
                     COUNTIF(cancellation_flag IS NULL) > 0
                     AND COUNTIF(cancellation_flag IS NOT NULL) > 0
                 )
            THEN MAX(cancellation_flag)
        END AS cancellation_flag,

        CASE
            WHEN COUNT(DISTINCT source_release) <= 1
                 AND NOT (
                     COUNTIF(source_release IS NULL) > 0
                     AND COUNTIF(source_release IS NOT NULL) > 0
                 )
            THEN MAX(source_release)
        END AS source_release,

        CASE
            WHEN
                COUNT(DISTINCT plan_days) > 1
                OR (
                    COUNTIF(plan_days IS NULL) > 0
                    AND COUNTIF(plan_days IS NOT NULL) > 0
                )

                OR COUNT(DISTINCT list_price_ntd) > 1
                OR (
                    COUNTIF(list_price_ntd IS NULL) > 0
                    AND COUNTIF(list_price_ntd IS NOT NULL) > 0
                )

                OR COUNT(DISTINCT recorded_amount_paid_ntd) > 1
                OR (
                    COUNTIF(recorded_amount_paid_ntd IS NULL) > 0
                    AND COUNTIF(recorded_amount_paid_ntd IS NOT NULL) > 0
                )

                OR COUNT(DISTINCT payment_method_id) > 1
                OR (
                    COUNTIF(payment_method_id IS NULL) > 0
                    AND COUNTIF(payment_method_id IS NOT NULL) > 0
                )

                OR COUNT(DISTINCT auto_renew_flag) > 1
                OR (
                    COUNTIF(auto_renew_flag IS NULL) > 0
                    AND COUNTIF(auto_renew_flag IS NOT NULL) > 0
                )

                OR COUNT(DISTINCT cancellation_flag) > 1
                OR (
                    COUNTIF(cancellation_flag IS NULL) > 0
                    AND COUNTIF(cancellation_flag IS NOT NULL) > 0
                )

            THEN 1
            ELSE 0
        END AS attribute_conflict_flag

    FROM dated_transactions

    WHERE event_group_sequence IS NOT NULL

    GROUP BY
        customer_id,
        transaction_month,
        event_group_sequence

),

monthly_lifecycle AS (

    SELECT
        customer_id,
        transaction_month,

        COUNT(*)
            AS ordered_event_group_count,

        COUNTIF(event_type = 'first_observed_subscription')
            AS first_observed_subscription_event_count,

        COUNTIF(event_type = 'renewal')
            AS renewal_event_count,

        COUNTIF(event_type = 'late_renewal')
            AS late_renewal_event_count,

        COUNTIF(event_type = 'reactivation')
            AS reactivation_event_count,

        COUNTIF(event_type = 'cancellation')
            AS cancellation_event_count,

        COUNTIF(event_type = 'unclassified')
            AS unclassified_event_count,

        COUNTIF(reported_state_unknown_flag = 1)
            AS reported_state_unknown_event_count,

        COUNTIF(renewal_reference_unknown_flag = 1)
            AS renewal_reference_unknown_event_count,

        COUNTIF(attribute_conflict_flag = 1)
            AS attribute_conflict_event_count

    FROM event_groups

    GROUP BY
        customer_id,
        transaction_month

),

/*
Latest ordered lifecycle event within each observed transaction month.

event_group_sequence already embodies the canonical KKBOX event ordering,
so we do not rebuild that ordering here.
*/
latest_monthly_event AS (

    SELECT
        customer_id,
        transaction_month,

        event_group_sequence
            AS latest_event_group_sequence,

        event_date
            AS latest_event_date,

        event_group_source_row_count
            AS latest_event_group_source_row_count,

        event_type
            AS latest_event_type,

        classification_reason
            AS latest_classification_reason,

        reported_expiry_after_event
            AS latest_reported_expiry_date,

        renewal_reference_expiry_after_event
            AS latest_renewal_reference_expiry_date,

        reported_state_unknown_flag
            AS latest_reported_state_unknown_flag,

        renewal_reference_unknown_flag
            AS latest_renewal_reference_unknown_flag,

        attribute_conflict_flag
            AS latest_attribute_conflict_flag,

        plan_days
            AS latest_plan_days,

        list_price_ntd
            AS latest_list_price_ntd,

        recorded_amount_paid_ntd
            AS latest_recorded_amount_paid_ntd,

        payment_method_id
            AS latest_payment_method_id,

        auto_renew_flag
            AS latest_auto_renew_flag,

        cancellation_flag
            AS latest_cancellation_flag,

        source_release
            AS latest_source_release

    FROM event_groups

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            customer_id,
            transaction_month
        ORDER BY
            event_group_sequence DESC
    ) = 1

)

SELECT
    r.customer_id,
    r.transaction_month,

    /* Raw transaction activity */
    r.first_transaction_date_in_month,
    r.last_transaction_date_in_month,

    r.transaction_record_count,
    r.transaction_day_count,

    r.cancellation_record_count,
    r.non_cancellation_record_count,

    /* Raw-record quality */
    r.unresolved_order_record_count,
    r.ordering_key_tie_record_count,
    r.expiry_before_transaction_record_count,
    r.cancellation_expiry_above_reference_count,

    /* Observed financial fields */
    r.missing_recorded_amount_count,
    r.observed_recorded_amount_paid_ntd,
    r.observed_non_cancellation_recorded_amount_ntd,

    /* Lifecycle event-group counts */
    COALESCE(l.ordered_event_group_count, 0)
        AS ordered_event_group_count,

    COALESCE(l.first_observed_subscription_event_count, 0)
        AS first_observed_subscription_event_count,

    COALESCE(l.renewal_event_count, 0)
        AS renewal_event_count,

    COALESCE(l.late_renewal_event_count, 0)
        AS late_renewal_event_count,

    COALESCE(l.reactivation_event_count, 0)
        AS reactivation_event_count,

    COALESCE(l.cancellation_event_count, 0)
        AS cancellation_event_count,

    COALESCE(l.unclassified_event_count, 0)
        AS unclassified_event_count,

    COALESCE(l.reported_state_unknown_event_count, 0)
        AS reported_state_unknown_event_count,

    COALESCE(l.renewal_reference_unknown_event_count, 0)
        AS renewal_reference_unknown_event_count,

    COALESCE(l.attribute_conflict_event_count, 0)
        AS attribute_conflict_event_count,

    /* Latest ordered event observed in the month */
    e.latest_event_group_sequence,
    e.latest_event_date,
    e.latest_event_group_source_row_count,

    e.latest_event_type,
    e.latest_classification_reason,

    e.latest_reported_expiry_date,
    e.latest_renewal_reference_expiry_date,

    e.latest_reported_state_unknown_flag,
    e.latest_renewal_reference_unknown_flag,
    e.latest_attribute_conflict_flag,

    /* Latest event-group commercial attributes */
    e.latest_plan_days,
    e.latest_list_price_ntd,
    e.latest_recorded_amount_paid_ntd,
    e.latest_payment_method_id,
    e.latest_auto_renew_flag,
    e.latest_cancellation_flag,
    e.latest_source_release

FROM monthly_records AS r

LEFT JOIN monthly_lifecycle AS l
    ON r.customer_id = l.customer_id
    AND r.transaction_month = l.transaction_month

LEFT JOIN latest_monthly_event AS e
    ON r.customer_id = e.customer_id
    AND r.transaction_month = e.transaction_month
