{{ config(
    materialized='table',
    partition_by={
        "field": "transaction_date",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=['customer_id']
) }}

/*
KKBOX-oriented event history; every staging row is preserved.
Source conventions: Kaggle's KKBOX challenge data description and the Scala
labeler reproduced in https://www.databricks.com/notebooks/churn/1-data-preparation.html
The original Kaggle Scala download has not been independently compared.

Ordering is a business convention, not recovered timestamps:
  date ASC, concatenated STRING(price, days, method) DESC,
  cancel ASC, non-cancel expiry ASC, cancellation expiry DESC.
Do not replace the concatenated string with a numeric tuple.

Rows tied on all labeler ordering keys share one event group. This does NOT
remove source rows or assert that they are duplicate financial transactions.
A group's lifecycle state is known even if non-ordering attributes disagree.

Two different expiry concepts are retained:
  reported_expiry_after_event: final record's expiry, for history snapshots;
  renewal_reference_expiry_after_event: reference for the next subscription,
    shortened by intervening cancellations, matching calculateRenewalGap.

Our lifecycle categories are analytical extensions of the binary churn target.
A returning subscription at a gap >= 30 days is called reactivation here.
This is not the full cutoff/cohort-based challenge labeling procedure.
*/
WITH source_quality AS (
    SELECT
        s.*,
        CONCAT(
            CAST(list_price_ntd AS STRING),
            CAST(plan_days AS STRING),
            CAST(payment_method_id AS STRING)
        ) AS kkbox_plan_signature,
        CASE
            WHEN customer_id IS NULL OR transaction_date IS NULL
              OR list_price_ntd IS NULL OR plan_days IS NULL
              OR payment_method_id IS NULL OR expiry_date IS NULL
              OR cancellation_flag IS NULL OR cancellation_flag NOT IN (0, 1)
            THEN 1 ELSE 0
        END AS invalid_ordering_input_flag
    FROM {{ ref('stg_transactions') }} AS s
),
source_context AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY customer_id, transaction_date)
            AS same_day_transaction_count,
        SUM(invalid_ordering_input_flag) OVER (PARTITION BY customer_id, transaction_date)
            AS invalid_ordering_rows_on_day,
        SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END)
            OVER (PARTITION BY customer_id) AS undated_customer_row_count
    FROM source_quality
),
ranked_source AS (
    SELECT
        *,
        -- Invalid days become a single unknown-state placeholder.
        DENSE_RANK() OVER (
            PARTITION BY customer_id
            ORDER BY
                transaction_date,
                CASE WHEN invalid_ordering_rows_on_day = 0
                    THEN kkbox_plan_signature END DESC,
                CASE WHEN invalid_ordering_rows_on_day = 0
                    THEN cancellation_flag END ASC,
                CASE WHEN invalid_ordering_rows_on_day = 0 AND cancellation_flag = 0
                    THEN expiry_date END ASC,
                CASE WHEN invalid_ordering_rows_on_day = 0 AND cancellation_flag = 1
                    THEN expiry_date END DESC
        ) AS ordering_group_sequence
    FROM source_context
),
event_groups AS (
    SELECT
        customer_id,
        ordering_group_sequence,
        MIN(transaction_date) AS transaction_date,
        COUNT(*) AS event_group_source_row_count,
        MAX(undated_customer_row_count) AS undated_customer_row_count,
        CASE WHEN MAX(invalid_ordering_rows_on_day) = 0 THEN 1 ELSE 0 END
            AS group_order_resolved_flag,
        CASE WHEN MAX(invalid_ordering_rows_on_day) = 0
            THEN MAX(cancellation_flag) END AS group_cancellation_flag,
        CASE WHEN MAX(invalid_ordering_rows_on_day) = 0
            THEN MAX(expiry_date) END AS group_expiry_date
    FROM ranked_source
    WHERE customer_id IS NOT NULL AND transaction_date IS NOT NULL
    GROUP BY customer_id, ordering_group_sequence
),
segments AS (
    SELECT
        *,
        -- A subscription resets the reference; cancellations can only shorten it.
        SUM(CASE WHEN group_cancellation_flag = 1 THEN 0 ELSE 1 END) OVER (
            PARTITION BY customer_id ORDER BY ordering_group_sequence
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS reference_segment_id
    FROM event_groups
),
reference_state AS (
    SELECT
        *,
        CASE
            WHEN undated_customer_row_count > 0 THEN NULL
            WHEN SUM(CASE WHEN group_order_resolved_flag = 0 THEN 1 ELSE 0 END)
                OVER segment_history > 0 THEN NULL
            ELSE MIN(group_expiry_date) OVER segment_history
        END AS renewal_reference_expiry_after_event,
        CASE WHEN undated_customer_row_count = 0
            THEN group_expiry_date END AS reported_expiry_after_event
    FROM segments
    WINDOW segment_history AS (
        PARTITION BY customer_id, reference_segment_id
        ORDER BY ordering_group_sequence
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
),
group_context AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY transaction_date)
            AS transaction_day_sequence,
        LAG(transaction_date) OVER customer_history AS previous_event_date,
        LEAD(transaction_date) OVER customer_history AS next_event_date,
        LAG(renewal_reference_expiry_after_event) OVER customer_history
            AS previous_renewal_reference_expiry_date,
        COALESCE(SUM(CASE WHEN group_cancellation_flag = 0 THEN 1 ELSE 0 END)
            OVER prior_history, 0) AS prior_subscription_event_group_count,
        COALESCE(SUM(CASE WHEN group_order_resolved_flag = 0 THEN 1 ELSE 0 END)
            OVER prior_history, 0) AS prior_unknown_event_group_count,
        CASE WHEN ordering_group_sequence = MAX(ordering_group_sequence)
            OVER (PARTITION BY customer_id, transaction_date)
            THEN 1 ELSE 0 END AS is_last_event_group_on_day,
        CASE WHEN ordering_group_sequence = MAX(ordering_group_sequence)
            OVER (PARTITION BY customer_id)
            THEN 1 ELSE 0 END AS is_latest_dated_event_group
    FROM reference_state
    WINDOW
        customer_history AS (
            PARTITION BY customer_id ORDER BY ordering_group_sequence
        ),
        prior_history AS (
            PARTITION BY customer_id ORDER BY ordering_group_sequence
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )
),
group_classification AS (
    SELECT
        *,
        DATE_DIFF(transaction_date, previous_renewal_reference_expiry_date, DAY)
            AS days_from_previous_expiry,
        CASE
            WHEN group_order_resolved_flag = 0 THEN 'ordering_inputs_missing_or_invalid'
            WHEN group_cancellation_flag = 1 THEN 'cancellation'
            WHEN undated_customer_row_count > 0 THEN 'undated_customer_history'
            WHEN prior_subscription_event_group_count = 0
                AND prior_unknown_event_group_count = 0
                THEN 'first_observed_subscription'
            WHEN previous_renewal_reference_expiry_date IS NULL
                THEN 'previous_reference_unknown'
            WHEN transaction_date <= previous_renewal_reference_expiry_date THEN 'renewal'
            WHEN DATE_DIFF(transaction_date, previous_renewal_reference_expiry_date, DAY) < 30
                THEN 'late_renewal'
            ELSE 'reactivation'
        END AS group_classification_reason
    FROM group_context
),
row_classification AS (
    SELECT
        s.*,
        CASE WHEN g.group_order_resolved_flag = 1
            THEN g.ordering_group_sequence END AS event_group_sequence,
        g.transaction_day_sequence,
        g.event_group_source_row_count,
        g.previous_event_date,
        g.next_event_date,
        g.previous_renewal_reference_expiry_date,
        g.renewal_reference_expiry_after_event,
        g.reported_expiry_after_event,
        g.days_from_previous_expiry,
        COALESCE(g.is_last_event_group_on_day, 0) AS is_last_event_group_on_day,
        COALESCE(g.is_latest_dated_event_group, 0) AS is_latest_dated_event_group,
        COALESCE(g.group_order_resolved_flag, 0) AS group_order_resolved_flag,
        CASE
            WHEN s.customer_id IS NULL THEN 'missing_customer_id'
            WHEN s.transaction_date IS NULL THEN 'missing_transaction_date'
            WHEN s.cancellation_flag IS NULL OR s.cancellation_flag NOT IN (0, 1)
                THEN 'invalid_cancellation_flag'
            WHEN s.cancellation_flag = 1 THEN 'cancellation'
            ELSE g.group_classification_reason
        END AS classification_reason
    FROM ranked_source AS s
    LEFT JOIN group_classification AS g
        ON s.customer_id = g.customer_id
        AND s.ordering_group_sequence = g.ordering_group_sequence
)
SELECT
    *,
    CASE
        WHEN cancellation_flag = 1 THEN 'cancellation'
        WHEN classification_reason IN (
            'first_observed_subscription', 'renewal', 'late_renewal', 'reactivation'
        ) THEN classification_reason
        ELSE 'unclassified'
    END AS event_type,
    CASE WHEN reported_expiry_after_event IS NULL THEN 1 ELSE 0 END
        AS reported_state_unknown_flag,
    CASE WHEN renewal_reference_expiry_after_event IS NULL THEN 1 ELSE 0 END
        AS renewal_reference_unknown_flag,
    CASE WHEN event_group_source_row_count > 1 THEN 1 ELSE 0 END
        AS ordering_key_tie_flag,
    -- Observational flags only: these do not invalidate an otherwise ordered event.
    CASE WHEN expiry_date < transaction_date THEN 1 ELSE 0 END
        AS expiry_before_transaction_flag,
    CASE WHEN cancellation_flag = 1
        AND expiry_date > previous_renewal_reference_expiry_date THEN 1 ELSE 0 END
        AS cancellation_expiry_above_reference_flag
FROM row_classification
