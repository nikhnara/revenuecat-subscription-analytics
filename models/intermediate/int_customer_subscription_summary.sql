/* @datacloud.settings
{
  "version": 1,
  "service": "BIG_QUERY",
  "connectionInfo": {
    "billingProjectId": "INHERIT",
    "location": "INHERIT"
  },
  "dialect": "GOOGLE_SQL"
}
*/

{{ config(materialized='table', cluster_by=['customer_id']) }}

WITH transactions AS (
    SELECT * FROM {{ ref('int_transaction') }} WHERE customer_id IS NOT NULL
),
customer_totals AS (
    SELECT
        customer_id,
        MIN(transaction_date) AS first_transaction_date,
        MAX(transaction_date) AS last_transaction_date,
        COUNT(*) AS total_transaction_count,
        COUNT(DISTINCT transaction_date) AS observed_transaction_day_count,
        SUM(CASE WHEN cancellation_flag = 1 THEN 1 ELSE 0 END) AS cancellation_record_count,
        SUM(CASE WHEN cancellation_flag = 0 THEN 1 ELSE 0 END) AS non_cancellation_record_count,
        SUM(CASE WHEN event_type = 'unclassified' THEN 1 ELSE 0 END) AS unclassified_record_count,
        MAX(CASE WHEN cancellation_flag = 1 THEN 1 ELSE 0 END) AS ever_cancelled_flag,
        MAX(CASE WHEN event_type = 'reactivation' THEN 1 ELSE 0 END) AS ever_reactivated_flag,
        SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END) AS undated_transaction_count,
        SUM(invalid_ordering_input_flag) AS invalid_ordering_input_record_count,
        SUM(CASE WHEN group_order_resolved_flag = 0 THEN 1 ELSE 0 END)
            AS unresolved_order_record_count,
        COUNT(DISTINCT CASE WHEN same_day_transaction_count > 1 THEN transaction_date END)
            AS multi_record_date_count,
        SUM(ordering_key_tie_flag) AS tied_ordering_key_record_count,
        SUM(expiry_before_transaction_flag) AS expiry_before_transaction_record_count,
        SUM(cancellation_expiry_above_reference_flag) AS cancellation_expiry_above_reference_count,
        SUM(CASE WHEN recorded_amount_paid_ntd IS NULL THEN 1 ELSE 0 END)
            AS missing_recorded_amount_count,
        -- NULL means incomplete; never present a partial sum as a complete total.
        CASE WHEN COUNT(recorded_amount_paid_ntd) = COUNT(*)
            THEN SUM(recorded_amount_paid_ntd) END AS observed_recorded_amount_paid_ntd,
        CASE WHEN SUM(CASE WHEN cancellation_flag = 0 AND recorded_amount_paid_ntd IS NULL
            THEN 1 ELSE 0 END) = 0
            THEN SUM(CASE WHEN cancellation_flag = 0 THEN recorded_amount_paid_ntd ELSE 0 END)
        END AS observed_non_cancellation_recorded_amount_ntd,
        CASE WHEN SUM(CASE WHEN cancellation_flag = 1 AND recorded_amount_paid_ntd IS NULL
            THEN 1 ELSE 0 END) = 0
            THEN SUM(CASE WHEN cancellation_flag = 1 THEN recorded_amount_paid_ntd ELSE 0 END)
        END AS observed_cancellation_recorded_amount_ntd,
        CASE WHEN SUM(CASE WHEN cancellation_flag = 0 AND plan_days IS NULL THEN 1 ELSE 0 END) = 0
            THEN SUM(CASE WHEN cancellation_flag = 0 THEN plan_days ELSE 0 END)
        END AS observed_non_cancellation_plan_days
    FROM transactions GROUP BY customer_id
),
lifecycle_groups AS (
    SELECT DISTINCT customer_id, event_group_sequence, event_type
    FROM transactions WHERE event_group_sequence IS NOT NULL
),
lifecycle_totals AS (
    SELECT
        customer_id,
        COUNT(*) AS ordered_event_group_count,
        SUM(CASE WHEN event_type = 'first_observed_subscription' THEN 1 ELSE 0 END) AS first_observed_subscription_event_group_count,
        SUM(CASE WHEN event_type = 'renewal' THEN 1 ELSE 0 END) AS renewal_event_group_count,
        SUM(CASE WHEN event_type = 'late_renewal' THEN 1 ELSE 0 END) AS late_renewal_event_group_count,
        SUM(CASE WHEN event_type = 'reactivation' THEN 1 ELSE 0 END) AS reactivation_event_group_count,
        SUM(CASE WHEN event_type = 'cancellation' THEN 1 ELSE 0 END) AS cancellation_event_group_count,
        SUM(CASE WHEN event_type = 'unclassified' THEN 1 ELSE 0 END) AS unclassified_event_group_count
    FROM lifecycle_groups GROUP BY customer_id
),
latest_event AS (
    SELECT
        customer_id,
        MAX(same_day_transaction_count) AS latest_day_record_count,
        COUNT(*) AS latest_event_group_source_row_count,
        MAX(group_order_resolved_flag) AS latest_order_resolved_flag,
        MAX(reported_expiry_after_event) AS latest_reported_expiry_date,
        MAX(renewal_reference_expiry_after_event) AS latest_renewal_reference_expiry_date,
        MAX(reported_state_unknown_flag) AS latest_reported_state_unknown_flag,
        MAX(renewal_reference_unknown_flag) AS latest_renewal_reference_unknown_flag,
        CASE WHEN MAX(group_order_resolved_flag) = 1
            AND COUNT(expiry_date) = COUNT(*) AND COUNT(DISTINCT expiry_date) = 1
            THEN MAX(expiry_date) END AS latest_recorded_expiry_date,
        CASE WHEN MAX(group_order_resolved_flag) = 1
            AND COUNT(plan_days) = COUNT(*) AND COUNT(DISTINCT plan_days) = 1
            THEN MAX(plan_days) END AS latest_plan_days,
        CASE WHEN MAX(group_order_resolved_flag) = 1
            AND COUNT(list_price_ntd) = COUNT(*) AND COUNT(DISTINCT list_price_ntd) = 1
            THEN MAX(list_price_ntd) END AS latest_list_price_ntd,
        CASE WHEN MAX(group_order_resolved_flag) = 1
            AND COUNT(recorded_amount_paid_ntd) = COUNT(*) AND COUNT(DISTINCT recorded_amount_paid_ntd) = 1
            THEN MAX(recorded_amount_paid_ntd) END AS latest_recorded_amount_paid_ntd,
        CASE WHEN MAX(group_order_resolved_flag) = 1
            AND COUNT(payment_method_id) = COUNT(*) AND COUNT(DISTINCT payment_method_id) = 1
            THEN MAX(payment_method_id) END AS latest_payment_method_id,
        CASE WHEN MAX(group_order_resolved_flag) = 1
            AND COUNT(auto_renew_flag) = COUNT(*) AND COUNT(DISTINCT auto_renew_flag) = 1
            THEN MAX(auto_renew_flag) END AS latest_auto_renew_flag,
        CASE WHEN MAX(group_order_resolved_flag) = 1
            AND COUNT(cancellation_flag) = COUNT(*) AND COUNT(DISTINCT cancellation_flag) = 1
            THEN MAX(cancellation_flag) END AS latest_cancellation_flag,
        CASE WHEN MAX(group_order_resolved_flag) = 1
            AND COUNT(event_type) = COUNT(*) AND COUNT(DISTINCT event_type) = 1
            THEN MAX(event_type) END AS latest_event_type,
        CASE WHEN MAX(group_order_resolved_flag) = 1
            AND COUNT(classification_reason) = COUNT(*) AND COUNT(DISTINCT classification_reason) = 1
            THEN MAX(classification_reason) END AS latest_classification_reason,
        CASE WHEN MAX(group_order_resolved_flag) = 1
            AND COUNT(source_release) = COUNT(*) AND COUNT(DISTINCT source_release) = 1
            THEN MAX(source_release) END AS latest_source_release,
        CASE WHEN
COUNT(DISTINCT expiry_date) + CASE WHEN COUNT(expiry_date) < COUNT(*) THEN 1 ELSE 0 END > 1
            OR COUNT(DISTINCT plan_days) + CASE WHEN COUNT(plan_days) < COUNT(*) THEN 1 ELSE 0 END > 1
            OR COUNT(DISTINCT list_price_ntd) + CASE WHEN COUNT(list_price_ntd) < COUNT(*) THEN 1 ELSE 0 END > 1
            OR COUNT(DISTINCT recorded_amount_paid_ntd) + CASE WHEN COUNT(recorded_amount_paid_ntd) < COUNT(*) THEN 1 ELSE 0 END > 1
            OR COUNT(DISTINCT payment_method_id) + CASE WHEN COUNT(payment_method_id) < COUNT(*) THEN 1 ELSE 0 END > 1
            OR COUNT(DISTINCT auto_renew_flag) + CASE WHEN COUNT(auto_renew_flag) < COUNT(*) THEN 1 ELSE 0 END > 1
            OR COUNT(DISTINCT cancellation_flag) + CASE WHEN COUNT(cancellation_flag) < COUNT(*) THEN 1 ELSE 0 END > 1
            OR COUNT(DISTINCT event_type) + CASE WHEN COUNT(event_type) < COUNT(*) THEN 1 ELSE 0 END > 1
            OR COUNT(DISTINCT classification_reason) + CASE WHEN COUNT(classification_reason) < COUNT(*) THEN 1 ELSE 0 END > 1
            OR COUNT(DISTINCT source_release) + CASE WHEN COUNT(source_release) < COUNT(*) THEN 1 ELSE 0 END > 1
            THEN 1 ELSE 0 END AS latest_attribute_conflict_flag
    FROM transactions WHERE is_latest_dated_event_group = 1
    GROUP BY customer_id
),
subscription_groups AS (
    SELECT customer_id, event_group_sequence,
        MAX(transaction_date) AS subscription_date,
        MAX(expiry_date) AS subscription_recorded_expiry
    FROM transactions
    WHERE cancellation_flag = 0 AND event_group_sequence IS NOT NULL
    GROUP BY customer_id, event_group_sequence
),
last_subscription AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY customer_id ORDER BY event_group_sequence DESC
    ) AS subscription_rank
    FROM subscription_groups
),
payment_method_counts AS (
    -- Frequency across source records; cancellations are included.
    SELECT customer_id, payment_method_id, COUNT(*) AS method_record_count
    FROM transactions WHERE payment_method_id IS NOT NULL
    GROUP BY customer_id, payment_method_id
),
payment_method_ranks AS (
    SELECT *, RANK() OVER (
        PARTITION BY customer_id ORDER BY method_record_count DESC
    ) AS method_rank
    FROM payment_method_counts
),
payment_method_modes AS (
    SELECT customer_id,
        CASE WHEN COUNT(*) = 1 THEN MAX(payment_method_id) END
            AS most_frequent_payment_method_id,
        COUNT(*) AS top_payment_method_count
    FROM payment_method_ranks WHERE method_rank = 1
    GROUP BY customer_id
)
SELECT
    c.*,
    COALESCE(g.ordered_event_group_count, 0) AS ordered_event_group_count,
    COALESCE(g.first_observed_subscription_event_group_count, 0) AS first_observed_subscription_event_group_count,
    COALESCE(g.renewal_event_group_count, 0) AS renewal_event_group_count,
    COALESCE(g.late_renewal_event_group_count, 0) AS late_renewal_event_group_count,
    COALESCE(g.reactivation_event_group_count, 0) AS reactivation_event_group_count,
    COALESCE(g.cancellation_event_group_count, 0) AS cancellation_event_group_count,
    COALESCE(g.unclassified_event_group_count, 0) AS unclassified_event_group_count,
    c.last_transaction_date AS latest_transaction_date,
    DATE_TRUNC(c.first_transaction_date, MONTH) AS first_transaction_month,
    DATE_TRUNC(c.last_transaction_date, MONTH) AS latest_transaction_month,
    COALESCE(l.latest_day_record_count, 0) AS latest_day_record_count,
    COALESCE(l.latest_event_group_source_row_count, 0) AS latest_event_group_source_row_count,
    CASE WHEN l.latest_event_group_source_row_count > 1 THEN 1 ELSE 0 END
        AS latest_ordering_key_tie_flag,
    COALESCE(l.latest_order_resolved_flag, 0) AS latest_order_resolved_flag,
    COALESCE(l.latest_attribute_conflict_flag, 0) AS latest_attribute_conflict_flag,
    l.latest_recorded_expiry_date,
    l.latest_plan_days,
    l.latest_list_price_ntd,
    l.latest_recorded_amount_paid_ntd,
    l.latest_payment_method_id,
    l.latest_auto_renew_flag,
    l.latest_cancellation_flag,
    l.latest_event_type,
    l.latest_classification_reason,
    l.latest_source_release,
    l.latest_reported_expiry_date,
    l.latest_renewal_reference_expiry_date,
    COALESCE(l.latest_reported_state_unknown_flag, 1) AS latest_reported_state_unknown_flag,
    COALESCE(l.latest_renewal_reference_unknown_flag, 1) AS latest_renewal_reference_unknown_flag,
    -- Historical observation only; not a fallback for latest subscription state.
    s.subscription_date AS last_ordered_subscription_date,
    DATE_TRUNC(s.subscription_date,MONTH) AS last_ordered_subscription_month,
    s.subscription_recorded_expiry AS last_ordered_subscription_recorded_expiry,
    p.most_frequent_payment_method_id,
    COALESCE(p.top_payment_method_count, 0) AS top_payment_method_count,
    CASE WHEN p.top_payment_method_count > 1 THEN 1 ELSE 0 END AS payment_method_mode_tie_flag
FROM customer_totals AS c
LEFT JOIN lifecycle_totals AS g ON c.customer_id = g.customer_id
LEFT JOIN latest_event AS l ON c.customer_id = l.customer_id
LEFT JOIN last_subscription AS s ON c.customer_id = s.customer_id AND s.subscription_rank = 1
LEFT JOIN payment_method_modes AS p ON c.customer_id = p.customer_id