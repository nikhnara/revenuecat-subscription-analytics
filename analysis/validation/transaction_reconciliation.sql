-- Read-only snapshot check. Uses materialized transaction, monthly, and
-- customer-summary models; no raw transaction tables are rescanned.
-- A zero reconciliation difference means the two model grains agree.
with transaction_rows as (
    select
        count(*) as transaction_records,
        countif(customer_id is null) as records_without_customer_id,
        countif(customer_id is not null) as identified_transaction_records,
        countif(customer_id is not null and transaction_date is not null)
            as identified_dated_transaction_records
    from {{ ref('int_transaction') }}
),
monthly as (
    select
        sum(transaction_record_count) as dated_transaction_records,
        sum(ordered_event_group_count) as ordered_event_groups,
        sum(renewal_event_count) as renewals,
        sum(late_renewal_event_count) as late_renewals,
        sum(reactivation_event_count) as reactivations,
        sum(cancellation_event_count) as cancellations
    from {{ ref('int_customer_subscription_monthly') }}
),
customer_summary as (
    select
        sum(total_transaction_count) as transaction_records,
        sum(undated_transaction_count) as undated_transaction_records,
        sum(ordered_event_group_count) as ordered_event_groups,
        sum(renewal_event_group_count) as renewals,
        sum(late_renewal_event_group_count) as late_renewals,
        sum(reactivation_event_group_count) as reactivations,
        sum(cancellation_event_group_count) as cancellations
    from {{ ref('int_customer_subscription_summary') }}
)
select
    t.transaction_records,
    t.records_without_customer_id,
    t.identified_transaction_records,
    s.transaction_records as summary_transaction_records,
    s.transaction_records - t.identified_transaction_records
        as summary_vs_identified_record_difference,
    t.identified_dated_transaction_records,
    m.dated_transaction_records as monthly_dated_transaction_records,
    m.dated_transaction_records - t.identified_dated_transaction_records
        as monthly_vs_dated_record_difference,
    s.undated_transaction_records,
    m.dated_transaction_records + s.undated_transaction_records
        - s.transaction_records as dated_plus_undated_record_difference,
    s.ordered_event_groups,
    m.ordered_event_groups as monthly_ordered_event_groups,
    m.ordered_event_groups - s.ordered_event_groups
        as ordered_event_group_difference,
    s.renewals,
    m.renewals - s.renewals as renewal_difference,
    s.late_renewals,
    m.late_renewals - s.late_renewals as late_renewal_difference,
    s.reactivations,
    m.reactivations - s.reactivations as reactivation_difference,
    s.cancellations,
    m.cancellations - s.cancellations as cancellation_difference
from transaction_rows as t
cross join monthly as m
cross join customer_summary as s
