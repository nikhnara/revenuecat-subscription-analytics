select customer_id
from {{ ref('int_customer_subscription_summary') }}
where ever_cancelled_flag is null
    or ever_cancelled_flag not in (0, 1)
    or ever_reactivated_flag is null
    or ever_reactivated_flag not in (0, 1)
    or latest_order_resolved_flag is null
    or latest_order_resolved_flag not in (0, 1)
    or latest_attribute_conflict_flag is null
    or latest_attribute_conflict_flag not in (0, 1)
