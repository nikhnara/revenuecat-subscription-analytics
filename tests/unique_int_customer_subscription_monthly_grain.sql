-- Narrow monthly-table scan; no raw transaction reread.
select customer_id, transaction_month
from {{ ref('int_customer_subscription_monthly') }}
group by customer_id, transaction_month
having count(*) > 1
