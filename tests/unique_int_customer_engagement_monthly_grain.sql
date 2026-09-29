-- Includes the preserved NULL activity_month bucket for invalid source dates.
select customer_id, activity_month
from {{ ref('int_customer_engagement_monthly') }}
group by customer_id, activity_month
having count(*) > 1
