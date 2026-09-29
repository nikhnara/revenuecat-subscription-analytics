-- Scan only the two grain columns of the monthly mart.
select customer_id, calendar_month
from {{ ref('mart_customer_month') }}
group by customer_id, calendar_month
having count(*) > 1
