with source_table as (
select *
from {{source('raw_data_sandbox','acme1_recharge_subscriptions')}}
)

SELECT id as subscription_id,
customer_id,
sku,
email,
product_title,
price,
status as subscription_status,
quantity,
created_at,
DATE(created_at) as created_date,
updated_at,
CASE WHEN status = 'CANCELLED' THEN COALESCE(cancelled_at, updated_at) ELSE cancelled_at END as cancelled_at,
cancellation_reason,
expire_after_specific_number_of_charges,
RANK() OVER (PARTITION BY customer_id ORDER BY created_at) as cust_sub_rank
FROM source_table
