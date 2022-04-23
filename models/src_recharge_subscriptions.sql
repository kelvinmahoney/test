with source_table as (
select *
from `{{source('wise-weaver-282922.raw_data_sandbox.acme1_recharge_subscriptions'}}
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
cancelled_at,
DATE(cancelled_at) as cancelled_date,
expire_after_specific_number_of_charges
FROM source_table
