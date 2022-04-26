with subscriptions as (
SELECT * FROM {{ref('src_recharge_subscriptions')}}
)

, date_table as (
SELECT * FROM {{ref('date_table')}}
)

, sub_creates as (
SELECT customer_id
, created_date as event_date
, 'create' as event_type
, 'subscribe' as reason
FROM subscriptions
) 

, sub_cancels as (
SELECT customer_id
, DATE(cancelled_at) as event_date
, 'cancel' as event_timestamp
, cancellation_reason as reason
FROM subscriptions
	WHERE subscription_status = 'CANCELLED'
)

, combined_events as (
SELECT * FROM sub_creates
UNION ALL 
SELECT * FROM sub_cancels
)

, reagg as (
SELECT event_date
, customer_id
, SUM(CASE WHEN event_type = 'create' then 1 else 0 end) as subscription_creates
, SUM(CASE WHEN event_type = 'cancel' then 1 else 0 end) as subscription_cancels
, SUM(CASE WHEN event_type = 'cancel' AND LOWER(reason) NOT LIKE '%max number of charge attempts%' then 1 else 0 end) as subscription_churns
FROM combined_events
GROUP BY 1,2
ORDER BY 1 DESC
)

, windows as (
SELECT *
, LEAD(event_date,1) OVER (PARTITION BY customer_id ORDER BY event_date) as next_event
, SUM(subscription_creates) OVER (PARTITION BY customer_id ORDER BY event_date) as create_running_total
, SUM(subscription_cancels) OVER (PARTITION BY customer_id ORDER BY event_date) as cancel_running_total
, SUM(subscription_churns) OVER (PARTITION BY customer_id ORDER BY event_date) as churn_running_total
FROM reagg
)

, customer_history as (
SELECT customer_id
, event_date as start_date
, IFNULL(next_event,CURRENT_DATE()) as end_date
, subscription_creates
, create_running_total
, cancel_running_total
, churn_running_total
, create_running_total - cancel_running_total as customer_total
FROM windows
)

SELECT *
FROM date_table
    LEFT JOIN customer_history
        ON date_table.date_day >= customer_history.start_date 
            and date_table.date_day < customer_history.end_date
