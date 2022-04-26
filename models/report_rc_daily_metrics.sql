with rc_subscriptions as (
SELECT *
, date(cancelled_at) as cancelled_date
, MIN(cancelled_at) OVER (PARTITION BY customer_id ORDER BY subscription_id) as min_cust_cancel
FROM {{ ref('src_recharge_subscriptions') }}
)

, date_table as (
SELECT * FROM {{ ref('date_table') }}
)

, customer_history as (
SELECT * FROM {{ ref('rc_customer_history') }}
)

, subscriptions_new as (
SELECT created_date
, count(subscription_id) as subscriptions_new
FROM rc_subscriptions
GROUP BY 1
)

, subscriptions_returning as (
SELECT created_date
, count(subscription_id) as subscriptions_returning
FROM rc_subscriptions
WHERE created_at > min_cust_cancel
GROUP BY 1
ORDER BY 1 DESC
)

, subscriptions_cancelled as (
SELECT cancelled_date
, count(subscription_id) as subscriptions_cancelled
FROM rc_subscriptions
WHERE lower(cancellation_reason) NOT LIKE '%max number of charge attempts%'
GROUP BY 1
)

, subscriptions_active as (
SELECT date_day, SUM(customer_total) as subscriptions_active
FROM customer_history
WHERE customer_total > 0
GROUP BY 1
ORDER BY 1 DESC
)

, subscriptions_churned as (
SELECT date_day
, SUM(churn_running_total) as subscriptions_churned
FROM customer_history
GROUP BY 1
ORDER BY 1 DESC
)

, subscribers_new as (
SELECT created_date
, count(distinct customer_id) as subscribers_new
FROM rc_subscriptions
WHERE cust_sub_rank = 1
GROUP BY 1
)

, subscribers_cancelled as (
SELECT start_date
, count(distinct customer_id) as subscribers_cancelled
FROM customer_history
WHERE customer_total = 0
GROUP BY 1
ORDER BY 1 DESC
)

,subscribers_active as (
SELECT date_day
, count(distinct customer_id) as subscribers_active
FROM customer_history
WHERE customer_total > 0
GROUP BY 1
ORDER BY 1 DESC
)

, subscribers_churned as (
SELECT date_day
, count(distinct customer_id) as subscribers_churned
FROM customer_history
WHERE customer_total = 0
GROUP BY 1
ORDER BY 1 DESC
)

SELECT date_table.date_day
, subscriptions_new.subscriptions_new
, subscriptions_returning.subscriptions_returning
, subscriptions_cancelled.subscriptions_cancelled
, subscriptions_active.subscriptions_active
, subscriptions_churned.subscriptions_churned
, subscribers_new.subscribers_new
, subscribers_cancelled.subscribers_cancelled
, subscribers_active.subscribers_active
, subscribers_churned.subscribers_churned
FROM date_table
	INNER JOIN subscriptions_new
		ON subscriptions_new.created_date = date_table.date_day
	INNER JOIN subscriptions_returning
		ON subscriptions_returning.created_date = date_table.date_day
	INNER JOIN subscriptions_cancelled
		ON subscriptions_cancelled.cancelled_date = date_table.date_day
	INNER JOIN subscriptions_active
		ON subscriptions_active.date_day = date_table.date_day
	INNER JOIN subscriptions_churned
		ON subscriptions_churned.date_day = date_table.date_day
	INNER JOIN subscribers_new
		ON subscribers_new.created_date = date_table.date_day
	INNER JOIN subscribers_cancelled
		ON subscribers_cancelled.start_date = date_table.date_day
	INNER JOIN subscribers_active
		ON subscribers_active.date_day = date_table.date_day
	INNER JOIN subscribers_churned
		ON subscribers_churned.date_day = date_table.date_day
ORDER BY 1 DESC


