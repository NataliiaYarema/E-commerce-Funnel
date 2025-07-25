WITH session_info AS (
SELECT
user_pseudo_id,
(SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id') AS session_id,
user_pseudo_id || CAST((SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id') AS STRING) AS user_session_id,
traffic_source.source,
traffic_source.medium,
traffic_source.name AS campaign,
geo.country,
device.category AS device_category,
device.language AS device_language,
device.operating_system,
REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'page_location'), r'(?:https:\/\/)?[^\/]+\/(.*)') AS landing_page
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS e 
WHERE event_name = 'session_start'),

events_info AS (
SELECT
TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
event_name,
(SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id') AS session_id,user_pseudo_id || CAST((SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id') AS STRING) AS user_session_id
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS e
WHERE event_name IN (
  'session_start', 
  'view_item', 
  'add_to_cart', 
  'begin_checkout', 
  'add_shipping_info', 
  'add_payment_info', 
  'purchase'
  )
)

SELECT
s.*,
e.event_timestamp,
e.event_name
FROM session_info s
LEFT JOIN events_info e
ON s.user_session_id = e.user_session_id
