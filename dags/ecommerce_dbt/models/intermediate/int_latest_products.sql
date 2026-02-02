{{ config(materialized='view') }}

SELECT DISTINCT ON (product_id)
    product_id,
    COALESCE(new_price, price) as current_price,
    category,
    event_timestamp as updated_at
FROM {{ ref('stg_events') }}
WHERE is_valid_event = true
ORDER BY product_id, event_timestamp DESC