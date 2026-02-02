WITH raw_data AS (
    SELECT * FROM {{ source('ecommerce_source', 'raw_events') }}
),

parsed_data AS (
    SELECT
        event_id,
        event_type,
        event_timestamp,
        ingestion_timestamp,
        
        -- EXTRACCIÓN DE JSON
        (payload->>'id')::int as product_id,
        (payload->>'price')::numeric as price,
        (payload->>'old_price')::numeric as old_price,
        (payload->>'new_price')::numeric as new_price,
        payload->>'category' as category,
        
        -- Flag de validez
        CASE 
            WHEN event_type = 'inventory_error' THEN false
            WHEN (payload->>'price')::numeric < 0 THEN false
            ELSE true 
        END as is_valid_event

    FROM raw_data
)

SELECT * FROM parsed_data