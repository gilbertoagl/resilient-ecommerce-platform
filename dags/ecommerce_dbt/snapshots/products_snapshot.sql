{% snapshot products_snapshot %}

{{
    config(
      target_schema='public',
      unique_key='product_id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

SELECT * FROM {{ ref('int_latest_products') }}

{% endsnapshot %}