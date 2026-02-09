{% snapshot snap_menu_item_prices %}

{{
    config(
        target_schema='snapshots',
        unique_key='menu_item_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select
    menu_item_id,
    item_name,
    category,
    unit_price,
    cost_price,
    venue_id,
    is_available,
    updated_at

from {{ ref('stg_menu_items') }}

{% endsnapshot %}
