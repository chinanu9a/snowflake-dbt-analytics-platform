{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        incremental_strategy='delete+insert'
    )
}}

with enriched_items as (

    select * from {{ ref('int_order_items_enriched') }}

    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}

),

final as (

    select
        order_item_id,
        order_id,
        menu_item_id,
        event_id,
        customer_id,
        employee_id,
        venue_id,
        order_timestamp,
        cast(order_timestamp as date)                   as order_date,
        order_status,
        payment_method,
        item_name,
        item_category,
        item_subcategory,
        is_alcoholic,

        -- measures
        quantity,
        unit_price,
        gross_sales,
        line_discount,
        net_sales,
        line_tax,
        line_total,
        cost_price,
        total_cost,
        gross_margin,

        created_at,
        updated_at

    from enriched_items

)

select * from final
