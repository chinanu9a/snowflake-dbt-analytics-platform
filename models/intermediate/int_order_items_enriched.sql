{{
    config(
        materialized='view'
    )
}}

with order_items as (

    select * from {{ ref('stg_order_items') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

menu_items as (

    select * from {{ ref('stg_menu_items') }}

),

enriched as (

    select
        oi.order_item_id,
        oi.order_id,
        oi.menu_item_id,
        o.event_id,
        o.customer_id,
        o.employee_id,
        o.venue_id,
        o.order_timestamp,
        o.order_status,
        o.payment_method,
        mi.item_name,
        mi.category                                     as item_category,
        mi.subcategory                                  as item_subcategory,
        mi.is_alcoholic,
        oi.quantity,
        oi.unit_price,
        (oi.unit_price * oi.quantity)                   as gross_sales,
        oi.line_discount,
        (oi.unit_price * oi.quantity - oi.line_discount) as net_sales,
        oi.line_tax,
        oi.line_total,
        mi.cost_price,
        (mi.cost_price * oi.quantity)                   as total_cost,
        (oi.unit_price * oi.quantity - oi.line_discount)
            - (mi.cost_price * oi.quantity)             as gross_margin,
        oi.created_at,
        oi.updated_at

    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
    inner join menu_items mi
        on oi.menu_item_id = mi.menu_item_id

)

select * from enriched
