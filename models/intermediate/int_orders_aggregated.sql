{{
    config(
        materialized='view'
    )
}}

with order_items as (

    select * from {{ ref('int_order_items_enriched') }}

),

aggregated as (

    select
        order_id,
        event_id,
        customer_id,
        employee_id,
        venue_id,
        order_timestamp,
        order_status,
        payment_method,

        count(distinct order_item_id)                   as line_item_count,
        sum(quantity)                                    as total_quantity,
        sum(gross_sales)                                as gross_sales,
        sum(line_discount)                              as total_discount,
        sum(net_sales)                                  as net_sales,
        sum(line_tax)                                   as total_tax,
        sum(line_total)                                 as order_total,
        sum(total_cost)                                 as total_cost,
        sum(gross_margin)                               as gross_margin,

        count(distinct case when is_alcoholic then menu_item_id end) as alcoholic_item_count,
        count(distinct menu_item_id)                    as distinct_item_count,

        min(created_at)                                 as created_at,
        max(updated_at)                                 as updated_at

    from order_items
    group by
        order_id,
        event_id,
        customer_id,
        employee_id,
        venue_id,
        order_timestamp,
        order_status,
        payment_method

)

select * from aggregated
