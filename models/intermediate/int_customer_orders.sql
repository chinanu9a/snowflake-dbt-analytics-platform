{{
    config(
        materialized='view'
    )
}}

with orders as (

    select * from {{ ref('int_orders_aggregated') }}

),

customer_stats as (

    select
        customer_id,

        count(distinct order_id)                        as total_orders,
        count(distinct event_id)                        as events_attended,
        count(distinct venue_id)                        as venues_visited,
        sum(gross_sales)                                as lifetime_gross_sales,
        sum(total_discount)                             as lifetime_discounts,
        sum(net_sales)                                  as lifetime_net_sales,
        sum(total_quantity)                             as lifetime_items_purchased,

        min(order_timestamp)                            as first_order_at,
        max(order_timestamp)                            as last_order_at,

        avg(net_sales)                                  as avg_order_value,

        case
            when count(distinct order_id) >= 3 then 'repeat'
            when count(distinct order_id) = 2 then 'returning'
            else 'one_time'
        end                                             as customer_frequency_segment

    from orders
    where customer_id is not null
    group by customer_id

)

select * from customer_stats
