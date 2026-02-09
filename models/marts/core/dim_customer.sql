{{
    config(
        materialized='table'
    )
}}

with customers as (

    select * from {{ ref('stg_customers') }}

),

customer_orders as (

    select * from {{ ref('int_customer_orders') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} as customer_key,
        c.customer_id,
        c.first_name,
        c.last_name,
        c.first_name || ' ' || c.last_name             as full_name,
        c.email,
        c.phone,
        c.loyalty_tier,
        c.loyalty_points,
        c.signup_date,
        c.birth_date,
        c.city,
        c.state,
        c.is_active,

        -- order history metrics
        coalesce(co.total_orders, 0)                    as total_orders,
        coalesce(co.events_attended, 0)                 as events_attended,
        coalesce(co.venues_visited, 0)                  as venues_visited,
        coalesce(co.lifetime_net_sales, 0)              as lifetime_net_sales,
        co.first_order_at,
        co.last_order_at,
        coalesce(co.avg_order_value, 0)                 as avg_order_value,
        coalesce(co.customer_frequency_segment, 'no_orders') as customer_frequency_segment,

        c.created_at,
        c.updated_at

    from customers c
    left join customer_orders co
        on c.customer_id = co.customer_id

)

select * from final
