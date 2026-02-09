{{
    config(
        materialized='table'
    )
}}

with line_items as (

    select * from {{ ref('fct_sales_line_items') }}

),

daily_sales as (

    select
        {{ dbt_utils.generate_surrogate_key(['venue_id', 'order_date']) }} as daily_venue_sales_key,
        venue_id,
        order_date,

        count(distinct order_id)                        as order_count,
        count(distinct customer_id)                     as unique_customers,
        sum(quantity)                                   as total_items_sold,
        sum(gross_sales)                                as gross_sales,
        sum(line_discount)                              as total_discounts,
        sum(net_sales)                                  as net_sales,
        sum(line_tax)                                   as total_tax,
        sum(total_cost)                                 as total_cost,
        sum(gross_margin)                               as gross_margin,

        -- category breakdowns
        sum(case when item_category = 'food' then net_sales else 0 end)
                                                        as food_net_sales,
        sum(case when item_category = 'beverage' then net_sales else 0 end)
                                                        as beverage_net_sales,
        sum(case when is_alcoholic then net_sales else 0 end)
                                                        as alcoholic_net_sales,

        -- average metrics
        round(avg(net_sales), 2)                        as avg_item_net_sales

    from line_items
    where order_status = 'completed'
    group by venue_id, order_date

)

select * from daily_sales
