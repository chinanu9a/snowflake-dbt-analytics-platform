{{
    config(
        materialized='view'
    )
}}

-- Semantic / metrics layer view providing pre-aggregated sales KPIs
-- for BI tools to consume without complex joins.

with sales as (

    select * from {{ ref('fct_daily_venue_sales') }}

),

venues as (

    select * from {{ ref('dim_venue') }}

),

final as (

    select
        s.venue_id,
        v.venue_name,
        v.venue_type,
        v.city,
        v.state,
        s.order_date,
        {{ fiscal_quarter('s.order_date') }}            as fiscal_quarter,
        {{ fiscal_year('s.order_date') }}               as fiscal_year,

        s.order_count,
        s.unique_customers,
        s.total_items_sold,
        s.gross_sales,
        s.total_discounts,
        s.net_sales,
        s.total_tax,
        s.total_cost,
        s.gross_margin,
        s.food_net_sales,
        s.beverage_net_sales,
        s.alcoholic_net_sales,
        s.avg_item_net_sales,

        case
            when s.gross_sales > 0
            then round(s.gross_margin / s.gross_sales * 100, 1)
            else 0
        end                                             as margin_pct,

        case
            when s.order_count > 0
            then round(s.net_sales / s.order_count, 2)
            else 0
        end                                             as avg_order_value

    from sales s
    left join venues v
        on s.venue_id = v.venue_id

)

select * from final
