{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

with orders_agg as (

    select * from {{ ref('int_orders_aggregated') }}

    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}

),

final as (

    select
        oa.order_id,
        oa.event_id,
        oa.customer_id,
        oa.employee_id,
        oa.venue_id,
        oa.order_timestamp,
        cast(oa.order_timestamp as date)                as order_date,
        oa.order_status,
        oa.payment_method,

        -- measures
        oa.line_item_count,
        oa.total_quantity,
        oa.gross_sales,
        oa.total_discount,
        oa.net_sales,
        oa.total_tax,
        oa.order_total,
        oa.total_cost,
        oa.gross_margin,
        oa.alcoholic_item_count,
        oa.distinct_item_count,

        -- tip from original order header
        o_tip.tip_amount,

        oa.created_at,
        oa.updated_at

    from orders_agg oa
    left join {{ ref('stg_orders') }} o_tip
        on oa.order_id = o_tip.order_id

)

select * from final
