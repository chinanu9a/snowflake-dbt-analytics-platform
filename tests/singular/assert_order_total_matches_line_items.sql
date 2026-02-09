-- Order-level net_sales should match the sum of line-item net_sales within a $0.02 tolerance.
-- Small rounding differences are expected between aggregated line items and order headers.

with order_line_totals as (

    select
        order_id,
        sum(net_sales) as line_items_net_sales
    from {{ ref('fct_sales_line_items') }}
    group by order_id

),

order_totals as (

    select
        order_id,
        net_sales as order_net_sales
    from {{ ref('fct_orders') }}

),

comparison as (

    select
        olt.order_id,
        olt.line_items_net_sales,
        ot.order_net_sales,
        abs(olt.line_items_net_sales - ot.order_net_sales) as diff
    from order_line_totals olt
    inner join order_totals ot
        on olt.order_id = ot.order_id

)

select *
from comparison
where diff > 0.02
