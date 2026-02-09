-- Every line item should have a matching order in fct_orders
select
    li.order_item_id,
    li.order_id
from {{ ref('fct_sales_line_items') }} li
left join {{ ref('fct_orders') }} o
    on li.order_id = o.order_id
where o.order_id is null
