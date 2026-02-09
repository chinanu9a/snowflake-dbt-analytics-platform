-- Net sales should never be negative (a returned item creates a refund order, not negative line items)
select
    order_item_id,
    net_sales
from {{ ref('fct_sales_line_items') }}
where net_sales < 0
