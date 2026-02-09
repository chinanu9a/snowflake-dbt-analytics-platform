-- Tax amounts should never be negative
select
    order_item_id,
    line_tax
from {{ ref('fct_sales_line_items') }}
where line_tax < 0
