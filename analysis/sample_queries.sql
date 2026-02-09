-- ============================================================================
-- Sample Analytical Queries
-- These queries demonstrate the types of business questions
-- the data model can answer.
-- ============================================================================


-- 1. Sales by venue and event
-- "Which venues and events drive the most revenue?"
select
    v.venue_name,
    e.event_name,
    e.event_type,
    e.event_date,
    count(distinct o.order_id)      as total_orders,
    sum(o.net_sales)                as net_sales,
    sum(o.gross_margin)             as gross_margin,
    round(avg(o.net_sales), 2)      as avg_order_value
from {{ ref('fct_orders') }} o
join {{ ref('dim_venue') }} v on o.venue_id = v.venue_id
join {{ ref('dim_event') }} e on o.event_id = e.event_id
group by 1, 2, 3, 4
order by net_sales desc;


-- 2. Top items with discount impact
-- "Which menu items have the highest discount rates, and what's the revenue impact?"
select
    li.item_name,
    li.item_category,
    sum(li.quantity)                 as units_sold,
    sum(li.gross_sales)             as gross_sales,
    sum(li.line_discount)           as total_discounts,
    sum(li.net_sales)               as net_sales,
    round(
        sum(li.line_discount) / nullif(sum(li.gross_sales), 0) * 100,
        1
    )                               as discount_rate_pct
from {{ ref('fct_sales_line_items') }} li
group by 1, 2
order by discount_rate_pct desc;


-- 3. Loyalty repeat rate
-- "What percentage of loyalty customers are repeat buyers by tier?"
select
    c.loyalty_tier,
    count(*)                                                as total_customers,
    sum(case when c.customer_frequency_segment = 'repeat' then 1 else 0 end)
                                                            as repeat_customers,
    round(
        sum(case when c.customer_frequency_segment = 'repeat' then 1 else 0 end)
        * 100.0 / count(*),
        1
    )                                                       as repeat_rate_pct,
    round(avg(c.lifetime_net_sales), 2)                     as avg_lifetime_value
from {{ ref('dim_customer') }} c
group by 1
order by repeat_rate_pct desc;


-- 4. Hourly sales patterns
-- "When do sales peak during events?"
select
    extract(hour from li.order_timestamp)   as order_hour,
    count(distinct li.order_id)             as order_count,
    sum(li.net_sales)                       as net_sales,
    sum(li.quantity)                        as items_sold
from {{ ref('fct_sales_line_items') }} li
group by 1
order by 1;


-- 5. Event attendance vs. per-capita spend
-- "How does attendance fill rate correlate with per-capita spending?"
select
    e.event_name,
    e.event_type,
    e.actual_attendance,
    e.attendance_fill_rate,
    sum(o.net_sales)                                    as total_net_sales,
    round(sum(o.net_sales) / nullif(e.actual_attendance, 0), 2)
                                                        as per_capita_spend
from {{ ref('fct_orders') }} o
join {{ ref('dim_event') }} e on o.event_id = e.event_id
group by 1, 2, 3, 4
order by per_capita_spend desc;
