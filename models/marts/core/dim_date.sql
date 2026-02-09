{{
    config(
        materialized='table'
    )
}}

{#
  Cross-platform date dimension generation.
  Uses adapter-specific SQL for Snowflake vs DuckDB.

  Date range: 2023-01-01 to 2025-12-31 (1096 days)
#}

{% set start_date = "2023-01-01" %}
{% set end_date = "2025-12-31" %}
{# Pre-calculated: 365 (2023) + 366 (2024 leap) + 365 (2025) = 1096 days #}
{% set num_days = 1096 %}

with date_spine as (

    {% if target.type == 'snowflake' %}
    -- Snowflake: use GENERATOR with constant rowcount
    select
        dateadd(day, row_number() over (order by seq4()) - 1, '{{ start_date }}'::date) as date_day
    from table(generator(rowcount => {{ num_days }}))

    {% else %}
    -- DuckDB / other: use generate_series
    select
        cast(unnest(generate_series(date '{{ start_date }}', date '{{ end_date }}', interval '1 day')) as date) as date_day

    {% endif %}

),

final as (

    select
        date_day,
        extract(year from date_day)                                     as year_number,
        extract(month from date_day)                                    as month_number,
        extract(day from date_day)                                      as day_of_month,
        extract(dayofweek from date_day)                                as day_of_week,
        extract(week from date_day)                                     as week_of_year,
        extract(quarter from date_day)                                  as quarter_number,

        -- Formatted strings (adapter-specific)
        {% if target.type == 'snowflake' %}
        to_char(date_day, 'YYYY-MM')                                    as year_month,
        to_char(date_day, 'MMMM')                                       as month_name,
        to_char(date_day, 'MON')                                        as month_name_short,
        dayname(date_day)                                               as day_name,
        left(dayname(date_day), 3)                                      as day_name_short,
        {% else %}
        strftime(date_day, '%Y-%m')                                     as year_month,
        strftime(date_day, '%B')                                        as month_name,
        strftime(date_day, '%b')                                        as month_name_short,
        strftime(date_day, '%A')                                        as day_name,
        strftime(date_day, '%a')                                        as day_name_short,
        {% endif %}

        -- Fiscal calendar (Oct = Q1)
        case
            when extract(month from date_day) >= 10
            then extract(year from date_day) + 1
            else extract(year from date_day)
        end                                                             as fiscal_year,

        case
            when extract(month from date_day) between 10 and 12 then 1
            when extract(month from date_day) between 1 and 3   then 2
            when extract(month from date_day) between 4 and 6   then 3
            when extract(month from date_day) between 7 and 9   then 4
        end                                                             as fiscal_quarter,

        -- Flags
        case
            when extract(dayofweek from date_day) in (0, 6) then true
            else false
        end                                                             as is_weekend

    from date_spine

)

select * from final
