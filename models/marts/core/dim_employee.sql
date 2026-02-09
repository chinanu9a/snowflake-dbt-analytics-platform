{{
    config(
        materialized='table'
    )
}}

with employees as (

    select * from {{ ref('stg_employees') }}

),

venues as (

    select * from {{ ref('stg_venues') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['e.employee_id']) }} as employee_key,
        e.employee_id,
        e.first_name,
        e.last_name,
        e.first_name || ' ' || e.last_name             as full_name,
        e.role,
        e.venue_id,
        v.venue_name,
        e.hire_date,
        e.hourly_rate,
        e.is_active,
        e.created_at,
        e.updated_at

    from employees e
    left join venues v
        on e.venue_id = v.venue_id

)

select * from final
