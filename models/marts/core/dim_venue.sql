{{
    config(
        materialized='table'
    )
}}

with venues as (

    select * from {{ ref('stg_venues') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['venue_id']) }} as venue_key,
        venue_id,
        venue_name,
        venue_type,
        city,
        state,
        country,
        capacity,
        timezone,
        opened_date,
        is_active,
        created_at,
        updated_at

    from venues

)

select * from final
