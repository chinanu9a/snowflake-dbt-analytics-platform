{{
    config(
        materialized='table'
    )
}}

with events as (

    select * from {{ ref('stg_events') }}

),

venues as (

    select * from {{ ref('stg_venues') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['e.event_id']) }} as event_key,
        e.event_id,
        e.venue_id,
        v.venue_name,
        e.event_name,
        e.event_type,
        e.event_date,
        e.event_start_time,
        e.event_end_time,
        e.expected_attendance,
        e.actual_attendance,
        e.is_cancelled,
        case
            when v.capacity > 0
            then round(cast(e.actual_attendance as decimal(10,4)) / v.capacity * 100, 1)
            else null
        end                                             as attendance_fill_rate,
        e.created_at,
        e.updated_at

    from events e
    left join venues v
        on e.venue_id = v.venue_id

)

select * from final
