{{
    config(
        materialized='view'
    )
}}

with events as (

    select * from {{ ref('stg_events') }}

),

venues as (

    select * from {{ ref('stg_venues') }}

),

bridge as (

    select
        {{ dbt_utils.generate_surrogate_key(['e.event_id', 'e.venue_id']) }} as event_venue_key,
        e.event_id,
        e.venue_id,
        v.venue_name,
        v.venue_type,
        v.city,
        v.state,
        e.event_name,
        e.event_type,
        e.event_date,
        e.actual_attendance,
        v.capacity,
        case
            when v.capacity > 0
            then round(cast(e.actual_attendance as decimal(10,4)) / v.capacity * 100, 1)
            else null
        end                                             as attendance_pct

    from events e
    inner join venues v
        on e.venue_id = v.venue_id

)

select * from bridge
