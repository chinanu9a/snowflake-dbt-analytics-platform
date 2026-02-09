{{
    config(
        materialized='table'
    )
}}

with suppliers as (

    select * from {{ ref('stg_suppliers') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['supplier_id']) }} as supplier_key,
        supplier_id,
        supplier_name,
        contact_name,
        contact_email,
        contact_phone,
        category,
        city,
        state,
        is_active,
        created_at,
        updated_at

    from suppliers

)

select * from final
