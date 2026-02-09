{{
    config(
        materialized='table'
    )
}}

with menu_items as (

    select * from {{ ref('stg_menu_items') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['menu_item_id']) }} as menu_item_key,
        menu_item_id,
        item_name,
        category,
        subcategory,
        unit_price,
        cost_price,
        case
            when cost_price > 0
            then round((unit_price - cost_price) / unit_price * 100, 1)
            else null
        end                                             as margin_pct,
        is_alcoholic,
        is_available,
        venue_id,
        created_at,
        updated_at

    from menu_items

)

select * from final
