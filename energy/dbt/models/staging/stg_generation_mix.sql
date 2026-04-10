with source as (
    select * from {{ source('energy_bronze', 'generation_mix') }}
),

cleaned as (
    select
        region_id,
        short_name                                          as region_name,
        fuel_type,
        percentage                                          as generation_pct,
        case
            when fuel_type in ('wind', 'solar', 'hydro')
                then 'renewable'
            when fuel_type in ('nuclear', 'biomass')
                then 'low_carbon'
            when fuel_type in ('gas', 'coal')
                then 'fossil'
            when fuel_type = 'imports'
                then 'imports'
            else 'other'
        end                                                 as fuel_category,
        to_timestamp(
            regexp_replace(period_from, 'Z$', ''),
            "yyyy-MM-dd'T'HH:mm"
        )                                                   as period_from,
        to_timestamp(
            regexp_replace(period_to, 'Z$', ''),
            "yyyy-MM-dd'T'HH:mm"
        )                                                   as period_to,
        batch_id,
        _ingested_at
    from source
)

select * from cleaned