with source as (
    select * from {{ source('energy_bronze', 'carbon_intensity') }}
),

cleaned as (
    select
        region_id,
        short_name                                          as region_name,
        dno_region,
        intensity_forecast                                  as carbon_intensity_gco2_kwh,
        intensity_index                                     as intensity_category,
        to_timestamp(
            regexp_replace(period_from, 'Z$', ''),
            "yyyy-MM-dd'T'HH:mm"
        )                                                   as period_from,
        to_timestamp(
            regexp_replace(period_to, 'Z$', ''),
            "yyyy-MM-dd'T'HH:mm"
        )                                                   as period_to,
        case
            when region_id between 1 and 14 then 'granular'
            else 'aggregate'
        end                                                 as region_type,
        case
            when region_id = 15 then 'England'
            when region_id = 16 then 'Scotland'
            when region_id = 17 then 'Wales'
            when region_id = 18 then 'GB'
            else short_name
        end                                                 as region_label,
        batch_id,
        _ingested_at
    from source
)

select * from cleaned