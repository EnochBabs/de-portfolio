{{ config(
    materialized='table',
    database='energy',
    schema='gold'
) }}

with base as (
    select * from {{ ref('int_time_features') }}
    where region_type = 'granular'
),

latest_period as (
    select max(period_from) as latest_from
    from base
),

latest_data as (
    select b.*
    from base b
    inner join latest_period l
        on b.period_from = l.latest_from
),

final as (
    select
        region_id,
        region_name,
        region_label,
        wind_pct,
        solar_pct,
        nuclear_pct,
        gas_pct,
        coal_pct,
        renewable_pct,
        low_carbon_pct,
        fossil_pct,
        imports_pct,
        clean_energy_pct,
        carbon_intensity_gco2_kwh,
        intensity_category,
        period_from
    from latest_data
    order by carbon_intensity_gco2_kwh asc
)

select * from final