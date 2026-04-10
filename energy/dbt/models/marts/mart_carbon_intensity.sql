{{ config(
    materialized='table',
    database='energy',
    schema='gold'
) }}

with base as (
    select * from {{ ref('int_time_features') }}
),

granular_regions as (
    select * from base
    where region_type = 'granular'
),

final as (
    select
        region_id,
        region_name,
        region_label,
        carbon_intensity_gco2_kwh,
        intensity_category,
        clean_energy_pct,
        renewable_pct,
        low_carbon_pct,
        fossil_pct,
        imports_pct,
        wind_pct,
        solar_pct,
        nuclear_pct,
        gas_pct,
        coal_pct,
        period_from,
        period_to,
        hour_of_day,
        day_of_week,
        day_type,
        time_period,
        month_of_year,
        _ingested_at
    from granular_regions
)

select * from final