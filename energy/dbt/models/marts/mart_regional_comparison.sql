{{ config(
    materialized='table',
    database='energy',
    schema='gold'
) }}

with base as (
    select * from {{ ref('int_time_features') }}
    where region_type = 'granular'
),

regional_summary as (
    select
        region_id,
        region_name,
        region_label,
        round(avg(carbon_intensity_gco2_kwh), 2)    as avg_intensity,
        round(min(carbon_intensity_gco2_kwh), 2)    as min_intensity,
        round(max(carbon_intensity_gco2_kwh), 2)    as max_intensity,
        round(avg(clean_energy_pct), 2)             as avg_clean_energy_pct,
        round(avg(renewable_pct), 2)                as avg_renewable_pct,
        round(avg(wind_pct), 2)                     as avg_wind_pct,
        round(avg(solar_pct), 2)                    as avg_solar_pct,
        round(avg(nuclear_pct), 2)                  as avg_nuclear_pct,
        round(avg(gas_pct), 2)                      as avg_gas_pct,
        count(distinct period_from)                 as data_points,
        mode() within group (
            order by intensity_category
        )                                           as most_common_category
    from base
    group by region_id, region_name, region_label
),

ranked as (
    select
        *,
        rank() over (order by avg_intensity asc)    as cleanest_rank,
        rank() over (order by avg_intensity desc)   as dirtiest_rank
    from regional_summary
)

select * from ranked