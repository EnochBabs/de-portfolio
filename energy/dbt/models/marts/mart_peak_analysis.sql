with base as (
    select * from {{ ref('int_time_features') }}
    where region_type = 'granular'
),

peak_summary as (
    select
        region_name,
        region_label,
        time_period,
        day_type,
        round(avg(carbon_intensity_gco2_kwh), 2)    as avg_intensity,
        round(avg(renewable_pct), 2)                as avg_renewable_pct,
        round(avg(fossil_pct), 2)                   as avg_fossil_pct,
        count(*)                                    as observations
    from base
    group by region_name, region_label, time_period, day_type
)

select * from peak_summary
order by region_name, time_period, day_type