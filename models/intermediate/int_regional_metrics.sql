with intensity as (
    select * from {{ ref('stg_carbon_intensity') }}
),

generation as (
    select * from {{ ref('stg_generation_mix') }}
),

generation_pivoted as (
    select
        region_id,
        period_from,
        sum(case when fuel_category = 'renewable'   then generation_pct else 0 end) as renewable_pct,
        sum(case when fuel_category = 'low_carbon'  then generation_pct else 0 end) as low_carbon_pct,
        sum(case when fuel_category = 'fossil'      then generation_pct else 0 end) as fossil_pct,
        sum(case when fuel_category = 'imports'     then generation_pct else 0 end) as imports_pct,
        sum(case when fuel_type = 'wind'            then generation_pct else 0 end) as wind_pct,
        sum(case when fuel_type = 'solar'           then generation_pct else 0 end) as solar_pct,
        sum(case when fuel_type = 'nuclear'         then generation_pct else 0 end) as nuclear_pct,
        sum(case when fuel_type = 'gas'             then generation_pct else 0 end) as gas_pct,
        sum(case when fuel_type = 'coal'            then generation_pct else 0 end) as coal_pct
    from generation
    group by region_id, period_from
),

joined as (
    select
        i.region_id,
        i.region_name,
        i.region_type,
        i.region_label,
        i.carbon_intensity_gco2_kwh,
        i.intensity_category,
        i.period_from,
        i.period_to,
        g.renewable_pct,
        g.low_carbon_pct,
        g.fossil_pct,
        g.imports_pct,
        g.wind_pct,
        g.solar_pct,
        g.nuclear_pct,
        g.gas_pct,
        g.coal_pct,
        round(g.renewable_pct + g.low_carbon_pct, 2) as clean_energy_pct,
        i._ingested_at
    from intensity i
    left join generation_pivoted g
        on i.region_id = g.region_id
        and i.period_from = g.period_from
)

select * from joined