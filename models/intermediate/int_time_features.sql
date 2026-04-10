with base as (
    select * from {{ ref('int_regional_metrics') }}
),

with_time_features as (
    select
        *,
        hour(period_from)                           as hour_of_day,
        dayofweek(period_from)                      as day_of_week,
        dayofmonth(period_from)                     as day_of_month,
        month(period_from)                          as month_of_year,
        case
            when hour(period_from) between 7 and 9
                then 'morning_peak'
            when hour(period_from) between 17 and 20
                then 'evening_peak'
            when hour(period_from) between 23 and 6
                then 'overnight'
            else 'daytime'
        end                                         as time_period,
        case
            when dayofweek(period_from) in (1, 7)
                then 'weekend'
            else 'weekday'
        end                                         as day_type
    from base
)

select * from with_time_features