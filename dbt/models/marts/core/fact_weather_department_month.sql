-- Grain: one department x month. Foreign keys to dim_department and dim_month.
-- Measures are the Spark monthly roll-up (station means, day counts scaled to
-- the whole month) plus anomalies against the department's own climatology for
-- that calendar month, so "a wet March in Brittany" and "a wet March in
-- Provence" are judged against different baselines.
--
-- The climatology is the mean over every year the warehouse holds. It is not a
-- 30-year WMO normal, but it is the same baseline for every route, which is all
-- the punctuality comparisons need.

with weather as (

    select * from {{ ref('stg_meteo__weather_monthly') }}

),

months as (

    select month, month_of_year, days_in_month
    from {{ ref('dim_month') }}

),

joined as (

    select
        w.*,
        m.month_of_year,
        m.days_in_month
    from weather as w
    inner join months as m on m.month = w.month

),

climatology as (

    select
        *,
        avg(precipitation_mm) over by_calendar_month   as normal_precipitation_mm,
        avg(temp_mean_c) over by_calendar_month        as normal_temp_mean_c,
        avg(wind_mean_ms) over by_calendar_month       as normal_wind_mean_ms,
        count(precipitation_mm) over by_calendar_month as normal_years_rain
    from joined
    window by_calendar_month as (partition by department, month_of_year)

)

select
    department_month_id,
    department,
    month,

    stations,
    stations_rain,
    stations_temp,
    stations_wind,

    precipitation_mm,
    rain_days,
    heavy_rain_days,
    temp_mean_c,
    temp_min_c,
    frost_days,
    wind_mean_ms,
    wind_max_ms,
    strong_wind_days,

    -- Shares of the month, so February and March compare fairly.
    round((rain_days / days_in_month)::numeric, 3)          as rain_day_share,
    round((frost_days / days_in_month)::numeric, 3)         as frost_day_share,
    round((strong_wind_days / days_in_month)::numeric, 3)   as strong_wind_day_share,

    -- Anomalies are only meaningful with at least two years to compare.
    normal_years_rain,
    case when normal_years_rain >= 2
        then round((precipitation_mm - normal_precipitation_mm)::numeric, 1)
    end                                                     as precipitation_anomaly_mm,
    case when normal_years_rain >= 2 and normal_precipitation_mm > 0
        then round((precipitation_mm / normal_precipitation_mm)::numeric, 2)
    end                                                     as precipitation_ratio,
    round((temp_mean_c - normal_temp_mean_c)::numeric, 1)   as temp_anomaly_c,
    round((wind_mean_ms - normal_wind_mean_ms)::numeric, 1) as wind_anomaly_ms,

    -- Simple, documented flags the marts group by (thresholds in DECISIONS.md).
    coalesce(precipitation_mm >= 1.5 * normal_precipitation_mm
        and normal_years_rain >= 2, false)                  as is_wet_month,
    coalesce(frost_days >= 10, false)                       as is_frosty_month,
    coalesce(strong_wind_days >= 5, false)                  as is_windy_month,

    loaded_at

from climatology
