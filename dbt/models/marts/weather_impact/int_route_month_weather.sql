-- Grain: one TGV route x service x month (the fact_route_month grain), with the
-- weather each endpoint's department had that month. A train is exposed to the
-- weather at both ends of its route, so the route-month takes the *worse* of the
-- two: wet if either end was wet, the larger rain ratio, the more frost days.
--
-- `weather_condition` collapses the three flags into one label so a route-month
-- is counted once when the marts compare conditions. Priority, most disruptive
-- first: frost (points, catenary icing) > wind (speed restrictions, debris on
-- the line) > rain (embankments, signalling faults) > calm. Route-months with no
-- weather at either end are `unknown` and are kept, not dropped: the marts show
-- how much of the traffic the weather comparison actually covers.

with facts as (

    select * from {{ ref('fact_route_month') }}

),

weather as (

    select
        department,
        month,
        precipitation_mm,
        precipitation_ratio,
        frost_days,
        strong_wind_days,
        wind_max_ms,
        is_wet_month,
        is_frosty_month,
        is_windy_month
    from {{ ref('fact_weather_department_month') }}

),

joined as (

    select
        f.route_month_id,
        f.route_id,
        f.service,
        f.month,
        f.departure_department,
        f.arrival_department,

        f.trains_planned,
        f.trains_cancelled,
        f.trains_run,
        f.trains_late_arrival,
        f.trains_late_over_15,
        f.trains_late_over_30,
        f.trains_late_over_60,
        f.trains_on_time,
        f.delay_arrival_minutes_total,

        dep.department is not null  as has_departure_weather,
        arr.department is not null  as has_arrival_weather,

        dep.precipitation_mm        as departure_precipitation_mm,
        arr.precipitation_mm        as arrival_precipitation_mm,
        dep.precipitation_ratio     as departure_precipitation_ratio,
        arr.precipitation_ratio     as arrival_precipitation_ratio,

        -- greatest() ignores nulls in Postgres, so one covered end is enough.
        greatest(dep.precipitation_ratio, arr.precipitation_ratio)
                                    as max_precipitation_ratio,
        greatest(dep.frost_days, arr.frost_days)
                                    as max_frost_days,
        greatest(dep.strong_wind_days, arr.strong_wind_days)
                                    as max_strong_wind_days,
        greatest(dep.wind_max_ms, arr.wind_max_ms)
                                    as max_wind_gust_ms,

        coalesce(dep.is_wet_month, false) or coalesce(arr.is_wet_month, false)
                                    as is_wet,
        coalesce(dep.is_frosty_month, false) or coalesce(arr.is_frosty_month, false)
                                    as is_frosty,
        coalesce(dep.is_windy_month, false) or coalesce(arr.is_windy_month, false)
                                    as is_windy

    from facts as f
    left join weather as dep
        on dep.department = f.departure_department and dep.month = f.month
    left join weather as arr
        on arr.department = f.arrival_department and arr.month = f.month

)

select
    *,
    case
        when has_departure_weather and has_arrival_weather then 'both'
        when has_departure_weather then 'departure_only'
        when has_arrival_weather then 'arrival_only'
        else 'none'
    end                                         as weather_coverage,
    case
        when not (has_departure_weather or has_arrival_weather) then 'unknown'
        when is_frosty then 'frost'
        when is_windy then 'wind'
        when is_wet then 'rain'
        else 'calm'
    end                                         as weather_condition

from joined
