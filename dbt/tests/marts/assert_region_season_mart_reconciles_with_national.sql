-- Each weather factor splits the same set of placed route-months into adverse and
-- normal, so per factor the region x season cells must add up to the same total,
-- and never to more than the national mart (which also counts unplaced routes).
-- A row returned here means a season join fanned out or a factor lost months.

with regional as (

    select weather_factor, sum(route_months) as route_months, sum(trains_run) as trains_run
    from {{ ref('mart_punctuality_by_region_season') }}
    group by weather_factor

),

national as (

    select weather_factor, sum(route_months) as route_months, sum(trains_run) as trains_run
    from {{ ref('mart_punctuality_by_weather') }}
    group by weather_factor

),

checked as (

    select
        r.weather_factor,
        r.route_months                                      as regional_route_months,
        n.route_months                                      as national_route_months,
        r.trains_run                                        as regional_trains_run,
        n.trains_run                                        as national_trains_run,
        (select count(distinct route_months) from regional) as distinct_factor_totals
    from regional as r
    left join national as n using (weather_factor)

)

select *
from checked
where national_route_months is null
   or regional_route_months > national_route_months
   or regional_trains_run > national_trains_run
   or distinct_factor_totals <> 1
