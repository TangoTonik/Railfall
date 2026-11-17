-- Every route-month with a value for a factor lands in exactly one band of that
-- factor: no gap between band edges drops it, no overlap counts it twice. Also
-- the bands starting at each flag threshold hold exactly the route-months past
-- it, so a band edge cannot drift away from the binary mart's threshold.
-- Returns the factors where either count disagrees.

with exposed as (

    select * from {{ ref('int_route_month_weather') }}
    where weather_coverage <> 'none'

),

expected as (

    select 'rain' as weather_factor,
        count(max_precipitation_ratio) as route_months,
        count(*) filter (where max_precipitation_ratio >= 1.5) as above_threshold
    from exposed
    union all
    select 'frost',
        count(max_frost_days),
        count(*) filter (where max_frost_days >= 10)
    from exposed
    union all
    select 'wind',
        count(max_strong_wind_days),
        count(*) filter (where max_strong_wind_days >= 5)
    from exposed

),

thresholds (weather_factor, threshold) as (

    values ('rain', 1.5::numeric), ('frost', 10), ('wind', 5)

),

banded as (

    select
        i.weather_factor,
        sum(i.route_months) as route_months,
        coalesce(sum(i.route_months) filter (where i.lower_bound >= t.threshold), 0)
            as above_threshold
    from {{ ref('mart_punctuality_by_weather_intensity') }} as i
    inner join thresholds as t on t.weather_factor = i.weather_factor
    group by 1

)

select e.*, b.route_months as banded_route_months, b.above_threshold as banded_above
from expected as e
left join banded as b on b.weather_factor = e.weather_factor
where e.route_months > 0
  and (b.route_months is distinct from e.route_months
       or b.above_threshold is distinct from e.above_threshold)
