-- Points of punctuality lost when a factor hits, worst first.
select
    weather_factor,
    route_months,
    arrival_punctuality_rate,
    cancellation_rate,
    punctuality_gap_pts
from marts.mart_punctuality_by_weather
where exposure = 'adverse'
order by punctuality_gap_pts
