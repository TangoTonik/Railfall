-- Where and when weather costs the most: only cells with both exposures.
select
    region_name,
    season,
    weather_factor,
    route_months,
    years,
    arrival_punctuality_rate,
    punctuality_gap_pts
from marts.mart_punctuality_by_region_season
where has_comparison and exposure = 'adverse'
order by punctuality_gap_pts
limit 50
