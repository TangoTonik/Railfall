-- Adverse vs normal months for each weather factor, side by side.
select
    weather_factor,
    exposure,
    route_months,
    arrival_punctuality_rate
from marts.mart_punctuality_by_weather
order by weather_factor, exposure
