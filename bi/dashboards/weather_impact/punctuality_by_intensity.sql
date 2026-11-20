-- Dose-response: punctuality per intensity band, baseline band first.
select
    weather_factor || ' · ' || intensity_band as band,
    weather_factor,
    band_order,
    route_months,
    arrival_punctuality_rate,
    punctuality_gap_pts
from marts.mart_punctuality_by_weather_intensity
order by weather_factor, band_order
