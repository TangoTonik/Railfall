-- Arrival punctuality over every TGV route-month, rebuilt from counts.
select
    round(sum(trains_on_time)::numeric / nullif(sum(trains_run), 0), 4) as arrival_punctuality_rate
from marts.fact_route_month
