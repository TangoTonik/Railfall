-- National arrival punctuality per month: the seasonal shape the weather explains.
select
    month,
    round(sum(trains_on_time)::numeric / nullif(sum(trains_run), 0), 4) as arrival_punctuality_rate,
    round(sum(trains_cancelled)::numeric / nullif(sum(trains_planned), 0), 4) as cancellation_rate
from marts.fact_route_month
group by month
order by month
