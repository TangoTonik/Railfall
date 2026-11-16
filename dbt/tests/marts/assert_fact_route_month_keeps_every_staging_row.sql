-- The fact inner-joins dim_route, which is driven by the same staging rows, so
-- it must keep every route-month. Any row returned here was silently dropped.

select s.route_month_id
from {{ ref('stg_sncf__punctuality_monthly') }} as s
left join {{ ref('fact_route_month') }} as f on f.route_month_id = s.route_month_id
where f.route_month_id is null
