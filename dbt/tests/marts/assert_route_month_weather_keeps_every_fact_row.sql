-- int_route_month_weather left-joins the weather onto fact_route_month, and the
-- weather fact is unique per department x month, so the grain must not change:
-- no route-month lost, none duplicated. Any row returned here is a fan-out or a drop.

with facts as (

    select count(*) as n from {{ ref('fact_route_month') }}

),

exposed as (

    select count(*) as n, count(distinct route_month_id) as n_distinct
    from {{ ref('int_route_month_weather') }}

)

select f.n as fact_rows, e.n as exposed_rows, e.n_distinct as exposed_distinct
from facts as f
cross join exposed as e
where f.n <> e.n or e.n <> e.n_distinct
