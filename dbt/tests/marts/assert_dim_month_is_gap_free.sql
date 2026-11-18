-- dim_month promises one row per month between the first and last month the
-- facts mention, with no hole: dashboards draw an empty bar from it instead of
-- silently joining two months together. Returns every month whose successor in
-- the calendar is not exactly one month later.

with ordered as (

    select
        month,
        lead(month) over (order by month) as next_month
    from {{ ref('dim_month') }}

)

select month, next_month
from ordered
where next_month is not null
  and next_month <> (month + interval '1 month')::date
