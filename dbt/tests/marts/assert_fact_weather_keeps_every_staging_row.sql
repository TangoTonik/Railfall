-- The weather fact inner-joins dim_month, whose calendar spans every staging
-- month, so no department-month may be lost. Any row returned was dropped.

select s.department_month_id
from {{ ref('stg_meteo__weather_monthly') }} as s
left join {{ ref('fact_weather_department_month') }} as f
    on f.department_month_id = s.department_month_id
where f.department_month_id is null
