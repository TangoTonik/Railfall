-- One row per French department, with its region and how well the warehouse
-- covers it: passenger stations (to place routes) and weather months (to
-- describe them). Departments without either are kept: "no data" is a fact
-- the dashboards must be able to show, not a missing row.

with departments as (

    select * from {{ ref('departments') }}

),

stations as (

    select department, count(*) as stations
    from {{ ref('stg_sncf__stations') }}
    group by department

),

weather as (

    select
        department,
        count(*)   as weather_months,
        min(month) as first_weather_month,
        max(month) as last_weather_month
    from {{ ref('stg_meteo__weather_monthly') }}
    group by department

)

select
    d.department,
    d.department_name,
    d.region_code,
    d.region_name,
    length(d.department) = 3                    as is_overseas,
    coalesce(s.stations, 0)                     as stations,
    coalesce(w.weather_months, 0)               as weather_months,
    w.first_weather_month,
    w.last_weather_month,
    w.department is not null                    as has_weather

from departments as d
left join stations as s on s.department = d.department
left join weather as w on w.department = d.department
