-- One row per TGV route (departure x arrival), placed on the map through the
-- departments and regions of both endpoints. Driven by the punctuality facts so
-- every route the fact table mentions exists here, even when Spark could not
-- resolve its endpoints (department and region then stay null, match = unmatched).

with routes as (

    select
        route_id,
        min(departure_station)  as departure_station,
        min(arrival_station)    as arrival_station,
        min(month)              as first_month,
        max(month)              as last_month,
        count(distinct month)   as months_reported,
        count(distinct service) as services,
        round(avg(avg_journey_min)::numeric, 1) as avg_journey_min
    from {{ ref('stg_sncf__punctuality_monthly') }}
    group by route_id

),

placed as (

    select * from {{ ref('stg_sncf__route_departments') }}

),

departments as (

    select department, region_code, region_name from {{ ref('departments') }}

)

select
    r.route_id,
    r.departure_station,
    r.arrival_station,
    r.departure_station || ' → ' || r.arrival_station           as route_label,
    p.departure_department,
    p.arrival_department,
    dep.region_code                                             as departure_region_code,
    dep.region_name                                             as departure_region,
    arr.region_code                                             as arrival_region_code,
    arr.region_name                                             as arrival_region,
    coalesce(p.departure_match, 'unmatched')                    as departure_match,
    coalesce(p.arrival_match, 'unmatched')                      as arrival_match,
    coalesce(p.is_domestic, false)                              as is_domestic,
    coalesce(dep.region_code = arr.region_code, false)          as is_intra_region,
    r.first_month,
    r.last_month,
    r.months_reported,
    r.services,
    r.avg_journey_min

from routes as r
left join placed as p on p.route_id = r.route_id
left join departments as dep on dep.department = p.departure_department
left join departments as arr on arr.department = p.arrival_department
