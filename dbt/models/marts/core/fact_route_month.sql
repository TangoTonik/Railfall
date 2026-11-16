-- Grain: one TGV route x service x month. Foreign keys to dim_route, dim_month
-- and (per endpoint) dim_department; measures are the AQST counts plus the
-- rates derived from them. Counts stay additive; rates are recomputed from
-- counts in the marts rather than averaged.

with punctuality as (

    select * from {{ ref('stg_sncf__punctuality_monthly') }}

),

routes as (

    select route_id, departure_department, arrival_department
    from {{ ref('dim_route') }}

)

select
    p.route_month_id,
    p.route_id,
    p.month,
    p.service,
    r.departure_department,
    r.arrival_department,

    p.trains_planned,
    p.trains_cancelled,
    p.trains_run,
    p.trains_late_departure,
    p.trains_late_arrival,
    p.trains_late_over_15,
    p.trains_late_over_30,
    p.trains_late_over_60,
    case
        when p.trains_run > 0
            then p.trains_run - p.trains_late_arrival
    end                                         as trains_on_time,

    p.avg_journey_min,
    p.avg_delay_all_arrival_min,
    p.avg_delay_late_arrival_min,
    -- Total minutes lost on arrival: additive, so the marts can rebuild a
    -- weighted average delay at any grain (region, season...).
    round((p.avg_delay_all_arrival_min * p.trains_run)::numeric, 1)
                                                as delay_arrival_minutes_total,

    p.arrival_punctuality_rate,
    p.cancellation_rate,

    p.pct_cause_external,
    p.pct_cause_infrastructure,
    p.pct_cause_traffic_management,
    p.pct_cause_rolling_stock,
    p.pct_cause_station_management,
    p.pct_cause_passenger_handling,

    p.loaded_at

from punctuality as p
inner join routes as r on r.route_id = p.route_id
