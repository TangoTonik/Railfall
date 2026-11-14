-- One row per TGV route-month (service x departure x arrival), typed and renamed
-- for modeling. Silver already dedups and rejects impossible rows; staging only
-- shapes: explicit column list (a new raw column must be opted into), the month
-- as a date and a surrogate key the star schema can join on.

with source as (

    select * from {{ source('raw', 'punctuality_monthly') }}

),

renamed as (

    select
        md5(concat_ws('|', month::text, service, route_id)) as route_month_id,
        month::date                                         as month,
        year::smallint                                      as year,
        route_id,
        service,
        departure_station,
        arrival_station,
        avg_journey_min,
        trains_planned,
        coalesce(trains_cancelled, 0)                       as trains_cancelled,
        trains_run,
        trains_late_departure,
        trains_late_arrival,
        trains_late_over_15,
        trains_late_over_30,
        trains_late_over_60,
        avg_delay_late_departure_min,
        avg_delay_all_departure_min,
        avg_delay_late_arrival_min,
        avg_delay_all_arrival_min,
        avg_delay_over_15_min,
        arrival_punctuality_rate,
        case
            when trains_planned > 0
                then round((coalesce(trains_cancelled, 0)::numeric / trains_planned), 4)
        end                                                 as cancellation_rate,
        -- AQST cause shares are percentages of late trains (0-100).
        pct_cause_external,
        pct_cause_infrastructure,
        pct_cause_traffic_management,
        pct_cause_rolling_stock,
        pct_cause_station_management,
        pct_cause_passenger_handling,
        _updated_at                                         as loaded_at

    from source

)

select * from renamed
