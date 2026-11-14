-- Daily weather per Météo-France station. Silver nulls out doubtful or
-- out-of-bounds measures, so nulls here mean "not measured", never zero.

with source as (

    select * from {{ source('raw', 'weather_daily') }}

),

renamed as (

    select
        station_id,
        day::date                       as day,
        month::date                     as month,
        year::smallint                  as year,
        department,
        station_name,
        latitude,
        longitude,
        altitude_m,
        precipitation_mm,
        temp_min_c,
        temp_max_c,
        temp_mean_c,
        wind_mean_ms,
        wind_max_hourly_ms,
        _updated_at                     as loaded_at

    from source

)

select * from renamed
