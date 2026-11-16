-- Weather per department and month, as Spark rolled it up from the daily
-- station readings. Each measure family carries the number of stations it rests
-- on (stations_rain / _temp / _wind), so thin departments stay visible.

with source as (

    select * from {{ source('raw', 'weather_monthly') }}

),

renamed as (

    select
        md5(concat_ws('|', department, month::text))  as department_month_id,
        department,
        month::date                                   as month,
        year::smallint                                as year,
        stations::integer                             as stations,
        stations_rain::integer                        as stations_rain,
        precipitation_mm,
        rain_days,
        heavy_rain_days,
        stations_temp::integer                        as stations_temp,
        temp_mean_c,
        temp_min_c,
        frost_days,
        stations_wind::integer                        as stations_wind,
        wind_mean_ms,
        wind_max_ms,
        strong_wind_days,
        _updated_at                                   as loaded_at

    from source

)

select * from renamed
