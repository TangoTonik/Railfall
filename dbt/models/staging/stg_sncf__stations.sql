-- SNCF passenger stations, one row per UIC code. The department is the INSEE
-- department (2A/2B for Corsica, three digits overseas) and is the bridge
-- between named stations in the punctuality export and departmental weather.

with source as (

    select * from {{ source('raw', 'stations') }}

),

renamed as (

    select
        uic_code,
        station_name,
        name_key,
        department,
        latitude,
        longitude,
        (latitude is not null and longitude is not null) as has_coordinates,
        _updated_at                                      as loaded_at

    from source

)

select * from renamed
