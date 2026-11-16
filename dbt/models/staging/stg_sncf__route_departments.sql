-- TGV route -> department of each endpoint, with how each side was resolved
-- (alias / foreign / exact / prefix / unmatched). A null department is kept on
-- purpose: foreign and unmatched endpoints must stay countable downstream.

with source as (

    select * from {{ source('raw', 'route_departments') }}

),

renamed as (

    select
        route_id,
        departure_station,
        arrival_station,
        departure_department,
        arrival_department,
        departure_match,
        arrival_match,
        is_domestic,
        _updated_at                     as loaded_at

    from source

)

select * from renamed
