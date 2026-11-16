-- Calendar of months spanning every month the facts mention, gap-free, so a
-- month with no train data still shows up as an empty bar instead of vanishing.
-- Seasons are meteorological (DJF, MAM, JJA, SON): December belongs to the
-- winter of the following year, so `season_year` keeps a winter in one piece.

with bounds as (

    select min(month) as first_month, max(month) as last_month
    from (
        select month from {{ ref('stg_sncf__punctuality_monthly') }}
        union all
        select month from {{ ref('stg_meteo__weather_monthly') }}
    ) as months

),

calendar as (

    select generate_series(first_month, last_month, interval '1 month')::date as month
    from bounds

)

select
    month,
    extract(year from month)::smallint                                  as year,
    extract(month from month)::smallint                                 as month_of_year,
    extract(quarter from month)::smallint                               as quarter,
    to_char(month, 'YYYY-MM')                                           as month_label,
    extract(day from (month + interval '1 month' - interval '1 day'))::smallint
                                                                        as days_in_month,
    case
        when extract(month from month) in (12, 1, 2) then 'winter'
        when extract(month from month) in (3, 4, 5) then 'spring'
        when extract(month from month) in (6, 7, 8) then 'summer'
        else 'autumn'
    end                                                                 as season,
    (extract(year from month)
        + case when extract(month from month) = 12 then 1 else 0 end)::smallint
                                                                        as season_year

from calendar
