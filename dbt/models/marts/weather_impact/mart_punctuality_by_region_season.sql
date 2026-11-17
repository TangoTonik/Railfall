-- Grain: region x meteorological season x weather factor (rain / wind / frost)
-- x exposure (adverse / normal). The same "hit vs not hit" comparison as
-- mart_punctuality_by_weather, cut by where and when it happened: frost in an
-- Alpine winter and rain in a Breton autumn are different stories.
--
-- A route-month belongs to the region of its departure department, falling back
-- to the arrival end for routes whose origin is abroad or unplaced. Seasons come
-- from dim_month (DJF / MAM / JJA / SON), pooled across years so each cell holds
-- enough route-months to compare; `years` says how many winters went into it.
--
-- Cells are sparse by nature (no frost in a Mediterranean summer), so a factor
-- with only one exposure in a region-season keeps its row and a null gap, and
-- `has_comparison` tells the dashboards which gaps are actually readable.

with exposed as (

    select * from {{ ref('int_route_month_weather') }}
    where weather_coverage <> 'none'

),

departments as (

    select department, region_code, region_name from {{ ref('dim_department') }}

),

months as (

    select month, season, season_year from {{ ref('dim_month') }}

),

placed as (

    select
        e.*,
        coalesce(dep.region_code, arr.region_code)  as region_code,
        coalesce(dep.region_name, arr.region_name)  as region_name,
        m.season,
        m.season_year
    from exposed as e
    inner join months as m on m.month = e.month
    left join departments as dep on dep.department = e.departure_department
    left join departments as arr on arr.department = e.arrival_department

),

by_factor as (

    select 'rain' as weather_factor, is_wet as is_adverse, * from placed
    union all
    select 'wind' as weather_factor, is_windy as is_adverse, * from placed
    union all
    select 'frost' as weather_factor, is_frosty as is_adverse, * from placed

),

aggregated as (

    select
        region_code,
        region_name,
        season,
        weather_factor,
        case when is_adverse then 'adverse' else 'normal' end  as exposure,
        count(*)                                                as route_months,
        count(distinct route_id)                                as routes,
        count(distinct season_year)                             as years,
        sum(trains_planned)                                     as trains_planned,
        sum(trains_cancelled)                                   as trains_cancelled,
        sum(trains_run)                                         as trains_run,
        sum(trains_late_arrival)                                as trains_late_arrival,
        sum(trains_late_over_15)                                as trains_late_over_15,
        sum(delay_arrival_minutes_total)                        as delay_arrival_minutes_total
    from by_factor
    -- Only routes with no French end at all lack a region; they cannot be placed.
    where region_code is not null
    group by 1, 2, 3, 4, 5

),

rated as (

    select
        *,
        {{ punctuality_rates() }}
    from aggregated

),

compared as (

    select
        *,
        max(case when exposure = 'normal' then arrival_punctuality_rate end)
            over (partition by region_code, season, weather_factor)
                                                    as normal_punctuality_rate,
        max(case when exposure = 'normal' then avg_delay_arrival_min end)
            over (partition by region_code, season, weather_factor)
                                                    as normal_avg_delay_min,
        count(*) over (partition by region_code, season, weather_factor) = 2
                                                    as has_comparison
    from rated

)

select
    region_code || '-' || season || '-' || weather_factor || '-' || exposure
                                                    as region_season_exposure_id,
    region_code,
    region_name,
    season,
    weather_factor,
    exposure,
    route_months,
    routes,
    years,
    trains_planned,
    trains_cancelled,
    trains_run,
    trains_late_arrival,
    trains_late_over_15,
    delay_arrival_minutes_total,
    arrival_punctuality_rate,
    late_over_15_rate,
    cancellation_rate,
    avg_delay_arrival_min,
    has_comparison,
    case when exposure = 'adverse' then
        round(100 * (arrival_punctuality_rate - normal_punctuality_rate), 2)
    end                                             as punctuality_gap_pts,
    case when exposure = 'adverse' then
        round(avg_delay_arrival_min - normal_avg_delay_min, 2)
    end                                             as delay_gap_min

from compared
