-- Grain: one weather factor (rain / wind / frost) x exposure (adverse / normal).
-- For each factor, every route-month with weather at one end at least is split
-- by whether that factor hit it, so "wet months vs the rest" reads off two rows.
-- The factors overlap on purpose (a month can be wet and windy): each pair of
-- rows is its own comparison, and the gap column is adverse minus normal.
--
-- Rates are recomputed from summed counts, never averaged, so a big route with
-- 600 trains weighs 600 times a small one with a single train.

with exposed as (

    select * from {{ ref('int_route_month_weather') }}
    where weather_coverage <> 'none'

),

by_factor as (

    select 'rain' as weather_factor, is_wet as is_adverse, * from exposed
    union all
    select 'wind' as weather_factor, is_windy as is_adverse, * from exposed
    union all
    select 'frost' as weather_factor, is_frosty as is_adverse, * from exposed

),

aggregated as (

    select
        weather_factor,
        case when is_adverse then 'adverse' else 'normal' end  as exposure,
        count(*)                                                as route_months,
        count(distinct route_id)                                as routes,
        sum(trains_planned)                                     as trains_planned,
        sum(trains_cancelled)                                   as trains_cancelled,
        sum(trains_run)                                         as trains_run,
        sum(trains_late_arrival)                                as trains_late_arrival,
        sum(trains_late_over_15)                                as trains_late_over_15,
        sum(trains_late_over_30)                                as trains_late_over_30,
        sum(delay_arrival_minutes_total)                        as delay_arrival_minutes_total
    from by_factor
    group by 1, 2

),

rated as (

    select
        *,
        {{ punctuality_rates() }}
    from aggregated

)

select
    weather_factor || '-' || exposure                               as weather_exposure_id,
    *,
    -- Percentage points of punctuality lost (negative) or gained when the factor
    -- hits; null on the normal row and when a factor never occurred.
    case when exposure = 'adverse' then round(100 * (
        arrival_punctuality_rate
        - max(case when exposure = 'normal' then arrival_punctuality_rate end)
            over (partition by weather_factor)
    ), 2) end                                                       as punctuality_gap_pts,
    case when exposure = 'adverse' then round(
        avg_delay_arrival_min
        - max(case when exposure = 'normal' then avg_delay_arrival_min end)
            over (partition by weather_factor)
    , 2) end                                                        as delay_gap_min

from rated
