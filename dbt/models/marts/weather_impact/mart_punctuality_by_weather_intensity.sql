-- Grain: weather factor (rain / wind / frost) x intensity band. Where
-- mart_punctuality_by_weather splits route-months in two at the flag
-- thresholds, this mart grades them, so the question "does it get worse the
-- harder it rains?" reads off one factor's rows in band_order.
--
-- Each factor is graded on the worse end of the route (int_route_month_weather):
--   rain  : precipitation as a ratio of the 1991-2020-style monthly normal
--   frost : frost days in the month
--   wind  : days with a strong gust in the month
-- Band edges are lower-inclusive. One edge per factor is the threshold of its
-- adverse flag in fact_weather_department_month (1.5 x normal, 10 frost days,
-- 5 windy days), so the bands above it are the "adverse" route-months of the
-- binary mart, only graded (for rain, up to the ratio's rounding to 0.01).
--
-- Each factor has one baseline band (normal rain, no frost, no wind); the gap
-- columns compare every band with it. Route-months with no value for a factor
-- (no climatology yet for the rain ratio) are left out of that factor only.

with exposed as (

    select * from {{ ref('int_route_month_weather') }}
    where weather_coverage <> 'none'

),

bands (weather_factor, intensity_band, band_order, lower_bound, upper_bound, is_baseline) as (

    values
        ('rain',  'dry',      1, null::numeric, 0.5::numeric, false),
        ('rain',  'normal',   2, 0.5,  1.5,  true),
        ('rain',  'wet',      3, 1.5,  2.0,  false),
        ('rain',  'very_wet', 4, 2.0,  3.0,  false),
        ('rain',  'extreme',  5, 3.0,  null, false),

        ('frost', 'none',     1, null, 1,    true),
        ('frost', 'light',    2, 1,    5,    false),
        ('frost', 'moderate', 3, 5,    10,   false),
        ('frost', 'hard',     4, 10,   20,   false),
        ('frost', 'severe',   5, 20,   null, false),

        ('wind',  'none',     1, null, 1,    true),
        ('wind',  'light',    2, 1,    3,    false),
        ('wind',  'moderate', 3, 3,    5,    false),
        ('wind',  'strong',   4, 5,    10,   false),
        ('wind',  'severe',   5, 10,   null, false)

),

measured as (

    select 'rain' as weather_factor, max_precipitation_ratio::numeric as intensity, *
    from exposed
    union all
    select 'frost' as weather_factor, max_frost_days::numeric as intensity, *
    from exposed
    union all
    select 'wind' as weather_factor, max_strong_wind_days::numeric as intensity, *
    from exposed

),

banded as (

    select
        b.intensity_band,
        b.band_order,
        b.lower_bound,
        b.upper_bound,
        b.is_baseline,
        m.*
    from measured as m
    inner join bands as b
        on  b.weather_factor = m.weather_factor
        and (b.lower_bound is null or m.intensity >= b.lower_bound)
        and (b.upper_bound is null or m.intensity < b.upper_bound)
    -- The inner join drops null intensities only: the bands cover the whole line.
    where m.intensity is not null

),

aggregated as (

    select
        weather_factor,
        intensity_band,
        band_order,
        lower_bound,
        upper_bound,
        is_baseline,
        count(*)                                                as route_months,
        count(distinct route_id)                                as routes,
        min(intensity)                                          as min_intensity,
        max(intensity)                                          as max_intensity,
        sum(trains_planned)                                     as trains_planned,
        sum(trains_cancelled)                                   as trains_cancelled,
        sum(trains_run)                                         as trains_run,
        sum(trains_late_arrival)                                as trains_late_arrival,
        sum(trains_late_over_15)                                as trains_late_over_15,
        sum(delay_arrival_minutes_total)                        as delay_arrival_minutes_total
    from banded
    group by 1, 2, 3, 4, 5, 6

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
        max(case when is_baseline then arrival_punctuality_rate end)
            over (partition by weather_factor)          as baseline_punctuality_rate,
        max(case when is_baseline then avg_delay_arrival_min end)
            over (partition by weather_factor)          as baseline_avg_delay_min
    from rated

)

select
    weather_factor || '-' || intensity_band             as weather_intensity_id,
    weather_factor,
    intensity_band,
    band_order,
    lower_bound,
    upper_bound,
    is_baseline,
    route_months,
    routes,
    min_intensity,
    max_intensity,
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
    -- Null on the baseline row itself, and for every band of a factor whose
    -- baseline never occurred in the data.
    case when not is_baseline then
        round(100 * (arrival_punctuality_rate - baseline_punctuality_rate), 2)
    end                                                 as punctuality_gap_pts,
    case when not is_baseline then
        round(avg_delay_arrival_min - baseline_avg_delay_min, 2)
    end                                                 as delay_gap_min

from compared
