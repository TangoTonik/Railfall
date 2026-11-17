{#
  The rates every weather-impact mart publishes, rebuilt from summed counts in
  the enclosing select. Never average per-route rates: a route with 600 trains
  must weigh 600 times a route with one. Expects the columns trains_run,
  trains_planned, trains_late_arrival, trains_late_over_15,
  trains_cancelled and delay_arrival_minutes_total.
#}
{% macro punctuality_rates() %}
        round(1 - trains_late_arrival::numeric / nullif(trains_run, 0), 4)
                                                    as arrival_punctuality_rate,
        round(trains_late_over_15::numeric / nullif(trains_run, 0), 4)
                                                    as late_over_15_rate,
        round(trains_cancelled::numeric / nullif(trains_planned, 0), 4)
                                                    as cancellation_rate,
        round(delay_arrival_minutes_total / nullif(trains_run, 0), 2)
                                                    as avg_delay_arrival_min
{%- endmacro %}
