{#
  Fails when the model's row count falls outside [min_rows, max_rows]. A model
  that builds successfully but empty (an inner join on a renamed key, a filter
  on the wrong month format) passes every column test vacuously; this is the
  test that notices. Returns one row with the actual count when it fails.
#}
{% test row_count_in_range(model, min_rows=1, max_rows=none) %}

with counted as (

    select count(*) as row_count from {{ model }}

)

select row_count
from counted
where row_count < {{ min_rows }}
{%- if max_rows is not none %}
   or row_count > {{ max_rows }}
{%- endif %}

{% endtest %}
