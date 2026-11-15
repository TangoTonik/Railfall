{#
  Fails for every non-null value of `column_name` outside [min_value, max_value].
  Either bound may be omitted; both are inclusive unless `inclusive=false`.
  Same contract as dbt_utils.accepted_range, kept local like
  unique_combination_of_columns (no `dbt deps` at build time). Nulls pass:
  "not measured" is a not_null question, not a range one.
#}
{% test accepted_range(model, column_name, min_value=none, max_value=none, inclusive=true) %}

{%- if min_value is none and max_value is none -%}
    {{ exceptions.raise_compiler_error("accepted_range needs min_value and/or max_value") }}
{%- endif -%}

select {{ column_name }} as out_of_range_value
from {{ model }}
where {{ column_name }} is not null
  and (
      false
      {%- if min_value is not none %}
      or {{ column_name }} {{ '<' if inclusive else '<=' }} {{ min_value }}
      {%- endif %}
      {%- if max_value is not none %}
      or {{ column_name }} {{ '>' if inclusive else '>=' }} {{ max_value }}
      {%- endif %}
  )

{% endtest %}
