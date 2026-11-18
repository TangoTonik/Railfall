{#
  Fails for every row where `expression` is not true. `where` narrows the rows
  checked (e.g. only route-months where trains ran). Nulls fail: an expression
  that cannot be evaluated is not a proven invariant; filter them out explicitly.
  Same contract as dbt_utils.expression_is_true, kept local (no `dbt deps`).
#}
{% test expression_is_true(model, expression, where_clause=none) %}

select *
from {{ model }}
where not coalesce(({{ expression }}), false)
{%- if where_clause %}
  and ({{ where_clause }})
{%- endif %}

{% endtest %}
