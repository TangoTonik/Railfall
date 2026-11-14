{#
  Fails for every combination of `columns` that appears more than once.
  Same contract as dbt_utils.unique_combination_of_columns, kept local so the
  project needs no `dbt deps` (no package hub at build time).
#}
{% test unique_combination_of_columns(model, columns) %}

select
    {{ columns | join(', ') }},
    count(*) as occurrences
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}
