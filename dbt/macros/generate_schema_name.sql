{#
  Use a model's custom schema verbatim (`staging`, `marts`) instead of dbt's
  default `<target schema>_<custom schema>`. The schemas are created and owned
  by Terraform; the transformer role has no CREATE on the database, so dbt must
  build into exactly those names and never invent new ones.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
