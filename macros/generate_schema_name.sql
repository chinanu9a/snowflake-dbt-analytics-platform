{#
  Override default schema name generation.
  When a custom schema is specified, use it directly (no prefix).
  This keeps schema names consistent across environments and ensures
  sources.yml schema references match the actual seed/model schemas.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
