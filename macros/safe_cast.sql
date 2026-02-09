{#
  Macro: safe_cast
  Safely casts a column to a target type, returning a default value on failure.
  Useful for handling late-arriving or malformed data in raw layers.
#}

{% macro safe_cast(column_name, target_type, default_value='null') %}
    try_cast({{ column_name }} as {{ target_type }})
{% endmacro %}
