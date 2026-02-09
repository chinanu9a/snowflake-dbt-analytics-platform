{#
  Macro: cents_to_dollars
  Converts a value stored in cents to dollars with 2 decimal places.
  Useful when source systems store monetary values in minor units.
#}

{% macro cents_to_dollars(column_name) %}
    round(cast({{ column_name }} as decimal(18,2)) / 100, 2)
{% endmacro %}
