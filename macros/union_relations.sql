{#
  Macro: fiscal_quarter
  Returns the fiscal quarter for a given date, assuming a fiscal year
  starting in October (common for food service / hospitality companies).
#}

{% macro fiscal_quarter(date_column) %}
    case
        when extract(month from {{ date_column }}) between 10 and 12 then 'Q1'
        when extract(month from {{ date_column }}) between 1 and 3   then 'Q2'
        when extract(month from {{ date_column }}) between 4 and 6   then 'Q3'
        when extract(month from {{ date_column }}) between 7 and 9   then 'Q4'
    end
{% endmacro %}

{% macro fiscal_year(date_column) %}
    case
        when extract(month from {{ date_column }}) >= 10
        then extract(year from {{ date_column }}) + 1
        else extract(year from {{ date_column }})
    end
{% endmacro %}
