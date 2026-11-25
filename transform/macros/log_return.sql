{# ln(value / value n observations earlier) within a series, NULL when not computable. #}
{% macro log_return(value, partition_by, order_by, n) -%}
    ln(nullif({{ value }}, 0)
       / nullif(lag({{ value }}, {{ n }}) over (partition by {{ partition_by }} order by {{ order_by }}), 0))
{%- endmacro %}
