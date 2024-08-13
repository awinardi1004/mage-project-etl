{% macro incremental_filter(column_name) %}
    {% if is_incremental() %}
        and {{ column_name }} > (
            select max({{ column_name }})
            from {{ this }}
        )
    {% else %}
        and 1=1
    {% endif %}
{% endmacro %}
