{% macro rename_circuit_id(model_name) %}
    {{ config(materialized='table') }}

    {%- set relation = ref(model_name) %}
    {%- set columns = adapter.get_columns_in_relation(relation) %}

    with renamed_columns as (
        select
        {% for col in columns %}
            {% if col.name | lower == 'circuitid' %}
                {{ col.name }} as CIRCUIT_ID
            {% else %}
                {{ col.name }}
            {% endif %}
            {% if not loop.last %}, {% endif %}
        {% endfor %}
        from {{ relation }}
    )

    select * from renamed_columns
{% endmacro %}