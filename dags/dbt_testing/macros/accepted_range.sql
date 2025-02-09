{% test accepted_range(model, column_name, min=0, max=999999) %}
    select *
    from {{ model }}
    where {{ column_name }} < {{ min }}
       or {{ column_name }} > {{ max }}
{% endtest %}