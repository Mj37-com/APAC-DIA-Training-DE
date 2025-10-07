{% test vip_flag(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} not in (0,1)
{% endtest %}