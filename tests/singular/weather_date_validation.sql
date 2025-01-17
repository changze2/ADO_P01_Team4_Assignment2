select
    *
from
    {{ ref('trans_api_weather') }}
where
    cast(DATE as DATE) < date('1950-01-01')

