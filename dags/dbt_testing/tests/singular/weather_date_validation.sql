select
    *
from
    {{ ref('TRANS_API_WEATHER') }}
where
    cast(DATE as DATE) < date('1950-01-01')

