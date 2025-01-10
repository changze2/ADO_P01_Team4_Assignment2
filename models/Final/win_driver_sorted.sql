with win_driver as (
    select * from {{ ref('win_driver') }}
)

select
    driver_name,
    wins
from win_driver
order by wins desc
