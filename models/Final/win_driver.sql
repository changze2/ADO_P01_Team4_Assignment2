with final_test_bi as (
    select 
        driver_name,
        POSITION
    from {{ ref('final_test_bi') }}
)

select
    driver_name,
    count(*) as wins
from final_test_bi
where POSITION = '1'
group by driver_name
having count(*) > 10
