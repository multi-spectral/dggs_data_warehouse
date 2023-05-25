--Implement temporal bucketing of data in range
--Based on this https://stackoverflow.com/questions/41232002/create-a-row-for-every-month-between-2-dates-in-postgresql


with q as (
    select * from main
    where variable='MEAN_POP_DENSITY'
    and value > 0
    limit 2
    )
select 
    generate_series(
        date_trunc('month', lower(validity)),
        upper(validity),
        '1 month')::date
    as month,
    value as mean_pop_density
from q;