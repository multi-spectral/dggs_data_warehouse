--Implement temporal bucketing of data in range
--Based on this https://stackoverflow.com/questions/41232002/create-a-row-for-every-month-between-2-dates-in-postgresql
--It seems to kind of work for shorter ranges too.
--For each data point, generates the list of all months contained, and replicates the data for each month
--Then the data cube part takes the average of that
--Works fine, but inaccurate for cases where the range's granularity is, say, 1.5 months
--Also need to store the first table in memory

with upsampled as (
    select 
        generate_series(
            date_trunc('month', lower(validity)),
            upper(validity),
            '1 month')::date
        as month,
        h3 as h3,
        value as pop_density
        from main
        where variable <> 'MEAN_POP_DENSITY'
)
select
    h3_cell_to_parent(h3,3) AS h3_res_3,
    month,
    avg(pop_density) AS mean_pop_density
from upsampled 
group by cube(
    h3_cell_to_parent(h3,3), 
    month
    );