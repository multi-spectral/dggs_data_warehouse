---This data cube gets multiple indicators without fusing them
---Definitely need some software tests to check that the result is correct


select
    h3_cell_to_parent(h3,3) AS h3_res_3,
    date_part('month',lower(validity)) AS month,
    avg(value) FILTER (WHERE variable = 'TRAFFIC_INCIDENTS') AS avg_traffic,
    avg(value) FILTER (WHERE variable = 'MEAN_POP_DENSITY') AS mean_pop_dens
from main 

group by cube(
    h3_cell_to_parent(h3,3), 
    date_part('month', lower(validity))
);

