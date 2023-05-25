select
    h3_cell_to_parent(h3,3) AS h3_res_3,
    date_part('month',lower(validity)) AS month,
    avg(value) AS avg_value
from main 
where variable = 'TRAFFIC_INCIDENTS'
group by cube(
    h3_cell_to_parent(h3,3), 
    date_part('month', lower(validity))
    );