SELECT 
    h3_cell_to_parent(h3, 5) as h3_5,
    EXTRACT(YEAR from datetime) as year,
    avg(value) from main_instantaneous
WHERE variable = 'TRAFFIC_EVENT'
GROUP BY cube(
    h3_cell_to_parent(h3, 5),
    EXTRACT(YEAR from datetime)
    );