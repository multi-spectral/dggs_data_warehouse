SELECT 
    h3ToParent(h3, 5) as h3_5,
    toYear(datetime) as year,
    avg(value) from main_instantaneous
WHERE variable = 'TRAFFIC_EVENT'
GROUP BY cube(h3_5, year);