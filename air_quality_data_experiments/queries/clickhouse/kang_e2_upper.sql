select 
    m.h3 AS h3,
    d.month AS month,
    AVG(value) AS avg_val
FROM main_instantaneous AS m
JOIN date AS d
ON d.date_id = m.date_id
GROUP BY h3, d.month;