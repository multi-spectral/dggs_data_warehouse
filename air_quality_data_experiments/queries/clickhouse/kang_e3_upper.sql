select 
    h3,
    date_id,
    AVG(value) AS avg_val
FROM main_instantaneous
GROUP BY h3, date_id;