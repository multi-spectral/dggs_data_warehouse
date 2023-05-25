--The following query aggregates over all the air quality data at resolution 5.
--Would be good to compare to h3pandas etc.

SELECT h3_cell_to_parent(h3,5) AS h3_05, 
AVG(value) AS avg_value
FROM main 
GROUP BY h3_cell_to_parent(h3,5);