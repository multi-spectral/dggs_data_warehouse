--In Kang et al., the full dataset size (unprocessed, with values only) is at least 10000*2411*(4*8) bits, or approx 94 MB.
--The following query represents query E1 from Kang et al.
--It should be run using a dataset of similar size.

select 
    m.h3 AS h3,
    d.year AS year,
    AVG(value) AS avg_val
FROM main_instantaneous AS m
JOIN date AS d
ON d.date_id = m.date_id
GROUP BY h3, d.year;