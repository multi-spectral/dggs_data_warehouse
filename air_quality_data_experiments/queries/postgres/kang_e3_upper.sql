--In Kang et al., the full dataset size (unprocessed, with values only) is at least 10000*2411*(4*8) bits, or approx 94 MB.
--The following query represents query E3 from Kang et al.
--It should be run using a dataset of similar size.

select 
    h3,
    date_id,
    AVG(value) AS avg_val
FROM main_instantaneous
GROUP BY h3, date_id;