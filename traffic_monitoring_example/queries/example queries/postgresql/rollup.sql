EXPLAIN ANALYZE select h3_cell_to_parent(h3,3)
as h4, validity, AVG(value) FROM main
WHERE variable = 'AADT'
GROUP BY ROLLUP (h4, validity);