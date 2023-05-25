SELECT h3_cell_to_parent(m.h3,5), AVG(m.value) AS avg_aadt
FROM main AS m
LEFT OUTER JOIN object_metadata as o
ON m.object_metadata_id = o.id
WHERE o.metadata @> '{"NUM_LANES":2}'::jsonb
AND m.variable = 'AADT'
AND m.validity @> date '2017-01-01'
GROUP BY h3_cell_to_parent(m.h3,5)
ORDER BY avg_aadt DESC;