SELECT * FROM (
    SELECT h3,
    AVG(value) FILTER (WHERE variable = 'AADT') AS aadt,
    AVG(value) FILTER (WHERE variable = 'MEAN_POP_DENSITY') AS pop_density
    FROM main
    GROUP BY h3
) AS q
WHERE aadt IS NOT NULL AND pop_density IS NOT NULL;