WITH pop_density AS (
    SELECT h3, AVG(value) AS value
    FROM main AS m
    WHERE m.variable = 'MEAN_POP_DENSITY'
    GROUP BY h3
), aadt AS (
    SELECT h3, AVG(value) AS value
    FROM main AS m
    WHERE m.variable = 'AADT'
    GROUP BY h3
)
SELECT a.h3, a.value AS aadt, p.value AS pop_density
FROM pop_density AS p
JOIN aadt AS a
ON p.h3 = a.h3;

