/*

    Query 1 from proposal

    Sample Query 1: In which regions (i.e. DGGS cells) 
    is the average number of daily traffic events/population 
    density among the top 50, and the population density 
    also above a certain threshold?

    General approach:

        Construct two separate aggregations with identical
    axes, one for count(TRAFFIC_INCIDENTS) and one for
    sum(POPULATION), restricting to a certain threshold
    for sum(POPULATION). Then, order by the obtained value,
    and get the first 50 rows only.

    Procedure:

    1. aggregate daily traffic event count
    2. aggregate average daily traffic event count by year and h3
    3. aggregate yearly pop density by h3, filtered by threshold
    4. join
    5. sort and filter 

*/

SELECT 
    h3ToString(h3) AS h3,
    t.ct / p.value AS ratio
FROM (
    SELECT 
        h3,
        toYear(datetime) AS year,
        count(value) AS ct
    FROM main_instantaneous
    WHERE variable = 'TRAFFIC_EVENT'
    GROUP BY CUBE(year,h3)   
) AS t
JOIN (
    SELECT h3, year, value
    FROM main_by_year
    WHERE variable = 'LANDSCAN_day'
    AND value > 100 --todo: add unit. this eliminates about half the data
) AS p
ON t.h3 = p.h3 AND t.year = p.year
ORDER BY ratio DESC
LIMIT 50;
