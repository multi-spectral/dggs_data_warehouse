/*

    Query 2 from proposal

    Sample Query 2: Construct a data cube for the value 
    number of traffic events/average annual daily traffic
    (AADT), as well as its rolled up version to h3_5
    in the DGGS dimension. Constrain by the following:
    - using only AADT records with more than 2 lanes in the road
    - over the dimensions:
        - h3_7
        - time (day of week, hour of day, etc. )

    Approach:
        1. filter the aadt dataset, which is already grouped by h3 and year
        2. get traffic event counts, agg by lower time (daily, in this case)

*/

WITH
    
    filtered_aadt AS (

        select 
            m.h3 as h3,
            m.year as year,
            m.value as aadt
        from 
            main_by_year as m
            join
            dataset_metadata as dm
            on
            m.dataset_metadata_id = dm.id
        where
            m.variable = 'AADT'
            -- TODO: and where num lanes > 2
    ),

-- daily_counts: pre-aggregated daily counts of traffic incidents
    daily_counts AS (
        select
        m.h3 AS h3,
        m.date_id as date_id,
        dt.year as year,
        count(m.value) FILTER (WHERE m.variable = 'TRAFFIC_EVENT') AS count_events
    from 
        main_instantaneous as m
        left outer join
        date as dt
        on m.date_id = dt.date_id

    group by m.date_id, year, h3

    having 
        count(m.value) FILTER (WHERE m.variable = 'TRAFFIC_EVENT') IS NOT NULL
        and 
        count(m.value) FILTER (WHERE m.variable = 'TRAFFIC_EVENT') > 0
    )

-- join datasets together
select
    dc.h3 as h3,
    dc.date_id AS date_id,
    AVG(dc.count_events/fa.aadt) AS variable --take the average over the date
from
    daily_counts as dc
    join
    filtered_aadt as fa
    on
        dc.year = fa.year
        and
        dc.h3 = fa.h3
group by 
    cube(
        dc.h3,
        dc.date_id
    )
;