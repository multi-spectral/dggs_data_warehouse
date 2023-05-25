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



with 

    -- daily_counts: pre-aggregated daily counts of traffic incidents
    daily_counts AS (
        select
        h3_cell_to_parent(m.h3,3) AS h3_res_3,
        m.date_id as date_id,
        dt.year as year,
        count(m.value) FILTER (WHERE m.variable = 'TRAFFIC_EVENT') AS count_events
    from 
        main_instantaneous as m
        left outer join
        date as dt
        on m.date_id = dt.date_id


    group by m.date_id, year, h3_res_3

    having 
        count(m.value) FILTER (WHERE m.variable = 'TRAFFIC_EVENT') IS NOT NULL
        and 
        count(m.value) FILTER (WHERE m.variable = 'TRAFFIC_EVENT') > 0
    ),

    -- aggregate pop density at h3 scale
    -- use only those regions in daily_counts
    
    pop_density AS (

        select 
            h3_cell_to_parent(h3,3) AS h3_res_3,
            year,
            sum(value) FILTER (WHERE variable = 'WORLD_POP') AS pop_sum
        from
            main_by_year
        WHERE
            h3_cell_to_parent(h3,3) in (select h3_res_3 from daily_counts)
        group by
            h3_res_3, year
        having
            sum(value) FILTER (WHERE variable = 'WORLD_POP') IS NOT NULL
            and 
            sum(value) FILTER (WHERE variable = 'WORLD_POP') > 0 --threshold
    )

    -- join datasets together
    select
        dc.h3_res_3 as h3_res_3,
        avg(dc.count_events/pd.pop_sum) AS variable
    from
        daily_counts as dc
        join
        pop_density as pd
        on
            dc.year = pd.year
            and
            dc.h3_res_3 = pd.h3_res_3
    group by
        dc.h3_res_3
    order by
        variable DESC
    limit
        50;


