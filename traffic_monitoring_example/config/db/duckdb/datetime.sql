/*

Adapted from: 

https://gist.github.com/duffn/38449526e00abb47f4ec292f0491313d

This SQL script populates the date table with dates between
August 1, 2016 - December 31, 2021 (1979 days)
As per the range of the LSTW dataset: https://smoosavi.org/datasets/lstw

How the date table was originally created:

CREATE TABLE date (
    date_id integer primary key,
    date_actual date,
    day_of_month integer,
    month integer,
    year integer,
    day_name VARCHAR(9) NOT NULL,
    day_of_week integer,
    day_of_year integer,
    month_name text,

);
*/

INSERT INTO 
    date
SELECT
    CAST(strftime(datum, '%Y%m%d') AS INTEGER) AS date_id,
    datum AS date_actual,
    date_part('day', datum) AS day_of_month,
    date_part('month', datum) AS month,
    date_part('year', datum) AS year,
    dayname(datum) AS day_name,
    date_part('isodow', datum) AS day_of_week,
    date_part('doy', datum) AS day_of_year,
    monthname(datum) AS month_name
FROM 
    (
        SELECT DATE '2016-08-01' +  day::INTEGER
        AS datum
        FROM RANGE(0, 1979) AS SEQUENCE(day) --All 1613 days between start and end
        GROUP BY day::INTEGER
     ) AS DQ 
ORDER BY 1;
        

