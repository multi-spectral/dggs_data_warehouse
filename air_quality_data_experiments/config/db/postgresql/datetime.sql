/*

Adapted from: 

https://gist.github.com/duffn/38449526e00abb47f4ec292f0491313d

This SQL script populates the date table
with the next 2411 dates
After and including 2015-01-01

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
    TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
    datum AS date_actual,
    EXTRACT(DAY from datum) AS day_of_month,
    EXTRACT(MONTH from datum) AS month,
    EXTRACT(YEAR from datum) AS year,
    TO_CHAR(datum, 'TMDay') AS day_name,
    EXTRACT(ISODOW FROM datum) AS day_of_week,
    EXTRACT(DOY FROM datum) AS day_of_year,
    TO_CHAR(datum, 'TMMonth') AS month_name
FROM 
    (
        SELECT '2015-01-01'::DATE + SEQUENCE.DAY AS datum
        FROM GENERATE_SERIES(0, 2411) AS SEQUENCE(DAY) --All 1613 days between start and end
        GROUP BY SEQUENCE.day
     ) DQ 
ORDER BY 1;
        