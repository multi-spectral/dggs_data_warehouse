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
    formatDateTime(datum, '%Y%m%d')::INT AS date_id,
    datum AS date_actual,
    toDayOfMonth(datum) AS day_of_month,
    toMonth(datum) AS month,
    toYear(datum) AS year,
    dateName('weekday', datum) AS day_name,
    toISOWeek(datum) AS day_of_week,
    toDayOfYear(datum) AS day_of_year,
    monthName(datum) AS month_name
FROM 
    (
        SELECT toDate('2016-08-01') + number as datum
        FROM numbers(1979) --All 1613 days between start and end
     ) DQ 
ORDER BY 1;
        