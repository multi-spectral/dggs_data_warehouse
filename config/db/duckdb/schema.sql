--Structure takes heavy inspiration from IDEAS paper:
--https://doi.org/10.1016/j.isprsjprs.2020.02.009
--See more here: https://colinr23.github.io/portfolio/portfolio-2/

install json;
load json;

DROP TABLE IF EXISTS dataset_metadata;
DROP TABLE IF EXISTS date;
DROP TABLE IF EXISTS main_instantaneous;
DROP TABLE IF EXISTS main_by_date;
DROP TABLE IF EXISTS main_by_month;
DROP TABLE IF EXISTS main_by_year;

--- Create the tables

CREATE TABLE dataset_metadata (
    id UUID,
    metadata JSON,
    dt_acquired timestamp, --NOT NULL
    source_name text --NOT NULL
);

CREATE TABLE date (
    date_id text,
    date_actual date,
    day_of_month integer,
    month integer,
    year integer,
    day_name text,
    day_of_week integer,
    day_of_year integer,
    month_name text

);



CREATE TABLE main_instantaneous (
    h3 UBIGINT,
    value DOUBLE,
    variable text, 
    date_id text,
    datetime TIMESTAMP,
    dataset_metadata_id UUID

);


CREATE TABLE main_by_date (
    h3 UBIGINT,
    value DOUBLE,
    variable text,
    date_id text,
    dataset_metadata_id UUID

);



CREATE TABLE main_by_year (
    h3 UBIGINT,
    value DOUBLE,
    variable text, 
    year integer, --time dimension...instead of timestamp! Includes whole year
    dataset_metadata_id UUID

);
