--Structure takes heavy inspiration from IDEAS paper:
--https://doi.org/10.1016/j.isprsjprs.2020.02.009
--See more here: https://colinr23.github.io/portfolio/portfolio-2/


--- Reset the data warehouse schema
--- This also drops all indices, extensions, etc.

DROP TABLE IF EXISTS date;

DROP TABLE IF EXISTS main_by_year;

DROP TABLE IF EXISTS main_by_month;

DROP TABLE IF EXISTS main_by_date;

DROP TABLE IF EXISTS main_instantaneous;

DROP TABLE IF EXISTS dataset_metadata;

--- Recreate the extensions
CREATE EXTENSION IF NOT EXISTS h3 CASCADE;

--- Create the tables

CREATE TABLE dataset_metadata (
    id serial PRIMARY KEY,
    metadata jsonb,
    dt_acquired timestamp, --NOT NULL
    source_name text --NOT NULL
);

CREATE TABLE date (
    date_id integer primary key,
    date_actual date,
    day_of_month integer,
    month integer,
    year integer,
    day_name VARCHAR(9) NOT NULL,
    day_of_week integer,
    day_of_year integer,
    month_name text

);


--TODO: implement view
-- view of which values are available at which granularites
CREATE TABLE data_index (
    variable text,
    granularity text

);


CREATE TABLE main_instantaneous (
    h3 h3index,
    value numeric,
    variable text, 
    date_id integer,
    datetime timestamp,
    dataset_metadata_id integer REFERENCES dataset_metadata (id)

);


CREATE TABLE main_by_date (
    h3 h3index,
    value numeric,
    variable text,
    date_id integer,
    dataset_metadata_id integer REFERENCES dataset_metadata (id)

);


CREATE TABLE main_by_year (
    h3 h3index,
    value numeric,
    variable text,
    year integer, --time dimension...instead of timestamp! Includes whole year
    dataset_metadata_id integer REFERENCES dataset_metadata (id)

);
