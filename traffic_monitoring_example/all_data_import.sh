#!/bin/bash
set -e

declare -a arr=(
    "datasets/processed/AADT/Maryland/data.csv"
    "datasets/processed/AADT/New York/2019.csv"
    "datasets/processed/AADT/New York/2020.csv"
    "datasets/processed/AADT/New York/2021.csv"
    "datasets/processed/AADT/UK/data.csv"
    "datasets/processed/AADT/France/2017.csv"
    "datasets/processed/AADT/France/2018.csv"
    "datasets/processed/AADT/France/2019.csv"
    "datasets/processed/LSTW/Weather/data.csv"
    "datasets/processed/LSTW/Traffic/data.csv"
    "datasets/processed/LANDSCAN/landscan-usa-2018-conus-day.csv"
    "datasets/processed/LANDSCAN/landscan-usa-2019-conus-day.csv"
    "datasets/processed/LANDSCAN/landscan-usa-2020-conus-day.csv"
    "datasets/processed/LANDSCAN/landscan-usa-2021-conus-day.csv"
    )

case "$(uname -sr)" in
    Linux*)

    # environment variables
    source ../config/env.sh
    ;;
esac


# get the spherical import dir into path
# also populate the datetime table (has a set range)
database_name='traffic_monitoring'
spherical_import_tool='./../spherical/target/debug/spherical_import'

case "$1" in 

    postgres*)

    port=$(psql -d postgres -q -c "show port;" | sed '3q;d' | cut -c2-) #get the port name and crop whitespace

    if [ -n "$PGPASSWORD"]; then
        echo "No password found in PGPASSWORD environment variable."
        echo "If you receive an error, set the PGPASSWORD environment variable to the postgres password."
        echo "To change the postgres password, run sudo -u $USER psql -d ${database_name} -c \"ALTER USER $USER PASSWORD '<password>'\""
        echo "And then run export PGPASSWORD=<password>"

        #sudo -u en_muchos_paises psql -d traffic_monitoring -c "ALTER USER en_muchos_paises PASSWORD '123'"


    else
        password_arg=":$PGPASSWORD"
    fi
    
    postgres_con_str="postgres://${USER}${password_arg}@localhost:${port}/${database_name}"
    echo $postgres_con_str

    psql -d postgres -q -c 'DROP DATABASE IF EXISTS traffic_monitoring'  1> /dev/null 2> /dev/null
    psql -d postgres -q -c 'CREATE DATABASE traffic_monitoring' 1> /dev/null 2> /dev/null
    psql -d ${database_name} -q -c 'CREATE EXTENSION postgis;' 1> /dev/null 2> /dev/null
    psql -d ${database_name} -q -c 'CREATE EXTENSION postgis_raster;' 1> /dev/null 2> /dev/null
    pgxn load h3 -d ${database_name}

    
    psql -d ${database_name} -q -f ../config/db/postgresql/schema.sql  1> /dev/null 2> /dev/null
    psql -d ${database_name} -q -f config/db/postgresql/datetime.sql   1> /dev/null 2> /dev/null

    # import row-by-row
        for csv_path in "${arr[@]}";
        do

            meta_path="$(sed -e 's/\.csv$/.mdjson/' <<< $csv_path)"

            ${spherical_import_tool} --db-name ${database_name} \
                                --metadata-path "${meta_path}" \
                                --con-str ${postgres_con_str}
        done
    ;;

    clickhouse*)

    pwd=
    pwd_opt=

    clickhouse_con_str="clickhouse://${USER}@localhost:8123/"


    if clickhouse-client -q 'DROP DATABASE IF EXISTS traffic_monitoring' > /dev/null; then 
        :
    else
	#For password  in sh see https://unix.stackexchange.com/questions/518380/read-s-gives-error-via-script
	printf 'Clickhouse user password:' >&2
        stty -echo
	read password
	stty echo
        pwd_opt="--password ${password}"
        clickhouse-client -q 'DROP DATABASE IF EXISTS traffic_monitoring' ${pwd_opt}
        
        

    fi
    
    clickhouse-client -q 'CREATE DATABASE traffic_monitoring' ${pwd_opt}
    clickhouse-client --database=${database_name} --multiquery ${pwd_opt} < ../config/db/clickhouse/schema.sql 
    clickhouse-client --database=${database_name} --multiquery ${pwd_opt} < config/db/clickhouse/datetime.sql
    
    
    # import row-by-row
        for csv_path in "${arr[@]}";
        do

            meta_path="$(sed -e 's/\.csv$/.mdjson/' <<< $csv_path)"

            ${spherical_import_tool} --db-name ${database_name} \
                                --metadata-path "${meta_path}" \
                                --con-str ${clickhouse_con_str}
        done

    ;;

    duckdb*)

        duckdb_path='duckdb/duck.db3'
        #H3_DUCKDB_EXT_PATH='../../../tools/h3-duckdb/build/release/'

        mkdir -p duckdb
        if [ -f ${duckdb_path} ]; then rm ${duckdb_path}; fi

        echo "Importing schema..."
        duckdb ${duckdb_path} -s ".read ../config/db/duckdb/schema.sql"
        
        echo "Importing date table..."

        duckdb ${duckdb_path} -s ".read config/db/duckdb/datetime.sql"
        
        echo "Importing data..."

        # import row-by-row
        for csv_path in "${arr[@]}";
        do

            meta_path="$(sed -e 's/\.csv$/.mdjson/' <<< $csv_path)"
            ${spherical_import_tool} --db-name ${database_name} \
                                --metadata-path "${meta_path}" \
                                --con-str ${duckdb_path}
        done


    ;;

    *)

    echo "Please provide a system: clickhouse, postgres, duckdb"

    ;;
esac



