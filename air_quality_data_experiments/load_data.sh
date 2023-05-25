#!/bin/sh

# bash script to load data into data warehouses for air quality data experiment
# these data warehouses should be limited to 4GB RAM

# Limiting ClickHouse RAM:
# https://clickhouse.com/docs/en/operations/settings/query-complexity
# In clickhouse-client:
# SET max_memory_usage = 4000000000

# Limiting DuckDB RAM:
# SET memory_limit='4GB';

# Limiting PostgreSQL RAM:
# This is a bit fuzzy, as exact limits cannot be set.
# Following the answer below:
# https://stackoverflow.com/questions/28844170/how-to-limit-the-memory-that-is-available-for-postgresql-server
# Actual max RAM = shared_buffers + (temp_buffers + work_mem) * max_connections
# See here for how to change these values:
# https://www.postgresql.org/docs/current/runtime-config-resource.html
# For now, we will keep it at the default level.
# Based on the default configuration,
# It makes sense to assume that Postgres will run similarly
# To how it would on a 4GB machine.
# Of course, it would be good to test this later.

set -e


database_name='airq_test'
metadata_path='out/airq.mdjson'
duckdb_path='db_test/airq.db3'

spherical_import_tool='./../spherical/target/debug/spherical_import'

case "$(uname -sr)" in
    Linux*)

    export H3_DUCKDB_EXT_PATH=$HOME/.dggs/h3_duckdb/h3_duckdb_extension/h3.duckdb_extension
    export PATH=$HOME/.dggs/h3_duckdb/duckdb/:$PATH
    ;;
esac

case "$1" in 

    clickhouse*)

    pwd=
    pwd_opt=

    echo "ClickHouse..."
    if clickhouse-client -q 'DROP DATABASE IF EXISTS airq_test' > /dev/null; then 
        :
    else
	#For password  in sh see https://unix.stackexchange.com/questions/518380/read-s-gives-error-via-script
	printf 'Clickhouse default user password:' >&2
        stty -echo
	read password
	stty echo
        pwd_opt="--password ${password}"
        clickhouse-client -q 'DROP DATABASE IF EXISTS airq_test' ${pwd_opt}
        
        

    fi
    
    clickhouse-client -q 'CREATE DATABASE airq_test' ${pwd_opt}
    clickhouse_con_str='clickhouse://localhost:8123/airq_test'
    clickhouse-client --database=${database_name} --multiquery ${pwd_opt} < ../config/db/clickhouse/schema.sql 
    clickhouse-client --database=${database_name} --multiquery ${pwd_opt} < config/db/clickhouse/datetime.sql
    ${spherical_import_tool} --db-name ${database_name} \
                            --metadata-path ${metadata_path} \
                            --con-str ${clickhouse_con_str} \
                            #${pwd_opt}
    ;;

    duckdb*)


    # Import data to duckdb
    echo "DuckDB..."
    mkdir -p db_test
    if [ -f ${duckdb_path} ]; then rm ${duckdb_path}; fi

    echo "Importing schema..."
    duckdb ${duckdb_path} -s ".read ../config/db/duckdb/schema.sql"
	
    echo "Importing date table..."

    duckdb ${duckdb_path} -s ".read config/db/duckdb/datetime.sql"
    
    echo "Importing data..."
    ${spherical_import_tool} --db-name ${database_name} \
                            --metadata-path ${metadata_path} \
                            --con-str ${duckdb_path}
    ;;

    postgres*)

    # Import data to postgresql
    echo "PostgreSQL..."
     
    postgres_con_str="postgres://$USER:@localhost/airq_test"
    psql -d postgres -q -c 'DROP DATABASE IF EXISTS airq_test'
    psql -d postgres -q -c 'CREATE DATABASE airq_test'
    pgxn load h3 -d ${database_name}
    psql -d ${database_name} -q -f ../config/db/postgresql/schema.sql
    psql -d ${database_name} -q -f config/db/postgresql/datetime.sql  
    ${spherical_import_tool} --db-name ${database_name} \
                            --metadata-path ${metadata_path} \
                            --con-str ${postgres_con_str}

    ;;

    *)

    echo "Please provide a system: clickhouse, postgres, duckdb"

    ;;
esac
