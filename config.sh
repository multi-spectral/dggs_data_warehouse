#!/bin/sh

set -e
# default config values
# TODO: is this safe?
. config/sys.conf

# install software
# this is OS-dependent


#TODO: check if these are available before installing them
# Mac: brew list --versions <package>
# Ideally will create separate postgres in project folder

source config/env.sh
case "$(uname -sr)" in

    Darwin*)
        echo 'Mac OS X'
        # Mac needs brew

       
        echo 'Installing PostgreSQL@14...'
        #brew install postgresql@14.7
        echo 'Installing Python@3.9.16...'
        #brew install python@3.9.16
        echo 'Installing pgxnclient@1.3.2...'
        #brew install pgxnclient@1.3.2

        #CLICKHOUSE
        # brew install clickhouse@22.7

        # create database dbname
        #clickhouse-client --multiquery < 'config/db/clickhouse/schema_clickhouse.sql'

        # Install cargo? Or use binary
        # Maybe need developer tools


        # start postgres
        #brew services start postgresql@14

        # create the database in the default instance
        psql postgres -c "CREATE DATABASE $dbname"

        # install h3 extension
         # need C tools for this
        #pgxn install h3
        pgxn load h3 -d $dbname

        # create the schema
        psql $dbname -a -f config/db/postgresql/schema.sql
        #psql $(dbname) -c config/db/postgresql/datetable.sql # do this for specific db

         


        ;;

    Linux*)

	#Build tools
	sudo apt-get install build-essential python3-pip cmake unzip pkg-config cargo libproj-dev libsqlite3-dev
        sudo apt-get install python3.10-venv python3-pytest
	alias gmake=make

	echo 'Linux'

    # setup virtualenv
    python3 -m venv traffic_monitoring_example/config/virtualenv/venv
    source traffic_monitoring_example/config/virtualenv/venv/bin/activate
    python3 -m pip install -r traffic_monitoring_example/config/virtualenv/requirements.txt
    python3 -m pip install reusable_etl_functionality/
    deactivate


    case "$1" in 

    postgres*)

        # Install software (python 3.9 and postgres 14) as needed
         #https://stackoverflow.com/questions/1298066/how-can-i-check-if-a-package-is-installed-and-install-it-if-not
        echo 'Installing PostgreSQL@14...'
	    sudo apt-get install postgresql-14 postgresql-client-14 libpq-dev postgresql-server-dev-14 postgis
        echo 'Installing pgxn...'
        sudo apt-get install pgxnclient

        # create current user if not exists
        sudo -u postgres psql -c "create user $USER WITH SUPERUSER" && true


        # install h3 extension
        sudo pgxn install h3
        #pgxn load h3 -d $dbname
        # copy file
        sharedir_extension="$(dirname $(pg_config --sharedir))/14/extension"

    ;;

    duckdb*)


    
        # DUCKDB
        if [[ ! -e $HOME/.dggs/h3_duck_db ]];
            then mkdir -p $HOME/.dggs/h3_duckdb
        fi

        if [[ ! -e $HOME/.dggs/duckdb_source ]];

        	then mkdir -p $HOME/.dggs/duckdb_source

        fi
        git clone https://github.com/isaacbrodsky/h3-duckdb.git $HOME/.dggs/duckdb_source
        cd $HOME/.dggs/duckdb
        git submodule update --init
        CMAKE_BUILD_PARALLEL_LEVEL=4 make duckdb_release release
        cd -        

        echo "acquiring AMD x64 version of DuckDB h3..."
        mkdir -p $HOME/.dggs

        #TODO: just build from source. download the repo, build and copy to path
        #TODO: then set the environment variable
        #wget "https://tubcloud.tu-berlin.de/s/a25KLrtZ3ojjatt/download/h3_duckdb_ubuntu_amd_64.tar.gz" -O $HOME/.dggs/h3_duckdb.tar.gz
        #tar xzf $HOME/.dggs/h3_duckdb.tar.gz -C $HOME/.dggs
        #rm $HOME/.dggs/h3_duckdb.tar.gz
        #export PATH=$HOME/.dggs/h3_duckdb/duckdb:$PATH
        #echo "Make sure to add duckdb to path by running: 'export PATH=\$HOME/.dggs/h3_duckdb/duckdb:$PATH'"
        
        # Copy the library to /usr/lib and use ldconfig to detect it
        sudo cp $HOME/.dggs/h3_duckdb/duckdb/libduckdb.so /usr/lib/
        sudo ldconfig

        ;;

    clickhouse*)

        # CLICKHOUSE
        # Instructions from official site
        sudo apt-get install -y apt-transport-https ca-certificates dirmngr
        GNUPGHOME=$(mktemp -d)
        sudo GNUPGHOME="$GNUPGHOME" gpg --no-default-keyring --keyring /usr/share/keyrings/clickhouse-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 8919F6BD2B48D754
        sudo rm -r "$GNUPGHOME"
        sudo chmod +r /usr/share/keyrings/clickhouse-keyring.gpg
        echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" | sudo tee \
            /etc/apt/sources.list.d/clickhouse.list
        sudo apt-get update
        sudo apt-get install -y clickhouse-server clickhouse-client
        sudo service clickhouse-server start
    
        ;;

    *)

            echo "Please provide a system: clickhouse, postgres, duckdb"
            exit

        ;;
    esac

    # build Rust binary
    cd spherical/spherical_import
    cargo build
    cd -


    

;;

    *)
        echo 'OS not supported'
        ;;
esac


# data download
# move this to traffic monitoring
#pip install gdown
#export PATH=$HOME/.local/bin:$PATH
#if [ ! -e traffic_monitoring_example/datasets/raw ];
#	then mkdir -p traffic_monitoring_example/datasets/raw/LSTW
#fi


# download LSTW
#gdown  https://drive.google.com/uc?id=1IOTGHBPt-0cI8KgHYlwHT62OeAPKpOBc -O traffic_monitoring_example/datasets/raw/LSTW/traffic.tar.gz
#gdown  https://drive.google.com/uc?id=1WPWSW0yY5SLzmAYZeey4kA4iwY8Zwcce -O traffic_monitoring_example/datasets/raw/LSTW/weather.tar.gz

#tar -zxvf traffic_monitoring_example/datasets/raw/LSTW/traffic.tar.gz
#tar -zxvf traffic_monitoring_example/datasets/raw/LSTW/weather.tar.gz

#rm traffic_monitoring_example/datasets/raw/LSTW/*.tar.gz


