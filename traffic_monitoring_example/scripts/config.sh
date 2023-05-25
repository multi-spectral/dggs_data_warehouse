# install python and process the data


. ../config/sys.conf

# TODO: also download the data into folder structure by URL

# array of paths to download
declare -a download_paths=(
    "scripts/LSTW/extract_transform_traffic.py"
    "scripts/LSTW/extract_transform_weather.py"
    "scripts/MARYLAND_AADT/extract_transform.py"
    "scripts/LANDSCAN/extract_transform.py"
    )


# python virtualenv setup
python -m pip install --upgrade pip
python -m venv config/virtualenv/venv 
source config/virtualenv/venv/bin/activate
pip install -r config/virtualenv/requirements.txt  


# make temp data dir
mkdir datasets/tmp
cd datasets/tmp
#download data
gdown 1WPWSW0yY5SLzmAYZeey4kA4iwY8Zwcce # LSTW weather
gdown 1IOTGHBPt-0cI8KgHYlwHT62OeAPKpOBc # LSTW traffic

# array of scripts to run
declare -a scripts=(
    "scripts/LSTW/extract_transform_traffic.py"
    "scripts/LSTW/extract_transform_weather.py"
    "scripts/MARYLAND_AADT/extract_transform.py"
    "scripts/LANDSCAN/extract_transform.py"
    )

# run each processign script
for script_path in "${scripts[@]}";
do
    echo $csv_path
    python $script_path;
done