#!/bin/sh
set -e

case "$(uname -sr)" in
    Linux*)

    #environment variables
    source ../config/env.sh
    ;;
esac

# source the Python virtual environment
source config/virtualenv/venv/bin/activate


echo "Processing all data. This may take quite some time..."
echo "To download all the preprocessed sample data, please consult the documentation."
# Process all the data
for import_script_path in ./scripts/**/*.py; do
    echo $import_script_path
    cd "$(dirname "$import_script_path")" >/dev/null
    script_name=$(basename "$import_script_path")
    python $script_name
    cd - >/dev/null
done


# deactivate the Python virtual environment
deactivate