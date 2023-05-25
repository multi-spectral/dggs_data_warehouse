### Overview

Tests for the datacube and ETL.

To run these tests, please first source the virtual environment:
```
source ../../config/virtualenv/venv/bin/activate
```
Then in each folder, run:
```
pytest
```

You will need to modify the connection string in these Python files to match your local PostgreSQL credentials.

The data cube tests may require >8GB of RAM to run (otherwise pytest may kill the process). If this happens,try configuring swap space, as the Python baseline dataset generation can run up against memory limits. 