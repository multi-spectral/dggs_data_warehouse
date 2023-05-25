### Overview

This Python module contains reusable functionality for ETL scripts. It was automatically installed by `../config.sh` into the virtual environment at `../traffic_monitoring_example/config/virtualenv/venv`. 

There was surprisingly little functionality that presented a good candidate for implementation in this module. Besides some helper functions, the main contribution is the `extract_categorical_variables_as_metadata` function. Given a list of metadata columns containing categorical variables in a pandas dataframe, this will map them to indexes of metadata entries to be added to the metadata json, and return a list of python objects representing that categorical subset of metadata. In other words, it avoids the use of boilerplate mapping categorical variables to metadata. 