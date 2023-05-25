### Overview

This folder contains an implementation of the data warehousing system for the domain of geospatial traffic monitoring data. This domain was chosen because data from a wide variety of regions can be integrated, as well as different original data formats (raster, vector) that include many sparsely distributed datasets.

### Datasets

* LSTW Traffic: acquired at https://smoosavi.org/datasets/lstw 
* LSTW Weather: acquired at https://smoosavi.org/datasets/lstw
* Maryland AADT https://data.imap.maryland.gov/maps/77010abe7558425997b4fcdab02e2b64
* New York State AADT https://www.dot.ny.gov/tdv
* UK AADF (AADT) https://roadtraffic.dft.gov.uk/downloads
* France AADT https://www.data.gouv.fr/en/datasets/trafic-moyen-journalier-annuel-sur-le-reseau-routier-national/
* Landscan HD Day https://landscan.ornl.gov/

These datasets are not provided here; in order to reproduce this example, datasets must be downloaded and placed in a `datasets/raw` folder here. 

### Scripts

The scripts in the `scripts` folder perform the extract and transform stages. They transform the data into a common format, described below.

### Processed data

Processed datasets are stored in the `datasets/processed` folder.

### Common data format

The format consists of a combination of `.csv` files (with standard headers) and a `.mdjson` file, which is JSON-formatted and contains the metadata and configuration for the dataset, which is used by the import tool. 

### Loading

In order to load the data, the script `all_data_import.sh` can be run (providing clickhouse, postgresql, or duckdb). This script uses the `spherical` tool (built at `../spherical/spherical_import') in order to load the data into the data warehouse.


### Visualizations

Some scripts to generate visualizations can be found in the `visualizations` folder.
