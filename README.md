### DGGS-based data warehouse for traffic monitoring


### Overview


The main system is a *domain-independent* DGGS-based data warehouse, in which converted raster, vector, (or in theory other) formats of geospatial data can be stored for arbitrary problem domains. The goal is to allow convenient exploration and analysis of datasets, especially involving integration of multiple datasets. It also allows the incorporation of heterogeneous metadata, which is useful for geospatial datasets.

The core system is currently implemented using three different data stores, which can be selected by the user. These are ClickHouse, DuckDB, and PostgreSQL. ClickHouse and DuckDB are optimized for OLAP, and can show better performance, whereas the PostgreSQL implementation can also make use of the data cube component.


### Traffic monitoring example

Another important component of this project is the implementation of an example using open data, in order to support a traffic monitoring use case. For more details, see the readme in that folder.

### Config

The configuration of this project consists of two parts: the configuration script `config.sh` and the `config` folder. The `config.sh` script installs all of the tools (note: Ubuntu only) that are needed for the system; the `config` folder contains additional configurations, including the basic schema DDL scripts for the data warehouse.

### Datacube

This folder contains a tool implementing a simple OLAP cube interface for the data warehouse. The interface allows a data cube to be created (within the data warehouse, for now, although future work could include creating or extending a separate system) for this. Additionally, the `rollup`, `drill-down`, `slice`, and `dice` operations can be executed upon the data cube.

### Reusable ETL functionality

This folder contains reusable functionality for ETL scripts.


### Architecture

The underlying architecture of the data warehouse is founded on the work of Robertson et al. 2020 (Paper here <https://www.sciencedirect.com/science/article/pii/S0924271620300502>).It is also informed by many of Kimball' dimensional modeling techniques <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques> (although this is not a Kimball-style data warehouse). The idea is to allow the schema to be domain-independent and support easy integration of data, as well as be suitable for aggregations and data cube functionality.

### Spherical (Load tool)

This folder contains the Rust source code and configuration for the load tool, which imports data in the standardized format into the data warehouse.


### Air quality data experiments

This folder contains files related to experiments measuring the query execution time of various aggregations on the data warehouse, given a specific, generated dataset. These queries are versions of the queries in <https://www.tandfonline.com/doi/full/10.1080/17538947.2014.962999>, and the experiment methodology follows their methodology.

