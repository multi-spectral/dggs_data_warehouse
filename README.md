### DGGS-based data warehouse for traffic monitoring


### Overview


The main system is a *domain-independent* DGGS-based data warehouse, in which converted raster, vector, (or in theory other) formats of geospatial data can be stored for arbitrary problem domains. The goal is to allow convenient exploration and analysis of datasets, especially involving integration of multiple datasets. It also allows the incorporation of heterogeneous metadata, which is useful for geospatial datasets.

The core system is currently implemented using three different data stores, which can be selected by the user. These are ClickHouse, DuckDB, and PostgreSQL. ClickHouse and DuckDB are optimized for OLAP, and can show better performance, whereas the PostgreSQL implementation can also make use of the data cube component.

The DGGS used in this project is the open-source [H3 DGGS]<https://github.com/uber/h3> developed by Uber. 

### Traffic monitoring example

Another important component of this project is the implementation of an example using open data, in order to support a traffic monitoring use case. For more details, see the readme in that folder.

### Config

The configuration of this project consists of two parts: the configuration script `config.sh` and the `config` folder. The `config.sh` script installs all of the tools (note: Ubuntu only) that are needed for the system; the `config` folder contains additional configurations, including the basic schema DDL scripts for the data warehouse.

### Datacube

This folder contains a tool implementing a simple 'DGGS data cube' interface for the data warehouse. The interface allows a data cube to be created (within the data warehouse, for now, although future work could include creating or extending a separate system) for this. The cube interface implements  the `rollup`, `drill-down`, `slice`, and `dice` operations.

The idea behind this is based on the work of Purss et al. (2019) (Paper here <https://www.utpjournals.press/doi/abs/10.3138/cart.54.1.2018-0017>), that hierarchical operations on a DGGS-based data cube can function based on set theory, rather than on an explicitly defined hierarchy. This is useful because the 

### Reusable extract/transform functionality

This folder contains reusable functionality for extract/transform scripts. These functions are employed in the traffic monitoring example's sample data processing scripts, which are found in `traffic_monitoring_example/scripts`. There is potential for this to be extended.


### Architecture

The underlying architecture of the data warehouse is founded on the work of Robertson et al. 2020 (Paper here <https://www.sciencedirect.com/science/article/pii/S0924271620300502>).It is also informed by many of Kimball' dimensional modeling techniques <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques> (although this is not a Kimball-style data warehouse). The idea is to allow the schema to be domain-independent and support easy integration of data, as well as be suitable for aggregations and data cube functionality.

### Spherical (Load tool)

This folder contains the Rust source code and configuration for the load tool, which imports data in the standardized format into the data warehouse.


### Air quality data experiments

This folder contains files related to experiments measuring the query execution time of various aggregations on the data warehouse, given a specific, generated dataset. These queries are versions of the queries in <https://www.tandfonline.com/doi/full/10.1080/17538947.2014.962999>, and the experiment methodology follows their methodology.

