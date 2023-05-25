### Overview

This folder contains the code for a simple datacube interface to the data warehouse. For now, it is only implemented in PostgreSQL.



This code should only be used locally, due to the way the SQL statements are constructed (i.e. never expose it over the network).

TODO: Prevent SQL injection, so that the tool can be used over the network.