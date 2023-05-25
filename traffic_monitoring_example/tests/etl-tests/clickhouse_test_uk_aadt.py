import pytest

import polars as pl
from clickhouse_driver import Client

data_path_2018 = "../datasets/raw/LANDSCAN/landscan-usa-2018-day-assets/landscan-usa-2018-day/landscan-usa-2018-conus-day.tif"