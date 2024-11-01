# ----------- Main info -----------
# Filename: __init__.py
# Date: 27.10.2024
# Description: 
# ---------------------------------

from __future__ import annotations

import datetime
import pandas as pd

from pathlib import Path
from airflow.decorators import task, dag


DB_NAME = 'fy3data'
GET_FILES = 'get_files.sql'
ROOT_PATH = '/opt/airflow/dags/get_buoy_data/'


@task()
def get_files() -> pd.DataFrame:
    """
    """

    from fy3_utils.connections.db_connection import DBConnection

    db_conn = DBConnection(DB_NAME)

    with db_conn.pg_engine().connect() as connection:
        with open(Path(ROOT_PATH) / 'sql' / GET_FILES, 'r') as q:
            query = q.read()

        files = pd.read_sql(query, connection)

    return files


@task()
def process_files(filenames: pd.DataFrame) -> pd.DataFrame:
    pass


@dag(
     schedule='0 * * * *',
     start_date=datetime.datetime(2024, 10, 27),
     catchup=False
)
def get_buoy_data() -> None:
    """
    """
    pass
get_buoy_data()
