# ----------- Main info -----------
# Filename: __init__.py
# Date: 22.10.2024
# Description: 
# ---------------------------------

from __future__ import annotations
from typing import List

import datetime

from pathlib import Path
from airflow.decorators import task, dag


DB_NAME = 'fy3data'
FILES_TABLE = 'files'
GET_CURR_FILES = 'get_curr_files.sql'
ROOT_PATH = '/opt/airflow/dags/get_files/'


@task()
def get_filenames() -> List[str]:
    """
    """
    
    import ftplib

    from fy3_utils.connections.ftp_connection import FTPConnection

    ftp_connection = FTPConnection('scanex')

    with ftplib.FTP(ftp_connection.host) as ftp:
        ftp.login(user=ftp_connection.user, passwd=ftp_connection.password)
        ftp.cwd(ftp_connection.directory)
        filenames = list(filter(lambda x: 'F3E_' in x, list(ftp.nlst())))
    
    return filenames

@task()
def update_filenames(new_files: List[str]) -> None:
    """
    """
    
    import pandas as pd

    from fy3_utils.connections.db_connection import DBConnection
    from fy3_utils.file_status import FileStatus

    db_conn = DBConnection(DB_NAME)

    with db_conn.pg_engine().connect() as connection:
        with open(Path(ROOT_PATH) / 'sql' / GET_CURR_FILES, 'r') as q:
            query = q.read()

        curr_files = pd.read_sql(query, connection)['filename']
        new_files = list(set(new_files) - set(curr_files))

        df = pd.DataFrame({'filename': new_files, 'status': [FileStatus.NOT_DOWNLOADED] * len(new_files)})
        df.to_sql(FILES_TABLE, connection, if_exists='append', index=False)

@dag(
     schedule='0 */1 * * *',
     start_date=datetime.datetime(2024, 10, 24),
     catchup=False
)
def get_files() -> None:
    """
    """
    
    update_filenames(get_filenames())
get_files()
