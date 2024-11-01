# ----------- Main info -----------
# Filename: __init__.py
# Date: 26.10.2024
# Description: 
# ---------------------------------

from __future__ import annotations

import datetime
import pandas as pd

from pathlib import Path
from airflow.decorators import task, dag

FY3E_START_YEAR = 2020
DB_NAME = 'fy3data'
FILES_TABLE = 'iquam_files'
GET_CURR_FILES = 'get_curr_files.sql'
DELETE_ROWS = 'delete_rows.sql'
ROOT_PATH = '/opt/airflow/dags/get_buoys/' # BOOOIIIIIIZZZZZ (Obama)


def ftp_date_to_datetime(ftp_date: str) -> datetime.datetime:
    """
    """
    
    return datetime.datetime.strptime(ftp_date[4:], '%Y%m%d%H%M%S')

@task()
def get_filenames() -> pd.DataFrame:
    """
    """
    
    import ftplib

    from fy3_utils.connections.ftp_connection import FTPConnection

    ftp_connection = FTPConnection('iquam')

    with ftplib.FTP(ftp_connection.host) as ftp:
        ftp.login(user=ftp_connection.user, passwd=ftp_connection.password)
        ftp.cwd(ftp_connection.directory)
        filenames = list(ftp.nlst())
        created_dates = [ftp_date_to_datetime(ftp.sendcmd('MDTM ' + filename)) for filename in filenames]
        file_dates = [datetime.datetime.strptime(filename[:6], '%Y%m') for filename in filenames]
    
    return pd.DataFrame({'filename': filenames, 'created_date': created_dates, 'file_date': file_dates})

@task()
def filter_filenames(filenames: pd.DataFrame) -> pd.DataFrame:
    return filenames[filenames['file_date'].dt.year >= FY3E_START_YEAR]

@task()
def update_filenames(filenames: pd.DataFrame) -> None:
    """
    """

    from sqlalchemy import text
    from string import Template
    from fy3_utils.connections.db_connection import DBConnection
    from fy3_utils.file_status import FileStatus

    db_conn = DBConnection(DB_NAME)

    with db_conn.pg_engine().connect() as connection:
        with open(Path(ROOT_PATH) / 'sql' / GET_CURR_FILES, 'r') as q:
            query = q.read()

        curr_files = pd.read_sql(query, connection)
        
        # Add new files
        file_mask = ~filenames['filename'].isin(curr_files['filename'])
        new_files = filenames[file_mask]
        new_files['status'] = FileStatus.NOT_DOWNLOADED
        new_files.to_sql(FILES_TABLE, connection, if_exists='append', index=False)

        # Delete old files with updated date
        update_files = curr_files.merge(filenames, how='inner', left_on='filename', right_on='filename')
        update_files.query('created_date_x != created_date_y', inplace=True)

        # Add old files
        if not update_files.empty:
            with open(Path(ROOT_PATH) / 'sql' / DELETE_ROWS) as q:
                query = Template(q.read()).substitute(id_on_del=', '.join(update_files['id']))
            
            connection.execute(text(query))

            update_files.drop(columns=['created_date_x',
                                       'filename_x', 
                                       'file_date_x', 
                                       'id', 'status'], axis=1) \
                        .rename(columns={'filename_y': 'filename', 
                                         'created_date_y': 'created_date',
                                         'file_date_y': 'file_date'})
            update_files['status'] = FileStatus.NOT_DOWNLOADED
            update_files.to_sql(FILES_TABLE, connection, if_exists='append', index=False)

@dag(
     schedule='0 0 * * *',
     start_date=datetime.datetime(2024, 10, 26),
     catchup=False
)
def get_buoys() -> None:
    """
    """
    
    update_filenames(filter_filenames(get_filenames()))
get_buoys()
