# ----------- Main info -----------
# Filename: __init__.py
# Date: 23.10.2024
# Description: 
# ---------------------------------

from __future__ import annotations
from typing import List

import datetime
import tempfile

from pathlib import Path
from airflow.decorators import task, dag


DB_NAME = 'fy3data'
BUCKET_NAME = 'fy3data'
GET_REMOTE_FILES = 'get_remote_files.sql'
UPDATE_STATUS = 'update_status.sql'
ROOT_PATH = '/opt/airflow/dags/process_files/'
RT_STPS_PATH = '/opt/kitfloppa/processing/rt-stps/'


def rt_stps(filepath: Path) -> None:
    """
    """

    import subprocess

    from lxml import etree

    from fy3_utils.connections.minio_connection import MinioConnection
    
    org_filepath = Path(filepath).with_suffix('.ORG')
    root_dir = Path(filepath).parent
    fy3_xml_template = Path(RT_STPS_PATH) / 'config' / 'fy3.xml'

    with tempfile.NamedTemporaryFile() as tempxml:
        with open(org_filepath, 'w', encoding='utf-8') as orgfile:
            pass

        with open(fy3_xml_template, 'r', encoding='utf-8') as xml_file:
            tree = etree.parse(xml_file)
            root = tree.getroot()
        
        root.set('id', 'FengYun 3E')
        root.find('./output_channels/file').set('directory', root_dir.as_posix())
        root.find('./output_channels/file').set('filename', org_filepath.name)
        root.find('./spacecrafts/spacecraft').set('label', 'fy3e')
        root.find('./spacecrafts/spacecraft').set('id', '52')
        tree.write(tempxml, encoding='utf-8')
        
        tempxml.flush()

        # tempxml.seek(0)
        # print(tempxml.read().decode('utf-8'))

        res = subprocess.run([Path(RT_STPS_PATH) / 'bin' / 'batch.sh', tempxml.name, filepath], capture_output=True)
        print(res)
        print(res.stdout.decode('utf-8'))

        client = MinioConnection('minio').get_connection()
        client.fput_object(BUCKET_NAME, org_filepath.name, org_filepath.as_posix())


def process(filename: str) -> bool:
    """
    """

    import time
    import ftplib
    
    from sqlalchemy import text
    from string import Template

    from fy3_utils.connections.ftp_connection import FTPConnection
    from fy3_utils.connections.db_connection import DBConnection
    
    ftp_connection = FTPConnection('scanex')
    db_conn = DBConnection(DB_NAME)

    with tempfile.TemporaryDirectory() as tmpdirname:
        for attempt in range(1, 4):
            print(f'Attempt {attempt}')
            try:
                with ftplib.FTP(ftp_connection.host) as ftp:
                    ftp.login(user=ftp_connection.user, passwd=ftp_connection.password)
                    ftp.cwd(ftp_connection.directory)
                    filepath = Path(tmpdirname) / filename

                    with open(filepath, 'wb') as file:
                        ftp.retrbinary('RETR ' + filename, file.write)
                print('Successfully downloaded')
                
                rt_stps(filepath)

                with open(Path(ROOT_PATH) / 'sql' / UPDATE_STATUS, 'r') as q:
                    with db_conn.pg_engine().connect() as connection:
                        query = Template(q.read()).substitute(filename=filename)
                        connection.execute(text(query))

                return True
            except Exception as e:
                print(f'Attempt {attempt} failed {e}')
                time.sleep(5)
    
    raise RuntimeError('All Attempts failed')

@task()
def get_remote_files() -> List[str]:
    """
    """

    import pandas as pd

    from fy3_utils.connections.db_connection import DBConnection
    from fy3_utils.file_status import FileStatus

    db_conn = DBConnection(DB_NAME)

    with db_conn.pg_engine().connect() as connection:
        with open(Path(ROOT_PATH) / 'sql' / GET_REMOTE_FILES, 'r') as q:
            query = q.read()

        remote_files = pd.read_sql(query, connection)

    return remote_files['filename'].to_list()

@task()
def procces_files(files: List[str]) -> List[str]:
    """
    """
    
    for file in files:
        process(file)
        break


@dag(
     schedule='*/15 * * * *',
     start_date=datetime.datetime(2024, 10, 23),
     catchup=False
    #  default_args={
    #     "retries": 3,
    #     "retry_delay": datetime.timedelta(seconds=2),
    #  }
)
def process_files() -> None:
    """
    """
    
    procces_files(get_remote_files())
process_files()
