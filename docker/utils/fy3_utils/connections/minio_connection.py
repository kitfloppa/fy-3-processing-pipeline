# ----------- Main info -----------
# Filename: ftp_connection.py
# Date: 22.10.2024
# Description: 
# ---------------------------------

from __future__ import annotations

from minio import Minio
from airflow.hooks.base import BaseHook


class MinioConnection:
    def __init__(self, conn_name: str) -> None:
        """
        """

        connection = BaseHook.get_connection(conn_name)

        self.__host = 'minio:9000'
        self.__id = connection.login
        self.__secret = connection.password

    def get_connection(self) -> Minio:
        client = Minio(self.__host,
                       access_key=self.__id,
                       secret_key=self.__secret,
                       secure=False
        )

        return client
