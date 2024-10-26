# ----------- Main info -----------
# Filename: ftp_connection.py
# Date: 22.10.2024
# Description: 
# ---------------------------------

from __future__ import annotations

from airflow.hooks.base import BaseHook


class FTPConnection:
    def __init__(self, conn_name: str) -> None:
        """
        """

        connection = BaseHook.get_connection(conn_name)

        self.__host = connection.host
        self.__dir = connection.schema
        self.__user = connection.login
        self.__password = connection.password

    @property
    def host(self) -> str:
        return self.__host

    @property
    def directory(self) -> str:
        return self.__dir
    
    @property
    def user(self) -> str:
        return self.__user
    
    @property
    def password(self) -> str:
        return self.__password
