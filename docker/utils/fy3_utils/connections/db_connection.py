# ----------- Main info -----------
# Filename: db_connection.py
# Date: 22.10.2024
# Description: 
# ---------------------------------

from __future__ import annotations

from airflow.hooks.base import BaseHook

from sqlalchemy.engine import Engine
from sqlalchemy import create_engine


class DBConnection:
    def __init__(self, conn_name: str) -> None:
        """
        """
        
        connection = BaseHook.get_connection(conn_name)
        
        self.__host = connection.host
        self.__user = connection.login
        self.__password = connection.password
        self.__port = connection.port
        self.__schema = connection.schema

    def pg_engine(self) -> Engine:
        """
        """
        
        # Create connection sting for PostgreSQL db
        conn_string = f'postgresql+psycopg2://{self.__user}:{self.__password}@{self.__host}:{self.__port}/{self.__schema}'

        return create_engine(conn_string)
