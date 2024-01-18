import requests
import pandas as pd
from dotenv import load_dotenv
import os


#imports used to for postgres load
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float # https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_core_creating_table.htm
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable 
from sqlalchemy import inspect
import pytest

DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
SERVER_NAME = os.environ.get("SERVER_NAME")
DATABASE_NAME = os.environ.get("DATABASE_NAME")
PORT = os.environ.get("PORT")


@pytest.fixture
def connect_to_postgres():   
# create connection to database 
    connection_url = URL.create(
        drivername = "postgresql+pg8000", 
        username = DB_USERNAME,
        password = DB_PASSWORD,
        host = SERVER_NAME, 
        port = PORT,
        database = DATABASE_NAME, 
    )

    engine = create_engine(connection_url)
    return engine



def test_inspect_database(connect_to_postgres): 
    engine = connect_to_postgres()
    inspector = inspect(engine)

    # Get table information and check to see if database is empty
    if "raw_crimes" not in inspector.get_table_names():
        print("Table doesn't exist")
    else:
        print(inspector.get_table_names())

    print(inspector.get_table_names())


