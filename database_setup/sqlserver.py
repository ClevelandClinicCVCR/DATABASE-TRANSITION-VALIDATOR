import urllib.parse

from sqlalchemy.engine.base import Engine
from sqlmodel import create_engine

from database_setup.config import get_sqlserver_config_values


def get_sqlserver_db_engine(
    index: int, name: str, log_queries: bool = False
) -> Engine:
    server, port, database, username, password = get_sqlserver_config_values(
        index=index, name=name
    )
    username = urllib.parse.quote_plus(username)
    password = urllib.parse.quote_plus(password)
    connection_string: str = (
        rf"mssql+pymssql://{username}:{password}@{server}:{port}/{database}?charset=utf8"
    )

    engine = create_engine(connection_string, echo=log_queries)
    assert engine.connect()
    return engine
