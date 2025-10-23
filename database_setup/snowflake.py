import os

from snowflake.sqlalchemy import URL
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql import text
from sqlmodel import create_engine

from magicmodels.utils.config import get_snowflake_config_values


def get_snowflake_database() -> str:
    """Return the appropriate database name for the current environment."""
    magic_environment = os.environ.get("MAGIC_ENVIRONMENT", "DEV")
    if magic_environment.upper() == "PROD":
        return "MAGIC_PROD"
    else:
        return "TEAMS_SILVER_CBIOPORTAL_DEV"


def get_snowflake_db_engine(log_queries: bool = False) -> Engine:
    """Return a Snowflake database engine."""
    account, user, role, warehouse = get_snowflake_config_values()
    engine = create_engine(
        URL(
            account=account,
            user=user,
            authenticator="externalbrowser",
            role=role,
            warehouse=warehouse,
            database=get_snowflake_database(),
        )
    )
    # check if engine connects. raise exception if it doesn't
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT CURRENT_VERSION()"))
    except Exception as e:
        raise Exception(f"Error connecting to Snowflake: {e}")
    return engine
