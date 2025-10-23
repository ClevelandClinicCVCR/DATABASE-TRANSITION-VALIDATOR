from pathlib import Path

import yaml

DATABASE_CREDENTIALS_FILE = "database_credentials_local.yml"


def get_yaml_config() -> dict:
    """Return a dict of the values from the config file."""
    config_file_path = Path(__file__).parent.parent / DATABASE_CREDENTIALS_FILE

    with open(config_file_path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    return config


def get_teradata_config_values(index: int, name: str) -> tuple:
    """Return a set of values for use connecting to Teradata."""
    config: dict = get_yaml_config()

    config_teradata = config.get("TERADATA", [])

    if not config_teradata or len(config_teradata) <= 0:
        raise ValueError(
            f"No Teradata databases found in {DATABASE_CREDENTIALS_FILE}"
        )

    if isinstance(index, int):
        if index < 0 or index >= len(config_teradata):
            raise IndexError(
                f"Index {index} is out of range for Teradata databases"
            )

        teradata_host = config_teradata[index].get("HOST")
        path_to_keys = config_teradata[index].get("PATH_TO_KEYS")
        username = config_teradata[index].get("USERNAME")
        password = config_teradata[index].get("PASSWORD")

        return teradata_host, path_to_keys, username, password

    else:
        if isinstance(name, str):
            for db in config_teradata:
                if db.get("NAME") == name:
                    teradata_host = db.get("HOST")
                    path_to_keys = db.get("PATH_TO_KEYS")
                    username = db.get("USERNAME")
                    password = db.get("PASSWORD")
                    return teradata_host, path_to_keys, username, password

            raise ValueError(
                f"Name '{name}' not found in Teradata databases. \n Index must be an integer, got {type(index).__name__}"
            )

        else:
            # try to extract default values at index 0 if no name nor index is provided
            teradata_host = config_teradata[0].get("HOST")
            path_to_keys = config_teradata[0].get("PATH_TO_KEYS")
            username = config_teradata[0].get("USERNAME")
            password = config_teradata[0].get("PASSWORD")

            return teradata_host, path_to_keys, username, password


def get_sqlserver_config_values(index: int, name: str) -> tuple:
    """Return a set of values for use connecting to the Pythia SQL Server database."""
    config: dict = get_yaml_config()

    config_ms_sql_server = config.get("MS_SQL_SERVER", [])

    if not config_ms_sql_server or len(config_ms_sql_server) <= 0:
        raise ValueError(
            f"No MS SQL Server databases found in {DATABASE_CREDENTIALS_FILE}"
        )

    if isinstance(index, int):
        if index < 0 or index >= len(config_ms_sql_server):
            raise IndexError(
                f"Index {index} is out of range for MS SQL Server databases"
            )

        server = config_ms_sql_server[index].get("HOST")
        port = config_ms_sql_server[index].get("PORT")
        database = config_ms_sql_server[index].get("DATABASE")
        username = config_ms_sql_server[index].get("USERNAME")
        password = config_ms_sql_server[index].get("PASSWORD")

        return server, port, database, username, password

    else:
        if isinstance(name, str):
            for db in config_ms_sql_server:
                if db.get("NAME") == name:
                    server = db.get("HOST")
                    port = db.get("PORT")
                    database = db.get("DATABASE")
                    username = db.get("USERNAME")
                    password = db.get("PASSWORD")

                    return server, port, database, username, password

            raise ValueError(
                f"Name '{name}' not found in MS SQL Server databases. \n Index must be an integer, got {type(index).__name__}"
            )
        else:
            # try to extract default values at index 0 if no name nor index is provided
            server = config_ms_sql_server[0].get("HOST")
            port = config_ms_sql_server[0].get("PORT")
            database = config_ms_sql_server[0].get("DATABASE")
            username = config_ms_sql_server[0].get("USERNAME")
            password = config_ms_sql_server[0].get("PASSWORD")

            return server, port, database, username, password


def get_snowflake_config_values() -> tuple:
    """Return a set of values for use connecting to Snowflake."""
    config: dict = get_yaml_config()

    account = config.get("SNOWFLAKE_ACCOUNT")
    user = config.get("SNOWFLAKE_USER")
    role = config.get("SNOWFLAKE_ROLE")
    warehouse = config.get("SNOWFLAKE_WAREHOUSE")

    return account, user, role, warehouse
