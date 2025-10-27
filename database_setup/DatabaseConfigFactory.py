from typing import Literal

from sqlalchemy.engine.base import Engine

from data_class.DatabaseConfig import DatabaseConfig
from database_setup.sqlserver import get_sqlserver_db_engine
from database_setup.teradata import get_teradata_db_engine


class DatabaseConfigFactory:
    """Factory for creating database configurations."""

    @staticmethod
    def create_teradata_config(
        settings, source_or_target_type: Literal["source", "target"]
    ) -> DatabaseConfig:
        """Create Teradata database configuration."""
        db_settings = settings["database_setting"][
            f"{source_or_target_type}_database"
        ]
        index = db_settings.get("index", None)
        name = db_settings.get("name", None)
        engine = get_teradata_db_engine(
            index=index,
            name=name,
        )

        schema = db_settings.get("schema", None)

        return DatabaseConfig(
            name=f"Teradata",
            engine=engine,
            schema=schema,
            source_or_target_type=source_or_target_type,
        )

    @staticmethod
    def create_sqlserver_config(
        settings, source_or_target_type: Literal["source", "target"]
    ) -> DatabaseConfig:
        """Create SQL Server database configuration."""
        db_settings = settings["database_setting"][
            f"{source_or_target_type}_database"
        ]
        index = db_settings.get("index", None)
        name = db_settings.get("name", None)
        engine = get_sqlserver_db_engine(
            index=index,
            name=name,
        )

        schema = db_settings.get("schema", None)

        return DatabaseConfig(
            name=f"MS_SQL_Server",
            engine=engine,
            schema=schema,
            source_or_target_type=source_or_target_type,
        )

    @staticmethod
    def create_config(
        settings, type: Literal["source", "target"]
    ) -> DatabaseConfig:
        """Create database configuration based on settings and type ('source' or 'target')."""
        db_type = None
        try:
            db_type = settings["database_setting"][f"{type}_database"]["type"]
        except (KeyError, TypeError) as e:
            print(f"Missing 'type' key in {type}_database settings.")

        if db_type == "Teradata":
            return DatabaseConfigFactory.create_teradata_config(settings, type)
        elif db_type == "MS_SQL_Server":
            return DatabaseConfigFactory.create_sqlserver_config(
                settings, type
            )
        else:
            print(f"Unsupported {type} database type: {db_type}")
            return None
